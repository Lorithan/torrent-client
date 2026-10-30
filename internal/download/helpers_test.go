package download

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

type availabilityDeadlineConn struct {
	pieceTestConn
	deadlines []time.Time
	failAt    int
	err       error
	reads     int
}

func (c *availabilityDeadlineConn) SetDeadline(d time.Time) error {
	c.deadlines = append(c.deadlines, d)
	if len(c.deadlines) == c.failAt {
		return c.err
	}
	return nil
}

func (c *availabilityDeadlineConn) Read(p []byte) (int, error) {
	c.reads++
	return c.pieceTestConn.Read(p)
}

func availabilityDeadlineFixture(t *testing.T) (*peer.Client, *torrent.TorrentFile, *availabilityDeadlineConn) {
	t.Helper()
	data := []byte("data")
	tf := &torrent.TorrentFile{Length: 4, PieceLength: 4, Pieces: [][20]byte{sha1.Sum(data)}}
	// Two keep-alives must not restart the availability timer.
	wire := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 4, 0, 0, 0, 0}
	wire = append(wire, 0, 0, 0, 13, 7, 0, 0, 0, 0, 0, 0, 0, 0)
	wire = append(wire, data...)
	conn := &availabilityDeadlineConn{pieceTestConn: pieceTestConn{incoming: bytes.NewReader(wire)}}
	client, err := peer.NewClient(conn, peer.Handshake{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	client.Choked = false
	return client, tf, conn
}

type pieceTestConn struct {
	net.Conn
	incoming *bytes.Reader
	outgoing bytes.Buffer
}

// Hold the first peer read until the test has checked a concurrent snapshot.
type statsGateConn struct {
	*pieceTestConn
	once    sync.Once
	entered chan struct{}
	release <-chan struct{}
}

func (c *statsGateConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered); <-c.release })
	return c.pieceTestConn.Read(p)
}

func (c *pieceTestConn) SetDeadline(time.Time) error { return nil }

func (c *pieceTestConn) Read(p []byte) (int, error) { return c.incoming.Read(p) }

func (c *pieceTestConn) Write(p []byte) (int, error) { return c.outgoing.Write(p) }

func downloadFixture(t *testing.T, pieceLength, fileLength, corruptIndex int) (*peer.Client, *torrent.TorrentFile, *pieceTestConn, []byte) {
	t.Helper()
	data := make([]byte, fileLength)
	for i := range data {
		data[i] = byte(i*31 + i/pieceLength)
	}
	tf := &torrent.TorrentFile{Length: int64(fileLength), PieceLength: int64(pieceLength)}
	var replies []byte
	for start, index := 0, 0; start < len(data); index++ {
		piece := data[start:min(start+pieceLength, len(data))]
		tf.Pieces = append(tf.Pieces, sha1.Sum(piece))
		for begin := 0; begin < len(piece); {
			length := min(16384, len(piece)-begin)
			replies = binary.BigEndian.AppendUint32(replies, uint32(9+length))
			replies = append(replies, byte(peer.MsgPiece))
			replies = binary.BigEndian.AppendUint32(replies, uint32(index))
			replies = binary.BigEndian.AppendUint32(replies, uint32(begin))
			block := append([]byte(nil), piece[begin:begin+length]...)
			if index == corruptIndex {
				block[0] ^= 0xff
			}
			replies = append(replies, block...)
			begin += length
		}
		start += len(piece)
	}
	conn := &pieceTestConn{incoming: bytes.NewReader(replies)}
	client, err := peer.NewClient(conn, peer.Handshake{}, len(tf.Pieces))
	if err != nil {
		t.Fatal(err)
	}
	client.Choked = false
	for index := range tf.Pieces {
		if err := client.Bitfield.SetPiece(index); err != nil {
			t.Fatal(err)
		}
	}
	return client, tf, conn, data
}

type downloadWrite struct {
	offset int64
	data   []byte
}

type completionTestWriterFunc func([]byte, int64) (int, error)

func (f completionTestWriterFunc) WriteAt(p []byte, offset int64) (int, error) { return f(p, offset) }

// These write-only fixtures deliberately fail if a download starts reading storage.
func (f completionTestWriterFunc) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("unexpected read from write-only test fixture")
}

type downloadTestWriter struct {
	writes []downloadWrite
	failAt int
	err    error
}

func (w *downloadTestWriter) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("unexpected read from write-only test fixture")
}

func (w *downloadTestWriter) WriteAt(p []byte, offset int64) (int, error) {
	w.writes = append(w.writes, downloadWrite{offset, append([]byte(nil), p...)})
	if len(w.writes) == w.failAt {
		return len(p) / 2, w.err
	}
	return len(p), nil
}

type pieceErrorTestConn struct {
	net.Conn
	reader io.Reader
	writer bytes.Buffer
}

func (c *pieceErrorTestConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func (c *pieceErrorTestConn) Write(p []byte) (int, error) { return c.writer.Write(p) }

func (c *pieceErrorTestConn) SetDeadline(time.Time) error { return nil }

type pieceTestTimeout struct{}

func (*pieceTestTimeout) Error() string { return "request timed out" }

func (*pieceTestTimeout) Timeout() bool { return true }

func (*pieceTestTimeout) Temporary() bool { return true }

type sessionHaveFailureConn struct {
	*pieceTestConn
	err error
}

func (c *sessionHaveFailureConn) Write(p []byte) (int, error) {
	if len(p) >= 5 && p[4] == byte(peer.MsgHave) {
		return 0, c.err
	}
	return c.pieceTestConn.Write(p)
}
