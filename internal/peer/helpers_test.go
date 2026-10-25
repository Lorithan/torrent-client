package peer

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"time"
)

type blockDeadlineConn struct {
	exchangeTestConn
	deadlines []time.Time
	events    []string
	failAt    int
	err       error
}

func (c *blockDeadlineConn) SetDeadline(d time.Time) error {
	c.deadlines = append(c.deadlines, d)
	if d.IsZero() {
		c.events = append(c.events, "clear")
	} else {
		c.events = append(c.events, "set")
	}
	if len(c.deadlines) == c.failAt {
		return c.err
	}
	return nil
}

func (c *blockDeadlineConn) Read(p []byte) (int, error) {
	c.events = append(c.events, "read")
	return c.exchangeTestConn.Read(p)
}

func (c *blockDeadlineConn) Write(p []byte) (int, error) {
	c.events = append(c.events, "write")
	return c.exchangeTestConn.Write(p)
}

func downloadTestPiece(index, begin uint32, data []byte) []byte {
	wire := binary.BigEndian.AppendUint32(nil, uint32(9+len(data)))
	wire = append(wire, 7)
	wire = binary.BigEndian.AppendUint32(wire, index)
	wire = binary.BigEndian.AppendUint32(wire, begin)
	return append(wire, data...)
}

type sendPartialErrorWriter struct {
	bytes.Buffer
	err error
}

func (w *sendPartialErrorWriter) Write(p []byte) (int, error) {
	n, _ := w.Buffer.Write(p[:2])
	return n, w.err
}

// Only Read, Write, and Close are relevant to these error-path tests.
type exchangeTestConn struct {
	net.Conn
	reader io.Reader
	writer io.Writer
	closed bool
	reads  int
}

func (c *exchangeTestConn) SetDeadline(time.Time) error { return nil }

func (c *exchangeTestConn) Read(p []byte) (int, error) { c.reads++; return c.reader.Read(p) }

func (c *exchangeTestConn) Write(p []byte) (int, error) { return c.writer.Write(p) }

func (c *exchangeTestConn) Close() error { c.closed = true; return nil }

type exchangeErrorWriter struct{ err error }

func (w exchangeErrorWriter) Write([]byte) (int, error) { return 0, w.err }

// Build the wire fixture independently of Serialize to check the reader's offsets.
func handshakeReadFixture() (Handshake, []byte) {
	h := Handshake{
		ReservedFlags: [8]byte{0, 1, 2, 4, 8, 16, 128, 255},
		InfoHash:      [20]byte{255, 0, 128, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
		PeerID:        [20]byte{'-', 'C', 'T', '0', '0', '0', '1', '-', 0, 255, 128, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	wire := append([]byte{19}, []byte("BitTorrent protocol")...)
	wire = append(wire, h.ReservedFlags[:]...)
	wire = append(wire, h.InfoHash[:]...)
	wire = append(wire, h.PeerID[:]...)
	return h, wire
}
