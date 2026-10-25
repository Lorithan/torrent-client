package download

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"io"
	"net"
	"testing"
	"testing/iotest"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

type pieceErrorTestConn struct {
	net.Conn
	reader io.Reader
	writer bytes.Buffer
}

func (c *pieceErrorTestConn) Read(p []byte) (int, error)  { return c.reader.Read(p) }
func (c *pieceErrorTestConn) Write(p []byte) (int, error) { return c.writer.Write(p) }
func (c *pieceErrorTestConn) SetDeadline(time.Time) error { return nil }

type pieceTestTimeout struct{}

func (*pieceTestTimeout) Error() string   { return "request timed out" }
func (*pieceTestTimeout) Timeout() bool   { return true }
func (*pieceTestTimeout) Temporary() bool { return true }

func TestDownloadPieceClassifiesPeerFailures(t *testing.T) {
	timeout := &pieceTestTimeout{}
	for _, tt := range []struct {
		name      string
		choked    bool
		reader    io.Reader
		operation string
		cause     error
	}{
		{"wait disconnect", true, iotest.ErrReader(io.EOF), "wait for piece 0", io.EOF},
		{"wait timeout", true, iotest.ErrReader(timeout), "wait for piece 0", timeout},
		{"wait malformed message", true, bytes.NewReader([]byte{0, 0, 0, 2, 1, 42}), "wait for piece 0", nil},
		{"block disconnect", false, iotest.ErrReader(io.EOF), "download piece 0 block at offset 0", io.EOF},
		{"block timeout", false, iotest.ErrReader(timeout), "download piece 0 block at offset 0", timeout},
		{"malformed piece", false, bytes.NewReader([]byte{0, 0, 0, 1, 7}), "download piece 0 block at offset 0", nil},
		{"hash mismatch", false, bytes.NewReader([]byte{0, 0, 0, 13, 7, 0, 0, 0, 0, 0, 0, 0, 0, 'D', 'a', 't', 'a'}), "verify piece 0", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := &pieceErrorTestConn{reader: tt.reader}
			client := &peer.Client{Conn: conn, Choked: tt.choked, PieceCount: 1, Bitfield: peer.Bitfield{0x80}}
			tf := &torrent.TorrentFile{Length: 4, PieceLength: 4, Pieces: [][20]byte{sha1.Sum([]byte("data"))}}
			got, err := DownloadPiece(client, tf, 0)
			var classified *PeerError
			if got != nil || !errors.As(err, &classified) {
				t.Fatalf("got (%x, %v), want nil data and PeerError", got, err)
			}
			if classified.Op != tt.operation {
				t.Errorf("operation = %q, want %q", classified.Op, tt.operation)
			}
			if classified.Err == nil {
				t.Error("classification lost underlying cause")
			}
			if tt.cause != nil && !errors.Is(err, tt.cause) {
				t.Errorf("underlying error %v not preserved in %v", tt.cause, err)
			}
			if tt.cause == timeout {
				var networkErr net.Error
				if !errors.As(err, &networkErr) || !networkErr.Timeout() {
					t.Error("timeout is not inspectable through wrapping")
				}
			}
		})
	}
}

func TestDownloadPieceDoesNotClassifyInvalidMetadataAsPeerFailure(t *testing.T) {
	for _, tt := range []struct {
		name                string
		length, pieceLength int64
		index               int
	}{
		{"negative index", 4, 4, -1},
		{"index beyond hashes", 4, 4, 1},
		{"zero piece length", 4, 0, 0},
		{"negative file length", -1, 4, 0},
		{"piece beyond file", 0, 4, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: tt.length, PieceLength: tt.pieceLength, Pieces: make([][20]byte, 1)}
			// A nil client proves local validation fails before accessing a peer.
			got, err := DownloadPiece(nil, tf, tt.index)
			if got != nil || err == nil {
				t.Fatalf("got (%x, %v), want nil data and validation error", got, err)
			}
			var classified *PeerError
			if errors.As(err, &classified) {
				t.Errorf("invalid metadata became retryable: %v", err)
			}
		})
	}
}
