package download

import (
	"bytes"
	"crypto/sha1"
	"errors"
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

func TestDownloadAvailabilityDeadlineIsNotRenewedByMessages(t *testing.T) {
	client, tf, conn := availabilityDeadlineFixture(t)
	dst := &downloadTestWriter{}
	before := time.Now()
	if err := Download(client, tf, dst); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	// One set/clear pair each for availability, the block request, and the have.
	if len(conn.deadlines) != 6 {
		t.Fatalf("deadline calls = %d, want 6; unrelated messages must not renew the availability deadline", len(conn.deadlines))
	}
	for i, d := range conn.deadlines {
		if i%2 == 1 {
			if !d.IsZero() {
				t.Errorf("deadline call %d should clear, got %v", i, d)
			}
		} else if d.Before(before.Add(30*time.Second)) || d.After(after.Add(30*time.Second)) {
			t.Errorf("deadline call %d should establish a fresh 30-second allowance, got %v", i, d)
		}
	}
	if len(dst.writes) != 1 || !bytes.Equal(dst.writes[0].data, []byte("data")) {
		t.Error("piece was not written correctly")
	}
}

func TestDownloadAvailabilityDeadlineErrors(t *testing.T) {
	for _, name := range []string{"setting", "clearing"} {
		t.Run(name, func(t *testing.T) {
			client, tf, conn := availabilityDeadlineFixture(t)
			if name == "clearing" {
				// Omit keep-alives here so this test specifically exercises the clear.
				remaining := make([]byte, conn.incoming.Len())
				_, _ = conn.incoming.Read(remaining)
				conn.incoming = bytes.NewReader(remaining[8:])
				conn.failAt = 2
			} else {
				conn.failAt = 1
			}
			wantErr := errors.New("deadline failed")
			conn.err = wantErr
			dst := &downloadTestWriter{}
			if err := Download(client, tf, dst); !errors.Is(err, wantErr) {
				t.Errorf("error = %v, want deadline error", err)
			}
			if conn.outgoing.Len() != 0 || len(dst.writes) != 0 {
				t.Error("performed a download after deadline failure")
			}
			if name == "setting" && conn.reads != 0 {
				t.Error("read before establishing availability deadline")
			}
		})
	}
}
