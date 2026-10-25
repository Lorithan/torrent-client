package download

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

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

func TestDownloadWritesCompleteFile(t *testing.T) {
	client, tf, conn, want := downloadFixture(t, 20000, 45001, -1)
	f, err := os.Create(filepath.Join(t.TempDir(), "output.bin"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if err := Download(client, tf, f); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Error("output differs from torrent contents")
	}
	stat, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != tf.Length {
		t.Errorf("file size = %d, want %d", stat.Size(), tf.Length)
	}
	if conn.incoming.Len() != 0 {
		t.Error("download left piece data unread")
	}
}

func TestDownloadDoesNotAnnounceBeforeWrite(t *testing.T) {
	client, tf, conn, _ := downloadFixture(t, 4, 10, -1)
	wantErr := errors.New("disk full")
	calls := 0
	dst := completionTestWriterFunc(func(p []byte, offset int64) (int, error) {
		calls++
		// Only the first block request may have been sent before this write.
		if conn.outgoing.Len() != 17 {
			t.Errorf("sent %d bytes before writing piece; want only one request (17), with no have", conn.outgoing.Len())
		}
		return 0, wantErr
	})
	if err := Download(client, tf, dst); !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want write error", err)
	}
	if calls != 1 || conn.outgoing.Len() != 17 {
		t.Errorf("writes = %d, sent bytes = %d; a failed write must not announce completion", calls, conn.outgoing.Len())
	}
}

func TestDownloadDoesNotWriteCorruptedPiece(t *testing.T) {
	client, tf, conn, data := downloadFixture(t, 4, 10, 1)
	dst := &downloadTestWriter{}
	if err := Download(client, tf, dst); err == nil {
		t.Fatal("accepted corrupted piece")
	}
	if len(dst.writes) != 1 {
		t.Fatalf("writes = %d, want only the first verified piece", len(dst.writes))
	}
	if dst.writes[0].offset != 0 || !bytes.Equal(dst.writes[0].data, data[:4]) {
		t.Error("first verified piece was written incorrectly")
	}
	if conn.outgoing.Len() != 43 {
		t.Errorf("sent %d bytes, want two requests and one verified-piece have (43)", conn.outgoing.Len())
	}
}

func TestDownloadStopsOnWriteError(t *testing.T) {
	for _, wantErr := range []error{errors.New("disk full"), io.ErrShortWrite} {
		t.Run(wantErr.Error(), func(t *testing.T) {
			client, tf, conn, _ := downloadFixture(t, 4, 10, -1)
			dst := &downloadTestWriter{failAt: 2, err: wantErr}
			err := Download(client, tf, dst)
			if !errors.Is(err, wantErr) {
				t.Errorf("error = %v, want %v", err, wantErr)
			}
			if len(dst.writes) != 2 || dst.writes[1].offset != 4 {
				t.Errorf("incorrect writes: %+v", dst.writes)
			}
			if conn.outgoing.Len() != 43 {
				t.Error("download continued requesting after write failure")
			}
		})
	}
}

func TestDownloadDoesNotWriteAfterConnectionFailure(t *testing.T) {
	client, tf, conn, _ := downloadFixture(t, 4, 10, -1)
	conn.incoming = bytes.NewReader(nil)
	dst := &downloadTestWriter{}
	if err := Download(client, tf, dst); !errors.Is(err, io.EOF) {
		t.Errorf("error = %v, want EOF", err)
	}
	if len(dst.writes) != 0 {
		t.Error("wrote data after failed first piece")
	}
}

func TestDownloadRejectsInvalidMetadataBeforeIO(t *testing.T) {
	for _, tt := range []struct {
		name                string
		length, pieceLength int64
		hashes              int
	}{
		{"zero piece length", 10, 0, 1},
		{"negative piece length", 10, -4, 1},
		{"negative file length", -1, 4, 0},
		{"missing final hash", 10, 4, 2},
		{"extra hash", 10, 4, 4},
		{"no hashes for nonempty file", 10, 4, 0},
		{"extra hash at exact boundary", 8, 4, 3},
		{"hash for empty file", 0, 4, 1},
		{"large length count", math.MaxInt64, 2, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := &pieceTestConn{incoming: bytes.NewReader(nil)}
			client := &peer.Client{Conn: conn}
			tf := &torrent.TorrentFile{Length: tt.length, PieceLength: tt.pieceLength, Pieces: make([][20]byte, tt.hashes)}
			dst := &downloadTestWriter{}
			if err := Download(client, tf, dst); err == nil {
				t.Fatal("accepted invalid metadata")
			}
			if conn.outgoing.Len() != 0 || len(dst.writes) != 0 {
				t.Error("invalid metadata performed network or output writes")
			}
			// A nil client and destination also prove validation doesn't access them.
			if err := Download(nil, tf, nil); err == nil {
				t.Error("accepted invalid metadata without dependencies")
			}
		})
	}
}

func TestDownloadEmptyFile(t *testing.T) {
	tf := &torrent.TorrentFile{Length: 0, PieceLength: 16384}
	if err := Download(nil, tf, nil); err != nil {
		t.Errorf("empty download should succeed without I/O: %v", err)
	}
}

func TestDownloadAdvertisedPiecesOutOfOrder(t *testing.T) {
	data := []byte("abcdefghij")
	tf := &torrent.TorrentFile{
		Length: 10, PieceLength: 4,
		Pieces: [][20]byte{sha1.Sum(data[:4]), sha1.Sum(data[4:8]), sha1.Sum(data[8:])},
	}
	var replies, wantRequests []byte
	// Advertise the final piece first, then pieces zero and one. Include a
	// duplicate have so a completed piece must not be downloaded twice.
	for _, index := range []int{2, 0, 1} {
		replies = append(replies, 0, 0, 0, 0) // keep-alive
		if index == 0 {
			replies = append(replies, 0, 0, 0, 5, 4, 0, 0, 0, 2)
		}
		replies = append(replies, 0, 0, 0, 5, 4, 0, 0, 0, byte(index))
		piece := data[index*4 : min(index*4+4, len(data))]
		replies = binary.BigEndian.AppendUint32(replies, uint32(9+len(piece)))
		replies = append(replies, byte(peer.MsgPiece))
		replies = binary.BigEndian.AppendUint32(replies, uint32(index))
		replies = binary.BigEndian.AppendUint32(replies, 0)
		replies = append(replies, piece...)
		wantRequests = append(wantRequests, 0, 0, 0, 13, 6, 0, 0, 0, byte(index), 0, 0, 0, 0)
		wantRequests = binary.BigEndian.AppendUint32(wantRequests, uint32(len(piece)))
		wantRequests = append(wantRequests, 0, 0, 0, 5, byte(peer.MsgHave), 0, 0, 0, byte(index))
	}
	conn := &pieceTestConn{incoming: bytes.NewReader(replies)}
	client, err := peer.NewClient(conn, peer.Handshake{}, 3)
	if err != nil {
		t.Fatal(err)
	}
	client.Choked = false
	dst := &downloadTestWriter{}
	if err := Download(client, tf, dst); err != nil {
		t.Fatalf("Download() error: %v", err)
	}
	if !bytes.Equal(conn.outgoing.Bytes(), wantRequests) {
		t.Errorf("requests = %x, want %x", conn.outgoing.Bytes(), wantRequests)
	}
	if len(dst.writes) != 3 {
		t.Fatalf("writes = %d, want 3", len(dst.writes))
	}
	for i, index := range []int{2, 0, 1} {
		want := data[index*4 : min(index*4+4, len(data))]
		if dst.writes[i].offset != int64(index*4) || !bytes.Equal(dst.writes[i].data, want) {
			t.Errorf("write %d = %+v, want offset %d, data %q", i, dst.writes[i], index*4, want)
		}
	}
}
