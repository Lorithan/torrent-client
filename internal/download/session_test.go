package download

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/lorithan/torrent-client/internal/torrent"
)

func TestNewSession(t *testing.T) {
	for _, tt := range []struct {
		name                  string
		length                int64
		pieces, bitfieldBytes int
	}{
		{"empty", 0, 0, 0},
		{"short only piece", 1, 1, 1},
		{"full only piece", 4, 1, 1},
		{"short final piece", 10, 3, 1},
		{"seven pieces", 28, 7, 1},
		{"eight pieces", 32, 8, 1},
		{"nine pieces", 36, 9, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: tt.length, PieceLength: 4, Pieces: make([][20]byte, tt.pieces)}
			dst := &downloadTestWriter{}
			s, err := NewSession(tf, dst)
			if err != nil || s == nil {
				t.Fatalf("NewSession() = (%v, %v), want session", s, err)
			}
			if s.torrent != tf || s.dest != dst {
				t.Error("constructor did not retain the supplied metadata and destination")
			}
			if s.completedCount != 0 || !bytes.Equal(s.completed, make([]byte, tt.bitfieldBytes)) {
				t.Errorf("initial progress = %d completed, bitfield %x; want zero completed and %d zero bytes", s.completedCount, s.completed, tt.bitfieldBytes)
			}
			if got := s.Complete(); got != (tt.pieces == 0) {
				t.Errorf("new session Complete() = %v, want %v", got, tt.pieces == 0)
			}
			if err := s.completed.Validate(tt.pieces); err != nil {
				t.Errorf("invalid initial completion bitfield: %v", err)
			}
			if len(dst.writes) != 0 {
				t.Error("constructor wrote output before downloading anything")
			}
		})
	}
}

func TestNewSessionRejectsInvalidMetadata(t *testing.T) {
	for _, tt := range []struct {
		name                string
		length, pieceLength int64
		hashes              int
	}{
		{"zero piece length", 4, 0, 1},
		{"negative piece length", 4, -1, 1},
		{"negative file length", -1, 4, 0},
		{"missing final hash", 10, 4, 2},
		{"extra hash", 10, 4, 4},
		{"extra hash at boundary", 8, 4, 3},
		{"hash for empty file", 0, 4, 1},
		{"large count", math.MaxInt64, 2, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: tt.length, PieceLength: tt.pieceLength, Pieces: make([][20]byte, tt.hashes)}
			dst := &downloadTestWriter{}
			if s, err := NewSession(tf, dst); s != nil || err == nil {
				t.Errorf("NewSession() = (%v, %v), want nil session and error", s, err)
			}
			if len(dst.writes) != 0 {
				t.Error("invalid metadata caused an output write")
			}
		})
	}
}

func TestNewSessionProgressIsIndependent(t *testing.T) {
	tf := &torrent.TorrentFile{Length: 36, PieceLength: 4, Pieces: make([][20]byte, 9)}
	sessions := make([]*Session, 2)
	for i := range sessions {
		s, err := NewSession(tf, &downloadTestWriter{})
		if err != nil {
			t.Fatal(err)
		}
		sessions[i] = s
	}
	if err := sessions[0].completed.SetPiece(8); err != nil {
		t.Fatal(err)
	}
	sessions[0].completedCount++
	if sessions[1].completed.HasPiece(8) || sessions[1].completedCount != 0 {
		t.Error("updating one session changed another session's progress")
	}
	for i, s := range sessions {
		if err := s.completed.Validate(9); err != nil {
			t.Error(fmt.Errorf("session %d: %w", i, err))
		}
	}
}
