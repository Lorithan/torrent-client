package download

import (
	"math"
	"testing"

	"github.com/lorithan/torrent-client/internal/torrent"
)

func TestPieceSize(t *testing.T) {
	for _, tt := range []struct {
		name                string
		length, pieceLength int64
		pieceCount, index   int
		want                int64
	}{
		{"first piece", 40960, 16384, 3, 0, 16384},
		{"middle piece", 40960, 16384, 3, 1, 16384},
		{"short final piece", 40960, 16384, 3, 2, 8192},
		{"full final piece", 32768, 16384, 2, 1, 16384},
		{"single short piece", 100, 16384, 1, 0, 100},
		{"single byte final piece", 16385, 16384, 2, 1, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: tt.length, PieceLength: tt.pieceLength, Pieces: make([][20]byte, tt.pieceCount)}
			got, err := PieceSize(tf, tt.index)
			if err != nil || got != tt.want {
				t.Errorf("PieceSize(%d) = (%d, %v), want (%d, nil)", tt.index, got, err, tt.want)
			}
		})
	}
}

func TestPieceSizeRejectsInvalidInput(t *testing.T) {
	for _, tt := range []struct {
		name                string
		length, pieceLength int64
		pieceCount, index   int
	}{
		{"negative index", 40960, 16384, 3, -1},
		{"index equals count", 40960, 16384, 3, 3},
		{"no pieces", 0, 16384, 0, 0},
		{"zero piece length", 100, 0, 1, 0},
		{"negative piece length", 100, -1, 1, 0},
		{"negative file length", -1, 16384, 1, 0},
		{"piece starts at file end", 16384, 16384, 2, 1},
		{"piece starts beyond file end", 100, 16384, 2, 1},
		{"piece in empty file", 0, 16384, 1, 0},
		{"piece offset overflow", 100, math.MaxInt64, 3, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: tt.length, PieceLength: tt.pieceLength, Pieces: make([][20]byte, tt.pieceCount)}
			got, err := PieceSize(tf, tt.index)
			if got != 0 || err == nil {
				t.Errorf("PieceSize(%d) = (%d, %v), want (0, error)", tt.index, got, err)
			}
		})
	}
}
