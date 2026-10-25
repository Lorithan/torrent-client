package download

import (
	"bytes"
	"testing"

	"github.com/lorithan/torrent-client/internal/peer"
)

func TestSelectPiece(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		available, completed peer.Bitfield
		count, want          int
		found                bool
	}{
		{"first piece", peer.Bitfield{0x80}, peer.Bitfield{0}, 8, 0, true},
		{"skip unavailable pieces", peer.Bitfield{0x20}, peer.Bitfield{0}, 8, 2, true},
		{"skip completed piece", peer.Bitfield{0xa0}, peer.Bitfield{0x80}, 8, 2, true},
		{"choose lowest qualifying index", peer.Bitfield{0x25}, peer.Bitfield{0}, 8, 2, true},
		{"across byte boundary", peer.Bitfield{0x01, 0x80}, peer.Bitfield{0x01, 0}, 9, 8, true},
		{"last valid piece", peer.Bitfield{0, 0x40}, peer.Bitfield{0, 0}, 10, 9, true},
		{"no advertised pieces", peer.Bitfield{0}, peer.Bitfield{0}, 8, 0, false},
		{"all available pieces completed", peer.Bitfield{0xa0}, peer.Bitfield{0xa0}, 8, 0, false},
		{"remaining pieces unavailable", peer.Bitfield{0x80}, peer.Bitfield{0xc0}, 8, 0, false},
		{"ignore padding flags", peer.Bitfield{0, 0x3f}, peer.Bitfield{0, 0}, 10, 0, false},
		{"zero pieces", nil, nil, 0, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			availableBefore := append([]byte(nil), tt.available...)
			completedBefore := append([]byte(nil), tt.completed...)
			got, found := SelectPiece(tt.available, tt.completed, tt.count)
			if got != tt.want || found != tt.found {
				t.Errorf("SelectPiece() = (%d, %v), want (%d, %v)", got, found, tt.want, tt.found)
			}
			if !bytes.Equal(tt.available, availableBefore) || !bytes.Equal(tt.completed, completedBefore) {
				t.Error("selection modified an input bitfield")
			}
		})
	}
}
