package peer

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBitfieldSetPiece(t *testing.T) {
	for _, tt := range []struct {
		index int
		want  Bitfield
	}{
		{0, Bitfield{0x80, 0}},
		{1, Bitfield{0x40, 0}},
		{7, Bitfield{0x01, 0}},
		{8, Bitfield{0, 0x80}},
		{15, Bitfield{0, 0x01}},
	} {
		t.Run(fmt.Sprintf("piece_%d", tt.index), func(t *testing.T) {
			b := make(Bitfield, 2)
			if err := b.SetPiece(tt.index); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(b, tt.want) {
				t.Errorf("bitfield = %x, want %x", b, tt.want)
			}
		})
	}
	// Setting an existing bit again must preserve it and every other flag.
	b := Bitfield{0xa1, 0x40}
	for _, index := range []int{9, 8, 8} {
		if err := b.SetPiece(index); err != nil {
			t.Fatal(err)
		}
	}
	if want := (Bitfield{0xa1, 0xc0}); !bytes.Equal(b, want) {
		t.Errorf("bitfield = %x, want %x", b, want)
	}
}

func TestBitfieldSetPieceBounds(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, tt := range []struct {
		name  string
		bits  Bitfield
		index int
	}{
		{"nil", nil, 0},
		{"empty", Bitfield{}, 0},
		{"negative", Bitfield{0xa1}, -1},
		{"minimum integer", Bitfield{0xa1}, -maxInt - 1},
		{"past end", Bitfield{0xa1}, 8},
		{"maximum integer", Bitfield{0xa1}, maxInt},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := append([]byte(nil), tt.bits...)
			if err := tt.bits.SetPiece(tt.index); err == nil {
				t.Errorf("SetPiece(%d) succeeded, want error", tt.index)
			}
			if !bytes.Equal(tt.bits, before) {
				t.Error("invalid index modified the bitfield")
			}
		})
	}
}

func TestBitfieldHasPiece(t *testing.T) {
	b := Bitfield{0b10100001, 0b01000000}
	before := append([]byte(nil), b...)
	// Explicit indices verify most-significant-bit-first ordering across bytes.
	wantPresent := map[int]bool{0: true, 2: true, 7: true, 9: true}
	for index := 0; index < 16; index++ {
		if got := b.HasPiece(index); got != wantPresent[index] {
			t.Errorf("HasPiece(%d) = %v, want %v", index, got, wantPresent[index])
		}
	}
	if !bytes.Equal(b, before) {
		t.Error("HasPiece modified the bitfield")
	}
}

func TestBitfieldHasPieceBounds(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, tt := range []struct {
		name  string
		bits  Bitfield
		index int
	}{
		{"nil bitfield", nil, 0},
		{"empty bitfield", Bitfield{}, 0},
		{"negative index", Bitfield{255}, -1},
		{"minimum integer", Bitfield{255}, -maxInt - 1},
		{"just past first byte", Bitfield{255}, 8},
		{"just past second byte", Bitfield{255, 255}, 16},
		{"maximum integer", Bitfield{255}, maxInt},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.bits.HasPiece(tt.index) {
				t.Errorf("HasPiece(%d) = true for bitfield %x, want false", tt.index, tt.bits)
			}
		})
	}
}

func TestBitfieldValidate(t *testing.T) {
	for _, tt := range []struct {
		name    string
		bits    Bitfield
		pieces  int
		wantErr bool
	}{
		{"zero pieces", nil, 0, false},
		{"one piece", Bitfield{0x80}, 1, false},
		{"seven pieces", Bitfield{0xfe}, 7, false},
		{"eight pieces", Bitfield{0xff}, 8, false},
		{"nine pieces", Bitfield{0xff, 0x80}, 9, false},
		{"ten pieces", Bitfield{0xff, 0xc0}, 10, false},
		{"sixteen pieces", Bitfield{0xff, 0xff}, 16, false},
		{"seventeen pieces", Bitfield{0xff, 0xff, 0x80}, 17, false},
		{"no pieces present", Bitfield{0, 0}, 10, false},
		{"negative piece count", nil, -1, true},
		{"missing byte", Bitfield{0}, 10, true},
		{"extra byte", Bitfield{0, 0, 0}, 10, true},
		{"extra byte at boundary", Bitfield{0, 0}, 8, true},
		{"extra byte for empty torrent", Bitfield{0}, 0, true},
		{"padding for one piece", Bitfield{0x81}, 1, true},
		{"padding for seven pieces", Bitfield{0xff}, 7, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := append([]byte(nil), tt.bits...)
			err := tt.bits.Validate(tt.pieces)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate(%d) error = %v, wantErr %v", tt.pieces, err, tt.wantErr)
			}
			if !bytes.Equal(tt.bits, before) {
				t.Error("Validate modified the bitfield")
			}
		})
	}
}

func TestBitfieldValidateRejectsEachPaddingBit(t *testing.T) {
	// Ten pieces leave six unused low bits in the second byte.
	for _, padding := range []byte{0x20, 0x10, 0x08, 0x04, 0x02, 0x01} {
		t.Run(fmt.Sprintf("padding_%02x", padding), func(t *testing.T) {
			b := Bitfield{0xff, 0xc0 | padding}
			if err := b.Validate(10); err == nil {
				t.Errorf("Validate accepted nonzero padding in %x", b)
			}
		})
	}
}
