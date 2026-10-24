package peer

import (
	"bytes"
	"testing"
)

func TestHandshakeSerialize(t *testing.T) {
	h := Handshake{
		ReservedFlags: [8]byte{0, 1, 2, 4, 8, 16, 128, 255},
		InfoHash:      [20]byte{0, 255, 128, ' ', '+', '&', '%', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
		PeerID:        [20]byte{'-', 'C', 'T', '0', '0', '0', '1', '-', 255, 0, 128, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	before := h
	got := h.Serialize()
	if len(got) != 68 {
		t.Fatalf("serialized length = %d, want 68", len(got))
	}
	if got[0] != 19 {
		t.Errorf("protocol length byte = %d, want 19", got[0])
	}
	if string(got[1:20]) != "BitTorrent protocol" {
		t.Errorf("protocol name = %q, want BitTorrent protocol", got[1:20])
	}
	for _, field := range []struct {
		name string
		got  []byte
		want []byte
	}{
		{"reserved flags", got[20:28], h.ReservedFlags[:]},
		{"info hash", got[28:48], h.InfoHash[:]},
		{"peer ID", got[48:68], h.PeerID[:]},
	} {
		if !bytes.Equal(field.got, field.want) {
			t.Errorf("%s = %x, want %x", field.name, field.got, field.want)
		}
	}
	if h != before {
		t.Error("Serialize modified its receiver")
	}
	// Changing a returned buffer must not change the handshake or another result.
	other := h.Serialize()
	got[28] ^= 0xff
	if h != before || !bytes.Equal(other, h.Serialize()) {
		t.Error("serialized buffer shares mutable storage with another result or the handshake")
	}
}

func TestHandshakeSerializeZeroValues(t *testing.T) {
	got := (Handshake{}).Serialize()
	if len(got) != 68 {
		t.Fatalf("serialized length = %d, want 68", len(got))
	}
	if got[0] != 19 || string(got[1:20]) != "BitTorrent protocol" {
		t.Errorf("unexpected protocol header: %x", got[:20])
	}
	if !bytes.Equal(got[20:], make([]byte, 48)) {
		t.Errorf("zero-valued fields serialized as %x, want 48 zero bytes", got[20:])
	}
}
