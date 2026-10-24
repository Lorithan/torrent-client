package peer

import (
	"bytes"
	"testing"
)

func TestNewRequest(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		index, begin, length uint32
		want                 []byte
	}{
		{"first block", 0, 0, 16384, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0}},
		{"later block", 256, 32768, 8192, []byte{0, 0, 1, 0, 0, 0, 128, 0, 0, 0, 32, 0}},
		{"single byte", 1, 2, 1, []byte{0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1}},
		{"distinct multibyte fields", 0x01020304, 0x05060708, 0x090a, []byte{1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 9, 10}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewRequest(tt.index, tt.begin, tt.length)
			if err != nil || got == nil {
				t.Fatalf("NewRequest() = (%v, %v), want message", got, err)
			}
			if got.ID != MsgRequest || !bytes.Equal(got.Payload, tt.want) {
				t.Errorf("request = ID %d, payload %x; want ID %d, payload %x", got.ID, got.Payload, MsgRequest, tt.want)
			}
			wantWire := append([]byte{0, 0, 0, 13, 6}, tt.want...)
			if wire := got.Serialize(); !bytes.Equal(wire, wantWire) {
				t.Errorf("wire = %x, want %x", wire, wantWire)
			}
		})
	}
}

func TestNewRequestRejectsInvalidLength(t *testing.T) {
	for _, length := range []uint32{0, 16385, ^uint32(0)} {
		got, err := NewRequest(0, 0, length)
		if got != nil || err == nil {
			t.Errorf("NewRequest length %d = (%v, %v), want nil message and error", length, got, err)
		}
	}
}
