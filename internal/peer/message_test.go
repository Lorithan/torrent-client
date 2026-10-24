package peer

import (
	"bytes"
	"testing"
)

func TestMessageSerialize(t *testing.T) {
	tests := []struct {
		name    string
		message Message
		want    []byte
	}{
		{"interested without payload", Message{ID: 2}, []byte{0, 0, 0, 1, 2}},
		{"choke is not keep alive", Message{ID: 0}, []byte{0, 0, 0, 1, 0}},
		{"have piece 256", Message{ID: 4, Payload: []byte{0, 0, 1, 0}}, []byte{0, 0, 0, 5, 4, 0, 0, 1, 0}},
		{"binary payload", Message{ID: 5, Payload: []byte{0, 255, 128}}, []byte{0, 0, 0, 4, 5, 0, 255, 128}},
		{
			"length exceeds one byte",
			Message{ID: 5, Payload: bytes.Repeat([]byte{0xaa}, 256)},
			append([]byte{0, 0, 1, 1, 5}, bytes.Repeat([]byte{0xaa}, 256)...),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Serialize panicked: %v", r)
				}
			}()
			before := append([]byte(nil), tt.message.Payload...)
			got := tt.message.Serialize()
			if !bytes.Equal(got, tt.want) {
				t.Errorf("Serialize() = %x, want %x", got, tt.want)
			}
			if !bytes.Equal(tt.message.Payload, before) {
				t.Error("Serialize modified the payload")
			}
			if len(tt.message.Payload) > 0 && len(got) > 5 {
				got[5] ^= 0xff
				if !bytes.Equal(tt.message.Payload, before) {
					t.Error("output buffer shares mutable storage with the payload")
				}
			}
		})
	}
}
