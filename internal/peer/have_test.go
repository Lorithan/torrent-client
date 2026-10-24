package peer

import (
	"bytes"
	"strconv"
	"testing"
)

func TestMessageHaveIndex(t *testing.T) {
	for _, tt := range []struct {
		name       string
		message    Message
		pieceCount int
		want       int
		wantErr    bool
	}{
		{"first piece", Message{ID: MsgHave, Payload: []byte{0, 0, 0, 0}}, 1, 0, false},
		{"last piece", Message{ID: MsgHave, Payload: []byte{0, 0, 0, 9}}, 10, 9, false},
		{"big endian", Message{ID: MsgHave, Payload: []byte{0, 0, 1, 0}}, 300, 256, false},
		{"wrong ID", Message{ID: MsgPiece, Payload: []byte{0, 0, 0, 0}}, 10, 0, true},
		{"nil payload", Message{ID: MsgHave}, 10, 0, true},
		{"short payload", Message{ID: MsgHave, Payload: []byte{0, 0, 0}}, 10, 0, true},
		{"long payload", Message{ID: MsgHave, Payload: []byte{0, 0, 0, 0, 0}}, 10, 0, true},
		{"negative count", Message{ID: MsgHave, Payload: []byte{0, 0, 0, 0}}, -1, 0, true},
		{"zero count", Message{ID: MsgHave, Payload: []byte{0, 0, 0, 0}}, 0, 0, true},
		{"index equals count", Message{ID: MsgHave, Payload: []byte{0, 0, 0, 10}}, 10, 0, true},
		{"index exceeds count", Message{ID: MsgHave, Payload: []byte{0, 0, 0, 11}}, 10, 0, true},
		{"maximum wire index exceeds count", Message{ID: MsgHave, Payload: []byte{255, 255, 255, 255}}, 10, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := append([]byte(nil), tt.message.Payload...)
			got, err := tt.message.HaveIndex(tt.pieceCount)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Errorf("HaveIndex(%d) = (%d, %v), want index %d, wantErr %v", tt.pieceCount, got, err, tt.want, tt.wantErr)
			}
			if !bytes.Equal(tt.message.Payload, before) {
				t.Error("HaveIndex modified the payload")
			}
		})
	}
}

func TestMessageHaveIndexLargePieceCount(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("piece counts above uint32 require a 64-bit int")
	}
	largeCount := uint64(1) << 32
	for _, tt := range []struct {
		name    string
		payload []byte
		want    uint64
	}{
		{"first piece", []byte{0, 0, 0, 0}, 0},
		{"maximum wire index", []byte{255, 255, 255, 255}, largeCount - 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := Message{ID: MsgHave, Payload: tt.payload}
			got, err := m.HaveIndex(int(largeCount))
			if err != nil || got != int(tt.want) {
				t.Errorf("HaveIndex(%d) = (%d, %v), want (%d, nil)", largeCount, got, err, tt.want)
			}
		})
	}
}
