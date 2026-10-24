package peer

import (
	"bytes"
	"fmt"
	"testing"
)

func TestMessagePieceBlock(t *testing.T) {
	for _, data := range [][]byte{{0}, {0, 255, 128, 1}, bytes.Repeat([]byte{0xaa}, 16384)} {
		t.Run(fmt.Sprintf("%d_data_bytes", len(data)), func(t *testing.T) {
			payload := append([]byte{1, 2, 3, 4, 5, 6, 7, 8}, data...)
			before := append([]byte(nil), payload...)
			m := Message{ID: MsgPiece, Payload: payload}
			got, err := m.PieceBlock()
			if err != nil || got == nil {
				t.Fatalf("PieceBlock() = (%v, %v), want block", got, err)
			}
			if got.Index != 0x01020304 || got.Begin != 0x05060708 || !bytes.Equal(got.Data, data) {
				t.Errorf("decoded index %x, begin %x, data length %d; want 01020304, 05060708, data length %d", got.Index, got.Begin, len(got.Data), len(data))
			}
			if !bytes.Equal(payload, before) {
				t.Error("decoder modified the message payload")
			}
			got.Data[0] ^= 0xff
			if !bytes.Equal(payload, before) {
				t.Error("block data mutation changed message payload")
			}
			got.Data[0] ^= 0xff
			payload[8] ^= 0xff
			if !bytes.Equal(got.Data, data) {
				t.Error("message payload mutation changed block data")
			}
		})
	}
}

func TestMessagePieceBlockRejectsInvalidInput(t *testing.T) {
	t.Run("wrong ID", func(t *testing.T) {
		m := Message{ID: MsgRequest, Payload: make([]byte, 12)}
		if got, err := m.PieceBlock(); got != nil || err == nil {
			t.Errorf("got (%v, %v), want nil block and error", got, err)
		}
	})
	for _, length := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 8 + 16385} {
		t.Run(fmt.Sprintf("payload_length_%d", length), func(t *testing.T) {
			m := Message{ID: MsgPiece, Payload: make([]byte, length)}
			if got, err := m.PieceBlock(); got != nil || err == nil {
				t.Errorf("got (%v, %v), want nil block and error", got, err)
			}
		})
	}
}
