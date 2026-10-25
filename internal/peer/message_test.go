package peer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
	"testing/iotest"
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

func TestReadMessage(t *testing.T) {
	for _, tt := range []struct {
		name    string
		wire    []byte
		id      MessageID
		payload []byte
	}{
		{"interested", []byte{0, 0, 0, 1, 2}, 2, nil},
		{"choke", []byte{0, 0, 0, 1, 0}, 0, nil},
		{"binary payload", []byte{0, 0, 0, 4, 5, 0, 255, 128}, 5, []byte{0, 255, 128}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("ReadMessage panicked: %v", r)
				}
			}()
			source := bytes.NewReader(append(append([]byte(nil), tt.wire...), 42))
			got, err := ReadMessage(iotest.OneByteReader(source))
			if err != nil || got == nil {
				t.Fatalf("got (%+v, %v), want message", got, err)
			}
			if got.ID != tt.id || !bytes.Equal(got.Payload, tt.payload) {
				t.Errorf("got %+v, want ID %d and payload %x", got, tt.id, tt.payload)
			}
			if source.Len() != 1 {
				t.Errorf("remaining bytes = %d, want 1", source.Len())
			}
		})
	}
}

func TestReadMessageKeepAlive(t *testing.T) {
	r := bytes.NewReader([]byte{0, 0, 0, 0, 42})
	got, err := ReadMessage(r)
	if got != nil || err != nil || r.Len() != 1 {
		t.Errorf("got (%+v, %v), remaining %d; want (nil, nil), remaining 1", got, err, r.Len())
	}
}

func TestReadMessageTruncated(t *testing.T) {
	wire := []byte{0, 0, 0, 4, 5, 0, 255, 128}
	for n := 0; n < len(wire); n++ {
		t.Run(fmt.Sprintf("%d_bytes", n), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked instead of returning an error: %v", r)
				}
			}()
			got, err := ReadMessage(bytes.NewReader(wire[:n]))
			if got != nil || err == nil {
				t.Errorf("got (%+v, %v), want nil message and error", got, err)
			}
		})
	}
}

func TestReadMessageSizeLimit(t *testing.T) {
	for _, size := range []uint32{maxMessageLength, maxMessageLength + 1, ^uint32(0)} {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("ReadMessage panicked: %v", r)
				}
			}()
			var prefix [4]byte
			binary.BigEndian.PutUint32(prefix[:], size)
			if size > maxMessageLength {
				r := bytes.NewReader(append(prefix[:], 42))
				got, err := ReadMessage(r)
				if got != nil || err == nil || r.Len() != 1 {
					t.Errorf("got (%+v, %v), remaining %d; want rejection before reading body", got, err, r.Len())
				}
				return
			}
			body := bytes.Repeat([]byte{0xaa}, int(size))
			body[0] = 5
			got, err := ReadMessage(io.MultiReader(bytes.NewReader(prefix[:]), bytes.NewReader(body)))
			if err != nil || got == nil || got.ID != 5 || !bytes.Equal(got.Payload, body[1:]) {
				t.Errorf("failed to read message exactly at size limit: %v", err)
			}
		})
	}
}

func TestReadMessageReadErrors(t *testing.T) {
	wantErr := errors.New("connection interrupted")
	wire := []byte{0, 0, 0, 4, 5, 0, 255, 128}
	for _, n := range []int{0, 2, 4, 6} {
		t.Run(fmt.Sprintf("after_%d_bytes", n), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("ReadMessage panicked: %v", r)
				}
			}()
			got, err := ReadMessage(io.MultiReader(bytes.NewReader(wire[:n]), iotest.ErrReader(wantErr)))
			if got != nil || !errors.Is(err, wantErr) {
				t.Errorf("got (%+v, %v), want underlying read error", got, err)
			}
		})
	}
}

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

func TestNewHave(t *testing.T) {
	for _, tt := range []struct {
		name    string
		index   uint32
		payload []byte
	}{
		{"zero", 0, []byte{0, 0, 0, 0}},
		{"multibyte", 0x01020304, []byte{1, 2, 3, 4}},
		{"maximum", ^uint32(0), []byte{255, 255, 255, 255}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := NewHave(tt.index)
			if m.ID != MsgHave || !bytes.Equal(m.Payload, tt.payload) {
				t.Errorf("message = %+v, want have with payload %x", m, tt.payload)
			}
			want := append([]byte{0, 0, 0, 5, 4}, tt.payload...)
			if got := m.Serialize(); !bytes.Equal(got, want) {
				t.Errorf("wire = %x, want %x", got, want)
			}
		})
	}
}

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
