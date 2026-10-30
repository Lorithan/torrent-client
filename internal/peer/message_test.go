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

func TestMessageBlockRequest(t *testing.T) {
	for _, tt := range []struct {
		name    string
		payload []byte
		want    BlockRequest
	}{
		{"zero index and offset, minimum length", []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, BlockRequest{Index: 0, Begin: 0, Length: 1}},
		{"big endian fields", []byte{1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 4, 5}, BlockRequest{Index: 0x01020304, Begin: 0x05060708, Length: 0x0405}},
		{"full uint32 values and maximum block", []byte{255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0x40, 0}, BlockRequest{Index: ^uint32(0), Begin: ^uint32(0), Length: 16384}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := bytes.Clone(tt.payload)
			got, err := (Message{ID: MsgRequest, Payload: tt.payload}).BlockRequest()
			if err != nil || got == nil {
				t.Fatalf("BlockRequest = %v, %v", got, err)
			}
			if *got != tt.want {
				t.Errorf("request = %+v, want %+v", *got, tt.want)
			}
			if !bytes.Equal(tt.payload, before) {
				t.Error("parsing changed the message payload")
			}
		})
	}
}

func TestMessageBlockRequestRejectsInvalidInput(t *testing.T) {
	t.Run("wrong message IDs", func(t *testing.T) {
		payload := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
		for _, id := range []MessageID{MsgChoke, MsgUnchoke, MsgInterested, MsgNotInterested, MsgHave, MsgBitfield, MsgPiece, MsgCancel, MessageID(255)} {
			if got, err := (Message{ID: id, Payload: payload}).BlockRequest(); got != nil || err == nil {
				t.Errorf("message ID %d = %v, %v; want nil request and error", id, got, err)
			}
		}
	})
	t.Run("payload sizes", func(t *testing.T) {
		for _, size := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 24} {
			payload := make([]byte, size)
			if size >= 12 {
				binary.BigEndian.PutUint32(payload[8:12], 1)
			}
			if got, err := (Message{ID: MsgRequest, Payload: payload}).BlockRequest(); got != nil || err == nil {
				t.Errorf("payload size %d = %v, %v; want nil request and error", size, got, err)
			}
		}
	})
	t.Run("block lengths", func(t *testing.T) {
		for _, length := range []uint32{0, 16385, ^uint32(0)} {
			payload := make([]byte, 12)
			binary.BigEndian.PutUint32(payload[8:12], length)
			if got, err := (Message{ID: MsgRequest, Payload: payload}).BlockRequest(); got != nil || err == nil {
				t.Errorf("block length %d = %v, %v; want nil request and error", length, got, err)
			}
		}
	})
}

func TestNewPiece(t *testing.T) {
	for _, tt := range []struct {
		name   string
		index  uint32
		begin  uint32
		data   []byte
		prefix []byte
	}{
		{"minimum block", 0, 0, []byte{0xab}, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{"big endian fields and binary data", 0x01020304, 0x05060708, []byte{0, 255, 128, 1}, []byte{1, 2, 3, 4, 5, 6, 7, 8}},
		{"maximum block and uint32 fields", ^uint32(0), ^uint32(0), bytes.Repeat([]byte{0xab}, 16384), bytes.Repeat([]byte{255}, 8)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewPiece(tt.index, tt.begin, tt.data)
			if err != nil || m == nil {
				t.Fatalf("NewPiece = %v, %v", m, err)
			}
			want := append(bytes.Clone(tt.prefix), tt.data...)
			if m.ID != MsgPiece || !bytes.Equal(m.Payload, want) {
				t.Fatalf("unexpected piece message: ID %d, payload length %d", m.ID, len(m.Payload))
			}
			wire := m.Serialize()
			if binary.BigEndian.Uint32(wire[:4]) != uint32(9+len(tt.data)) || wire[4] != byte(MsgPiece) || !bytes.Equal(wire[5:], want) {
				t.Error("serialized piece has incorrect message length, ID, or payload")
			}
			block, err := m.PieceBlock()
			if err != nil || block == nil {
				t.Fatalf("piece response could not be parsed: %v", err)
			}
			if block.Index != tt.index || block.Begin != tt.begin || !bytes.Equal(block.Data, tt.data) {
				t.Error("piece parser did not recover the transmitted block")
			}
		})
	}
}

func TestNewPieceOwnsPayloadData(t *testing.T) {
	data := make([]byte, 3, 32)
	copy(data, []byte{1, 2, 3})
	m, err := NewPiece(7, 16, data)
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 99
	if !bytes.Equal(m.Payload[8:], []byte{1, 2, 3}) {
		t.Fatal("caller mutation changed the outgoing piece payload")
	}
	m.Payload[9] = 88
	if data[1] != 2 {
		t.Error("message mutation changed the caller's data")
	}
}

func TestNewPieceRejectsInvalidLength(t *testing.T) {
	for _, data := range [][]byte{nil, {}, make([]byte, 16385)} {
		if m, err := NewPiece(0, 0, data); m != nil || err == nil {
			t.Errorf("data length %d = %v, %v; want nil message and error", len(data), m, err)
		}
	}
}
