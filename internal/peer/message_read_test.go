package peer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/iotest"
)

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
