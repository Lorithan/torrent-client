package peer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/iotest"
)

// Build the wire fixture independently of Serialize to check the reader's offsets.
func handshakeReadFixture() (Handshake, []byte) {
	h := Handshake{
		ReservedFlags: [8]byte{0, 1, 2, 4, 8, 16, 128, 255},
		InfoHash:      [20]byte{255, 0, 128, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
		PeerID:        [20]byte{'-', 'C', 'T', '0', '0', '0', '1', '-', 0, 255, 128, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	wire := append([]byte{19}, []byte("BitTorrent protocol")...)
	wire = append(wire, h.ReservedFlags[:]...)
	wire = append(wire, h.InfoHash[:]...)
	wire = append(wire, h.PeerID[:]...)
	return h, wire
}

func TestReadHandshake(t *testing.T) {
	want, wire := handshakeReadFixture()
	for _, tt := range []struct {
		name string
		r    io.Reader
	}{
		{"complete input", bytes.NewReader(wire)},
		{"one byte per read", iotest.OneByteReader(bytes.NewReader(wire))},
		// A complete handshake must succeed without requesting EOF or more data.
		{"no read beyond handshake", io.MultiReader(bytes.NewReader(wire), iotest.ErrReader(errors.New("read past handshake")))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadHandshake(tt.r)
			if err != nil {
				t.Fatalf("ReadHandshake() error: %v", err)
			}
			if got == nil || *got != want {
				t.Errorf("handshake = %+v, want %+v", got, want)
			}
		})
	}
}

func TestReadHandshakePreservesFollowingMessage(t *testing.T) {
	_, wire := handshakeReadFixture()
	following := []byte{0, 0, 0, 1, 2}
	r := bytes.NewReader(append(wire, following...))
	if _, err := ReadHandshake(r); err != nil {
		t.Fatalf("ReadHandshake() error: %v", err)
	}
	remaining, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(remaining, following) {
		t.Errorf("remaining bytes = %x, want %x", remaining, following)
	}
}

func TestReadHandshakeTruncated(t *testing.T) {
	_, wire := handshakeReadFixture()
	for n := 0; n < len(wire); n++ {
		t.Run(fmt.Sprintf("%d_bytes", n), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked instead of returning an error: %v", r)
				}
			}()
			got, err := ReadHandshake(bytes.NewReader(wire[:n]))
			if err == nil || got != nil {
				t.Errorf("got (%+v, %v), want nil handshake and error", got, err)
			}
		})
	}
}

func TestReadHandshakeInvalidProtocol(t *testing.T) {
	for _, name := range []string{"wrong length", "wrong protocol name"} {
		t.Run(name, func(t *testing.T) {
			_, wire := handshakeReadFixture()
			if name == "wrong length" {
				wire[0] = 18
			} else {
				wire[1] = 'X'
			}
			got, err := ReadHandshake(bytes.NewReader(wire))
			if err == nil || got != nil {
				t.Errorf("got (%+v, %v), want nil handshake and error", got, err)
			}
		})
	}
}

func TestReadHandshakeRejectsLengthBeforeReadingFurther(t *testing.T) {
	for _, length := range []byte{0, 18, 20, 255} {
		t.Run(fmt.Sprintf("length_%d", length), func(t *testing.T) {
			_, wire := handshakeReadFixture()
			wire[0] = length
			r := bytes.NewReader(wire)
			got, err := ReadHandshake(r)
			if err == nil || got != nil {
				t.Errorf("got (%+v, %v), want nil handshake and error", got, err)
			}
			if r.Len() != len(wire)-1 {
				t.Errorf("consumed %d bytes, want only the length byte", len(wire)-r.Len())
			}
		})
	}
}

func TestReadHandshakeReadError(t *testing.T) {
	_, wire := handshakeReadFixture()
	wantErr := errors.New("connection interrupted")
	for _, n := range []int{0, 1, 30, 67} {
		t.Run(fmt.Sprintf("after_%d_bytes", n), func(t *testing.T) {
			r := io.MultiReader(bytes.NewReader(wire[:n]), iotest.ErrReader(wantErr))
			got, err := ReadHandshake(r)
			if got != nil || !errors.Is(err, wantErr) {
				t.Errorf("got (%+v, %v), want nil handshake and underlying read error", got, err)
			}
		})
	}
}
