package peer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"testing/iotest"
	"time"
)

func TestExchangeHandshakeOverConnection(t *testing.T) {
	local, _ := handshakeReadFixture()
	remote := local
	remote.PeerID = [20]byte{'r', 'e', 'm', 'o', 't', 'e'}
	remote.ReservedFlags = [8]byte{0, 0, 0, 0, 0, 0, 0, 1}
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	deadline := time.Now().Add(3 * time.Second)
	if err := client.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := server.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		var incoming [68]byte
		if _, err := io.ReadFull(server, incoming[:]); err != nil {
			done <- err
			return
		}
		if !bytes.Equal(incoming[:], local.Serialize()) {
			done <- fmt.Errorf("remote received incorrect local handshake: %x", incoming)
			return
		}
		// Send one byte at a time to exercise fragmented network reads.
		for _, b := range remote.Serialize() {
			if _, err := server.Write([]byte{b}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	got, err := ExchangeHandshake(client, local)
	if err != nil {
		t.Fatalf("ExchangeHandshake() error: %v", err)
	}
	if got == nil || *got != remote {
		t.Errorf("remote handshake = %+v, want %+v", got, remote)
	}
	if err := <-done; err != nil {
		t.Fatalf("remote side: %v", err)
	}
	// The caller must still own an open connection after the exchange.
	written := make(chan error, 1)
	go func() { _, err := server.Write([]byte{42}); written <- err }()
	var next [1]byte
	if _, err := io.ReadFull(client, next[:]); err != nil || next[0] != 42 {
		t.Errorf("connection unusable after handshake: byte %d, error %v", next[0], err)
	}
	if err := <-written; err != nil {
		t.Errorf("write after handshake: %v", err)
	}
}

func TestExchangeHandshakeErrors(t *testing.T) {
	local, wire := handshakeReadFixture()
	mismatch := append([]byte(nil), wire...)
	mismatch[28] ^= 0xff
	readErr := errors.New("connection read failed")
	writeErr := errors.New("connection write failed")
	for _, tt := range []struct {
		name     string
		reader   io.Reader
		writeErr error
		wantErr  error
	}{
		{"write failure", bytes.NewReader(wire), writeErr, writeErr},
		{"read failure", iotest.ErrReader(readErr), nil, readErr},
		{"truncated remote handshake", bytes.NewReader(wire[:40]), nil, io.ErrUnexpectedEOF},
		{"mismatched torrent", bytes.NewReader(mismatch), nil, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var outgoing bytes.Buffer
			conn := &exchangeTestConn{reader: tt.reader, writer: &outgoing}
			if tt.writeErr != nil {
				conn.writer = exchangeErrorWriter{err: tt.writeErr}
			}
			got, err := ExchangeHandshake(conn, local)
			if err == nil || got != nil {
				t.Fatalf("got (%+v, %v), want nil handshake and error", got, err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("error = %v, want underlying error %v", err, tt.wantErr)
			}
			if tt.writeErr != nil && conn.reads != 0 {
				t.Error("read remote handshake after write failure")
			}
			if tt.writeErr == nil && !bytes.Equal(outgoing.Bytes(), wire) {
				t.Error("did not send the complete local handshake")
			}
			if conn.closed {
				t.Error("exchange closed a caller-owned connection")
			}
		})
	}
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
