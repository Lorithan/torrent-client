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

// Only Read, Write, and Close are relevant to these error-path tests.
type exchangeTestConn struct {
	net.Conn
	reader io.Reader
	writer io.Writer
	closed bool
	reads  int
}

func (c *exchangeTestConn) Read(p []byte) (int, error)  { c.reads++; return c.reader.Read(p) }
func (c *exchangeTestConn) Write(p []byte) (int, error) { return c.writer.Write(p) }
func (c *exchangeTestConn) Close() error                { c.closed = true; return nil }

type exchangeErrorWriter struct{ err error }

func (w exchangeErrorWriter) Write([]byte) (int, error) { return 0, w.err }

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
