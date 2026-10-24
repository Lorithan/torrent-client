package peer

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"
)

func TestClientWaitForUnchoke(t *testing.T) {
	// Keep-alive, bitfield, have piece 9, choke, unchoke, then another message.
	incoming := []byte{
		0, 0, 0, 0,
		0, 0, 0, 3, 5, 0x80, 0,
		0, 0, 0, 5, 4, 0, 0, 0, 9,
		0, 0, 0, 1, 0,
		0, 0, 0, 1, 1,
		0, 0, 0, 1, 0,
	}
	source := bytes.NewReader(incoming)
	var outgoing bytes.Buffer
	conn := &exchangeTestConn{reader: iotest.OneByteReader(source), writer: &outgoing}
	c := &Client{Conn: conn, Choked: true, PieceCount: 10, Bitfield: make(Bitfield, 2)}
	if err := c.WaitForUnchoke(); err != nil {
		t.Fatal(err)
	}
	if c.Choked || !bytes.Equal(c.Bitfield, []byte{0x80, 0x40}) {
		t.Errorf("state: choked %v, bitfield %x; want false, 8040", c.Choked, c.Bitfield)
	}
	if !bytes.Equal(outgoing.Bytes(), []byte{0, 0, 0, 1, 2}) {
		t.Errorf("sent %x, want one interested message", outgoing.Bytes())
	}
	if source.Len() != 5 {
		t.Errorf("remaining bytes = %d, want next message left unread (5)", source.Len())
	}
	if conn.closed {
		t.Error("wait closed caller-owned connection")
	}
}

func TestClientWaitForUnchokeAlreadyUnchoked(t *testing.T) {
	var outgoing bytes.Buffer
	conn := &exchangeTestConn{reader: iotest.ErrReader(errors.New("unexpected read")), writer: &outgoing}
	c := &Client{Conn: conn, Choked: false}
	if err := c.WaitForUnchoke(); err != nil {
		t.Fatal(err)
	}
	if conn.reads != 0 || !bytes.Equal(outgoing.Bytes(), []byte{0, 0, 0, 1, 2}) {
		t.Errorf("reads = %d, sent %x; want no reads and one interested message", conn.reads, outgoing.Bytes())
	}
}

func TestClientWaitForUnchokeErrors(t *testing.T) {
	writeErr := errors.New("write failed")
	readErr := errors.New("read failed")
	for _, tt := range []struct {
		name     string
		reader   io.Reader
		writeErr error
		wantErr  error
	}{
		{"send failure", bytes.NewReader(nil), writeErr, writeErr},
		{"read failure", iotest.ErrReader(readErr), nil, readErr},
		{"peer disconnected", bytes.NewReader(nil), nil, io.EOF},
		{"malformed unchoke", bytes.NewReader([]byte{0, 0, 0, 2, 1, 42}), nil, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var outgoing bytes.Buffer
			conn := &exchangeTestConn{reader: tt.reader, writer: &outgoing}
			if tt.writeErr != nil {
				conn.writer = exchangeErrorWriter{err: tt.writeErr}
			}
			c := &Client{Conn: conn, Choked: true}
			err := c.WaitForUnchoke()
			if err == nil {
				t.Error("wait reported success after failure")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("error = %v, want underlying error %v", err, tt.wantErr)
			}
			if !c.Choked {
				t.Error("failed wait marked client unchoked")
			}
			if tt.writeErr != nil && conn.reads != 0 {
				t.Error("read after failed send")
			}
			if conn.closed {
				t.Error("failed wait closed caller-owned connection")
			}
		})
	}
}
