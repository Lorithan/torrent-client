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

func TestClientHandleMessage(t *testing.T) {
	for _, tt := range []struct {
		name          string
		message       *Message
		initialChoked bool
		wantChoked    bool
		wantBits      Bitfield
		wantErr       bool
	}{
		{"keep alive", nil, true, true, Bitfield{0x80, 0}, false},
		{"choke", &Message{ID: MsgChoke}, false, true, Bitfield{0x80, 0}, false},
		{"unchoke", &Message{ID: MsgUnchoke}, true, false, Bitfield{0x80, 0}, false},
		{"have", &Message{ID: MsgHave, Payload: []byte{0, 0, 0, 9}}, true, true, Bitfield{0x80, 0x40}, false},
		{"bitfield", &Message{ID: MsgBitfield, Payload: []byte{0x20, 0x80}}, true, true, Bitfield{0x20, 0x80}, false},
		{"malformed choke", &Message{ID: MsgChoke, Payload: []byte{1}}, false, false, Bitfield{0x80, 0}, true},
		{"malformed unchoke", &Message{ID: MsgUnchoke, Payload: []byte{1}}, true, true, Bitfield{0x80, 0}, true},
		{"short have", &Message{ID: MsgHave, Payload: []byte{0}}, true, true, Bitfield{0x80, 0}, true},
		{"out of range have", &Message{ID: MsgHave, Payload: []byte{0, 0, 0, 10}}, true, true, Bitfield{0x80, 0}, true},
		{"wrong bitfield length", &Message{ID: MsgBitfield, Payload: []byte{0x80}}, true, true, Bitfield{0x80, 0}, true},
		{"nonzero padding", &Message{ID: MsgBitfield, Payload: []byte{0x80, 1}}, true, true, Bitfield{0x80, 0}, true},
		{"unhandled message", &Message{ID: MsgInterested}, true, true, Bitfield{0x80, 0}, false},
		{"unknown ID", &Message{ID: MessageID(255)}, true, true, Bitfield{0x80, 0}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{PieceCount: 10, Choked: tt.initialChoked, Bitfield: Bitfield{0x80, 0}}
			var before []byte
			if tt.message != nil {
				before = append([]byte(nil), tt.message.Payload...)
			}
			err := c.HandleMessage(tt.message)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if c.Choked != tt.wantChoked || !bytes.Equal(c.Bitfield, tt.wantBits) {
				t.Errorf("state: choked %v, bits %x; want choked %v, bits %x", c.Choked, c.Bitfield, tt.wantChoked, tt.wantBits)
			}
			if tt.message != nil && !bytes.Equal(tt.message.Payload, before) {
				t.Error("handler modified message payload")
			}
		})
	}
}

func TestClientHandleBitfieldCopiesPayload(t *testing.T) {
	for _, name := range []string{"payload mutation", "client mutation"} {
		t.Run(name, func(t *testing.T) {
			c := &Client{PieceCount: 10, Bitfield: make(Bitfield, 2)}
			m := &Message{ID: MsgBitfield, Payload: []byte{0x80, 0}}
			if err := c.HandleMessage(m); err != nil {
				t.Fatal(err)
			}
			if name == "payload mutation" {
				m.Payload[0] = 0
				if !c.Bitfield.HasPiece(0) {
					t.Error("message payload mutation changed client bitfield")
				}
			} else {
				if err := c.HandleMessage(&Message{ID: MsgHave, Payload: []byte{0, 0, 0, 9}}); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(m.Payload, []byte{0x80, 0}) {
					t.Error("have update changed the earlier bitfield message payload")
				}
			}
		})
	}
}

func TestClientHandleHaveReportsSetPieceFailure(t *testing.T) {
	// Client fields are exported, so inconsistent state can currently be constructed.
	c := &Client{PieceCount: 10, Bitfield: Bitfield{0x80}}
	err := c.HandleMessage(&Message{ID: MsgHave, Payload: []byte{0, 0, 0, 9}})
	if err == nil {
		t.Error("handler hid SetPiece's out-of-bounds error")
	}
	if !bytes.Equal(c.Bitfield, []byte{0x80}) {
		t.Error("failed update changed the bitfield")
	}
}

func TestClientSendMessage(t *testing.T) {
	for _, tt := range []struct {
		name    string
		message Message
		want    []byte
	}{
		{"interested", Message{ID: MsgInterested}, []byte{0, 0, 0, 1, 2}},
		{"have", Message{ID: MsgHave, Payload: []byte{0, 0, 1, 0}}, []byte{0, 0, 0, 5, 4, 0, 0, 1, 0}},
		{"binary payload", Message{ID: MsgBitfield, Payload: []byte{255, 0, 128}}, []byte{0, 0, 0, 4, 5, 255, 0, 128}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var outgoing bytes.Buffer
			conn := &exchangeTestConn{writer: &outgoing}
			c := &Client{Conn: conn, Choked: true, PieceCount: 8, Bitfield: Bitfield{0x80}}
			if err := c.SendMessage(tt.message); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(outgoing.Bytes(), tt.want) {
				t.Errorf("sent %x, want %x", outgoing.Bytes(), tt.want)
			}
			if !c.Choked || c.PieceCount != 8 || !bytes.Equal(c.Bitfield, []byte{0x80}) {
				t.Error("sending changed peer state")
			}
			if conn.closed || conn.reads != 0 {
				t.Error("sending closed or read from the connection")
			}
		})
	}
}

func TestClientSendMessageWriteErrors(t *testing.T) {
	wantErr := errors.New("write failed")
	partial := &sendPartialErrorWriter{err: wantErr}
	for _, tt := range []struct {
		name   string
		writer io.Writer
	}{
		{"no bytes sent", exchangeErrorWriter{err: wantErr}},
		{"partial write", partial},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := &exchangeTestConn{writer: tt.writer}
			c := &Client{Conn: conn}
			if err := c.SendMessage(Message{ID: MsgInterested}); !errors.Is(err, wantErr) {
				t.Errorf("error = %v, want underlying error %v", err, wantErr)
			}
			if conn.closed || conn.reads != 0 {
				t.Error("failed send closed or read from connection")
			}
		})
	}
	if partial.Len() != 2 {
		t.Errorf("sent %d bytes after partial failure, want 2", partial.Len())
	}
}

func TestNewClient(t *testing.T) {
	conn, other := net.Pipe()
	t.Cleanup(func() { conn.Close(); other.Close() })
	remote, _ := handshakeReadFixture()
	for _, tt := range []struct {
		pieces int
		bytes  int
	}{
		{0, 0}, {1, 1}, {7, 1}, {8, 1}, {9, 2}, {16, 2}, {17, 3},
	} {
		t.Run(fmt.Sprintf("%d_pieces", tt.pieces), func(t *testing.T) {
			got, err := NewClient(conn, remote, tt.pieces)
			if err != nil || got == nil {
				t.Fatalf("NewClient() = (%v, %v), want client", got, err)
			}
			if got.Conn != conn || got.Handshake != remote || got.PieceCount != tt.pieces {
				t.Errorf("client did not preserve constructor inputs: %+v", got)
			}
			if !got.Choked {
				t.Error("new client must start choked")
			}
			if !bytes.Equal(got.Bitfield, make([]byte, tt.bytes)) {
				t.Errorf("bitfield = %x, want %d zero bytes", got.Bitfield, tt.bytes)
			}
			if err := got.Bitfield.Validate(tt.pieces); err != nil {
				t.Errorf("initial bitfield is invalid: %v", err)
			}
		})
	}
}

func TestNewClientRejectsNegativePieceCount(t *testing.T) {
	conn := &exchangeTestConn{}
	got, err := NewClient(conn, Handshake{}, -1)
	if got != nil || err == nil {
		t.Errorf("NewClient() = (%v, %v), want nil client and error", got, err)
	}
	if conn.closed {
		t.Error("constructor closed the caller's connection on failure")
	}
}

func TestNewClientBitfieldsAreIndependent(t *testing.T) {
	conn, other := net.Pipe()
	t.Cleanup(func() { conn.Close(); other.Close() })
	first, err := NewClient(conn, Handshake{}, 8)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewClient(other, Handshake{}, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Bitfield.SetPiece(0); err != nil {
		t.Fatal(err)
	}
	if !first.Bitfield.HasPiece(0) || second.Bitfield.HasPiece(0) {
		t.Error("updating one client's bitfield affected another client")
	}
}

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

func TestWaitForPieceDeadline(t *testing.T) {
	for _, tt := range []struct {
		name     string
		choked   bool
		bits     Bitfield
		messages []byte
	}{
		{"wait for unchoke", true, Bitfield{0x80}, []byte{0, 0, 0, 1, 1}},
		{"wait for availability", false, Bitfield{0}, []byte{0, 0, 0, 5, 4, 0, 0, 0, 0}},
		{"wait for both with unrelated messages", true, Bitfield{0}, []byte{0, 0, 0, 0, 0, 0, 0, 5, 4, 0, 0, 0, 1, 0, 0, 0, 5, 4, 0, 0, 0, 0, 0, 0, 0, 1, 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := bytes.NewReader(append(tt.messages, 42))
			conn := &blockDeadlineConn{exchangeTestConn: exchangeTestConn{reader: iotest.OneByteReader(source)}}
			c := &Client{Conn: conn, PieceCount: 8, Bitfield: tt.bits, Choked: tt.choked}
			before := time.Now()
			if err := c.WaitForPiece(0); err != nil {
				t.Fatal(err)
			}
			after := time.Now()
			if c.Choked || !c.Bitfield.HasPiece(0) {
				t.Error("returned before both readiness conditions were satisfied")
			}
			if len(conn.deadlines) != 2 {
				t.Fatalf("deadline calls = %d, want one set and one clear", len(conn.deadlines))
			}
			if d := conn.deadlines[0]; d.Before(before.Add(30*time.Second)) || d.After(after.Add(30*time.Second)) {
				t.Errorf("deadline %v does not give a 30-second allowance", d)
			}
			if !conn.deadlines[1].IsZero() || conn.events[0] != "set" || conn.events[len(conn.events)-1] != "clear" {
				t.Error("deadline does not enclose the complete wait")
			}
			if source.Len() != 1 {
				t.Error("consumed bytes beyond the readiness messages")
			}
		})
	}
}

func TestWaitForPieceAlreadyReady(t *testing.T) {
	c := &Client{PieceCount: 8, Bitfield: Bitfield{0x80}}
	// A nil connection proves the fast path performs no connection operations.
	if err := c.WaitForPiece(0); err != nil {
		t.Fatal(err)
	}
}

func TestWaitForPieceErrors(t *testing.T) {
	readErr := errors.New("connection read failed")
	deadlineErr := errors.New("deadline failed")
	for _, tt := range []struct {
		name   string
		reader io.Reader
		failAt int
		cause  error
	}{
		{"set deadline", bytes.NewReader(nil), 1, deadlineErr},
		{"clear deadline", bytes.NewReader([]byte{0, 0, 0, 1, 1}), 2, deadlineErr},
		{"read failure", iotest.ErrReader(readErr), 0, readErr},
		{"disconnect", bytes.NewReader(nil), 0, io.EOF},
		{"malformed unchoke", bytes.NewReader([]byte{0, 0, 0, 2, 1, 42}), 0, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := &blockDeadlineConn{exchangeTestConn: exchangeTestConn{reader: tt.reader}, failAt: tt.failAt, err: deadlineErr}
			c := &Client{Conn: conn, Choked: true, PieceCount: 8, Bitfield: Bitfield{0x80}}
			err := c.WaitForPiece(0)
			if err == nil || (tt.cause != nil && !errors.Is(err, tt.cause)) {
				t.Errorf("error = %v, want failure preserving %v", err, tt.cause)
			}
			if tt.failAt == 1 && conn.reads != 0 {
				t.Error("read after failing to set deadline")
			}
			if conn.closed {
				t.Error("wait closed caller-owned connection")
			}
		})
	}
}

func TestWaitForPieceRejectsInvalidIndexBeforeIO(t *testing.T) {
	for _, index := range []int{-1, 8} {
		conn := &blockDeadlineConn{exchangeTestConn: exchangeTestConn{reader: bytes.NewReader(nil)}}
		c := &Client{Conn: conn, PieceCount: 8, Bitfield: Bitfield{0x80}}
		if err := c.WaitForPiece(index); err == nil {
			t.Errorf("index %d was accepted", index)
		}
		if len(conn.deadlines) != 0 || conn.reads != 0 {
			t.Errorf("invalid index %d performed connection operations", index)
		}
	}
}
