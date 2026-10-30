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
		{"unhandled message", &Message{ID: MsgCancel}, true, true, Bitfield{0x80, 0}, false},
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

func TestClientHandleInterest(t *testing.T) {
	for _, initial := range []bool{false, true} {
		for _, choking := range []bool{false, true} {
			for _, id := range []MessageID{MsgInterested, MsgNotInterested} {
				for _, malformed := range []bool{false, true} {
					t.Run(fmt.Sprintf("initial=%t/choking=%t/id=%d/malformed=%t", initial, choking, id, malformed), func(t *testing.T) {
						c := &Client{
							PeerInterested: initial, AmChoking: choking, Choked: !choking,
							PieceCount: 8, Bitfield: Bitfield{0x80},
						}
						m := &Message{ID: id}
						if malformed {
							m.Payload = []byte{42}
						}
						err := c.HandleMessage(m)
						if (err != nil) != malformed {
							t.Fatalf("error=%v, want error=%t", err, malformed)
						}
						want := id == MsgInterested
						if malformed {
							want = initial
						}
						if c.PeerInterested != want {
							t.Errorf("PeerInterested=%t, want %t", c.PeerInterested, want)
						}
						if c.AmChoking != choking || c.Choked != !choking || c.PieceCount != 8 || !bytes.Equal(c.Bitfield, []byte{0x80}) {
							t.Error("interest message changed unrelated state")
						}
						if malformed && !bytes.Equal(m.Payload, []byte{42}) {
							t.Error("handler changed message payload")
						}
					})
				}
			}
		}
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

func TestClientSetChoking(t *testing.T) {
	for _, initial := range []bool{false, true} {
		for _, choking := range []bool{false, true} {
			t.Run(fmt.Sprintf("initial=%t/choking=%t", initial, choking), func(t *testing.T) {
				var outgoing bytes.Buffer
				conn := &exchangeTestConn{writer: &outgoing}
				c := &Client{Conn: conn, AmChoking: initial, Choked: !choking, PeerInterested: true, PieceCount: 8, Bitfield: Bitfield{0x80}}
				if err := c.SetChoking(choking); err != nil {
					t.Fatal(err)
				}
				id := byte(1) // Unchoke on the wire.
				if choking {
					id = 0
				}
				if !bytes.Equal(outgoing.Bytes(), []byte{0, 0, 0, 1, id}) {
					t.Errorf("wire=%x", outgoing.Bytes())
				}
				if c.AmChoking != choking {
					t.Errorf("AmChoking=%t, want %t", c.AmChoking, choking)
				}
				if c.Choked != !choking || !c.PeerInterested || c.PieceCount != 8 || !bytes.Equal(c.Bitfield, []byte{0x80}) {
					t.Error("changed unrelated state")
				}
				if conn.closed || conn.reads != 0 {
					t.Error("closed or read from caller's connection")
				}
			})
		}
	}
}

func TestClientSetChokingSendFailurePreservesState(t *testing.T) {
	wantErr := errors.New("choking message write failed")
	for _, initial := range []bool{false, true} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("initial=%t/partial=%t", initial, partial), func(t *testing.T) {
				var writer io.Writer = exchangeErrorWriter{err: wantErr}
				if partial {
					writer = &sendPartialErrorWriter{err: wantErr}
				}
				conn := &exchangeTestConn{writer: writer}
				c := &Client{Conn: conn, AmChoking: initial, Choked: true, PeerInterested: true}
				if err := c.SetChoking(!initial); !errors.Is(err, wantErr) {
					t.Errorf("error=%v, want %v", err, wantErr)
				}
				if c.AmChoking != initial || !c.Choked || !c.PeerInterested {
					t.Error("failed send changed state")
				}
				if conn.closed || conn.reads != 0 {
					t.Error("failed send closed or read from caller's connection")
				}
			})
		}
	}
}

func TestClientSendBitfield(t *testing.T) {
	for _, tc := range []struct {
		name      string
		count     int
		available Bitfield
		want      []byte
	}{
		{"partial byte", 10, Bitfield{0xa0, 0x40}, []byte{0, 0, 0, 3, 5, 0xa0, 0x40}},
		{"whole byte", 8, Bitfield{0xff}, []byte{0, 0, 0, 2, 5, 0xff}},
		{"no completed pieces", 8, Bitfield{0}, []byte{0, 0, 0, 2, 5, 0}},
		{"empty torrent", 0, nil, []byte{0, 0, 0, 1, 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var outgoing bytes.Buffer
			conn := &exchangeTestConn{writer: &outgoing}
			remote := NewBitfield(tc.count)
			if tc.count > 0 {
				remote[0] = 0x80
			}
			beforeRemote := bytes.Clone(remote)
			beforeAvailable := bytes.Clone(tc.available)
			c := &Client{Conn: conn, PieceCount: tc.count, Bitfield: remote, Choked: true, AmChoking: true, PeerInterested: true}
			if err := c.SendBitfield(tc.available); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(outgoing.Bytes(), tc.want) {
				t.Errorf("wire=%x, want %x", outgoing.Bytes(), tc.want)
			}
			if !bytes.Equal(c.Bitfield, beforeRemote) || !bytes.Equal(tc.available, beforeAvailable) {
				t.Error("sending changed local or remote availability")
			}
			if !c.Choked || !c.AmChoking || !c.PeerInterested || c.PieceCount != tc.count {
				t.Error("sending changed connection state")
			}
			if conn.closed || conn.reads != 0 {
				t.Error("sending closed or read from connection")
			}
		})
	}
}

func TestClientSendBitfieldRejectsInvalidAvailability(t *testing.T) {
	for _, tc := range []struct {
		name      string
		count     int
		available Bitfield
	}{
		{"too short", 10, Bitfield{0x80}},
		{"too long", 8, Bitfield{0x80, 0}},
		{"nonzero padding", 10, Bitfield{0x80, 1}},
		{"negative count", -1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var outgoing bytes.Buffer
			conn := &exchangeTestConn{writer: &outgoing}
			c := &Client{Conn: conn, PieceCount: tc.count, Bitfield: Bitfield{0x80}}
			before := bytes.Clone(tc.available)
			if err := c.SendBitfield(tc.available); err == nil {
				t.Error("invalid availability returned nil error")
			}
			if outgoing.Len() != 0 {
				t.Error("invalid availability was sent")
			}
			if !bytes.Equal(tc.available, before) || !bytes.Equal(c.Bitfield, []byte{0x80}) {
				t.Error("validation changed availability")
			}
			if conn.closed || conn.reads != 0 {
				t.Error("validation closed or read from connection")
			}
		})
	}
}

func TestClientSendBitfieldPropagatesWriteErrors(t *testing.T) {
	wantErr := errors.New("bitfield write failed")
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%t", partial), func(t *testing.T) {
			var writer io.Writer = exchangeErrorWriter{err: wantErr}
			if partial {
				writer = &sendPartialErrorWriter{err: wantErr}
			}
			conn := &exchangeTestConn{writer: writer}
			c := &Client{Conn: conn, PieceCount: 8, Bitfield: Bitfield{0x40}, AmChoking: true}
			available := Bitfield{0x80}
			if err := c.SendBitfield(available); !errors.Is(err, wantErr) {
				t.Errorf("error=%v, want %v", err, wantErr)
			}
			if !bytes.Equal(c.Bitfield, []byte{0x40}) || !bytes.Equal(available, []byte{0x80}) || !c.AmChoking {
				t.Error("failed send changed state")
			}
			if conn.closed || conn.reads != 0 {
				t.Error("failed send closed or read from connection")
			}
		})
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
			if !got.AmChoking || got.PeerInterested {
				t.Errorf("new client upload state: AmChoking=%t, PeerInterested=%t; want true, false", got.AmChoking, got.PeerInterested)
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

func TestWaitForAvailabilityChange(t *testing.T) {
	for _, tt := range []struct {
		name    string
		initial Bitfield
		prefix  []byte
		change  []byte
		want    Bitfield
	}{
		{"have extends known availability", Bitfield{0x80}, nil, NewHave(1).Serialize(), Bitfield{0xc0}},
		{"initial bitfield", Bitfield{0}, nil, (Message{ID: MsgBitfield, Payload: []byte{0x40}}).Serialize(), Bitfield{0x40}},
		{"skip keepalive unchoke and duplicate have", Bitfield{0x80}, append([]byte{0, 0, 0, 0, 0, 0, 0, 1, 1}, NewHave(0).Serialize()...), NewHave(1).Serialize(), Bitfield{0xc0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			trailing := NewHave(2).Serialize()
			wire := append(append(append([]byte(nil), tt.prefix...), tt.change...), trailing...)
			source := bytes.NewReader(wire)
			conn := &blockDeadlineConn{exchangeTestConn: exchangeTestConn{reader: iotest.OneByteReader(source)}}
			c := &Client{Conn: conn, Choked: true, PieceCount: 8, Bitfield: append(Bitfield(nil), tt.initial...)}
			before := time.Now()
			if err := c.WaitForAvailabilityChange(); err != nil {
				t.Fatal(err)
			}
			after := time.Now()
			if !bytes.Equal(c.Bitfield, tt.want) || source.Len() != len(trailing) {
				t.Errorf("availability = %x, unread bytes = %d; want %x and %d", c.Bitfield, source.Len(), tt.want, len(trailing))
			}
			if len(conn.deadlines) != 2 || conn.deadlines[0].Before(before.Add(30*time.Second)) || conn.deadlines[0].After(after.Add(30*time.Second)) || !conn.deadlines[1].IsZero() {
				t.Errorf("deadline calls = %v; want one thirty-second deadline and one clear", conn.deadlines)
			}
			if len(conn.events) == 0 || conn.events[0] != "set" || conn.closed {
				t.Error("deadline did not precede I/O or wait closed caller-owned connection")
			}
		})
	}
}

func TestWaitForAvailabilityChangeErrors(t *testing.T) {
	readErr, deadlineErr := errors.New("read failed"), errors.New("deadline failed")
	for _, tt := range []struct {
		name   string
		reader io.Reader
		failAt int
		cause  error
	}{
		{"set deadline", bytes.NewReader(nil), 1, deadlineErr},
		{"clear deadline", bytes.NewReader(NewHave(1).Serialize()), 2, deadlineErr},
		{"read failure", iotest.ErrReader(readErr), 0, readErr},
		{"disconnect", bytes.NewReader(nil), 0, io.EOF},
		{"unchanged bitfield then disconnect", bytes.NewReader((Message{ID: MsgBitfield, Payload: []byte{0x80}}).Serialize()), 0, io.EOF},
		{"out of range have", bytes.NewReader(NewHave(8).Serialize()), 0, nil},
		{"malformed have", bytes.NewReader((Message{ID: MsgHave, Payload: []byte{1}}).Serialize()), 0, nil},
		{"malformed bitfield", bytes.NewReader((Message{ID: MsgBitfield, Payload: []byte{0x80, 0}}).Serialize()), 0, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := &blockDeadlineConn{exchangeTestConn: exchangeTestConn{reader: tt.reader}, failAt: tt.failAt, err: deadlineErr}
			c := &Client{Conn: conn, PieceCount: 8, Bitfield: Bitfield{0x80}}
			err := c.WaitForAvailabilityChange()
			if err == nil || (tt.cause != nil && !errors.Is(err, tt.cause)) {
				t.Fatalf("error = %v, want failure preserving %v", err, tt.cause)
			}
			if tt.failAt == 1 && conn.reads != 0 {
				t.Error("read after failing to set deadline")
			}
			if conn.closed {
				t.Error("availability wait closed caller-owned connection")
			}
		})
	}
}
