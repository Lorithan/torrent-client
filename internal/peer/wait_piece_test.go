package peer

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"
	"time"
)

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
