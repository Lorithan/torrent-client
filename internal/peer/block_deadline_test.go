package peer

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

type blockDeadlineConn struct {
	exchangeTestConn
	deadlines []time.Time
	events    []string
	failAt    int
	err       error
}

func (c *blockDeadlineConn) SetDeadline(d time.Time) error {
	c.deadlines = append(c.deadlines, d)
	if d.IsZero() {
		c.events = append(c.events, "clear")
	} else {
		c.events = append(c.events, "set")
	}
	if len(c.deadlines) == c.failAt {
		return c.err
	}
	return nil
}
func (c *blockDeadlineConn) Read(p []byte) (int, error) {
	c.events = append(c.events, "read")
	return c.exchangeTestConn.Read(p)
}
func (c *blockDeadlineConn) Write(p []byte) (int, error) {
	c.events = append(c.events, "write")
	return c.exchangeTestConn.Write(p)
}

func TestDownloadBlockDeadlineCoversRequestAndResponse(t *testing.T) {
	var replies []byte
	for _, begin := range []uint32{0, 3} {
		// Neither keep-alive nor have should renew the request's deadline.
		replies = append(replies, 0, 0, 0, 0, 0, 0, 0, 5, 4, 0, 0, 0, 1)
		replies = append(replies, downloadTestPiece(0, begin, []byte{1, 2, 3})...)
	}
	var outgoing bytes.Buffer
	conn := &blockDeadlineConn{exchangeTestConn: exchangeTestConn{reader: bytes.NewReader(replies), writer: &outgoing}}
	c := &Client{Conn: conn, PieceCount: 8, Bitfield: Bitfield{0x80}}
	for _, begin := range []uint32{0, 3} {
		conn.events = nil
		conn.deadlines = nil
		before := time.Now()
		if _, err := c.DownloadBlock(0, begin, 3); err != nil {
			t.Fatal(err)
		}
		after := time.Now()
		if len(conn.events) == 0 || conn.events[0] != "set" {
			t.Errorf("deadline must precede all I/O, got events %v", conn.events)
		}
		if len(conn.deadlines) != 2 {
			t.Fatalf("deadline calls = %d, want one set and one clear", len(conn.deadlines))
		}
		d := conn.deadlines[0]
		if d.Before(before.Add(30*time.Second)) || d.After(after.Add(30*time.Second)) {
			t.Errorf("deadline %v does not give this request a fresh 30-second allowance", d)
		}
		if !conn.deadlines[1].IsZero() || conn.events[len(conn.events)-1] != "clear" {
			t.Error("deadline was not cleared after completing the block")
		}
	}
}

func TestDownloadBlockDeadlineErrors(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		name := "setting"
		if failAt == 2 {
			name = "clearing"
		}
		t.Run(name, func(t *testing.T) {
			wantErr := errors.New("deadline operation failed")
			var outgoing bytes.Buffer
			conn := &blockDeadlineConn{
				exchangeTestConn: exchangeTestConn{reader: bytes.NewReader(downloadTestPiece(0, 0, []byte{1})), writer: &outgoing},
				failAt:           failAt, err: wantErr,
			}
			c := &Client{Conn: conn, PieceCount: 8, Bitfield: Bitfield{0x80}}
			got, err := c.DownloadBlock(0, 0, 1)
			if got != nil || !errors.Is(err, wantErr) {
				t.Errorf("got (%x, %v), want nil data and deadline error", got, err)
			}
			if failAt == 1 && (outgoing.Len() != 0 || conn.reads != 0) {
				t.Error("performed I/O after failing to establish the deadline")
			}
		})
	}
}
