package peer

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

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

type sendPartialErrorWriter struct {
	bytes.Buffer
	err error
}

func (w *sendPartialErrorWriter) Write(p []byte) (int, error) {
	n, _ := w.Buffer.Write(p[:2])
	return n, w.err
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
