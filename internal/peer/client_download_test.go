package peer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"testing/iotest"
)

func downloadTestPiece(index, begin uint32, data []byte) []byte {
	wire := binary.BigEndian.AppendUint32(nil, uint32(9+len(data)))
	wire = append(wire, 7)
	wire = binary.BigEndian.AppendUint32(wire, index)
	wire = binary.BigEndian.AppendUint32(wire, begin)
	return append(wire, data...)
}

func TestClientDownloadBlock(t *testing.T) {
	for _, name := range []string{"first piece", "later piece", "keep alive and have before block"} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("DownloadBlock panicked: %v", r)
				}
			}()
			index := uint32(9)
			if name == "first piece" {
				index = 0
			}
			data := []byte{0, 255, 128}
			wire := downloadTestPiece(index, 16384, data)
			if name == "keep alive and have before block" {
				wire = append([]byte{0, 0, 0, 0, 0, 0, 0, 5, 4, 0, 0, 0, 1}, wire...)
			}
			// A following choke must remain unread after this block completes.
			wire = append(wire, 0, 0, 0, 1, 0)
			source := bytes.NewReader(wire)
			var outgoing bytes.Buffer
			conn := &exchangeTestConn{reader: iotest.OneByteReader(source), writer: &outgoing}
			c := &Client{Conn: conn, PieceCount: 10, Bitfield: Bitfield{0x80, 0x40}}
			got, err := c.DownloadBlock(index, 16384, 3)
			if err != nil {
				t.Fatalf("DownloadBlock() error: %v", err)
			}
			if !bytes.Equal(got, data) {
				t.Errorf("data = %x, want %x", got, data)
			}
			wantRequest := []byte{0, 0, 0, 13, 6, 0, 0, 0, byte(index), 0, 0, 64, 0, 0, 0, 0, 3}
			if !bytes.Equal(outgoing.Bytes(), wantRequest) {
				t.Errorf("request = %x, want %x", outgoing.Bytes(), wantRequest)
			}
			if source.Len() != 5 || c.Choked {
				t.Error("read beyond completed block")
			}
			if name == "keep alive and have before block" && !c.Bitfield.HasPiece(1) {
				t.Error("intervening have did not update state")
			}
			if conn.closed {
				t.Error("download closed caller-owned connection")
			}
		})
	}
}

func TestClientDownloadBlockRejectsBeforeSending(t *testing.T) {
	for _, tt := range []struct {
		name          string
		choked        bool
		count         int
		bits          Bitfield
		index, length uint32
	}{
		{"choked", true, 10, Bitfield{0x80, 0x40}, 9, 3},
		{"index equals count", false, 9, Bitfield{0x80, 0xc0}, 9, 3},
		{"piece unavailable", false, 10, Bitfield{0x80, 0}, 9, 3},
		{"zero request length", false, 10, Bitfield{0x80, 0x40}, 9, 0},
		{"oversized request", false, 10, Bitfield{0x80, 0x40}, 9, 16385},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var outgoing bytes.Buffer
			conn := &exchangeTestConn{reader: bytes.NewReader(nil), writer: &outgoing}
			c := &Client{Conn: conn, Choked: tt.choked, PieceCount: tt.count, Bitfield: tt.bits}
			got, err := c.DownloadBlock(tt.index, 0, tt.length)
			if got != nil || err == nil {
				t.Errorf("got (%x, %v), want nil data and error", got, err)
			}
			if outgoing.Len() != 0 || conn.reads != 0 {
				t.Error("invalid request performed network I/O")
			}
		})
	}
}

func TestClientDownloadBlockResponseErrors(t *testing.T) {
	readErr := errors.New("read failed")
	writeErr := errors.New("write failed")
	for _, tt := range []struct {
		name                       string
		wire                       []byte
		readErr, writeErr, wantErr error
	}{
		{name: "wrong piece", wire: downloadTestPiece(8, 16384, []byte{1, 2, 3})},
		{name: "wrong offset", wire: downloadTestPiece(9, 0, []byte{1, 2, 3})},
		{name: "wrong data length", wire: downloadTestPiece(9, 16384, []byte{1, 2})},
		{name: "malformed piece", wire: []byte{0, 0, 0, 1, 7}},
		{name: "choke while waiting", wire: []byte{0, 0, 0, 1, 0}},
		{name: "malformed state message", wire: []byte{0, 0, 0, 2, 1, 42}},
		{name: "disconnect", wantErr: io.EOF},
		{name: "read failure", readErr: readErr, wantErr: readErr},
		{name: "send failure", writeErr: writeErr, wantErr: writeErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var outgoing bytes.Buffer
			conn := &exchangeTestConn{reader: bytes.NewReader(tt.wire), writer: &outgoing}
			if tt.readErr != nil {
				conn.reader = iotest.ErrReader(tt.readErr)
			}
			if tt.writeErr != nil {
				conn.writer = exchangeErrorWriter{err: tt.writeErr}
			}
			c := &Client{Conn: conn, PieceCount: 10, Bitfield: Bitfield{0x80, 0x40}}
			got, err := c.DownloadBlock(9, 16384, 3)
			if got != nil || err == nil {
				t.Errorf("got (%x, %v), want nil data and error", got, err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("error = %v, want underlying error %v", err, tt.wantErr)
			}
			if tt.name == "choke while waiting" && !c.Choked {
				t.Error("choke did not update client state")
			}
			if tt.writeErr != nil && conn.reads != 0 {
				t.Error("read after send failure")
			}
			if conn.closed {
				t.Error("download closed caller-owned connection")
			}
		})
	}
}
