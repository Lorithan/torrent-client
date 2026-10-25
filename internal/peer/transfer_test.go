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

func TestConnect(t *testing.T) {
	local, localWire := handshakeReadFixture()
	remote := local
	remote.PeerID = [20]byte{'r', 'e', 'm', 'o', 't', 'e'}
	for _, name := range []string{"success", "wrong info hash", "truncated handshake", "invalid protocol"} {
		t.Run(name, func(t *testing.T) {
			listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { listener.Close() })
			if err := listener.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			wire := remote.Serialize()
			switch name {
			case "wrong info hash":
				wire[28] ^= 0xff
			case "truncated handshake":
				wire = wire[:40]
			case "invalid protocol":
				wire[1] = 'X'
			}
			done := make(chan error, 1)
			go func() {
				conn, err := listener.AcceptTCP()
				if err != nil {
					done <- err
					return
				}
				defer conn.Close()
				if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
					done <- err
					return
				}
				var incoming [68]byte
				if _, err := io.ReadFull(conn, incoming[:]); err != nil {
					done <- err
					return
				}
				if !bytes.Equal(incoming[:], localWire) {
					done <- fmt.Errorf("incorrect outgoing handshake: %x", incoming)
					return
				}
				if _, err := conn.Write(wire); err != nil {
					done <- err
					return
				}
				if name == "truncated handshake" {
					if err := conn.CloseWrite(); err != nil {
						done <- err
						return
					}
				}
				var next [1]byte
				_, err = conn.Read(next[:])
				if name == "success" {
					if err != nil || next[0] != 42 {
						done <- fmt.Errorf("returned connection unusable: byte %d, error %v", next[0], err)
						return
					}
				} else if !errors.Is(err, io.EOF) {
					done <- fmt.Errorf("client did not close rejected connection: %v", err)
					return
				}
				done <- nil
			}()

			conn, got, err := Connect(*listener.Addr().(*net.TCPAddr), local)
			if conn != nil {
				defer conn.Close()
			}
			if name == "success" {
				if err != nil || conn == nil || got == nil || *got != remote {
					t.Fatalf("Connect() = (%v, %+v, %v), want open connection and remote handshake", conn, got, err)
				}
				if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				if _, err := conn.Write([]byte{42}); err != nil {
					t.Fatalf("write after Connect: %v", err)
				}
			} else if err == nil || conn != nil || got != nil {
				t.Errorf("Connect() = (%v, %+v, %v), want nil connection, nil handshake, and error", conn, got, err)
			}
			if err := <-done; err != nil {
				t.Error(err)
			}
		})
	}
}
