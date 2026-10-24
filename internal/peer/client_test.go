package peer

import (
	"bytes"
	"fmt"
	"net"
	"testing"
)

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
