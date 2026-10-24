package peer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

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
