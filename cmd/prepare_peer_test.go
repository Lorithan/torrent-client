package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/peer"
)

func TestPreparePeer(t *testing.T) {
	for _, scenario := range []string{"success", "invalid piece count", "malformed unchoke", "disconnect before unchoke"} {
		t.Run(scenario, func(t *testing.T) {
			local := peer.Handshake{InfoHash: [20]byte{1, 2, 3}, PeerID: [20]byte{'l'}}
			remote := peer.Handshake{InfoHash: local.InfoHash, PeerID: [20]byte{'r'}}
			listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { listener.Close() })
			if err := listener.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				conn, err := listener.AcceptTCP()
				if err != nil {
					done <- err
					return
				}
				defer conn.Close()
				if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
					done <- err
					return
				}
				var handshake [68]byte
				if _, err := io.ReadFull(conn, handshake[:]); err != nil {
					done <- err
					return
				}
				if !bytes.Equal(handshake[:], local.Serialize()) {
					done <- fmt.Errorf("incorrect outgoing handshake")
					return
				}
				if _, err := conn.Write(remote.Serialize()); err != nil {
					done <- err
					return
				}
				if scenario != "invalid piece count" {
					var interested [5]byte
					if _, err := io.ReadFull(conn, interested[:]); err != nil {
						done <- err
						return
					}
					if !bytes.Equal(interested[:], []byte{0, 0, 0, 1, 2}) {
						done <- fmt.Errorf("expected interested, got %x", interested)
						return
					}
					if scenario == "disconnect before unchoke" {
						done <- nil
						return
					}
					messages := []byte{0, 0, 0, 2, 5, 0x80, 0, 0, 0, 1, 1}
					if scenario == "malformed unchoke" {
						messages = []byte{0, 0, 0, 2, 1, 42}
					}
					if _, err := conn.Write(messages); err != nil {
						done <- err
						return
					}
				}
				if scenario == "success" {
					var next [5]byte
					if _, err := io.ReadFull(conn, next[:]); err != nil {
						done <- fmt.Errorf("returned connection was unusable: %w", err)
						return
					}
					if !bytes.Equal(next[:], []byte{0, 0, 0, 1, 3}) {
						done <- fmt.Errorf("incorrect follow-up message: %x", next)
						return
					}
				}
				var next [1]byte
				if _, err := conn.Read(next[:]); err != io.EOF {
					done <- fmt.Errorf("expected connection cleanup, got %v", err)
					return
				}
				done <- nil
			}()
			count := 1
			if scenario == "invalid piece count" {
				count = -1
			}
			client, err := preparePeer(*listener.Addr().(*net.TCPAddr), local, count)
			if client != nil {
				t.Cleanup(func() { client.Conn.Close() })
			}
			if scenario == "success" {
				if err != nil || client == nil {
					t.Fatalf("got (%v, %v), want ready client", client, err)
				}
				if client.Choked || !client.Bitfield.HasPiece(0) || client.Handshake != remote {
					t.Error("returned client is not in the expected ready state")
				}
				if err := client.SendMessage(peer.Message{ID: peer.MsgNotInterested}); err != nil {
					t.Errorf("returned connection is closed: %v", err)
				}
				client.Conn.Close()
			} else {
				if err == nil || client != nil {
					t.Fatalf("got (%v, %v), want nil client and error", client, err)
				}
				var classified *download.PeerError
				isPeerFailure := errors.As(err, &classified)
				if isPeerFailure != (scenario != "invalid piece count") {
					t.Errorf("incorrect peer classification for %s: %v", scenario, err)
				}
				if scenario == "disconnect before unchoke" && !errors.Is(err, io.EOF) {
					t.Errorf("underlying EOF was lost: %v", err)
				}
			}
			if err := <-done; err != nil {
				t.Error(err)
			}
		})
	}
}

func TestPreparePeerClassifiesHandshakeFailure(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1}}
	addr, done := startSelectionTestPeer(t, local, local, true)
	client, err := preparePeer(addr, local, 1)
	if client != nil {
		defer client.Conn.Close()
	}
	var classified *download.PeerError
	if client != nil || !errors.As(err, &classified) {
		t.Errorf("got (%v, %v), want nil client and PeerError", client, err)
	}
	if err := <-done; err != nil {
		t.Error(err)
	}
}
