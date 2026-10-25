package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
)

func startSelectionTestPeer(t *testing.T, local, remote peer.Handshake, reject bool) (net.TCPAddr, <-chan error) {
	t.Helper()
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
		var wire [68]byte
		if _, err := io.ReadFull(conn, wire[:]); err != nil {
			done <- err
			return
		}
		if !bytes.Equal(wire[:], local.Serialize()) {
			done <- fmt.Errorf("incorrect outgoing handshake")
			return
		}
		reply := remote.Serialize()
		if reject {
			reply = []byte{0}
		}
		if _, err := conn.Write(reply); err != nil {
			done <- err
			return
		}
		var next [1]byte
		if _, err := conn.Read(next[:]); err != io.EOF {
			done <- fmt.Errorf("expected client cleanup, got %v", err)
			return
		}
		done <- nil
	}()
	return *listener.Addr().(*net.TCPAddr), done
}

func startFallbackTestPeer(t *testing.T, local peer.Handshake, serve func(net.Conn) error) (net.TCPAddr, <-chan error) {
	t.Helper()
	return startFallbackTestPeerWithVerifier(t, local, serve, nil)
}

func startFallbackTestPeerWithVerifier(t *testing.T, local peer.Handshake, serve func(net.Conn) error, verify func([68]byte) error) (net.TCPAddr, <-chan error) {
	t.Helper()
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
		listener.Close() // One attempt per fixture; unintended retries fail promptly.
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
		if verify != nil {
			if err := verify(handshake); err != nil {
				done <- err
				return
			}
		} else if !bytes.Equal(handshake[:], local.Serialize()) {
			done <- fmt.Errorf("incorrect local handshake")
			return
		}
		remote := peer.Handshake{InfoHash: local.InfoHash, PeerID: [20]byte{'r'}}
		if _, err := conn.Write(remote.Serialize()); err != nil {
			done <- err
			return
		}
		var interested [5]byte
		if _, err := io.ReadFull(conn, interested[:]); err != nil {
			done <- err
			return
		}
		if !bytes.Equal(interested[:], []byte{0, 0, 0, 1, 2}) {
			done <- fmt.Errorf("expected interested, got %x", interested)
			return
		}
		if _, err := conn.Write([]byte{0, 0, 0, 2, 5, 0xc0, 0, 0, 0, 1, 1}); err != nil {
			done <- err
			return
		}
		done <- serve(conn)
	}()
	return *listener.Addr().(*net.TCPAddr), done
}

func fallbackTestRequest(conn net.Conn, index uint32) error {
	var request [17]byte
	if _, err := io.ReadFull(conn, request[:]); err != nil {
		return err
	}
	want := []byte{0, 0, 0, 13, 6, 0, 0, 0, byte(index), 0, 0, 0, 0, 0, 0, 0, 4}
	if !bytes.Equal(request[:], want) {
		return fmt.Errorf("request = %x, want %x", request, want)
	}
	return nil
}

func fallbackTestReply(conn net.Conn, index uint32, data []byte) error {
	wire := binary.BigEndian.AppendUint32(nil, 13)
	wire = append(wire, byte(peer.MsgPiece))
	wire = binary.BigEndian.AppendUint32(wire, index)
	wire = binary.BigEndian.AppendUint32(wire, 0)
	_, err := conn.Write(append(wire, data...))
	return err
}

func fallbackTestHave(conn net.Conn, index uint32) error {
	var have [9]byte
	if _, err := io.ReadFull(conn, have[:]); err != nil {
		return err
	}
	if !bytes.Equal(have[:], []byte{0, 0, 0, 5, 4, 0, 0, 0, byte(index)}) {
		return fmt.Errorf("incorrect have: %x", have)
	}
	return nil
}

type fallbackTestWriter struct {
	data  []byte
	calls int
	err   error
}

func (w *fallbackTestWriter) WriteAt(p []byte, offset int64) (int, error) {
	w.calls++
	if w.err != nil {
		return 0, w.err
	}
	return copy(w.data[int(offset):], p), nil
}

func fallbackTestExpectClosed(conn net.Conn) error {
	var next [1]byte
	_, err := conn.Read(next[:])
	if err != io.EOF {
		return fmt.Errorf("expected peer cleanup without another request or have, got byte %x and error %v", next, err)
	}
	return nil
}
