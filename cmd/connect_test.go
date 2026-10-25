package main

import (
	"bytes"
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

func TestConnectToPeerRetriesAfterHandshakeFailure(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("connection failure panicked instead of trying next peer: %v", r)
		}
	}()
	local := peer.Handshake{InfoHash: [20]byte{1, 2, 3}, PeerID: [20]byte{'l'}}
	remote := peer.Handshake{InfoHash: local.InfoHash, PeerID: [20]byte{'r'}}
	bad, badDone := startSelectionTestPeer(t, local, remote, true)
	good, goodDone := startSelectionTestPeer(t, local, remote, false)
	conn, got, err := connectToPeer([]net.TCPAddr{bad, good}, local)
	if conn != nil {
		t.Cleanup(func() { conn.Close() })
	}
	if err != nil || conn == nil || got == nil || *got != remote {
		t.Fatalf("got (%v, %+v, %v), want successful second peer", conn, got, err)
	}
	if conn.RemoteAddr().String() != good.String() {
		t.Errorf("connected to %s, want %s", conn.RemoteAddr(), good.String())
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-badDone; err != nil {
		t.Errorf("failed peer: %v", err)
	}
	if err := <-goodDone; err != nil {
		t.Errorf("successful peer: %v", err)
	}
}

func TestConnectToPeerAllFail(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("connection failures panicked instead of returning an error: %v", r)
		}
	}()
	local := peer.Handshake{InfoHash: [20]byte{1}}
	first, firstDone := startSelectionTestPeer(t, local, local, true)
	second, secondDone := startSelectionTestPeer(t, local, local, true)
	conn, got, err := connectToPeer([]net.TCPAddr{first, second}, local)
	if conn != nil {
		defer conn.Close()
	}
	if conn != nil || got != nil || err == nil {
		t.Fatalf("got (%v, %+v, %v), want nil connection, nil handshake, error", conn, got, err)
	}
	if err := <-firstDone; err != nil {
		t.Errorf("first peer: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Errorf("second peer: %v", err)
	}
}

func TestConnectToPeerEmpty(t *testing.T) {
	conn, got, err := connectToPeer(nil, peer.Handshake{})
	if conn != nil || got != nil || err == nil {
		t.Errorf("got (%v, %+v, %v), want nil connection, nil handshake, error", conn, got, err)
	}
}
