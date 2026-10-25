package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/torrent"
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

func TestDownloadFromPeersRetriesAfterHandshakeFailure(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1}}
	bad, badDone := startSelectionTestPeer(t, local, local, true)
	good, goodDone := startFallbackTestPeer(t, local, func(conn net.Conn) error {
		for _, index := range []uint32{0, 1} {
			if err := fallbackTestRequest(conn, index); err != nil {
				return err
			}
			if err := fallbackTestReply(conn, index, []byte("data")); err != nil {
				return err
			}
			if err := fallbackTestHave(conn, index); err != nil {
				return err
			}
		}
		var next [1]byte
		if _, err := conn.Read(next[:]); err != io.EOF {
			return fmt.Errorf("expected cleanup, got %v", err)
		}
		return nil
	})
	hash := sha1.Sum([]byte("data"))
	tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: [][20]byte{hash, hash}}
	dst := &fallbackTestWriter{data: make([]byte, 8)}
	session, err := download.NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := downloadFromPeers([]net.TCPAddr{bad, good}, local, 2, session); err != nil {
		t.Fatal(err)
	}
	if !session.Complete() || !bytes.Equal(dst.data, []byte("datadata")) {
		t.Error("second peer did not complete output")
	}
	if err := <-badDone; err != nil {
		t.Error(err)
	}
	if err := <-goodDone; err != nil {
		t.Error(err)
	}
}

func TestDownloadFromPeersAllPreparationsFail(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1}}
	first, firstDone := startSelectionTestPeer(t, local, local, true)
	second, secondDone := startSelectionTestPeer(t, local, local, true)
	tf := &torrent.TorrentFile{Length: 4, PieceLength: 4, Pieces: make([][20]byte, 1)}
	dst := &fallbackTestWriter{data: make([]byte, 4)}
	session, err := download.NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := downloadFromPeers([]net.TCPAddr{first, second}, local, 1, session); err == nil {
		t.Error("expected peer exhaustion error")
	}
	if session.Complete() || dst.calls != 0 {
		t.Error("failed preparation changed output or progress")
	}
	if err := <-firstDone; err != nil {
		t.Error(err)
	}
	if err := <-secondDone; err != nil {
		t.Error(err)
	}
}

func TestDownloadFromPeersNoAddressesWhileIncomplete(t *testing.T) {
	tf := &torrent.TorrentFile{Length: 4, PieceLength: 4, Pieces: make([][20]byte, 1)}
	session, err := download.NewSession(tf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := downloadFromPeers(nil, peer.Handshake{}, 1, session); err == nil {
		t.Error("expected peer exhaustion error")
	}
}
