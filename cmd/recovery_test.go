package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

func fallbackTestExpectClosed(conn net.Conn) error {
	var next [1]byte
	_, err := conn.Read(next[:])
	if err != io.EOF {
		return fmt.Errorf("expected peer cleanup without another request or have, got byte %x and error %v", next, err)
	}
	return nil
}

func TestDownloadFromPeersRetriesCorruptPiece(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1}}
	bad, badDone := startFallbackTestPeer(t, local, func(conn net.Conn) error {
		if err := fallbackTestRequest(conn, 0); err != nil {
			return err
		}
		if err := fallbackTestReply(conn, 0, []byte("Abcd")); err != nil {
			return err
		}
		// No have may be sent for corrupt bytes, and rejection must close the peer.
		return fallbackTestExpectClosed(conn)
	})
	good, goodDone := startFallbackTestPeer(t, local, func(conn net.Conn) error {
		for index, data := range []string{"abcd", "efgh"} {
			if err := fallbackTestRequest(conn, uint32(index)); err != nil {
				return err
			}
			if err := fallbackTestReply(conn, uint32(index), []byte(data)); err != nil {
				return err
			}
			if err := fallbackTestHave(conn, uint32(index)); err != nil {
				return err
			}
		}
		return fallbackTestExpectClosed(conn)
	})
	tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: [][20]byte{sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))}}
	dst := &fallbackTestWriter{data: make([]byte, 8)}
	session, err := download.NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := downloadFromPeers([]net.TCPAddr{bad, good}, local, 2, session); err != nil {
		t.Fatal(err)
	}
	if !session.Complete() || dst.calls != 2 || !bytes.Equal(dst.data, []byte("abcdefgh")) {
		t.Errorf("corrupt piece was committed or recovery failed: complete %v, writes %d, output %q", session.Complete(), dst.calls, dst.data)
	}
	if err := <-badDone; err != nil {
		t.Errorf("bad peer: %v", err)
	}
	if err := <-goodDone; err != nil {
		t.Errorf("replacement peer: %v", err)
	}
}

func TestDownloadFromPeersExhaustsTransferFailuresAndClosesConnections(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1}}
	corrupt, corruptDone := startFallbackTestPeer(t, local, func(conn net.Conn) error {
		if err := fallbackTestRequest(conn, 0); err != nil {
			return err
		}
		if err := fallbackTestReply(conn, 0, []byte("bad!")); err != nil {
			return err
		}
		return fallbackTestExpectClosed(conn)
	})
	truncated, truncatedDone := startFallbackTestPeer(t, local, func(conn net.Conn) error {
		if err := fallbackTestRequest(conn, 0); err != nil {
			return err
		}
		if err := fallbackTestReply(conn, 0, []byte("ab")); err != nil {
			return err
		}
		// End the response stream but keep the read side open to observe cleanup.
		if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
			return err
		}
		return fallbackTestExpectClosed(conn)
	})
	tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: [][20]byte{sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))}}
	dst := &fallbackTestWriter{data: make([]byte, 8)}
	session, err := download.NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	err = downloadFromPeers([]net.TCPAddr{corrupt, truncated}, local, 2, session)
	if err == nil || !strings.Contains(err.Error(), "exhausted peers") {
		t.Errorf("error = %v, want exhaustion", err)
	}
	if session.Complete() || dst.calls != 0 {
		t.Error("failed transfers changed committed progress or output")
	}
	if err := <-corruptDone; err != nil {
		t.Errorf("corrupt peer: %v", err)
	}
	if err := <-truncatedDone; err != nil {
		t.Errorf("truncated peer: %v", err)
	}
}
