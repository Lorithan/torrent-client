package main

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

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

func TestDownloadFromPeersContinuesAfterInterruptedPiece(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1}}
	first, firstDone := startFallbackTestPeer(t, local, func(conn net.Conn) error {
		if err := fallbackTestRequest(conn, 0); err != nil {
			return err
		}
		if err := fallbackTestReply(conn, 0, []byte("abcd")); err != nil {
			return err
		}
		if err := fallbackTestHave(conn, 0); err != nil {
			return err
		}
		if err := fallbackTestRequest(conn, 1); err != nil {
			return err
		}
		// Advertise four data bytes but disconnect after sending only two.
		return fallbackTestReply(conn, 1, []byte("ef"))
	})
	second, secondDone := startFallbackTestPeer(t, local, func(conn net.Conn) error {
		if err := fallbackTestRequest(conn, 1); err != nil {
			return err
		}
		if err := fallbackTestReply(conn, 1, []byte("efgh")); err != nil {
			return err
		}
		if err := fallbackTestHave(conn, 1); err != nil {
			return err
		}
		var next [1]byte
		if _, err := conn.Read(next[:]); err != io.EOF {
			return fmt.Errorf("expected coordinator cleanup, got %v", err)
		}
		return nil
	})
	tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: [][20]byte{sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))}}
	dst := &fallbackTestWriter{data: make([]byte, 8)}
	s, err := download.NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := downloadFromPeers([]net.TCPAddr{first, second}, local, 2, s); err != nil {
		t.Fatal(err)
	}
	if !s.Complete() || dst.calls != 2 || !bytes.Equal(dst.data, []byte("abcdefgh")) {
		t.Errorf("incorrect completion: complete %v, writes %d, data %q", s.Complete(), dst.calls, dst.data)
	}
	if err := <-firstDone; err != nil {
		t.Errorf("first peer: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Errorf("second peer: %v", err)
	}
}

func TestDownloadFromPeersCompletedSession(t *testing.T) {
	s, err := download.NewSession(&torrent.TorrentFile{PieceLength: 4}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := downloadFromPeers(nil, peer.Handshake{}, 0, s); err != nil {
		t.Errorf("already completed session should succeed without peers: %v", err)
	}
}

func TestDownloadFromPeersStopsOnStorageError(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1}}
	addr, done := startFallbackTestPeer(t, local, func(conn net.Conn) error {
		if err := fallbackTestRequest(conn, 0); err != nil {
			return err
		}
		if err := fallbackTestReply(conn, 0, []byte("abcd")); err != nil {
			return err
		}
		var next [1]byte
		if _, err := conn.Read(next[:]); err != io.EOF {
			return fmt.Errorf("expected cleanup without have, got %v", err)
		}
		return nil
	})
	wantErr := errors.New("disk full")
	dst := &fallbackTestWriter{err: wantErr}
	tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: [][20]byte{sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))}}
	s, err := download.NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	// Observe the spare listener directly: checking only the returned error could
	// miss an unnecessary attempt that happens before returning the storage cause.
	spare, listenErr := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if listenErr != nil {
		t.Fatal(listenErr)
	}
	t.Cleanup(func() { spare.Close() })
	attempted := make(chan bool, 1)
	go func() {
		conn, err := spare.AcceptTCP()
		if err != nil {
			attempted <- false
			return
		}
		conn.Close()
		attempted <- true
	}()
	err = downloadFromPeers([]net.TCPAddr{addr, *spare.Addr().(*net.TCPAddr)}, local, 2, s)
	spare.Close()
	if <-attempted {
		t.Error("storage failure attempted a replacement peer")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want storage cause", err)
	}
	if s.Complete() || dst.calls != 1 {
		t.Error("storage failure changed progress or repeated output work")
	}
	if err := <-done; err != nil {
		t.Error(err)
	}
}

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
