package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

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

func TestRunContinuesWithSecondPeer(t *testing.T) {
	firstHash, secondHash := sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))
	info := "d6:lengthi8e4:name8:file.bin12:piece lengthi4e6:pieces40:" + string(firstHash[:]) + string(secondHash[:]) + "e"
	infoHash := sha1.Sum([]byte(info))
	local := peer.Handshake{InfoHash: infoHash}
	// The CLI creates a random session ID. Hand the announced value to each fake
	// peer through a channel to check identity reuse without shared-memory races.
	announcedIDs := make(chan string, 2)
	verify := func(wire [68]byte) error {
		var id string
		select {
		case id = <-announcedIDs:
		default:
			return fmt.Errorf("peer connected without an announce")
		}
		if wire[0] != 19 || string(wire[1:20]) != "BitTorrent protocol" || !bytes.Equal(wire[28:48], infoHash[:]) || string(wire[48:]) != id {
			return fmt.Errorf("handshake does not match torrent and announced session identity")
		}
		return nil
	}
	first, firstDone := startFallbackTestPeerWithVerifier(t, local, func(conn net.Conn) error {
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
		return fallbackTestReply(conn, 1, []byte("ef"))
	}, verify)
	second, secondDone := startFallbackTestPeerWithVerifier(t, local, func(conn net.Conn) error {
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
			return fmt.Errorf("expected CLI cleanup, got %v", err)
		}
		return nil
	}, verify)
	var compact []byte
	for _, addr := range []net.TCPAddr{first, second} {
		compact = append(compact, 127, 0, 0, 1)
		compact = binary.BigEndian.AppendUint16(compact, uint16(addr.Port))
	}
	tracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("peer_id")
		announcedIDs <- id
		announcedIDs <- id
		_, _ = io.WriteString(w, "d8:intervali1800e5:peers12:"+string(compact)+"e")
	}))
	t.Cleanup(tracker.Close)
	dir := t.TempDir()
	torrentPath, outputPath := filepath.Join(dir, "input.torrent"), filepath.Join(dir, "output.bin")
	meta := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(tracker.URL), tracker.URL, info)
	if err := os.WriteFile(torrentPath, meta, 0600); err != nil {
		t.Fatal(err)
	}
	prepareRun(t, "-torrent", torrentPath, "-output", outputPath)
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if err := <-firstDone; err != nil {
		t.Errorf("first peer: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Errorf("second peer: %v", err)
	}
	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("abcdefgh")) {
		t.Errorf("saved %q, want abcdefgh", got)
	}
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
