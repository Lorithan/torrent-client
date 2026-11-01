package tui

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
)

var manualSwarmDir = flag.String("manual-swarm-dir", "", "opt-in local swarm fixture directory; see docs/VERIFYING.md")

// TestManualSwarm is an interactive test fixture, skipped in ordinary test runs.
// It serves synthetic content only and keeps personal torrents out of the repo.
func TestManualSwarm(t *testing.T) {
	if *manualSwarmDir == "" {
		t.Skip("interactive fixture; see docs/VERIFYING.md")
	}
	dir, err := filepath.Abs(*manualSwarmDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	const pieceLength = 16 * 1024
	payload := make([]byte, 2*pieceLength+733)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	var hashes []byte
	for begin := 0; begin < len(payload); begin += pieceLength {
		hash := sha1.Sum(payload[begin:min(begin+pieceLength, len(payload))])
		hashes = append(hashes, hash[:]...)
	}
	name := "local-demo.bin"
	info := fmt.Appendf(nil, "d6:lengthi%de4:name%d:%s12:piece lengthi%de6:pieces%d:", len(payload), len(name), name, pieceLength, len(hashes))
	info = append(append(info, hashes...), 'e')
	infoHash := sha1.Sum(info)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	var workers sync.WaitGroup
	var listeners []net.Listener
	var tracker *httptest.Server
	t.Cleanup(func() {
		stop()
		for _, listener := range listeners {
			listener.Close()
		}
		if tracker != nil {
			tracker.Close()
		}
		workers.Wait()
	})
	var compact []byte
	for range 2 {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		compact = binary.BigEndian.AppendUint16(append(compact, 127, 0, 0, 1), uint16(listener.Addr().(*net.TCPAddr).Port))
		workers.Go(func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				workers.Go(func() {
					if err := serveManualSeed(ctx, conn, infoHash, payload, pieceLength); err != nil && ctx.Err() == nil {
						t.Errorf("fixture peer: %v", err)
					}
				})
			}
		})
	}
	tracker = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("info_hash") != string(infoHash[:]) {
			http.Error(w, "unexpected torrent", http.StatusBadRequest)
			return
		}
		_, _ = fmt.Fprintf(w, "d8:intervali30e5:peers%d:%se", len(compact), compact)
		fmt.Printf("announce event=%q downloaded=%s uploaded=%s left=%s port=%s\n", q.Get("event"), q.Get("downloaded"), q.Get("uploaded"), q.Get("left"), q.Get("port"))
		if q.Get("event") == "completed" {
			workers.Go(func() {
				if err := probeManualSeed(ctx, q.Get("port"), q.Get("peer_id"), infoHash, payload, pieceLength); err != nil {
					if ctx.Err() == nil {
						t.Errorf("seed upload verification: %v", err)
					}
					return
				}
				fmt.Println("UPLOAD VERIFIED: final five bytes of the short final piece match the source; stop the client before stopping this fixture.")
			})
		}
	}))
	meta := fmt.Appendf(nil, "d8:announce%d:%s4:info", len(tracker.URL), tracker.URL)
	meta = append(append(meta, info...), 'e')
	for _, file := range []struct {
		name string
		data []byte
	}{{"local.torrent", meta}, {"expected.bin", payload}} {
		f, err := os.OpenFile(filepath.Join(dir, file.name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatalf("use a fresh fixture directory: %v", err)
		}
		_, writeErr := f.Write(file.data)
		closeErr := f.Close()
		if writeErr != nil {
			t.Fatal(writeErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	fmt.Printf("LOCAL SWARM READY\nTorrent: %s\nExpected: %s\nBytes: %d (two full pieces and a 733-byte final piece)\nSHA256: %x\n", filepath.Join(dir, "local.torrent"), filepath.Join(dir, "expected.bin"), len(payload), sha256.Sum256(payload))
	fmt.Println("Start the CLI or TUI in another terminal with default seeding enabled. Wait for UPLOAD VERIFIED, then quit the client. Ctrl+C here stops the fixture.")
	<-ctx.Done()
}

func serveManualSeed(ctx context.Context, conn net.Conn, hash [20]byte, payload []byte, pieceLength int) error {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	if _, err := peer.AcceptHandshake(conn, peer.Handshake{InfoHash: hash, PeerID: [20]byte{'f', 'i', 'x', 't', 'u', 'r', 'e'}}); err != nil {
		return err
	}
	for _, msg := range []peer.Message{{ID: peer.MsgBitfield, Payload: []byte{0xe0}}, {ID: peer.MsgUnchoke}} {
		if _, err := conn.Write(msg.Serialize()); err != nil {
			return err
		}
	}
	for {
		if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return err
		}
		msg, err := peer.ReadMessage(conn)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if msg == nil || msg.ID != peer.MsgRequest {
			continue
		}
		request, err := msg.BlockRequest()
		if err != nil {
			return err
		}
		index, begin, length := int64(request.Index), int64(request.Begin), int64(request.Length)
		pieceStart := index * int64(pieceLength)
		if pieceStart >= int64(len(payload)) || begin+length > min(int64(pieceLength), int64(len(payload))-pieceStart) {
			return fmt.Errorf("request outside fixture: %+v", request)
		}
		delay := time.NewTimer(750 * time.Millisecond)
		select {
		case <-delay.C:
		case <-ctx.Done():
			delay.Stop()
			return nil
		}
		data := payload[pieceStart+begin : pieceStart+begin+length]
		piece, err := peer.NewPiece(request.Index, request.Begin, data)
		if err != nil {
			return err
		}
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		if _, err := conn.Write(piece.Serialize()); err != nil {
			return err
		}
	}
}

func probeManualSeed(ctx context.Context, port, peerID string, hash [20]byte, payload []byte, pieceLength int) error {
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return fmt.Errorf("invalid client listening port %q", port)
	}
	dialer := net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp4", net.JoinHostPort("127.0.0.1", port))
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	remote, err := peer.ExchangeHandshake(conn, peer.Handshake{InfoHash: hash, PeerID: [20]byte{'p', 'r', 'o', 'b', 'e'}})
	if err != nil {
		return err
	}
	if string(remote.PeerID[:]) != peerID {
		return fmt.Errorf("incoming peer identity differs from tracker announce")
	}
	msg, err := peer.ReadMessage(conn)
	if err != nil || msg == nil || msg.ID != peer.MsgBitfield || !bytes.Equal(msg.Payload, []byte{0xe0}) {
		return fmt.Errorf("completed availability=%v, %v", msg, err)
	}
	if _, err := conn.Write((peer.Message{ID: peer.MsgInterested}).Serialize()); err != nil {
		return err
	}
	msg, err = peer.ReadMessage(conn)
	if err != nil || msg == nil || msg.ID != peer.MsgUnchoke {
		return fmt.Errorf("expected unchoke: %v, %v", msg, err)
	}
	lastIndex := (len(payload) - 1) / pieceLength
	begin := len(payload) - lastIndex*pieceLength - 5
	request, err := peer.NewRequest(uint32(lastIndex), uint32(begin), 5)
	if err != nil {
		return err
	}
	if _, err := conn.Write(request.Serialize()); err != nil {
		return err
	}
	msg, err = peer.ReadMessage(conn)
	if err != nil || msg == nil {
		return fmt.Errorf("upload response: %v", err)
	}
	block, err := msg.PieceBlock()
	if err != nil || block.Index != uint32(lastIndex) || block.Begin != uint32(begin) || !bytes.Equal(block.Data, payload[len(payload)-5:]) {
		return fmt.Errorf("incorrect upload data: %+v, %v", block, err)
	}
	return nil
}
