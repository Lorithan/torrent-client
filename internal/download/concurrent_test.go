package download

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

func TestSessionDownloadFromPeersStartsWorkersAndClosesConnections(t *testing.T) {
	f := newWorkerTestFixture(t, -1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := f.session.DownloadFromPeers(ctx, []*peer.Client{f.client}, 1)
	if err != nil {
		t.Fatalf("valid peer batch failed: %v", err)
	}
	if !f.session.Complete() || f.session.Stats().Left != 0 || f.session.Stats().Downloaded != 8 || len(f.writer.writes) != 2 {
		t.Fatal("peer batch did not finish verified output")
	}
	if !bytes.Equal(f.writer.writes[0].data, f.data[:4]) || !bytes.Equal(f.writer.writes[1].data, f.data[4:]) {
		t.Error("stored output differs from verified pieces")
	}
	select {
	case <-f.conn.closed:
	default:
		t.Error("batch returned without closing its worker connection")
	}
	if ctx.Err() != nil {
		t.Error("batch cleanup canceled or outlasted its parent context")
	}
}

func TestSessionDownloadFromPeersRejectsInvalidBatchAndClosesConnections(t *testing.T) {
	for _, scenario := range []string{"zero limit", "negative limit", "too many clients", "nil client", "nil connection", "wrong piece count", "duplicate client"} {
		t.Run(scenario, func(t *testing.T) {
			first, second := newWorkerTestFixture(t, -1), newWorkerTestFixture(t, -1)
			t.Cleanup(func() { first.conn.Close(); second.conn.Close() })
			// Report invalid-input cleanup panics as failures while allowing other
			// validation cases to run.
			defer func() {
				if value := recover(); value != nil {
					t.Fatalf("invalid batch caused cleanup panic: %v", value)
				}
			}()
			clients := []*peer.Client{first.client}
			limit := 3
			switch scenario {
			case "zero limit":
				limit = 0
			case "negative limit":
				limit = -1
			case "too many clients":
				clients, limit = []*peer.Client{first.client, second.client}, 1
			case "nil client":
				clients = []*peer.Client{first.client, nil, second.client}
			case "nil connection":
				clients = []*peer.Client{first.client, {PieceCount: 2}, second.client}
			case "wrong piece count":
				first.client.PieceCount = 1
			case "duplicate client":
				clients = []*peer.Client{first.client, first.client}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := first.session.DownloadFromPeers(ctx, clients, limit)
			var peerErr *PeerError
			if err == nil || errors.As(err, &peerErr) {
				t.Errorf("invalid batch error = %v, want fatal validation error", err)
			}
			if first.conn.outgoing.Len() != 0 || second.conn.outgoing.Len() != 0 || first.session.Stats().Downloaded != 0 || first.session.Stats().Left != 8 {
				t.Error("invalid batch started network work or changed session progress")
			}
			for _, client := range clients {
				if client == nil || client.Conn == nil {
					continue
				}
				conn := first.conn
				if client == second.client {
					conn = second.conn
				}
				select {
				case <-conn.closed:
				default:
					t.Error("invalid batch retained a supplied connection")
				}
			}
		})
	}
}

func TestSessionDownloadFromPeersAlreadyCanceled(t *testing.T) {
	f := newWorkerTestFixture(t, -1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := f.session.DownloadFromPeers(ctx, []*peer.Client{f.client}, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if f.conn.outgoing.Len() != 0 {
		t.Error("already canceled batch started network work")
	}
	select {
	case <-f.conn.closed:
	default:
		t.Error("canceled batch retained its connection")
	}
}

type concurrentPiecePeerTest struct {
	client *peer.Client
	done   chan struct{}
	err    error
}

func startConcurrentPiecePeerTest(t *testing.T, index uint32, data []byte, requests chan<- uint32, release <-chan struct{}) *concurrentPiecePeerTest {
	t.Helper()
	local, remote := net.Pipe()
	client, err := peer.NewClient(local, peer.Handshake{}, 2)
	if err != nil {
		local.Close()
		remote.Close()
		t.Fatal(err)
	}
	client.Choked = false
	if err := client.Bitfield.SetPiece(int(index)); err != nil {
		t.Fatal(err)
	}
	f := &concurrentPiecePeerTest{client: client, done: make(chan struct{})}
	go func() {
		defer close(f.done)
		defer remote.Close()
		f.err = func() error {
			if err := remote.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				return err
			}
			request, err := peer.ReadMessage(remote)
			if err != nil {
				return err
			}
			if request == nil || request.ID != peer.MsgRequest || len(request.Payload) != 12 || binary.BigEndian.Uint32(request.Payload[:4]) != index || binary.BigEndian.Uint32(request.Payload[4:8]) != 0 || binary.BigEndian.Uint32(request.Payload[8:]) != uint32(len(data)) {
				return fmt.Errorf("peer %d received unexpected request: %+v", index, request)
			}
			requests <- index
			<-release // Both peers must receive requests before either can respond.
			payload := binary.BigEndian.AppendUint32(nil, index)
			payload = binary.BigEndian.AppendUint32(payload, 0)
			payload = append(payload, data...)
			if _, err := remote.Write((peer.Message{ID: peer.MsgPiece, Payload: payload}).Serialize()); err != nil {
				return err
			}
			have, err := peer.ReadMessage(remote)
			if err != nil {
				return err
			}
			if have == nil || have.ID != peer.MsgHave || len(have.Payload) != 4 || binary.BigEndian.Uint32(have.Payload) != index {
				return fmt.Errorf("peer %d received unexpected announcement: %+v", index, have)
			}
			var extra [1]byte
			_, err = remote.Read(extra[:])
			if err != io.EOF {
				return fmt.Errorf("peer %d expected connection cleanup, got %v", index, err)
			}
			return nil
		}()
	}()
	t.Cleanup(func() {
		local.Close()
		remote.Close()
		select {
		case <-f.done:
		case <-time.After(time.Second):
			t.Error("fake complementary peer did not finish")
		}
	})
	return f
}

func TestSessionDownloadFromPeersOverlapsComplementaryTransfers(t *testing.T) {
	pieces := [][]byte{[]byte("abcd"), []byte("efgh")}
	tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: [][20]byte{sha1.Sum(pieces[0]), sha1.Sum(pieces[1])}}
	dst := &downloadTestWriter{}
	s, err := NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	requests := make(chan uint32, 2)
	first := startConcurrentPiecePeerTest(t, 0, pieces[0], requests, release)
	second := startConcurrentPiecePeerTest(t, 1, pieces[1], requests, release)
	runDone := make(chan struct{})
	var runErr error
	go func() {
		defer close(runDone)
		runErr = s.DownloadFromPeers(ctx, []*peer.Client{first.client, second.client}, 2)
	}()
	t.Cleanup(func() {
		cancel()
		unblock()
		select {
		case <-runDone:
		case <-time.After(time.Second):
			t.Error("parallel download did not join its workers")
		}
	})
	seen := make(map[uint32]bool)
	for len(seen) < 2 {
		select {
		case index := <-requests:
			if seen[index] {
				t.Fatal("duplicate assignment reached the same peer")
			}
			seen[index] = true
		case <-runDone:
			t.Fatalf("download returned before both peers received work: %v", runErr)
		case <-ctx.Done():
			t.Fatal("requests did not overlap: both peers must be active before responses are released")
		}
	}
	if stats := s.Stats(); stats.Downloaded != 0 || stats.Left != 8 {
		t.Errorf("stats before either response = %+v, want downloaded 0 and left 8", stats)
	}
	unblock()
	select {
	case <-runDone:
	case <-ctx.Done():
		t.Fatal("parallel download did not finish after peers responded")
	}
	if runErr != nil {
		t.Fatal(runErr)
	}
	for _, f := range []*concurrentPiecePeerTest{first, second} {
		select {
		case <-f.done:
			if f.err != nil {
				t.Error(f.err)
			}
		case <-time.After(time.Second):
			t.Error("peer did not observe connection closure before completion")
		}
	}
	if !s.Complete() || s.completedCount != 2 || s.Stats().Downloaded != 8 || s.Stats().Left != 0 || len(dst.writes) != 2 {
		t.Fatal("parallel transfer did not count each piece exactly once")
	}
	output := make([]byte, 8)
	for _, write := range dst.writes {
		copy(output[int(write.offset):], write.data)
	}
	if !bytes.Equal(output, []byte("abcdefgh")) {
		t.Errorf("output = %q, want abcdefgh", output)
	}
	if ctx.Err() != nil {
		t.Error("successful batch cleanup canceled its parent context")
	}
}

func startServingConcurrentPeerTest(t *testing.T, pieceCount int, available peer.Bitfield, serve func(net.Conn) error) *concurrentPiecePeerTest {
	t.Helper()
	local, remote := net.Pipe()
	client, err := peer.NewClient(local, peer.Handshake{}, pieceCount)
	if err != nil {
		local.Close()
		remote.Close()
		t.Fatal(err)
	}
	if err := available.Validate(pieceCount); err != nil {
		local.Close()
		remote.Close()
		t.Fatal(err)
	}
	copy(client.Bitfield, available)
	client.Choked = false
	f := &concurrentPiecePeerTest{client: client, done: make(chan struct{})}
	go func() {
		defer close(f.done)
		defer remote.Close()
		f.err = func() error {
			if err := remote.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				return err
			}
			return serve(remote)
		}()
	}()
	t.Cleanup(func() {
		local.Close()
		remote.Close()
		select {
		case <-f.done:
		case <-time.After(time.Second):
			t.Error("fake recovery peer did not finish")
		}
	})
	return f
}

func waitConcurrentSignalTest(ctx context.Context, signal <-chan struct{}) error {
	select {
	case <-signal:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func expectConcurrentRequestTest(conn net.Conn, index uint32) error {
	m, err := peer.ReadMessage(conn)
	if err != nil {
		return err
	}
	if m == nil || m.ID != peer.MsgRequest || len(m.Payload) != 12 || binary.BigEndian.Uint32(m.Payload[:4]) != index || binary.BigEndian.Uint32(m.Payload[4:8]) != 0 || binary.BigEndian.Uint32(m.Payload[8:]) != 4 {
		return fmt.Errorf("expected request for piece %d at offset 0, length 4; got %+v", index, m)
	}
	return nil
}

func concurrentPieceWireTest(index uint32, data string) []byte {
	payload := binary.BigEndian.AppendUint32(nil, index)
	payload = binary.BigEndian.AppendUint32(payload, 0)
	return (peer.Message{ID: peer.MsgPiece, Payload: append(payload, data...)}).Serialize()
}

func expectConcurrentHaveTest(conn net.Conn, index uint32) error {
	m, err := peer.ReadMessage(conn)
	if err != nil {
		return err
	}
	if m == nil || m.ID != peer.MsgHave || len(m.Payload) != 4 || binary.BigEndian.Uint32(m.Payload) != index {
		return fmt.Errorf("expected have for piece %d, got %+v", index, m)
	}
	return nil
}

func expectConcurrentClosedTest(conn net.Conn) error {
	var extra [1]byte
	_, err := conn.Read(extra[:])
	if err != io.EOF {
		return fmt.Errorf("expected cleanup without further messages, got %v", err)
	}
	return nil
}

func waitConcurrentPeersTest(t *testing.T, peers ...*concurrentPiecePeerTest) {
	t.Helper()
	for _, f := range peers {
		select {
		case <-f.done:
			if f.err != nil {
				t.Error(f.err)
			}
		case <-time.After(time.Second):
			t.Error("fake peer did not observe cleanup")
		}
	}
}

func TestSessionDownloadFromPeersRecoversDuringOtherTransfer(t *testing.T) {
	for _, failure := range []string{"disconnect", "corrupt piece"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			otherRequested, failed, replacementRequested := make(chan struct{}), make(chan struct{}), make(chan struct{})
			first := startServingConcurrentPeerTest(t, 3, peer.Bitfield{0xc0}, func(conn net.Conn) error {
				defer close(failed)
				if err := expectConcurrentRequestTest(conn, 0); err != nil {
					return err
				}
				if err := waitConcurrentSignalTest(ctx, otherRequested); err != nil {
					return err
				}
				if _, err := conn.Write(concurrentPieceWireTest(0, "abcd")); err != nil {
					return err
				}
				if err := expectConcurrentHaveTest(conn, 0); err != nil {
					return err
				}
				if err := expectConcurrentRequestTest(conn, 1); err != nil {
					return err
				}
				if failure == "disconnect" {
					wire := concurrentPieceWireTest(1, "efgh")
					_, err := conn.Write(wire[:len(wire)-2])
					return err
				}
				if _, err := conn.Write(concurrentPieceWireTest(1, "xxxx")); err != nil {
					return err
				}
				return expectConcurrentClosedTest(conn)
			})
			other := startServingConcurrentPeerTest(t, 3, peer.Bitfield{0x20}, func(conn net.Conn) error {
				if err := expectConcurrentRequestTest(conn, 2); err != nil {
					return err
				}
				close(otherRequested)
				// Keep this transfer active until failed work reaches its replacement.
				if err := waitConcurrentSignalTest(ctx, replacementRequested); err != nil {
					return err
				}
				if _, err := conn.Write(concurrentPieceWireTest(2, "ijkl")); err != nil {
					return err
				}
				if err := expectConcurrentHaveTest(conn, 2); err != nil {
					return err
				}
				return expectConcurrentClosedTest(conn)
			})
			replacement := startServingConcurrentPeerTest(t, 3, peer.Bitfield{0}, func(conn net.Conn) error {
				if err := waitConcurrentSignalTest(ctx, failed); err != nil {
					return err
				}
				if _, err := conn.Write(peer.NewHave(1).Serialize()); err != nil {
					return err
				}
				if err := expectConcurrentRequestTest(conn, 1); err != nil {
					return err
				}
				close(replacementRequested)
				if _, err := conn.Write(concurrentPieceWireTest(1, "efgh")); err != nil {
					return err
				}
				if err := expectConcurrentHaveTest(conn, 1); err != nil {
					return err
				}
				return expectConcurrentClosedTest(conn)
			})
			tf := &torrent.TorrentFile{Length: 12, PieceLength: 4, Pieces: [][20]byte{sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh")), sha1.Sum([]byte("ijkl"))}}
			dst := &downloadTestWriter{}
			s, err := NewSession(tf, dst)
			if err != nil {
				t.Fatal(err)
			}
			err = s.DownloadFromPeers(ctx, []*peer.Client{first.client, other.client, replacement.client}, 3)
			if err != nil {
				t.Fatalf("parallel recovery failed: %v", err)
			}
			waitConcurrentPeersTest(t, first, other, replacement)
			wantDownloaded := int64(12)
			if failure == "corrupt piece" {
				wantDownloaded += 4
			}
			if !s.Complete() || s.completedCount != 3 || s.Stats().Left != 0 || s.Stats().Downloaded != wantDownloaded || len(dst.writes) != 3 {
				t.Fatalf("recovery accounting: stats %+v, completions %d, writes %d", s.Stats(), s.completedCount, len(dst.writes))
			}
			output := make([]byte, 12)
			seen := make(map[int64]bool)
			for _, write := range dst.writes {
				if seen[write.offset] {
					t.Error("verified piece was written twice")
				}
				seen[write.offset] = true
				copy(output[int(write.offset):], write.data)
			}
			if string(output) != "abcdefghijkl" {
				t.Errorf("recovered output = %q", output)
			}
			if ctx.Err() != nil {
				t.Error("successful recovery canceled its parent context")
			}
		})
	}
}

func TestSessionDownloadFromPeersCancellationJoinsStalledWorkers(t *testing.T) {
	for _, operation := range []string{"download", "availability"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var fixtures []*concurrentPiecePeerTest
			var observed []*workerIOTestConn
			var clients []*peer.Client
			for index := uint32(0); index < 2; index++ {
				available := peer.Bitfield{0}
				if operation == "download" {
					available[0] = byte(0x80 >> index)
				}
				f := startServingConcurrentPeerTest(t, 2, available, func(conn net.Conn) error {
					if operation == "download" {
						if err := expectConcurrentRequestTest(conn, index); err != nil {
							return err
						}
					}
					// No response: cancellation must interrupt the client's pending read.
					return expectConcurrentClosedTest(conn)
				})
				conn := &workerIOTestConn{Conn: f.client.Conn, readStarted: make(chan struct{}), writeStarted: make(chan struct{})}
				f.client.Conn = conn
				fixtures, observed, clients = append(fixtures, f), append(observed, conn), append(clients, f.client)
			}
			tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: make([][20]byte, 2)}
			dst := &downloadTestWriter{}
			s, err := NewSession(tf, dst)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			var runErr error
			go func() { defer close(done); runErr = s.DownloadFromPeers(ctx, clients, 2) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("canceled peer batch retained running workers")
				}
			})
			for _, conn := range observed {
				select {
				case <-conn.readStarted:
				case <-done:
					t.Fatalf("batch exited before workers stalled: %v", runErr)
				case <-ctx.Done():
					t.Fatal("workers never entered their stalled reads")
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("canceling stalled workers waited for their thirty-second deadlines")
			}
			if !errors.Is(runErr, context.Canceled) {
				t.Errorf("error = %v, want context.Canceled", runErr)
			}
			waitConcurrentPeersTest(t, fixtures...)
			if s.Complete() || s.completedCount != 0 || s.Stats().Downloaded != 0 || s.Stats().Left != 8 || len(dst.writes) != 0 {
				t.Error("canceled stalled batch committed output or completion")
			}
		})
	}
}

func TestSessionDownloadFromPeersPreservesProgressAcrossBatches(t *testing.T) {
	first := newWorkerTestFixture(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := first.session.DownloadFromPeers(ctx, []*peer.Client{first.client}, 1)
	var peerErr *PeerError
	if !errors.As(err, &peerErr) {
		t.Fatalf("first batch error = %v, want peer exhaustion", err)
	}
	if first.session.completedCount != 1 || !first.session.completed.HasPiece(0) || first.session.completed.HasPiece(1) || first.session.Stats().Left != 4 {
		t.Fatal("exhausted batch lost or incorrectly committed progress")
	}
	second := newWorkerTestFixture(t, -1)
	// The replacement must request only piece one, so expose its matching reply.
	if _, err := second.conn.incoming.Seek(17, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := first.session.DownloadFromPeers(ctx, []*peer.Client{second.client}, 1); err != nil {
		t.Fatal(err)
	}
	if !first.session.Complete() || first.session.Stats().Left != 0 || first.session.Stats().Downloaded != 12 || len(first.writer.writes) != 2 {
		t.Fatal("replacement batch did not preserve exactly-once completion and retry statistics")
	}
	wire := bytes.NewReader(second.conn.outgoing.Bytes())
	for _, id := range []peer.MessageID{peer.MsgRequest, peer.MsgHave} {
		m, err := peer.ReadMessage(wire)
		if err != nil || m == nil || m.ID != id || len(m.Payload) < 4 || binary.BigEndian.Uint32(m.Payload[:4]) != 1 {
			t.Fatalf("replacement batch requested completed work or sent unexpected message: %+v, %v", m, err)
		}
	}
	if wire.Len() != 0 {
		t.Error("replacement batch issued extra network messages")
	}
}

func TestSessionDownloadFromPeersStorageFailureStopsWholeBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	requests := make(chan uint32, 2)
	pieces := []string{"abcd", "efgh"}
	var fixtures []*concurrentPiecePeerTest
	var clients []*peer.Client
	for index := uint32(0); index < 2; index++ {
		f := startServingConcurrentPeerTest(t, 2, peer.Bitfield{byte(0x80 >> index)}, func(conn net.Conn) error {
			if err := expectConcurrentRequestTest(conn, index); err != nil {
				return err
			}
			requests <- index
			if err := waitConcurrentSignalTest(ctx, release); err != nil {
				return err
			}
			if _, err := conn.Write(concurrentPieceWireTest(index, pieces[index])); err != nil {
				// Another worker's storage error can cancel this in-flight reply.
				if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
					return nil
				}
				return err
			}
			// Neither piece may be announced after the first disk write fails.
			return expectConcurrentClosedTest(conn)
		})
		fixtures, clients = append(fixtures, f), append(clients, f.client)
	}
	want := errors.New("disk full")
	dst := &downloadTestWriter{failAt: 1, err: want}
	tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: [][20]byte{sha1.Sum([]byte(pieces[0])), sha1.Sum([]byte(pieces[1]))}}
	s, err := NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var runErr error
	go func() { defer close(done); runErr = s.DownloadFromPeers(ctx, clients, 2) }()
	t.Cleanup(func() {
		cancel()
		unblock()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("fatal storage error retained running workers")
		}
	})
	for i := 0; i < 2; i++ {
		select {
		case <-requests:
		case <-done:
			t.Fatalf("batch stopped before concurrent requests: %v", runErr)
		case <-ctx.Done():
			t.Fatal("both workers never requested their pieces")
		}
	}
	unblock()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("storage failure did not stop the parallel batch promptly")
	}
	var peerErr *PeerError
	if !errors.Is(runErr, want) || errors.As(runErr, &peerErr) {
		t.Fatalf("error = %v, want original fatal disk error", runErr)
	}
	waitConcurrentPeersTest(t, fixtures...)
	stats := s.Stats()
	if s.Complete() || s.completedCount != 0 || stats.Left != 8 || stats.Downloaded < 4 || stats.Downloaded > 8 || len(dst.writes) != 1 {
		t.Errorf("fatal failure committed progress or retried storage: stats %+v, completions %d, writes %d", stats, s.completedCount, len(dst.writes))
	}
	if ctx.Err() != nil {
		t.Error("fatal batch cleanup canceled its parent context")
	}
}
