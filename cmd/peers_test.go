package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

func testPeerQueue(addrs ...net.TCPAddr) *peerQueue {
	q := newPeerQueue()
	q.Add(addrs)
	return q
}

func TestPreparePeerBatchSkipsPeerFailuresAndBoundsAttempts(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1}}
	good, goodDone := startFallbackTestPeer(t, local, fallbackTestExpectClosed)
	bad, badDone := startSelectionTestPeer(t, local, local, true)
	untouched := net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
	q := testPeerQueue(good, bad, untouched)
	clients, err := preparePeerBatch(context.Background(), q, local, 2, 2)
	for _, client := range clients {
		t.Cleanup(func() { client.Conn.Close() })
	}
	if err != nil || len(clients) != 1 {
		t.Fatalf("batch = %d clients, %v; want one prepared client", len(clients), err)
	}
	if err := clients[0].Conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Errorf("helper closed successfully returned connection: %v", err)
	}
	if addr, ok := q.Next(); !ok || addr.String() != untouched.String() {
		t.Error("candidate limit was treated as a success count and consumed extra addresses")
	}
	clients[0].Conn.Close()
	for _, done := range []<-chan error{goodDone, badDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("prepared or rejected peer did not observe cleanup")
		}
	}
}

func TestPreparePeerBatchEmptyInvalidAndCanceled(t *testing.T) {
	clients, err := preparePeerBatch(context.Background(), newPeerQueue(), peer.Handshake{}, 1, 4)
	if err != nil || len(clients) != 0 {
		t.Errorf("empty queue = %v, %v; want empty success", clients, err)
	}
	for _, limit := range []int{0, -1} {
		q := testPeerQueue(net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
		clients, err := preparePeerBatch(context.Background(), q, peer.Handshake{}, 1, limit)
		if err == nil || len(clients) != 0 {
			t.Errorf("invalid limit %d accepted", limit)
		}
		if _, ok := q.Next(); !ok {
			t.Error("invalid limit consumed a candidate")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q := testPeerQueue(net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	clients, err = preparePeerBatch(ctx, q, peer.Handshake{}, 1, 1)
	if !errors.Is(err, context.Canceled) || len(clients) != 0 {
		t.Errorf("canceled batch = %v, %v", clients, err)
	}
	if _, ok := q.Next(); !ok {
		t.Error("already canceled batch consumed a candidate")
	}
}

func TestPreparePeerBatchCancellationClosesPreparedClients(t *testing.T) {
	for _, limit := range []int{1, 2} {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			local := peer.Handshake{InfoHash: [20]byte{1}}
			accepted := make(chan net.Conn, 1)
			addr, done := startFallbackTestPeerWithVerifier(t, local, func(conn net.Conn) error {
				accepted <- conn
				return fallbackTestExpectClosed(conn)
			}, func([68]byte) error {
				// Cancel after preparation begins but before the handshake finishes.
				// The helper must clean up rather than handing this client onward.
				cancel()
				return nil
			})
			untouched := net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
			q := testPeerQueue(addr, untouched)
			clients, err := preparePeerBatch(ctx, q, local, 2, limit)
			for _, client := range clients {
				t.Cleanup(func() { client.Conn.Close() })
			}
			if !errors.Is(err, context.Canceled) || len(clients) != 0 {
				t.Errorf("cancellation during preparation returned %d clients and %v; want no clients and context.Canceled", len(clients), err)
			}
			select {
			case conn := <-accepted:
				t.Cleanup(func() { conn.Close() })
			case <-time.After(time.Second):
				t.Fatal("fake peer did not reach prepared state")
			}
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(time.Second):
				t.Error("cancellation leaked a prepared connection")
			}
			if addr, ok := q.Next(); !ok || addr.String() != untouched.String() {
				t.Error("cancellation consumed another candidate")
			}
		})
	}
}

func TestPreparePeerBatchFatalPreparationStops(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1}}
	addr, done := startSelectionTestPeer(t, local, local, false)
	untouched := net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
	q := testPeerQueue(addr, untouched)
	clients, err := preparePeerBatch(context.Background(), q, local, -1, 2)
	var peerErr *download.PeerError
	if err == nil || errors.As(err, &peerErr) || len(clients) != 0 {
		t.Fatalf("fatal metadata preparation = %v, %v", clients, err)
	}
	if addr, ok := q.Next(); !ok || addr.String() != untouched.String() {
		t.Error("fatal failure continued to another peer")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("fatal preparation failure did not close its connection")
	}
}

func TestDownloadFromPeersRetainsQueuedAddressAfterEmptyBatch(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1}}
	badFirst, badFirstDone := startSelectionTestPeer(t, local, local, true)
	badSecond, badSecondDone := startSelectionTestPeer(t, local, local, true)
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
		return fallbackTestExpectClosed(conn)
	})
	hash := sha1.Sum([]byte("data"))
	tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: [][20]byte{hash, hash}}
	dst := &fallbackTestWriter{data: make([]byte, 8)}
	s, err := download.NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	refresh := make(chan struct{}, 4)
	err = downloadFromPeers(ctx, refresh, testPeerQueue(badFirst, badSecond, good), local, 2, 2, s, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("good peer after empty batch was dropped: %v", err)
	}
	if !s.Complete() || string(dst.data) != "datadata" {
		t.Error("queued good peer did not finish the file")
	}
	if len(refresh) != 0 {
		t.Error("requested rediscovery despite an untried queued peer")
	}
	for _, done := range []<-chan error{badFirstDone, badSecondDone, goodDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("peer did not observe connection cleanup")
		}
	}
}

func TestWaitForPeerAvailable(t *testing.T) {
	q := newPeerQueue()
	want := net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 6881}
	q.Add([]net.TCPAddr{want})
	refresh := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := waitForPeer(ctx, refresh, q, time.Second)
	if err != nil || got.String() != want.String() {
		t.Fatalf("waitForPeer = %v, %v; want %s, nil", got, err, want.String())
	}
	if len(refresh) != 0 {
		t.Fatal("requested a refresh despite an available peer")
	}
	if _, ok := q.Next(); ok {
		t.Fatal("returned peer was not removed from the queue")
	}
}

func TestWaitForPeerAlreadyCanceled(t *testing.T) {
	want := net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 6881}
	q := testPeerQueue(want)
	refresh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := waitForPeer(ctx, refresh, q, time.Second)
	if !errors.Is(err, context.Canceled) || got.IP != nil || got.Port != 0 || got.Zone != "" {
		t.Fatalf("waitForPeer = %v, %v; want zero address and context.Canceled", got, err)
	}
	if len(refresh) != 0 {
		t.Fatal("canceled wait requested a refresh")
	}
	if addr, ok := q.Next(); !ok || addr.String() != want.String() {
		t.Fatal("canceled wait consumed an available peer")
	}
}

type peerWaitTestResult struct {
	addr net.TCPAddr
	err  error
}

func TestWaitForPeerRequestsRefresh(t *testing.T) {
	q := newPeerQueue()
	refresh := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan peerWaitTestResult, 1)
	go func() {
		addr, err := waitForPeer(ctx, refresh, q, time.Second)
		done <- peerWaitTestResult{addr, err}
	}()
	select {
	case <-refresh:
	case <-ctx.Done():
		t.Fatal("empty queue did not request a refresh")
	}
	want := net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 6881}
	q.Add([]net.TCPAddr{want})
	select {
	case got := <-done:
		if got.err != nil || got.addr.String() != want.String() {
			t.Fatalf("waitForPeer = %v, %v; want %s, nil", got.addr, got.err, want.String())
		}
	case <-ctx.Done():
		t.Fatal("wait did not return the discovered peer")
	}
}

func TestWaitForPeerTimeout(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("refresh_already_pending=%t", pending), func(t *testing.T) {
			q := newPeerQueue()
			refresh := make(chan struct{}, 1)
			if pending {
				refresh <- struct{}{}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan peerWaitTestResult, 1)
			go func() {
				addr, err := waitForPeer(ctx, refresh, q, 25*time.Millisecond)
				done <- peerWaitTestResult{addr, err}
			}()
			select {
			case got := <-done:
				if !errors.Is(got.err, context.DeadlineExceeded) || got.addr.IP != nil || got.addr.Port != 0 || got.addr.Zone != "" {
					t.Fatalf("waitForPeer = %v, %v; want zero address and deadline error", got.addr, got.err)
				}
				if ctx.Err() != nil {
					t.Fatal("discovery timeout canceled or outlasted the parent context")
				}
				if len(refresh) != 1 {
					t.Fatal("expected one pending refresh request")
				}
			case <-ctx.Done():
				t.Fatal("wait did not honor its shorter discovery timeout")
			}
		})
	}
}

func TestWaitForPeerParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	refresh := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		_, err := waitForPeer(ctx, refresh, newPeerQueue(), time.Hour)
		done <- err
	}()
	select {
	case <-refresh:
	case <-time.After(time.Second):
		t.Fatal("wait never requested discovery")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("parent cancellation did not interrupt discovery")
	}
}

func TestDownloadFromPeersUsesFirstDiscoveredPeer(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1}}
	addr, peerDone := startFallbackTestPeer(t, local, func(conn net.Conn) error {
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
		return fallbackTestExpectClosed(conn)
	})
	hash := sha1.Sum([]byte("data"))
	tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: [][20]byte{hash, hash}}
	session, err := download.NewSession(tf, &fallbackTestWriter{data: make([]byte, 8)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	q := newPeerQueue()
	refresh := make(chan struct{}, 1)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		select {
		case <-refresh:
			q.Add([]net.TCPAddr{addr})
		case <-ctx.Done():
		}
	}()
	err = downloadFromPeers(ctx, refresh, q, local, 2, 1, session, 25*time.Millisecond)
	cancel()
	<-workerDone
	if err != nil || !session.Complete() {
		t.Fatalf("first discovered peer was not used to finish the download: %v", err)
	}
	if err := <-peerDone; err != nil {
		t.Fatal(err)
	}
}

func TestDownloadFromPeersBoundsRediscoveryWithoutProgress(t *testing.T) {
	// A listening port that has been closed fails connection attempts promptly.
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	addr := *listener.Addr().(*net.TCPAddr)
	listener.Close()
	tf := &torrent.TorrentFile{Length: 4, PieceLength: 4, Pieces: [][20]byte{sha1.Sum([]byte("data"))}}
	session, err := download.NewSession(tf, &fallbackTestWriter{data: make([]byte, 4)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	q := newPeerQueue()
	refresh := make(chan struct{}, 1)
	counts := make(chan int, 1)
	go func() {
		count := 0
		defer func() { counts <- count }()
		for {
			select {
			case <-ctx.Done():
				return
			case <-refresh:
				count++
				if count > 3 {
					cancel()
					return
				}
				q.Add([]net.TCPAddr{addr})
			}
		}
	}()
	err = downloadFromPeers(ctx, refresh, q, peer.Handshake{}, 1, 1, session, 25*time.Millisecond)
	cancel()
	count := <-counts
	if count != 3 {
		t.Errorf("rediscovery requests = %d, want 3", count)
	}
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want retry exhaustion before parent cancellation", err)
	}
}

func TestDownloadFromPeersResetsDiscoveryBudgetAfterProgress(t *testing.T) {
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
		return fallbackTestExpectClosed(conn)
	})
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	bad := *listener.Addr().(*net.TCPAddr)
	listener.Close()
	tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: [][20]byte{sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))}}
	dst := &fallbackTestWriter{data: make([]byte, 8)}
	session, err := download.NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	q := newPeerQueue()
	refresh := make(chan struct{}, 1)
	counts := make(chan int, 1)
	go func() {
		count := 0
		defer func() { counts <- count }()
		for _, addr := range []net.TCPAddr{bad, bad, first, second} {
			select {
			case <-ctx.Done():
				return
			case <-refresh:
				count++
				q.Add([]net.TCPAddr{addr})
			}
		}
	}()
	err = downloadFromPeers(ctx, refresh, q, local, 2, 1, session, 25*time.Millisecond)
	cancel()
	count := <-counts
	if err != nil || !session.Complete() || !bytes.Equal(dst.data, []byte("abcdefgh")) {
		t.Fatalf("download after verified progress = %v; complete %t, output %q", err, session.Complete(), dst.data)
	}
	if count != 4 {
		t.Errorf("refreshes = %d, want 4 across the progress reset", count)
	}
	for _, done := range []<-chan error{firstDone, secondDone} {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
}

func TestPeerQueueAdd(t *testing.T) {
	q := newPeerQueue()
	first := net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 6881}
	second := net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 6882}
	third := net.TCPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 6881}
	q.Add(nil)
	q.Add([]net.TCPAddr{first, first, second})
	q.Add([]net.TCPAddr{second, third, first})
	want := []net.TCPAddr{first, second, third}
	if len(q.pending) != len(want) || len(q.queue) != len(want) {
		t.Fatalf("pending = %d, membership = %d; want %d each", len(q.pending), len(q.queue), len(want))
	}
	for i, addr := range want {
		if q.pending[i].String() != addr.String() {
			t.Errorf("pending[%d] = %s, want %s", i, q.pending[i].String(), addr.String())
		}
		if _, exists := q.queue[addr.String()]; !exists {
			t.Errorf("missing membership for %s", addr.String())
		}
	}
}

func TestPeerQueueAddNotifies(t *testing.T) {
	q := newPeerQueue()
	assertNoNotification := func() {
		t.Helper()
		select {
		case <-q.ready:
			t.Fatal("unexpected queue notification")
		default:
		}
	}
	assertNoNotification()
	q.Add(nil)
	assertNoNotification()
	addr := net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 6881}
	q.Add([]net.TCPAddr{addr})
	select {
	case <-q.ready:
	default:
		t.Fatal("adding a new peer did not notify the consumer")
	}
	q.Add([]net.TCPAddr{addr})
	assertNoNotification()
	if _, ok := q.Next(); !ok {
		t.Fatal("notification consumed the queued peer")
	}
	q.Add([]net.TCPAddr{addr})
	select {
	case <-q.ready:
	default:
		t.Fatal("re-adding a removed peer did not notify the consumer")
	}
}

func TestPeerQueueAddWithPendingNotification(t *testing.T) {
	q := newPeerQueue()
	// Model an unconsumed notification from an earlier addition.
	q.ready <- struct{}{}
	done := make(chan struct{})
	go func() {
		q.Add([]net.TCPAddr{{IP: net.IPv4(192, 0, 2, 1), Port: 6881}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Add blocked while a notification was already pending")
	}
	select {
	case <-q.ready:
	default:
		t.Fatal("Add removed the pending notification")
	}
	if _, ok := q.Next(); !ok {
		t.Fatal("peer was not queued")
	}
}

func TestPeerQueueNext(t *testing.T) {
	q := newPeerQueue()
	assertEmpty := func() {
		t.Helper()
		addr, ok := q.Next()
		if ok || addr.IP != nil || addr.Port != 0 || addr.Zone != "" {
			t.Fatalf("empty Next returned %+v, %t", addr, ok)
		}
	}
	assertEmpty()
	first := net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 6881}
	second := net.TCPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 6881}
	q.Add([]net.TCPAddr{first, second})
	addr, ok := q.Next()
	if !ok || addr.String() != first.String() {
		t.Fatalf("Next = %v, %t; want %s, true", addr, ok, first.String())
	}
	if _, exists := q.queue[first.String()]; exists {
		t.Fatal("removed peer remains in membership map")
	}
	// Re-added peers go behind addresses that are still waiting.
	q.Add([]net.TCPAddr{first})
	for _, want := range []net.TCPAddr{second, first} {
		addr, ok := q.Next()
		if !ok || addr.String() != want.String() {
			t.Fatalf("Next = %v, %t; want %s, true", addr, ok, want.String())
		}
	}
	assertEmpty()
	assertEmpty()
	if len(q.queue) != 0 {
		t.Errorf("membership contains %d entries after draining", len(q.queue))
	}
}

func TestPeerQueueWaitNextAvailable(t *testing.T) {
	q := newPeerQueue()
	want := net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 6881}
	q.Add([]net.TCPAddr{want})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := q.WaitNext(ctx)
	if err != nil || got.String() != want.String() {
		t.Fatalf("WaitNext = %v, %v; want %s, nil", got, err, want.String())
	}
	if _, ok := q.Next(); ok {
		t.Fatal("WaitNext did not remove the returned peer")
	}
}

func TestPeerQueueWaitNextCanceledWithAvailablePeer(t *testing.T) {
	q := newPeerQueue()
	want := net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 6881}
	q.Add([]net.TCPAddr{want})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := q.WaitNext(ctx)
	if !errors.Is(err, context.Canceled) || got.IP != nil || got.Port != 0 || got.Zone != "" {
		t.Fatalf("WaitNext = %v, %v; want zero address and context.Canceled", got, err)
	}
	if addr, ok := q.Next(); !ok || addr.String() != want.String() {
		t.Fatal("canceled wait consumed a queued peer")
	}
}

// Observe entry into the blocking select without relying on scheduling sleeps.
type peerQueueWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *peerQueueWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestPeerQueueWaitNextWakesOnAdd(t *testing.T) {
	q := newPeerQueue()
	base, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx := &peerQueueWaitContext{Context: base, waiting: make(chan struct{})}
	want := net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 6881}
	type result struct {
		addr net.TCPAddr
		err  error
	}
	done := make(chan result, 1)
	go func() { addr, err := q.WaitNext(ctx); done <- result{addr, err} }()
	select {
	case <-ctx.waiting:
	case <-base.Done():
		t.Fatal("WaitNext did not reach the wait")
	}
	added := make(chan struct{})
	go func() { q.Add([]net.TCPAddr{want}); close(added) }()
	select {
	case <-added:
	case <-base.Done():
		t.Fatal("Add blocked while WaitNext was waiting")
	}
	select {
	case got := <-done:
		if got.err != nil || got.addr.String() != want.String() {
			t.Fatalf("WaitNext = %v, %v; want %s, nil", got.addr, got.err, want.String())
		}
	case <-base.Done():
		t.Fatal("WaitNext did not wake for a new peer")
	}
}

func TestPeerQueueWaitNextDeadline(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("stale_notification=%t", stale), func(t *testing.T) {
			q := newPeerQueue()
			if stale {
				q.Add([]net.TCPAddr{{IP: net.IPv4(192, 0, 2, 1), Port: 6881}})
				q.Next()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				addr, err := q.WaitNext(ctx)
				if addr.IP != nil || addr.Port != 0 || addr.Zone != "" {
					done <- fmt.Errorf("returned nonzero address: %v", addr)
					return
				}
				done <- err
			}()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("WaitNext error = %v, want context.DeadlineExceeded", err)
				}
			case <-time.After(time.Second):
				t.Fatal("WaitNext did not return after its deadline")
			}
		})
	}
}

func TestPeerQueueConcurrentNext(t *testing.T) {
	q := newPeerQueue()
	const total = 128
	for i := 0; i < total; i++ {
		q.Add([]net.TCPAddr{{IP: net.IPv4(192, 0, 2, byte(i+1)), Port: 6881}})
	}
	results := make(chan string, total)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				addr, ok := q.Next()
				if !ok {
					return
				}
				results <- addr.String()
			}
		}()
	}
	wg.Wait()
	close(results)
	seen := make(map[string]bool)
	for addr := range results {
		if seen[addr] {
			t.Errorf("peer returned more than once: %s", addr)
		}
		seen[addr] = true
	}
	if len(seen) != total || len(q.queue) != 0 || len(q.pending) != 0 {
		t.Errorf("unique peers = %d, membership = %d, pending = %d; want %d, 0, 0", len(seen), len(q.queue), len(q.pending), total)
	}
}

func TestPeerQueueConcurrentAdd(t *testing.T) {
	q := newPeerQueue()
	const callers = 32
	shared := net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 6881}
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			unique := net.TCPAddr{IP: net.IPv4(198, 51, 100, byte(i+1)), Port: 6881}
			q.Add([]net.TCPAddr{shared, unique, shared})
		}(i)
	}
	wg.Wait()
	if len(q.pending) != callers+1 || len(q.queue) != callers+1 {
		t.Fatalf("pending = %d, membership = %d; want %d each", len(q.pending), len(q.queue), callers+1)
	}
	seen := make(map[string]bool)
	for _, addr := range q.pending {
		key := addr.String()
		if seen[key] {
			t.Errorf("duplicate queued address %s", key)
		}
		seen[key] = true
		if _, exists := q.queue[key]; !exists {
			t.Errorf("missing membership for %s", key)
		}
	}
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
	if err := downloadFromPeers(context.Background(), nil, testPeerQueue(bad, good), local, 2, 1, session, 25*time.Millisecond); err != nil {
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
	if err := downloadFromPeers(context.Background(), nil, testPeerQueue(first, second), local, 1, 1, session, 25*time.Millisecond); err == nil {
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
	if err := downloadFromPeers(context.Background(), nil, newPeerQueue(), peer.Handshake{}, 1, 1, session, 25*time.Millisecond); err == nil {
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
	if err := downloadFromPeers(context.Background(), nil, testPeerQueue(first, second), local, 2, 1, s, 25*time.Millisecond); err != nil {
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
	if err := downloadFromPeers(context.Background(), nil, newPeerQueue(), peer.Handshake{}, 0, 1, s, 25*time.Millisecond); err != nil {
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
	err = downloadFromPeers(context.Background(), nil, testPeerQueue(addr, *spare.Addr().(*net.TCPAddr)), local, 2, 1, s, 25*time.Millisecond)
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
	if err := downloadFromPeers(context.Background(), nil, testPeerQueue(bad, good), local, 2, 1, session, 25*time.Millisecond); err != nil {
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
	err = downloadFromPeers(context.Background(), nil, testPeerQueue(corrupt, truncated), local, 2, 1, session, 25*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "peer discovery attempts") {
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
