package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
)

type uploadTestConn struct {
	net.Conn
	wire    bytes.Buffer
	err     error
	partial bool
}

type uploadReaderTestConn struct {
	net.Conn
	reader      io.Reader
	deadlineErr error
}

func (c *uploadReaderTestConn) Read(p []byte) (int, error)      { return c.reader.Read(p) }
func (c *uploadReaderTestConn) SetReadDeadline(time.Time) error { return c.deadlineErr }

func startUploadReaderTest(t *testing.T, ctx context.Context, cancel context.CancelFunc, conn net.Conn, results chan uploadReadResult) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); readUploadMessages(ctx, conn, results) }()
	t.Cleanup(func() {
		cancel()
		// Drain broken implementations too, so a failing test does not leak a reader.
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-done:
				return
			case <-results:
			case <-timer.C:
				t.Error("reader did not terminate during test cleanup")
				return
			}
		}
	})
	return done
}

func TestReadUploadMessagesCanceledWithoutReceiver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results := make(chan uploadReadResult)
	conn := &uploadReaderTestConn{reader: bytes.NewReader(nil)}
	done := startUploadReaderTest(t, ctx, cancel, conn, results)
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("already-canceled reader blocked trying to publish a cancellation error")
	}
}

func TestReadUploadMessagesDeliversMessagesThenStopsOnEOF(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan uploadReadResult, 3)
	conn := &uploadReaderTestConn{reader: bytes.NewReader([]byte{0, 0, 0, 0, 0, 0, 0, 1, 2})}
	done := startUploadReaderTest(t, ctx, cancel, conn, results)
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("reader continued reading after publishing EOF")
	}
	if len(results) != 3 {
		t.Fatalf("result count=%d, want keep-alive, interested, EOF", len(results))
	}
	keepAlive, interested, failure := <-results, <-results, <-results
	if keepAlive.message != nil || keepAlive.err != nil {
		t.Errorf("keep-alive=%+v", keepAlive)
	}
	if interested.err != nil || interested.message == nil || interested.message.ID != peer.MsgInterested {
		t.Errorf("interested=%+v", interested)
	}
	if failure.message != nil || !errors.Is(failure.err, io.EOF) {
		t.Errorf("final result=%+v, want EOF", failure)
	}
}

func TestReadUploadMessagesDeadlineFailureDeliveryCanBeCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan uploadReadResult)
	sentinel := errors.New("read deadline failed")
	conn := &uploadReaderTestConn{reader: bytes.NewReader(nil), deadlineErr: sentinel}
	done := startUploadReaderTest(t, ctx, cancel, conn, results)
	cancel()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("deadline error delivery ignored cancellation")
	}
}

func (c *uploadTestConn) Write(p []byte) (int, error) {
	if c.err != nil {
		if c.partial {
			n, _ := c.wire.Write(p[:2])
			return n, c.err
		}
		return 0, c.err
	}
	return c.wire.Write(p)
}

func TestSessionUploadBlock(t *testing.T) {
	dst := &blockTestStorage{data: []byte("abcdefghij")}
	s := blockTestSession(t, dst, 4)
	if err := s.markComplete(1); err != nil {
		t.Fatal(err)
	}
	if err := s.markComplete(2); err != nil {
		t.Fatal(err)
	}
	conn := &uploadTestConn{}
	// A peer choking our downloads may still receive uploads from us.
	c := &peer.Client{Conn: conn, PeerInterested: true, Choked: true, PieceCount: 3, Bitfield: peer.Bitfield{0x80}}
	before := s.Stats()
	for _, request := range []peer.BlockRequest{
		{Index: 1, Begin: 1, Length: 2},
		{Index: 2, Length: 2},
	} {
		if err := s.UploadBlock(c, request); err != nil {
			t.Fatal(err)
		}
	}
	want := []byte{
		0, 0, 0, 11, 7, 0, 0, 0, 1, 0, 0, 0, 1, 'f', 'g',
		0, 0, 0, 11, 7, 0, 0, 0, 2, 0, 0, 0, 0, 'i', 'j',
	}
	if !bytes.Equal(conn.wire.Bytes(), want) {
		t.Errorf("wire=%x, want %x", conn.wire.Bytes(), want)
	}
	after := s.Stats()
	if after.Uploaded != before.Uploaded+4 || after.Downloaded != before.Downloaded || after.Left != before.Left {
		t.Errorf("stats before=%+v after=%+v, want only 4 uploaded data bytes added", before, after)
	}
	if !c.Choked || c.AmChoking || !c.PeerInterested || !bytes.Equal(c.Bitfield, []byte{0x80}) {
		t.Error("upload changed peer state")
	}
}

func TestSessionUploadBlockFailures(t *testing.T) {
	storageErr := errors.New("upload disk read failed")
	writeErr := errors.New("upload socket write failed")
	for _, tc := range []struct {
		name                              string
		choking, uninterested, unverified bool
		request                           peer.BlockRequest
		storageErr, writeErr              error
		short, partial                    bool
		wantReads                         int
	}{
		{name: "choking", choking: true, request: peer.BlockRequest{Length: 2}},
		{name: "not interested", uninterested: true, request: peer.BlockRequest{Length: 2}},
		{name: "unverified", unverified: true, request: peer.BlockRequest{Length: 2}},
		{name: "invalid index", request: peer.BlockRequest{Index: 1, Length: 2}},
		{name: "invalid range", request: peer.BlockRequest{Begin: 3, Length: 2}},
		{name: "zero length", request: peer.BlockRequest{}},
		{name: "storage error", request: peer.BlockRequest{Length: 2}, storageErr: storageErr, wantReads: 1},
		{name: "short read", request: peer.BlockRequest{Length: 2}, short: true, wantReads: 1},
		{name: "write error", request: peer.BlockRequest{Length: 2}, writeErr: writeErr, wantReads: 1},
		{name: "partial write error", request: peer.BlockRequest{Length: 2}, writeErr: writeErr, partial: true, wantReads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := &blockTestStorage{data: []byte("abcd"), err: tc.storageErr, short: tc.short}
			s := blockTestSession(t, dst, 4)
			if !tc.unverified {
				if err := s.markComplete(0); err != nil {
					t.Fatal(err)
				}
			}
			conn := &uploadTestConn{err: tc.writeErr, partial: tc.partial}
			c := &peer.Client{Conn: conn, AmChoking: tc.choking, PeerInterested: !tc.uninterested}
			before := s.Stats()
			err := s.UploadBlock(c, tc.request)
			if err == nil {
				t.Fatal("expected upload failure")
			}
			for _, underlying := range []error{tc.storageErr, tc.writeErr} {
				if underlying != nil && !errors.Is(err, underlying) {
					t.Errorf("error=%v, want underlying %v", err, underlying)
				}
			}
			if s.Stats() != before {
				t.Error("failed upload changed counters")
			}
			if dst.reads != tc.wantReads {
				t.Errorf("reads=%d, want %d", dst.reads, tc.wantReads)
			}
			wantWire := 0
			if tc.partial {
				wantWire = 2
			}
			if conn.wire.Len() != wantWire {
				t.Errorf("sent %d bytes, want %d", conn.wire.Len(), wantWire)
			}
		})
	}
}

func TestSessionHandleUploadMessageConversation(t *testing.T) {
	dst := &blockTestStorage{data: []byte("abcd")}
	s := blockTestSession(t, dst, 4)
	if err := s.markComplete(0); err != nil {
		t.Fatal(err)
	}
	conn := &uploadTestConn{}
	c := &peer.Client{Conn: conn, PieceCount: 1, Bitfield: peer.Bitfield{0}, AmChoking: true, Choked: true}
	for _, msg := range []*peer.Message{
		nil,
		{ID: peer.MsgInterested},
		{ID: peer.MsgHave, Payload: []byte{0, 0, 0, 0}},
		{ID: peer.MsgRequest, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2}},
		{ID: peer.MsgNotInterested},
	} {
		if err := s.handleUploadMessage(c, msg); err != nil {
			t.Fatal(err)
		}
	}
	want := []byte{
		0, 0, 0, 1, 1, // Unchoke after interest.
		0, 0, 0, 11, 7, 0, 0, 0, 0, 0, 0, 0, 1, 'b', 'c',
		0, 0, 0, 1, 0, // Choke after interest is withdrawn.
	}
	if !bytes.Equal(conn.wire.Bytes(), want) {
		t.Errorf("wire=%x, want %x", conn.wire.Bytes(), want)
	}
	if !c.AmChoking || c.PeerInterested || !c.Choked || !c.Bitfield.HasPiece(0) {
		t.Errorf("unexpected final state: %+v", c)
	}
	if got := s.Stats().Uploaded; got != 2 {
		t.Errorf("Uploaded=%d, want 2", got)
	}
	if dst.reads != 1 {
		t.Errorf("storage reads=%d, want 1", dst.reads)
	}
}

func TestSessionHandleUploadMessageRejectsMalformedMessages(t *testing.T) {
	for _, id := range []peer.MessageID{peer.MsgInterested, peer.MsgNotInterested, peer.MsgRequest, peer.MsgHave} {
		for _, initial := range []bool{false, true} {
			t.Run(fmt.Sprintf("id=%d/initialInterest=%t", id, initial), func(t *testing.T) {
				dst := &blockTestStorage{data: []byte("abcd")}
				s := blockTestSession(t, dst, 4)
				conn := &uploadTestConn{}
				c := &peer.Client{Conn: conn, PieceCount: 1, Bitfield: peer.Bitfield{0}, PeerInterested: initial, AmChoking: true}
				if err := s.handleUploadMessage(c, &peer.Message{ID: id, Payload: []byte{42}}); err == nil {
					t.Fatal("expected malformed message error")
				}
				if c.PeerInterested != initial || !c.AmChoking || c.Bitfield[0] != 0 {
					t.Error("malformed message changed state")
				}
				if dst.reads != 0 || conn.wire.Len() != 0 || s.Stats().Uploaded != 0 {
					t.Error("malformed message performed upload work")
				}
			})
		}
	}
}

func TestSessionHandleUploadMessageInterestReplyFailure(t *testing.T) {
	wantErr := errors.New("interest reply failed")
	for _, id := range []peer.MessageID{peer.MsgInterested, peer.MsgNotInterested} {
		t.Run(fmt.Sprintf("id=%d", id), func(t *testing.T) {
			dst := &blockTestStorage{data: []byte("abcd")}
			s := blockTestSession(t, dst, 4)
			interested := id == peer.MsgInterested
			conn := &uploadTestConn{err: wantErr}
			c := &peer.Client{Conn: conn, PeerInterested: !interested, AmChoking: interested}
			if err := s.handleUploadMessage(c, &peer.Message{ID: id}); !errors.Is(err, wantErr) {
				t.Errorf("error=%v, want %v", err, wantErr)
			}
			if c.PeerInterested != interested {
				t.Error("did not record valid incoming interest state")
			}
			if c.AmChoking != interested {
				t.Error("failed send changed choking state")
			}
			if dst.reads != 0 || s.Stats().Uploaded != 0 {
				t.Error("interest reply performed block upload work")
			}
		})
	}
}

func TestSessionHandleUploadMessageKeepAlive(t *testing.T) {
	dst := &blockTestStorage{data: []byte("abcd")}
	s := blockTestSession(t, dst, 4)
	// A keep-alive needs neither client state nor connection operations.
	if err := s.handleUploadMessage(nil, nil); err != nil {
		t.Fatal(err)
	}
	if dst.reads != 0 || s.Stats().Uploaded != 0 {
		t.Error("keep-alive performed upload work")
	}
}

type serveTestConn struct {
	*uploadTestConn
	mu        sync.Mutex
	reader    *bytes.Reader
	deadlines []string
	closes    int
}

func (c *serveTestConn) Read(p []byte) (int, error) { return c.reader.Read(p) }
func (c *serveTestConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes++
	return nil
}
func (c *serveTestConn) SetDeadline(time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadlines = append(c.deadlines, "both")
	return nil
}
func (c *serveTestConn) SetReadDeadline(time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadlines = append(c.deadlines, "read")
	return nil
}
func (c *serveTestConn) SetWriteDeadline(time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadlines = append(c.deadlines, "write")
	return nil
}

func TestSessionServePeerConversation(t *testing.T) {
	dst := &blockTestStorage{data: []byte("abcd")}
	s := blockTestSession(t, dst, 4)
	if err := s.markComplete(0); err != nil {
		t.Fatal(err)
	}
	incoming := []byte{
		0, 0, 0, 1, 2, // Interested.
		0, 0, 0, 13, 6, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2,
	}
	conn := &serveTestConn{uploadTestConn: &uploadTestConn{}, reader: bytes.NewReader(incoming)}
	c := &peer.Client{Conn: conn, PieceCount: 1, Bitfield: peer.Bitfield{0}, AmChoking: true}
	if err := s.ServePeer(context.Background(), c); !errors.Is(err, io.EOF) {
		t.Errorf("error=%v, want EOF", err)
	}
	want := []byte{
		0, 0, 0, 2, 5, 0x80, // Initial local availability.
		0, 0, 0, 1, 1,
		0, 0, 0, 11, 7, 0, 0, 0, 0, 0, 0, 0, 1, 'b', 'c',
	}
	if !bytes.Equal(conn.wire.Bytes(), want) {
		t.Errorf("wire=%x, want %x", conn.wire.Bytes(), want)
	}
	if conn.closes < 1 {
		t.Error("connection was not closed")
	}
	if got := s.Stats().Uploaded; got != 2 {
		t.Errorf("Uploaded=%d, want 2", got)
	}
	counts := make(map[string]int)
	for _, direction := range conn.deadlines {
		counts[direction]++
	}
	if len(conn.deadlines) == 0 || conn.deadlines[0] != "write" || counts["write"] != 3 || counts["read"] != 3 || counts["both"] != 0 {
		t.Errorf("deadlines=%v, want initial write deadline, three read and three write deadlines", conn.deadlines)
	}
}

type serveGateConn struct {
	net.Conn
	reading chan struct{}
	writing chan struct{}
}

func (c *serveGateConn) Read(p []byte) (int, error) {
	select {
	case c.reading <- struct{}{}:
	default:
	}
	return c.Conn.Read(p)
}

func (c *serveGateConn) Write(p []byte) (int, error) {
	select {
	case c.writing <- struct{}{}:
	default:
	}
	return c.Conn.Write(p)
}

func TestSessionServePeerCancellationUnblocksIO(t *testing.T) {
	for _, phase := range []string{"initial write", "idle read", "have write"} {
		t.Run(phase, func(t *testing.T) {
			dst := &blockTestStorage{data: []byte("abcd")}
			s := blockTestSession(t, dst, 4)
			local, remote := net.Pipe()
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() { cancel(); local.Close(); remote.Close() })
			if err := remote.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			gate := &serveGateConn{Conn: local, reading: make(chan struct{}, 1), writing: make(chan struct{}, 1)}
			c := &peer.Client{Conn: gate, PieceCount: 1, Bitfield: peer.Bitfield{0}, AmChoking: true}
			done := make(chan error, 1)
			go func() { done <- s.ServePeer(ctx, c) }()
			if phase != "initial write" {
				msg, err := peer.ReadMessage(remote)
				if err != nil || msg == nil || msg.ID != peer.MsgBitfield {
					t.Fatalf("initial message=%v, err=%v", msg, err)
				}
			}
			if phase == "have write" {
				<-gate.writing // Consume the initial bitfield write notification.
				if err := s.markComplete(0); err != nil {
					t.Fatal(err)
				}
			}
			entered := gate.writing
			if phase == "idle read" {
				entered = gate.reading
			}
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("peer did not reach expected I/O operation")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("error=%v, want context.Canceled", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cancellation did not unblock peer I/O")
			}
		})
	}
}

func TestSessionServePeerBroadcastsAvailabilityAndServesNewPieces(t *testing.T) {
	dst := &blockTestStorage{data: []byte("abcdefghijklmnop")}
	s := blockTestSession(t, dst, 4)
	if err := s.markComplete(0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	remotes := make([]net.Conn, 0, 2)
	for range 2 {
		local, remote := net.Pipe()
		gate := &serveGateConn{Conn: local, reading: make(chan struct{}, 1), writing: make(chan struct{}, 1)}
		if err := remote.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cancel(); local.Close(); remote.Close() })
		c := &peer.Client{Conn: gate, PieceCount: 4, Bitfield: peer.Bitfield{0}, AmChoking: true}
		done := make(chan error, 1)
		go func() { done <- s.ServePeer(ctx, c) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("peer handler did not terminate")
			}
		})
		select {
		case <-gate.writing:
		case <-time.After(time.Second):
			t.Fatal("initial bitfield write did not start")
		}
		remotes = append(remotes, remote)
	}
	// Both initial snapshots already exist, but their writes are blocked. These
	// completions must be coalesced without losing either piece or blocking download.
	if err := s.markComplete(1); err != nil {
		t.Fatal(err)
	}
	if err := s.markComplete(2); err != nil {
		t.Fatal(err)
	}
	expect := func(conn net.Conn, id peer.MessageID, payload []byte) {
		t.Helper()
		msg, err := peer.ReadMessage(conn)
		if err != nil || msg == nil || msg.ID != id || !bytes.Equal(msg.Payload, payload) {
			t.Fatalf("message=%v, err=%v; want ID=%d payload=%x", msg, err, id, payload)
		}
	}
	for _, remote := range remotes {
		expect(remote, peer.MsgBitfield, []byte{0x80})
		expect(remote, peer.MsgHave, []byte{0, 0, 0, 1})
		expect(remote, peer.MsgHave, []byte{0, 0, 0, 2})
	}
	// A subsequent completion checks that both peers switched notification channels.
	if err := s.markComplete(3); err != nil {
		t.Fatal(err)
	}
	for _, remote := range remotes {
		expect(remote, peer.MsgHave, []byte{0, 0, 0, 3})
		if _, err := remote.Write([]byte{0, 0, 0, 1, 2}); err != nil {
			t.Fatal(err)
		}
		// Any repeated have would appear here instead of the expected unchoke.
		expect(remote, peer.MsgUnchoke, nil)
		if _, err := remote.Write([]byte{0, 0, 0, 13, 6, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4}); err != nil {
			t.Fatal(err)
		}
		expect(remote, peer.MsgPiece, []byte{0, 0, 0, 3, 0, 0, 0, 0, 'm', 'n', 'o', 'p'})
		// A subsequent response confirms the previous upload handler, including its
		// counter update after Write returns, has finished.
		if _, err := remote.Write([]byte{0, 0, 0, 1, 3}); err != nil {
			t.Fatal(err)
		}
		expect(remote, peer.MsgChoke, nil)
	}
	if got := s.Stats().Uploaded; got != 8 {
		t.Errorf("Uploaded=%d, want 8", got)
	}
	// Cleanup cancels and joins both readers/handlers before test state is released.
}
