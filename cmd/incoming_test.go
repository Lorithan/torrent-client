package main

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

	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

type incomingTestConn struct {
	net.Conn
	reader                *bytes.Reader
	wire                  bytes.Buffer
	deadlines             []time.Time
	deadlineFailAt        int
	err                   error
	writeErr              error
	reads, writes, closes int
}

func (c *incomingTestConn) SetDeadline(d time.Time) error {
	c.deadlines = append(c.deadlines, d)
	if len(c.deadlines) == c.deadlineFailAt {
		return c.err
	}
	return nil
}
func (c *incomingTestConn) Read(p []byte) (int, error) {
	c.reads++
	if len(c.deadlines) == 0 {
		return 0, errors.New("read before handshake deadline")
	}
	return c.reader.Read(p)
}
func (c *incomingTestConn) Write(p []byte) (int, error) {
	c.writes++
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return c.wire.Write(p)
}
func (c *incomingTestConn) Close() error { c.closes++; return nil }

func TestPrepareIncomingPeer(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1, 2, 3}, PeerID: [20]byte{'l'}}
	remote := local
	remote.PeerID = [20]byte{'r'}
	remote.ReservedFlags = [8]byte{1}
	conn := &incomingTestConn{reader: bytes.NewReader(remote.Serialize())}
	before := time.Now()
	c, err := prepareIncomingPeer(conn, local, 10)
	after := time.Now()
	if err != nil || c == nil {
		t.Fatalf("prepareIncomingPeer()=(%v, %v)", c, err)
	}
	if c.Conn != conn || c.Handshake != remote || c.PieceCount != 10 {
		t.Errorf("client did not preserve connection, remote identity, and piece count: %+v", c)
	}
	if !c.Choked || !c.AmChoking || c.PeerInterested || !bytes.Equal(c.Bitfield, []byte{0, 0}) {
		t.Errorf("unexpected initial state: %+v", c)
	}
	if !bytes.Equal(conn.wire.Bytes(), local.Serialize()) {
		t.Errorf("handshake reply=%x", conn.wire.Bytes())
	}
	if conn.closes != 0 {
		t.Error("successful setup closed caller's connection")
	}
	if len(conn.deadlines) != 2 {
		t.Fatalf("deadline calls=%d, want 2", len(conn.deadlines))
	}
	if conn.deadlines[0].Before(before.Add(10*time.Second)) || conn.deadlines[0].After(after.Add(10*time.Second)) {
		t.Error("initial deadline does not allow 10 seconds")
	}
	if !conn.deadlines[1].IsZero() {
		t.Error("handshake deadline was not cleared")
	}
}

func TestPrepareIncomingPeerClosesOnFailure(t *testing.T) {
	local := peer.Handshake{InfoHash: [20]byte{1}}
	wire := local.Serialize()
	wrong := bytes.Clone(wire)
	wrong[28] ^= 0xff
	sentinel := errors.New("incoming setup failed")
	for _, tc := range []struct {
		name              string
		wire              []byte
		deadlineFailAt    int
		writeErr          error
		pieceCount        int
		wantErr           error
		wantDeadlineCalls int
	}{
		{"set deadline", wire, 1, nil, 1, sentinel, 1},
		{"truncated handshake", wire[:40], 0, nil, 1, io.ErrUnexpectedEOF, 1},
		{"wrong torrent", wrong, 0, nil, 1, nil, 1},
		{"reply failure", wire, 0, sentinel, 1, sentinel, 1},
		{"clear deadline", wire, 2, nil, 1, sentinel, 2},
		{"constructor failure", wire, 0, nil, -1, nil, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &incomingTestConn{reader: bytes.NewReader(tc.wire), deadlineFailAt: tc.deadlineFailAt, err: sentinel, writeErr: tc.writeErr}
			c, err := prepareIncomingPeer(conn, local, tc.pieceCount)
			if c != nil || err == nil {
				t.Fatalf("prepareIncomingPeer()=(%v, %v), want nil client and error", c, err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("error=%v, want underlying %v", err, tc.wantErr)
			}
			if conn.closes != 1 {
				t.Errorf("Close calls=%d, want 1", conn.closes)
			}
			if len(conn.deadlines) != tc.wantDeadlineCalls {
				t.Errorf("deadline calls=%d, want %d", len(conn.deadlines), tc.wantDeadlineCalls)
			}
			if tc.deadlineFailAt == 1 && (conn.reads != 0 || conn.writes != 0) {
				t.Error("performed I/O after deadline setup failed")
			}
			if (tc.name == "truncated handshake" || tc.name == "wrong torrent") && conn.writes != 0 {
				t.Error("replied to invalid handshake")
			}
		})
	}
}

func TestServeIncomingPeerAlreadyCanceledClosesConnection(t *testing.T) {
	local, remote := net.Pipe()
	t.Cleanup(func() { local.Close(); remote.Close() })
	if err := remote.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := serveIncomingPeer(ctx, local, peer.Handshake{}, 0, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("error=%v, want context.Canceled", err)
	}
	var data [1]byte
	if _, err := remote.Read(data[:]); !errors.Is(err, io.EOF) {
		t.Errorf("connection after return: %v, want EOF", err)
	}
}

type incomingReadGateConn struct {
	net.Conn
	reading chan struct{}
}

func (c *incomingReadGateConn) Read(p []byte) (int, error) {
	select {
	case c.reading <- struct{}{}:
	default:
	}
	return c.Conn.Read(p)
}

func TestServeIncomingPeerCancellationDuringHandshake(t *testing.T) {
	local, remote := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); local.Close(); remote.Close() })
	conn := &incomingReadGateConn{Conn: local, reading: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() { done <- serveIncomingPeer(ctx, conn, peer.Handshake{}, 0, nil) }()
	select {
	case <-conn.reading:
	case <-time.After(2 * time.Second):
		t.Fatal("handshake read did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not interrupt handshake")
	}
}

func TestServeIncomingPeerHandsOffToSession(t *testing.T) {
	session, err := download.NewSession(&torrent.TorrentFile{PieceLength: 4}, nil)
	if err != nil {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); local.Close(); remote.Close() })
	if err := remote.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	handshake := peer.Handshake{InfoHash: [20]byte{1}, PeerID: [20]byte{'l'}}
	remoteHandshake := handshake
	remoteHandshake.PeerID = [20]byte{'r'}
	done := make(chan error, 1)
	go func() { done <- serveIncomingPeer(ctx, local, handshake, 0, session) }()
	if _, err := remote.Write(remoteHandshake.Serialize()); err != nil {
		t.Fatal(err)
	}
	reply, err := peer.ReadHandshake(remote)
	if err != nil || reply == nil || *reply != handshake {
		t.Fatalf("reply=%+v, err=%v", reply, err)
	}
	msg, err := peer.ReadMessage(remote)
	if err != nil || msg == nil || msg.ID != peer.MsgBitfield || len(msg.Payload) != 0 {
		t.Fatalf("initial availability=%v, err=%v", msg, err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serving did not stop after cancellation")
	}
}

type incomingTestListener struct {
	connections chan net.Conn
	fail        chan error
	closed      chan struct{}
	once        sync.Once
}

func newIncomingTestListener() *incomingTestListener {
	return &incomingTestListener{connections: make(chan net.Conn), fail: make(chan error), closed: make(chan struct{})}
}
func (l *incomingTestListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connections:
		return conn, nil
	case err := <-l.fail:
		return nil, err
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *incomingTestListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *incomingTestListener) Addr() net.Addr { return &net.TCPAddr{} }

func offerIncomingTestConn(t *testing.T, listener *incomingTestListener, conn net.Conn) {
	t.Helper()
	select {
	case listener.connections <- conn:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not accept connection")
	}
}

func expectIncomingTestEOF(t *testing.T, conn net.Conn) {
	t.Helper()
	// net.Pipe can reject setting a deadline after its other endpoint closes.
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	var data [1]byte
	if _, err := conn.Read(data[:]); !errors.Is(err, io.EOF) {
		t.Errorf("read error=%v, want EOF", err)
	}
}

func TestServeIncomingPeersBoundsHandshakesAndJoinsOnExit(t *testing.T) {
	for _, acceptFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("acceptFailure=%t", acceptFails), func(t *testing.T) {
			listener := newIncomingTestListener()
			ctx, cancel := context.WithCancel(context.Background())
			first, remoteFirst := net.Pipe()
			second, remoteSecond := net.Pipe()
			t.Cleanup(func() {
				cancel()
				listener.Close()
				first.Close()
				remoteFirst.Close()
				second.Close()
				remoteSecond.Close()
			})
			gate := &incomingReadGateConn{Conn: first, reading: make(chan struct{}, 1)}
			done := make(chan error, 1)
			go func() { done <- serveIncomingPeers(ctx, listener, peer.Handshake{}, 0, nil, 1) }()
			offerIncomingTestConn(t, listener, gate)
			select {
			case <-gate.reading:
			case <-time.After(2 * time.Second):
				t.Fatal("first handshake did not start")
			}
			offerIncomingTestConn(t, listener, second)
			expectIncomingTestEOF(t, remoteSecond)
			wantErr := error(context.Canceled)
			if acceptFails {
				wantErr = errors.New("listener failed")
				select {
				case listener.fail <- wantErr:
				case <-time.After(2 * time.Second):
					t.Fatal("server stopped accepting after rejecting excess peer")
				}
			} else {
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, wantErr) {
					t.Errorf("error=%v, want %v", err, wantErr)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("server did not join handshake handler during shutdown")
			}
			expectIncomingTestEOF(t, remoteFirst)
			select {
			case <-listener.closed:
			default:
				t.Error("server left listener open")
			}
		})
	}
}

func TestServeIncomingPeersInvalidLimitClosesListener(t *testing.T) {
	for _, limit := range []int{0, -1} {
		listener := newIncomingTestListener()
		if err := serveIncomingPeers(context.Background(), listener, peer.Handshake{}, 0, nil, limit); err == nil {
			t.Errorf("limit=%d returned nil error", limit)
		}
		select {
		case <-listener.closed:
		default:
			t.Errorf("limit=%d left listener open", limit)
		}
	}
}
