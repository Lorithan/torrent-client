package transfer

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
)

var _ net.Conn = (*trackedConn)(nil)

type trackedTestConn struct {
	net.Conn
	closes atomic.Int32
	err    error
}

func (c *trackedTestConn) Close() error { c.closes.Add(1); return c.err }

func TestTrackedConnConcurrentClose(t *testing.T) {
	wantErr := errors.New("underlying close failed")
	underlying := &trackedTestConn{err: wantErr}
	var callbacks atomic.Int32
	c := &trackedConn{Conn: underlying, onClose: func() {
		if underlying.closes.Load() != 1 {
			t.Error("callback ran before underlying close")
		}
		callbacks.Add(1)
	}}
	var callers sync.WaitGroup
	for range 32 {
		callers.Go(func() {
			if err := c.Close(); !errors.Is(err, wantErr) {
				t.Errorf("Close()=%v, want original %v", err, wantErr)
			}
		})
	}
	callers.Wait()
	if got := underlying.closes.Load(); got != 1 {
		t.Errorf("underlying closes=%d, want 1", got)
	}
	if got := callbacks.Load(); got != 1 {
		t.Errorf("callbacks=%d, want 1", got)
	}
	if err := c.Close(); !errors.Is(err, wantErr) {
		t.Errorf("later Close()=%v, want original error", err)
	}
}

func TestTrackedConnAllowsNilCallback(t *testing.T) {
	for _, err := range []error{nil, errors.New("close failure")} {
		underlying := &trackedTestConn{err: err}
		c := &trackedConn{Conn: underlying}
		for range 2 {
			if got := c.Close(); got != err {
				t.Errorf("Close()=%v, want %v", got, err)
			}
		}
		if got := underlying.closes.Load(); got != 1 {
			t.Errorf("underlying closes=%d, want 1", got)
		}
	}
}

func TestEngineTracksIndependentPeerLifetimes(t *testing.T) {
	wantErr := errors.New("close failed")
	e := &Engine{status: Status{Phase: PhaseDownloading, DownloadRate: 100}}
	first, second := &trackedTestConn{err: wantErr}, &trackedTestConn{}
	a, b := e.trackPeer(first), e.trackPeer(second)
	if got := e.Status().ActivePeers; got != 2 {
		t.Fatalf("registered peers=%d, want 2", got)
	}
	if err := a.Close(); !errors.Is(err, wantErr) {
		t.Errorf("first close=%v, want %v", err, wantErr)
	}
	if got := e.Status().ActivePeers; got != 1 {
		t.Errorf("peers after first close=%d, want 1", got)
	}
	if err := a.Close(); !errors.Is(err, wantErr) {
		t.Errorf("repeated close=%v", err)
	}
	if got := e.Status().ActivePeers; got != 1 {
		t.Errorf("repeated close changed count to %d", got)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if got := e.Status(); got != (Status{Phase: PhaseDownloading, DownloadRate: 100}) {
		t.Errorf("final status=%+v", got)
	}
	if first.closes.Load() != 1 || second.closes.Load() != 1 {
		t.Error("underlying connection closed more than once")
	}
}

func TestEngineTracksConcurrentPeers(t *testing.T) {
	e := &Engine{}
	const count = 32
	connections := make(chan net.Conn, count)
	var registrations sync.WaitGroup
	for range count {
		registrations.Go(func() { connections <- e.trackPeer(&trackedTestConn{}) })
	}
	registrations.Wait()
	close(connections)
	if got := e.Status().ActivePeers; got != count {
		t.Fatalf("registered peers=%d, want %d", got, count)
	}
	var closes sync.WaitGroup
	for conn := range connections {
		for range 2 {
			closes.Go(func() {
				if err := conn.Close(); err != nil {
					t.Error(err)
				}
			})
		}
	}
	closes.Wait()
	if got := e.Status().ActivePeers; got != 0 {
		t.Errorf("peers after concurrent closes=%d, want 0", got)
	}
}
