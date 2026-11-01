package transfer

import (
	"net"
	"sync"
)

type trackedConn struct {
	net.Conn
	once     sync.Once
	closeErr error
	onClose  func()
}

func (tc *trackedConn) Close() error {
	tc.once.Do(func() {
		tc.closeErr = tc.Conn.Close()
		if tc.onClose != nil {
			tc.onClose()
		}
	})

	return tc.closeErr
}

func (e *Engine) trackPeer(conn net.Conn) net.Conn {
	e.mu.Lock()
	e.status.ActivePeers++
	e.mu.Unlock()

	onClose := func() {
		e.mu.Lock()
		e.status.ActivePeers--
		e.mu.Unlock()
	}

	return &trackedConn{
		Conn:    conn,
		onClose: onClose,
	}
}
