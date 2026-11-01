package transfer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/peer"
)

type peerQueue struct {
	pending []net.TCPAddr
	queue   map[string]struct{}
	lock    sync.Mutex
	ready   chan struct{}
}

func newPeerQueue() *peerQueue {
	return &peerQueue{
		queue: make(map[string]struct{}),
		ready: make(chan struct{}, 1),
	}
}

func (q *peerQueue) Add(addrs []net.TCPAddr) {
	q.lock.Lock()
	defer q.lock.Unlock()

	inserted := false
	for _, addr := range addrs {
		if _, exists := q.queue[addr.String()]; exists {
			continue
		}
		inserted = true

		q.pending = append(q.pending, addr)
		q.queue[addr.String()] = struct{}{}
	}

	if inserted {
		select {
		case q.ready <- struct{}{}:
		default:
		}
	}
}

func (q *peerQueue) Next() (net.TCPAddr, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if len(q.pending) == 0 {
		return net.TCPAddr{}, false
	}

	addr := q.pending[0]
	q.pending = q.pending[1:]
	delete(q.queue, addr.String())

	return addr, true
}

func (q *peerQueue) WaitNext(ctx context.Context) (net.TCPAddr, error) {
	for {
		err := ctx.Err()
		if err != nil {
			return net.TCPAddr{}, err
		}

		addr, next := q.Next()
		if next {
			return addr, nil
		}

		select {
		case <-ctx.Done():
			return net.TCPAddr{}, ctx.Err()
		case <-q.ready:
		}
	}
}

func waitForPeer(
	ctx context.Context,
	refresh chan<- struct{},
	peers *peerQueue,
	waitTimeout time.Duration,
) (net.TCPAddr, error) {
	err := ctx.Err()
	if err != nil {
		return net.TCPAddr{}, err
	}

	peer, next := peers.Next()
	if next {
		return peer, nil
	}

	select {
	case refresh <- struct{}{}:
	default:
	}

	childCtx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()

	return peers.WaitNext(childCtx)
}

func preparePeerBatch(
	ctx context.Context,
	peers *peerQueue,
	local peer.Handshake,
	pieceCount int,
	workerLimit int,
) ([]*peer.Client, error) {
	if workerLimit < 1 {
		return nil, fmt.Errorf("worker limit must be greater than 0")
	}

	results := make([]*peer.Client, 0, workerLimit)

	for range workerLimit {
		if cancelled := ctx.Err(); cancelled != nil {
			for _, client := range results {
				client.Conn.Close()
			}
			return nil, cancelled
		}

		addr, next := peers.Next()
		if !next {
			break
		}

		client, err := preparePeer(addr, local, pieceCount)
		if err != nil {
			if _, ok := errors.AsType[*download.PeerError](err); ok {
				log.Printf("error establishing peer: %v", err)

				continue
			} else {
				for _, client := range results {
					client.Conn.Close()
				}

				return nil, err
			}
		}

		results = append(results, client)
	}

	if cancelled := ctx.Err(); cancelled != nil {
		for _, client := range results {
			client.Conn.Close()
		}
		return nil, cancelled
	}

	return results, nil
}

func preparePeer(addr net.TCPAddr, local peer.Handshake, pieceCount int) (*peer.Client, error) {
	conn, received, err := peer.Connect(addr, local)
	if err != nil {
		return nil, &download.PeerError{
			Op:  fmt.Sprintf("connect to peer at addr %s", addr.String()),
			Err: err,
		}
	}

	log.Printf("recieved peerID: %x", received.PeerID)

	client, err := peer.NewClient(conn, *received, pieceCount)
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = client.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		conn.Close()
		return nil, &download.PeerError{
			Op:  "set unchoke deadline",
			Err: err,
		}
	}
	err = client.WaitForUnchoke()
	if err != nil {
		conn.Close()
		return nil, &download.PeerError{
			Op:  "wait for unchoke",
			Err: err,
		}
	}
	err = client.Conn.SetDeadline(time.Time{})
	if err != nil {
		conn.Close()
		return nil, &download.PeerError{
			Op:  "clear unchoke deadline",
			Err: err,
		}
	}

	return client, nil
}

func (e *Engine) downloadFromPeers(
	ctx context.Context,
	refresh chan<- struct{},
	peers *peerQueue,
	local peer.Handshake,
	pieceCount int,
	session *download.Session,
) error {
	const attemptsAllowed = 3

	attempts := 0
	left := session.Stats().Left
	for {
		newLeft := session.Stats().Left
		if newLeft < left {
			left = newLeft
			attempts = 0
		}

		err := ctx.Err()
		if err != nil {
			return err
		}

		if session.Complete() {
			return nil
		}

		var peerErr *download.PeerError

		e.setPhase(PhaseConnecting)
		preparedPeers, err := preparePeerBatch(ctx, peers, local, pieceCount, e.config.Workers)
		if err != nil {
			return err
		}
		if len(preparedPeers) > 0 {
			for _, client := range preparedPeers {
				client.Conn = e.trackPeer(client.Conn)
			}

			e.setPhase(PhaseDownloading)
			err := session.DownloadFromPeers(ctx, preparedPeers, e.config.Workers)
			if err != nil {
				retryable := errors.As(err, &peerErr)
				if retryable {
					log.Printf("error downloading from peers: %s", err)
					continue
				} else {
					return err
				}
			} else {
				continue
			}
		}

		addr, next := peers.Next()
		if !next {
			if attempts == attemptsAllowed {
				return fmt.Errorf("exceed peer discovery attempts")
			}
			attempts += 1

			e.setPhase(PhaseWaiting)
			addr, err = waitForPeer(ctx, refresh, peers, e.config.PeerWaitTimeout)
			if err != nil {
				if cancelled := ctx.Err(); cancelled != nil {
					return cancelled
				}
				if err == context.DeadlineExceeded {
					continue
				}

				return err
			}
		}
		peers.Add([]net.TCPAddr{addr})

		if session.Complete() {
			return nil
		}
	}
}
