package main

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
