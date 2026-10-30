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

func prepareIncomingPeer(
	conn net.Conn,
	local peer.Handshake,
	pieceCount int,
) (*peer.Client, error) {
	err := conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		conn.Close()
		return nil, err
	}
	remote, err := peer.AcceptHandshake(conn, local)
	if err != nil {
		conn.Close()
		return nil, err
	}
	err = conn.SetDeadline(time.Time{})
	if err != nil {
		conn.Close()
		return nil, err
	}

	client, err := peer.NewClient(conn, *remote, pieceCount)
	if err != nil {
		conn.Close()
	}
	return client, err
}

func serveIncomingPeer(
	ctx context.Context,
	conn net.Conn,
	local peer.Handshake,
	pieceCount int,
	session *download.Session,
) error {
	stop := context.AfterFunc(ctx, func() {
		conn.Close()
	})
	defer stop()

	if cancelled := ctx.Err(); cancelled != nil {
		conn.Close()
		return cancelled
	}

	client, err := prepareIncomingPeer(conn, local, pieceCount)
	if err != nil {
		if cancelled := ctx.Err(); cancelled != nil {
			return cancelled
		}
		return err
	}

	return session.ServePeer(ctx, client)
}

func serveIncomingPeers(
	ctx context.Context,
	listener net.Listener,
	local peer.Handshake,
	pieceCount int,
	session *download.Session,
	maxPeers int,
) error {
	if maxPeers <= 0 {
		listener.Close()
		return fmt.Errorf("max peers should be greater than 0")
	}

	childCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	slots := make(chan struct{}, maxPeers)

	stop := context.AfterFunc(childCtx, func() {
		listener.Close()
	})
	defer stop()

	cleanup := func() {
		cancel()
		listener.Close()
		wg.Wait()
	}
	defer cleanup()

	for {
		conn, err := listener.Accept()
		if err != nil {
			cancelled := childCtx.Err()
			if cancelled != nil {
				return cancelled
			}
			return err
		}

		cancelled := childCtx.Err()
		if cancelled != nil {
			conn.Close()
			return cancelled
		}

		select {
		case slots <- struct{}{}:
			wg.Go(func() {
				release := func() {
					<-slots
				}
				defer release()

				err := serveIncomingPeer(childCtx, conn, local, pieceCount, session)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						log.Printf("error serving peer: %v", err)
					}
				}
			})
		default:
			conn.Close()
		}
	}
}
