package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
	"github.com/lorithan/torrent-client/internal/tracker"
)

func main() {
	const waitTimeout = 45 * time.Second // Maybe later this can be a user configured param
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, waitTimeout); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("error: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, waitTimeout time.Duration) error {
	torrentPath := flag.String("torrent", "", "location of torrent file")
	output := flag.String("output", "", "destination of download")
	nPort := flag.Int("port", 0, "port number to listen on")
	workers := flag.Int("workers", 4, "number of workers dedicated to downloading")
	seed := flag.Bool("seed", true, "continue seed the torrent after download is complete")

	flag.Parse()

	if *torrentPath == "" {
		return fmt.Errorf("torrent file not specified")
	}

	if *output == "" {
		return fmt.Errorf("output destination not specified")
	}

	if *nPort < 0 || *nPort > math.MaxUint16 {
		return fmt.Errorf("port out of range")
	}
	port := uint16(*nPort)

	if *workers < 1 {
		return fmt.Errorf("workers must be greater than 0")
	}

	torrentFile, err := os.ReadFile(*torrentPath)
	if err != nil {
		return err
	}

	torrentData, err := torrent.ParseTorrentFile(torrentFile)
	if err != nil {
		return err
	}

	log.Printf("Torrent file read. Name: '%s', file length: %d, piece count: %d", torrentData.Name, torrentData.Length, len(torrentData.Pieces))

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return fmt.Errorf("unable to get port")
	}

	log.Printf("current addr: %d", addr.Port)

	file, err := os.OpenFile(*output, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	session, err := download.NewSession(torrentData, file)
	if err != nil {
		return err
	}

	peerID := tracker.GeneratePeerID()
	httpClient := http.Client{
		Timeout: 15 * time.Second,
	}

	params := tracker.AnnounceParams{
		Downloaded: 0,
		Uploaded:   0,
		Left:       torrentData.Length,
		Event:      tracker.AnnounceEventStarted,
	}

	response, err := tracker.Announce(torrentData, peerID, uint16(addr.Port), &httpClient, params)
	if err != nil {
		return err
	}

	peers := newPeerQueue()
	peers.Add(response.Peers)

	regularAnnounce := func() (*tracker.TrackerResponse, error) {
		stats := session.Stats()
		params := tracker.AnnounceParams{
			Downloaded: stats.Downloaded,
			Uploaded:   stats.Uploaded,
			Left:       stats.Left,
			Event:      tracker.AnnounceEventDefault,
		}
		response, err := tracker.Announce(torrentData, peerID, uint16(addr.Port), &httpClient, params)
		if err != nil {
			return response, err
		}
		peers.Add(response.Peers)
		return response, err
	}

	defer func() {
		stats := session.Stats()
		params := tracker.AnnounceParams{
			Downloaded: stats.Downloaded,
			Uploaded:   stats.Uploaded,
			Left:       stats.Left,
			Event:      tracker.AnnounceEventStopped,
		}
		_, err := tracker.Announce(torrentData, peerID, uint16(addr.Port), &httpClient, params)
		if err != nil {
			log.Printf("stopped announce error: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	refreshPeer := make(chan struct{}, 1)
	minDelay := 5 * time.Second
	stopUpdates := startTrackerUpdates(ctx, refreshPeer, response.Interval, minDelay, regularAnnounce)
	defer stopUpdates()

	log.Printf("interval: %s, peers: %v", response.Interval, response.Peers)

	local := peer.Handshake{
		InfoHash: torrentData.InfoHash,
		PeerID:   peerID,
	}

	incomingCtx, cancelIncoming := context.WithCancel(ctx)
	var incomingErr error
	incomingDone := make(chan struct{})
	go func() {
		defer close(incomingDone)
		incomingErr = serveIncomingPeers(incomingCtx, listener, local, len(torrentData.Pieces), session, 8)
		if incomingErr != nil && !errors.Is(incomingErr, context.Canceled) {
			cancel()
		}
	}()
	stopIncoming := func() {
		cancelIncoming()
		<-incomingDone
	}
	defer stopIncoming()

	err = downloadFromPeers(ctx, refreshPeer, peers, local, len(torrentData.Pieces), *workers, session, waitTimeout)
	stopUpdates()
	if err != nil {
		stopIncoming()
		if incomingErr != nil && !errors.Is(incomingErr, context.Canceled) {
			return incomingErr
		}
		return err
	}

	if torrentData.Length > 0 {
		stats := session.Stats()
		params = tracker.AnnounceParams{
			Uploaded:   stats.Uploaded,
			Downloaded: stats.Downloaded,
			Left:       stats.Left,
			Event:      tracker.AnnounceEventCompleted,
		}

		_, err = tracker.Announce(torrentData, peerID, uint16(addr.Port), &httpClient, params)
		if err != nil {
			log.Printf("complete announce error: %s", err)
		}
	}

	log.Printf("download complete")

	if *seed {
		stopSeedingUpdates := startTrackerUpdates(ctx, refreshPeer, response.Interval, minDelay, regularAnnounce)
		defer stopSeedingUpdates()
		log.Print("seeding started")

		<-ctx.Done()
		stopSeedingUpdates()
	}

	stopIncoming()
	if incomingErr != nil && !errors.Is(incomingErr, context.Canceled) {
		return incomingErr
	}

	err = file.Close()
	if err != nil {
		return err
	}

	return ctx.Err()
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

func downloadFromPeers(
	ctx context.Context,
	refresh chan<- struct{},
	peers *peerQueue,
	local peer.Handshake,
	pieceCount int,
	workerLimit int,
	session *download.Session,
	waitTimeout time.Duration,
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

		preparedPeers, err := preparePeerBatch(ctx, peers, local, pieceCount, workerLimit)
		if err != nil {
			return err
		}
		if len(preparedPeers) > 0 {
			err := session.DownloadFromPeers(ctx, preparedPeers, workerLimit)
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

			addr, err = waitForPeer(ctx, refresh, peers, waitTimeout)
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
