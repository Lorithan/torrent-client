package transfer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
	"github.com/lorithan/torrent-client/internal/tracker"
)

func (e *Engine) run(ctx context.Context) error {
	torrentFile, err := os.ReadFile(e.config.TorrentPath)
	if err != nil {
		return err
	}

	torrentData, err := torrent.ParseTorrentFile(torrentFile)
	if err != nil {
		return err
	}

	log.Printf("Torrent file read. Name: '%s', file length: %d, piece count: %d", torrentData.Name, torrentData.Length, len(torrentData.Pieces))

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", e.config.Port))
	if err != nil {
		return err
	}
	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return fmt.Errorf("unable to get port")
	}

	log.Printf("current addr: %d", addr.Port)

	file, err := os.OpenFile(e.config.OutputPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	session, err := download.NewSession(torrentData, file)
	if err != nil {
		return err
	}
	e.updateProgress(session.Progress())

	updateProgress := func() {
		e.updateProgress(session.Progress())
	}
	defer updateProgress()

	stopProgressUpdate := e.startProgressUpdates(ctx, session, 250*time.Millisecond)
	defer stopProgressUpdate()

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

	e.setPhase(PhaseDiscovering)
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
		incomingErr = e.serveIncomingPeers(incomingCtx, listener, local, len(torrentData.Pieces), session, 8)
		if incomingErr != nil && !errors.Is(incomingErr, context.Canceled) {
			cancel()
		}
	}()
	stopIncoming := func() {
		cancelIncoming()
		<-incomingDone
	}
	defer stopIncoming()

	err = e.downloadFromPeers(ctx, refreshPeer, peers, local, len(torrentData.Pieces), session)
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

	if e.config.Seed {
		e.setPhase(PhaseSeeding)
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
