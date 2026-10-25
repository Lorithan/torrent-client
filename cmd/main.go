package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
	"github.com/lorithan/torrent-client/internal/tracker"
)

func main() {
	if err := run(); err != nil {
		log.Printf("error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	torrentPath := flag.String("torrent", "", "location of torrent file")
	output := flag.String("output", "", "destination of download")
	nPort := flag.Int("port", 0, "port number to listen on")

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

	log.Printf("interval: %s, peers: %v", response.Interval, response.Peers)

	local := peer.Handshake{
		InfoHash: torrentData.InfoHash,
		PeerID:   peerID,
	}

	file, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	session, err := download.NewSession(torrentData, file)
	if err != nil {
		return err
	}

	err = downloadFromPeers(response.Peers, local, len(torrentData.Pieces), session)
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	log.Printf("download complete")

	return nil
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
	addrs []net.TCPAddr,
	local peer.Handshake,
	pieceCount int,
	session *download.Session,
) error {
	if session.Complete() {
		return nil
	}

	var peerErr *download.PeerError
	for _, addr := range addrs {
		client, err := preparePeer(addr, local, pieceCount)
		if err != nil {
			retryable := errors.As(err, &peerErr)
			if retryable {
				log.Printf("error preparing peer %s: %s", addr.String(), err)
				continue
			} else {
				return err
			}
		}

		err = session.DownloadFrom(client)
		client.Conn.Close()
		if err != nil {
			retryable := errors.As(err, &peerErr)
			if retryable {
				log.Printf("download from peer %s failed: %s", addr.String(), err)
			} else {
				return err
			}
		}

		if session.Complete() {
			return nil
		}
	}

	return fmt.Errorf("exhausted peers before session completed")
}
