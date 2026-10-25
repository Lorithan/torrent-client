package main

import (
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

	response, err := tracker.Announce(torrentData, peerID, uint16(addr.Port), &httpClient)
	if err != nil {
		return err
	}

	log.Printf("interval: %s, peers: %v", response.Interval, response.Peers)

	if len(response.Peers) == 0 {
		return fmt.Errorf("No peers found")
	}

	handshake := peer.Handshake{
		InfoHash: torrentData.InfoHash,
		PeerID:   peerID,
	}

	conn, receivedHandshake, err := connectToPeer(response.Peers, handshake)
	if err != nil {
		return err
	}
	defer conn.Close()

	log.Printf("recieved peerID: %x", receivedHandshake.PeerID)

	client, err := peer.NewClient(conn, *receivedHandshake, len(torrentData.Pieces))
	if err != nil {
		return err
	}

	err = client.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		return err
	}
	err = client.WaitForUnchoke()
	if err != nil {
		return err
	}
	err = client.Conn.SetDeadline(time.Time{})
	if err != nil {
		return err
	}

	file, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	err = download.Download(client, torrentData, file)
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

func connectToPeer(addrs []net.TCPAddr, local peer.Handshake) (net.Conn, *peer.Handshake, error) {
	for _, addr := range addrs {
		conn, received, err := peer.Connect(addr, local)
		if err != nil {
			log.Printf("error at addr %s: %s", &addr, err)
		} else {
			return conn, received, err
		}
	}

	return nil, nil, fmt.Errorf("no successful connection with peers")
}
