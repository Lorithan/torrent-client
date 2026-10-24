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
	client := http.Client{
		Timeout: 15 * time.Second,
	}

	response, err := tracker.Announce(torrentData, peerID, uint16(addr.Port), &client)
	if err != nil {
		return err
	}

	log.Printf("interval: %s, peers: %v", response.Interval, response.Peers)

	return nil
}
