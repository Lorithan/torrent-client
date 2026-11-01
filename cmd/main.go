package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lorithan/torrent-client/internal/transfer"
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

	config := transfer.Config{
		TorrentPath:     *torrentPath,
		OutputPath:      *output,
		Port:            port,
		Workers:         *workers,
		Seed:            *seed,
		PeerWaitTimeout: waitTimeout,
	}

	engine, err := transfer.New(config)
	if err != nil {
		return err
	}

	return engine.Run(ctx)
}
