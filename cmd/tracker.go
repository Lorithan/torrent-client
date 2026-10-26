package main

import (
	"context"
	"log"
	"time"

	"github.com/lorithan/torrent-client/internal/tracker"
)

func runTrackerUpdates(
	ctx context.Context,
	interval time.Duration,
	announce func() (*tracker.TrackerResponse, error),
) {
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			response, err := announce()
			if err != nil {
				log.Printf("regular announce error: %v", err)
			} else {
				interval = response.Interval
			}
		}
		timer.Reset(interval)
	}
}
