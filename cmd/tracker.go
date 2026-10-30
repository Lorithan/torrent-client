package main

import (
	"context"
	"log"
	"time"

	"github.com/lorithan/torrent-client/internal/tracker"
)

func runTrackerUpdates(
	ctx context.Context,
	refresh <-chan struct{},
	interval time.Duration,
	minDelay time.Duration,
	announce func() (*tracker.TrackerResponse, error),
) {
	timer := time.NewTimer(interval)
	defer timer.Stop()

	nextAllowed := time.Now().Add(minDelay)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-refresh:
		}
		timeLeft := time.Until(nextAllowed)
		if timeLeft > 0 {
			cooldownTimer := time.NewTimer(timeLeft)
			select {
			case <-ctx.Done():
				cooldownTimer.Stop()
				return
			case <-cooldownTimer.C:
				cooldownTimer.Stop()
			}
		}

		err := ctx.Err()
		if err != nil {
			return
		}

		response, err := announce()
		nextAllowed = time.Now().Add(minDelay)
		if err != nil {
			log.Printf("regular announce error: %v", err)
		} else {
			interval = response.Interval
		}
		timer.Reset(interval)
	}
}

func startTrackerUpdates(
	ctx context.Context,
	refresh <-chan struct{},
	interval time.Duration,
	minDelay time.Duration,
	announce func() (*tracker.TrackerResponse, error),
) func() {
	childCtx, cancelChild := context.WithCancel(ctx)
	completion := make(chan struct{})

	go func() {
		runTrackerUpdates(childCtx, refresh, interval, minDelay, announce)
		close(completion)
	}()

	return func() {
		cancelChild()
		<-completion
	}
}
