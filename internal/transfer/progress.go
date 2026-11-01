package transfer

import (
	"context"
	"time"

	"github.com/lorithan/torrent-client/internal/download"
)

func (e *Engine) startProgressUpdates(
	ctx context.Context,
	session *download.Session,
	interval time.Duration,
) func() {
	childCtx, childCancel := context.WithCancel(ctx)
	completion := make(chan struct{})
	ticker := time.NewTicker(interval)

	currentTime := time.Now()
	currentStats := session.Progress()

	go func() {
		defer close(completion)
		defer ticker.Stop()
		for {
			select {
			case <-childCtx.Done():
				return
			case <-ticker.C:
				newStats := session.Progress()
				newTime := time.Now()
				elapsed := newTime.Sub(currentTime)

				downloadRate, uploadRate := calculateRates(currentStats.Stats, newStats.Stats, elapsed)

				e.mu.Lock()
				e.status.DownloadRate = downloadRate
				e.status.UploadRate = uploadRate
				e.status.Progress = newStats
				e.mu.Unlock()

				currentStats = newStats
				currentTime = newTime
			}
		}
	}()

	return func() {
		childCancel()
		<-completion
	}
}

func calculateRates(
	previous download.Stats,
	current download.Stats,
	elapsed time.Duration,
) (float64, float64) {
	if elapsed <= 0 {
		return 0, 0
	}

	downloadRate := float64(current.Downloaded-previous.Downloaded) / elapsed.Seconds()
	uploadRate := float64(current.Uploaded-previous.Uploaded) / elapsed.Seconds()
	return downloadRate, uploadRate
}
