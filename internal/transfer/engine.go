package transfer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/lorithan/torrent-client/internal/download"
)

type Engine struct {
	config  Config
	status  Status
	started bool
	mu      sync.Mutex
}

func New(config Config) (*Engine, error) {
	if config.TorrentPath == "" {
		return nil, fmt.Errorf("torrent path cannot be empty")
	}
	if config.OutputPath == "" {
		return nil, fmt.Errorf("output path cannot be empty")
	}
	if config.Workers < 1 {
		return nil, fmt.Errorf("workers must be greater than 0")
	}
	if config.PeerWaitTimeout <= 0 {
		return nil, fmt.Errorf("peer wait timeout must be greater than 0")
	}

	return &Engine{
		config: config,
		status: Status{
			Phase: PhaseStarting,
		},
	}, nil
}

func (e *Engine) Run(ctx context.Context) error {
	e.mu.Lock()

	if e.started {
		e.mu.Unlock()
		return fmt.Errorf("engine already started")
	}

	e.started = true
	e.mu.Unlock()

	if cancelled := ctx.Err(); cancelled != nil {
		e.finish(cancelled)
		return cancelled
	}

	err := e.run(ctx)
	e.finish(err)

	return err
}

func (e *Engine) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.status
}

func (e *Engine) setPhase(phase Phase) {
	e.mu.Lock()
	e.status.Phase = phase
	e.mu.Unlock()
}

func (e *Engine) finish(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err == nil || errors.Is(err, context.Canceled) {
		e.status.Phase = PhaseStopped
		e.status.Err = nil
	} else {
		e.status.Phase = PhaseFailed
		e.status.Err = err
	}

	e.status.ActivePeers = 0
	e.status.DownloadRate = 0
	e.status.UploadRate = 0
}

func (e *Engine) updateProgress(progress download.Progress) {
	e.mu.Lock()
	e.status.Progress = progress
	e.mu.Unlock()
}
