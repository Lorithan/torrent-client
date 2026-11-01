package tui

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/transfer"
)

var _ Runner = (*managedRunner)(nil)

func TestManagedRunnerSharesOneExecutionAndResult(t *testing.T) {
	wantErr := errors.New("transfer failed after cleanup")
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	firstCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fake := &fakeRunner{run: func(ctx context.Context) error {
		if ctx != firstCtx {
			t.Error("underlying runner received a later caller's context")
		}
		close(started)
		select {
		case <-release:
			return wantErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	r := &managedRunner{Runner: fake}
	const laterCallers = 32
	results := make(chan error, laterCallers+1)
	go func() { results <- r.Run(firstCtx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first execution did not start")
	}
	for range laterCallers {
		go func() { results <- r.Run(context.Background()) }()
	}
	select {
	case err := <-results:
		t.Fatalf("Run returned before underlying execution completed: %v", err)
	default:
	}
	unblock()
	for range laterCallers + 1 {
		select {
		case err := <-results:
			if !errors.Is(err, wantErr) {
				t.Errorf("Run()=%v, want shared result %v", err, wantErr)
			}
		case <-time.After(time.Second):
			t.Fatal("caller did not receive completed result")
		}
	}
	if fake.runCalls.Load() != 1 {
		t.Errorf("underlying executions=%d, want 1", fake.runCalls.Load())
	}
	if err := r.Run(context.Background()); !errors.Is(err, wantErr) {
		t.Errorf("later call=%v, want saved failure", err)
	}
}

func TestManagedRunnerCancellationAndLiveStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	started := make(chan struct{})
	fake := &fakeRunner{status: transfer.Status{Phase: transfer.PhaseStarting}, run: func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	r := &managedRunner{Runner: fake}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("execution did not start")
	}
	fake.mu.Lock()
	fake.status = transfer.Status{Phase: transfer.PhaseDownloading, ActivePeers: 2}
	fake.mu.Unlock()
	if got := r.Status(); got.Phase != transfer.PhaseDownloading || got.ActivePeers != 2 {
		t.Errorf("delegated status=%+v", got)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run()=%v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not finish execution")
	}
	if err := r.Run(context.Background()); !errors.Is(err, context.Canceled) {
		t.Errorf("later call=%v, want saved cancellation", err)
	}
	if fake.runCalls.Load() != 1 {
		t.Error("later call restarted canceled transfer")
	}
}
