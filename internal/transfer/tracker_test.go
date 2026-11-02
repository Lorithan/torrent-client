package transfer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/tracker"
)

func startTrackerUpdatesTest(t *testing.T, interval time.Duration, announce func(context.Context) (*tracker.TrackerResponse, error)) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	return startTrackerUpdatesWithRefreshTest(t, nil, interval, announce)
}

func startTrackerUpdatesWithRefreshTest(t *testing.T, refresh <-chan struct{}, interval time.Duration, announce func(context.Context) (*tracker.TrackerResponse, error)) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	return startTrackerUpdatesWithDelayTest(t, refresh, interval, 0, announce)
}

func startTrackerUpdatesWithDelayTest(t *testing.T, refresh <-chan struct{}, interval, minDelay time.Duration, announce func(context.Context) (*tracker.TrackerResponse, error)) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); runTrackerUpdates(ctx, refresh, interval, minDelay, announce) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("tracker update worker did not stop after cancellation")
		}
	})
	return cancel, done
}

func TestTrackerUpdatesRefreshBeforeInterval(t *testing.T) {
	for _, failFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("first_announce_fails=%t", failFirst), func(t *testing.T) {
			refresh := make(chan struct{}, 1)
			calls := make(chan int, 2)
			attempt := 0
			cancel, done := startTrackerUpdatesWithRefreshTest(t, refresh, time.Hour, func(context.Context) (*tracker.TrackerResponse, error) {
				attempt++
				calls <- attempt
				if failFirst && attempt == 1 {
					return nil, errors.New("tracker temporarily unavailable")
				}
				return &tracker.TrackerResponse{Interval: time.Hour}, nil
			})
			for want := 1; want <= 2; want++ {
				refresh <- struct{}{}
				select {
				case got := <-calls:
					if got != want {
						t.Errorf("announce attempt = %d, want %d", got, want)
					}
				case <-time.After(time.Second):
					t.Fatalf("refresh %d did not trigger an announce before the periodic interval", want)
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("worker did not stop after refreshes")
			}
		})
	}
}

func TestTrackerUpdatesMinimumDelay(t *testing.T) {
	for _, failFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("first_announce_fails=%t", failFirst), func(t *testing.T) {
			const minDelay = 30 * time.Millisecond
			refresh := make(chan struct{}, 1)
			calls := make(chan time.Time, 2)
			started := time.Now()
			attempt := 0
			startTrackerUpdatesWithDelayTest(t, refresh, time.Hour, minDelay, func(context.Context) (*tracker.TrackerResponse, error) {
				attempt++
				calls <- time.Now()
				if failFirst && attempt == 1 {
					return nil, errors.New("tracker temporarily unavailable")
				}
				return &tracker.TrackerResponse{Interval: time.Hour}, nil
			})
			previous := started
			for i := 0; i < 2; i++ {
				refresh <- struct{}{}
				select {
				case at := <-calls:
					if elapsed := at.Sub(previous); elapsed < minDelay {
						t.Errorf("announce %d after %v, want at least %v", i+1, elapsed, minDelay)
					}
					previous = at
				case <-time.After(time.Second):
					t.Fatalf("refresh %d never announced after its cooldown", i+1)
				}
			}
		})
	}
}

func TestTrackerUpdatesCancelBeforeInterval(t *testing.T) {
	calls := make(chan struct{}, 1)
	cancel, done := startTrackerUpdatesTest(t, time.Hour, func(context.Context) (*tracker.TrackerResponse, error) {
		calls <- struct{}{}
		return &tracker.TrackerResponse{Interval: time.Hour}, nil
	})
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop while waiting")
	}
	select {
	case <-calls:
		t.Error("announced before the initial interval elapsed")
	default:
	}
}

func TestTrackerUpdatesRetryAfterFailure(t *testing.T) {
	calls := make(chan int, 4)
	attempt := 0
	startTrackerUpdatesTest(t, time.Millisecond, func(context.Context) (*tracker.TrackerResponse, error) {
		attempt++
		calls <- attempt
		if attempt == 1 {
			return nil, errors.New("tracker temporarily unavailable")
		}
		return &tracker.TrackerResponse{Interval: time.Hour}, nil
	})
	for want := 1; want <= 2; want++ {
		select {
		case got := <-calls:
			if got != want {
				t.Errorf("attempt = %d, want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("announce attempt %d never ran; failures must schedule another wait", want)
		}
	}
}

func TestTrackerUpdatesUseReturnedInterval(t *testing.T) {
	calls := make(chan struct{}, 4)
	startTrackerUpdatesTest(t, time.Millisecond, func(context.Context) (*tracker.TrackerResponse, error) {
		calls <- struct{}{}
		return &tracker.TrackerResponse{Interval: time.Hour}, nil
	})
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("initial interval never triggered an announce")
	}
	select {
	case <-calls:
		t.Error("worker reused the short initial interval instead of the returned hour")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestStopTrackerUpdatesCancelsInFlightAnnounce(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	finished := make(chan struct{})
	release := make(chan struct{})
	stop := startTrackerUpdates(parent, nil, time.Millisecond, 0, func(ctx context.Context) (*tracker.TrackerResponse, error) {
		close(started)
		select {
		case <-ctx.Done():
		case <-release: // Failure cleanup must also unblock a misrouted context.
		}
		close(finished)
		return nil, ctx.Err()
	})
	t.Cleanup(func() {
		cancel()
		close(release)
		stop()
	})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("tracker worker never entered its announce callback")
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stop()
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stopping tracker updates did not cancel the in-flight announce")
	}
	select {
	case <-finished:
	default:
		t.Error("stop returned before its announce callback finished")
	}
	if parent.Err() != nil {
		t.Error("stopping the tracker worker canceled the parent session")
	}
}
