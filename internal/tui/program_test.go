package tui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
)

func headlessOptions() []tea.ProgramOption {
	return []tea.ProgramOption{tea.WithInput(nil), tea.WithOutput(io.Discard), tea.WithoutRenderer()}
}

func TestProgramReturnsTransferResult(t *testing.T) {
	for _, result := range []error{nil, errors.New("transfer failed")} {
		t.Run(fmt.Sprintf("result=%v", result), func(t *testing.T) {
			runner := &fakeRunner{run: func(context.Context) error { return result }}
			opts := append(headlessOptions(), tea.WithFilter(func(_ tea.Model, msg tea.Msg) tea.Msg {
				// Simulate the program being dismissed once the transfer completes.
				if _, ok := msg.(runFinishedMsg); ok {
					return tea.QuitMsg{}
				}
				return msg
			}))
			if err := Run(context.Background(), runner, opts...); err != result {
				t.Errorf("Run()=%v, want %v", err, result)
			}
			if runner.runCalls.Load() != 1 {
				t.Errorf("underlying runs=%d, want 1", runner.runCalls.Load())
			}
		})
	}
}

func TestProgramStartupCancellationStillJoinsRunner(t *testing.T) {
	uiCtx, cancelUI := context.WithCancel(context.Background())
	cancelUI()
	cleaned := make(chan struct{})
	runner := &fakeRunner{run: func(ctx context.Context) error {
		<-ctx.Done()
		close(cleaned)
		return ctx.Err()
	}}
	opts := append(headlessOptions(), tea.WithContext(uiCtx))
	err := Run(context.Background(), runner, opts...)
	if !errors.Is(err, tea.ErrProgramKilled) {
		t.Errorf("Run()=%v, want terminal program error", err)
	}
	select {
	case <-cleaned:
	default:
		t.Error("program returned before runner cleanup")
	}
	if runner.runCalls.Load() != 1 {
		t.Errorf("underlying runs=%d, want 1", runner.runCalls.Load())
	}
}

func TestProgramWaitsForCleanupOnCancellation(t *testing.T) {
	for _, killUI := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminalCanceled=%t", killUI), func(t *testing.T) {
			parent, cancelParent := context.WithCancel(context.Background())
			uiCtx, cancelUI := context.WithCancel(context.Background())
			started, cleaning, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			runner := &fakeRunner{run: func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				close(cleaning)
				<-release
				return ctx.Err()
			}}
			opts := headlessOptions()
			if killUI {
				opts = append(opts, tea.WithContext(uiCtx))
			}
			done := make(chan error, 1)
			t.Cleanup(func() { cancelParent(); cancelUI(); unblock() })
			go func() { done <- Run(parent, runner, opts...) }()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("runner did not start")
			}
			wantErr := error(context.Canceled)
			if killUI {
				cancelUI()
				wantErr = tea.ErrProgramKilled
			} else {
				cancelParent()
			}
			select {
			case <-cleaning:
			case <-time.After(2 * time.Second):
				t.Fatal("program did not request transfer cleanup")
			}
			select {
			case err := <-done:
				t.Fatalf("program returned before cleanup finished: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			unblock()
			select {
			case err := <-done:
				if !errors.Is(err, wantErr) {
					t.Errorf("Run()=%v, want %v", err, wantErr)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("program did not finish after cleanup")
			}
			if killUI && parent.Err() != nil {
				t.Error("terminal shutdown canceled caller's parent context")
			}
			if runner.runCalls.Load() != 1 {
				t.Errorf("underlying runs=%d, want 1", runner.runCalls.Load())
			}
		})
	}
}
