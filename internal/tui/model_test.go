package tui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/transfer"
)

var _ tea.Model = Model{}

func TestCompactViewReservesFooterRows(t *testing.T) {
	for _, state := range []struct {
		name               string
		stopping, finished bool
		footer             string
	}{
		{"running", false, false, "q: stop/quit"},
		{"stopping", true, false, "Stopping..."},
		{"finished", false, true, "q: quit"},
		{"finished after stopping", true, true, "q: quit"},
	} {
		for _, height := range []int{0, 1, 2, 3} {
			for _, width := range []int{1, 20} {
				t.Run(fmt.Sprintf("%s/height=%d/width=%d", state.name, height, width), func(t *testing.T) {
					m := Model{status: transfer.Status{Phase: transfer.PhaseDownloading}, stopping: state.stopping, finished: state.finished}
					text := ansi.Strip(m.compactView(width, height))
					lines := strings.Split(text, "\n")
					wantRows := height
					if wantRows == 0 {
						wantRows = 3
					}
					if len(lines) != wantRows {
						t.Errorf("rows=%d, want %d: %q", len(lines), wantRows, text)
					}
					for _, line := range lines {
						if lipgloss.Width(line) > width {
							t.Errorf("line exceeds width %d: %q", width, line)
						}
					}
					footer := strings.TrimSpace(lines[len(lines)-1])
					if width >= 20 {
						if footer != state.footer {
							t.Errorf("last row=%q, want footer %q", footer, state.footer)
						}
						if wantRows >= 2 && !strings.Contains(lines[0], "Resize terminal") {
							t.Error("resize instruction missing")
						}
						if wantRows >= 3 && (len(lines) < 3 || !strings.Contains(lines[1], string(m.status.Phase))) {
							t.Error("phase missing from middle row")
						}
					} else if !strings.HasPrefix(footer, state.footer[:1]) {
						t.Errorf("narrow footer lost its leading control/state: %q", footer)
					}
				})
			}
		}
	}
}

func TestViewShowsLifecycleAndFailureDetails(t *testing.T) {
	primary := errors.New("output disk is full")
	secondary := errors.New("secondary run failure")
	for _, tc := range []struct {
		name                           string
		stopping, finished             bool
		statusErr, runErr              error
		footer, wantError, hiddenError string
	}{
		{name: "running", footer: "q / Ctrl+C: stop and quit"},
		{name: "stopping", stopping: true, footer: "Stopping transfer; waiting for cleanup..."},
		{name: "finished", finished: true, footer: "q / Ctrl+C: quit"},
		{name: "finished after stopping", stopping: true, finished: true, footer: "q / Ctrl+C: quit"},
		{name: "status error takes priority", finished: true, statusErr: primary, runErr: secondary, footer: "q / Ctrl+C: quit", wantError: primary.Error(), hiddenError: secondary.Error()},
		{name: "run error fallback", finished: true, runErr: secondary, footer: "q / Ctrl+C: quit", wantError: secondary.Error()},
		{name: "normal cancellation", finished: true, runErr: context.Canceled, footer: "q / Ctrl+C: quit", hiddenError: context.Canceled.Error()},
		{name: "wrapped cancellation", finished: true, runErr: fmt.Errorf("stopped: %w", context.Canceled), footer: "q / Ctrl+C: quit", hiddenError: context.Canceled.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// No runner is supplied: rendering must use only the cached snapshot.
			m := Model{status: transfer.Status{Phase: transfer.PhaseDownloading, Err: tc.statusErr,
				Progress: download.Progress{Name: "file.bin", TotalBytes: 8, VerifiedBytes: 4}},
				stopping: tc.stopping, finished: tc.finished, runErr: tc.runErr}
			text := ansi.Strip(m.View().Content)
			for _, section := range []string{"Torrent:", "Phase:", "Verified:", "Peers:", "Download:", "Upload:"} {
				if !strings.Contains(text, section) {
					t.Errorf("view lost status section %q", section)
					break
				}
			}
			if !strings.HasSuffix(strings.TrimRight(text, " \n"), tc.footer) {
				t.Errorf("view footer missing %q: %q", tc.footer, text)
			}
			if tc.wantError != "" && !strings.Contains(text, tc.wantError) {
				t.Errorf("view hid failure %q", tc.wantError)
			}
			if tc.hiddenError != "" && strings.Contains(text, tc.hiddenError) {
				t.Errorf("view exposed unwanted failure %q", tc.hiddenError)
			}
			if tc.wantError == "" && strings.Contains(text, "Error ") {
				t.Error("view reported an error during normal lifecycle")
			}
		})
	}
}

func TestViewWrapsLongContentToTerminalWidth(t *testing.T) {
	for _, tc := range []struct {
		name             string
		width, wantWidth int
	}{
		{"unknown size", 0, 80},
		{"wide terminal", 160, 80},
		{"normal terminal", 80, 80},
		{"narrow terminal", 20, 20},
		{"single column", 1, 1},
		{"negative width", -1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := strings.Repeat("long-torrent-name-", 12)
			if tc.wantWidth >= 20 {
				name += "日本語-🙂-e\u0301.iso"
			}
			detail := strings.Repeat("very-long-error-path-", 10)
			m := Model{width: tc.width, finished: true,
				status: transfer.Status{Phase: transfer.PhaseFailed, Err: errors.New(detail), Progress: download.Progress{Name: name}}}
			text := m.View().Content
			for _, line := range strings.Split(text, "\n") {
				if width := lipgloss.Width(line); width > tc.wantWidth {
					t.Errorf("line width=%d, exceeds %d", width, tc.wantWidth)
					break
				}
			}
			compact := strings.Join(strings.Fields(ansi.Strip(text)), "")
			if tc.wantWidth >= 20 && (!strings.Contains(compact, name) || !strings.Contains(compact, detail)) {
				t.Error("wrapping dropped torrent-name or error text")
			}
		})
	}
}

func TestViewSelectsCompactLayoutAfterWrappingAndResizing(t *testing.T) {
	m := Model{finished: true, status: transfer.Status{Phase: transfer.PhaseFailed,
		Progress: download.Progress{Name: strings.Repeat("long-torrent-name-", 12)},
		Err:      errors.New(strings.Repeat("long-error-path-", 12))}}
	original := m.status
	for _, tc := range []struct {
		name          string
		width, height int
		compact       bool
	}{
		{"full", 80, 40, false},
		{"width too small", 19, 0, true},
		{"one row", 80, 1, true},
		{"two rows", 80, 2, true},
		{"three rows", 80, 3, true},
		{"wrapped content exceeds height", 20, 12, true},
		{"height unknown", 20, 0, false},
		{"expanded again", 80, 40, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			updated, cmd := m.Update(tea.WindowSizeMsg{Width: tc.width, Height: tc.height})
			m = updated.(Model)
			if cmd != nil || m.status != original {
				t.Fatal("resize changed transfer state or scheduled work")
			}
			text := ansi.Strip(m.View().Content)
			if tc.height > 0 && lipgloss.Height(text) > tc.height {
				t.Errorf("rendered rows=%d exceed height=%d", lipgloss.Height(text), tc.height)
			}
			if tc.compact {
				if strings.Contains(text, "Verified:") {
					t.Error("small terminal still shows full layout")
				}
				if !strings.HasSuffix(strings.TrimSpace(text), "q: quit") {
					t.Error("compact layout hid quit instruction")
				}
			} else if !strings.Contains(text, "Verified:") {
				t.Error("full layout was not restored")
			}
		})
	}
}

func TestUpdateRefreshesSnapshotAndReschedules(t *testing.T) {
	runner := &fakeRunner{status: transfer.Status{Phase: transfer.PhaseStarting}}
	m := New(context.Background(), runner)
	t.Cleanup(m.cancel)
	want := transfer.Status{Phase: transfer.PhaseDownloading, ActivePeers: 2,
		Progress: download.Progress{Name: "file.bin", TotalBytes: 8, VerifiedBytes: 4}}
	runner.mu.Lock()
	runner.status = want
	runner.mu.Unlock()
	updated, cmd := m.Update(refreshMsg{})
	next, ok := updated.(Model)
	if !ok {
		t.Fatalf("updated model=%T", updated)
	}
	if next.status != want || next.finished || next.runErr != nil {
		t.Errorf("updated model=%+v", next)
	}
	if cmd == nil {
		t.Error("active transfer did not schedule another refresh")
	}
	if m.status.Phase != transfer.PhaseStarting {
		t.Error("Update changed original value receiver")
	}
	if runner.runCalls.Load() != 0 {
		t.Error("refresh restarted engine")
	}
}

func TestUpdateCompletionRetainsResultAndStopsRefreshes(t *testing.T) {
	failure := errors.New("transfer failed")
	for _, tc := range []struct {
		name   string
		err    error
		status transfer.Status
	}{
		{"success", nil, transfer.Status{Phase: transfer.PhaseStopped}},
		{"failure", failure, transfer.Status{Phase: transfer.PhaseFailed, Err: failure}},
		{"cancellation", context.Canceled, transfer.Status{Phase: transfer.PhaseStopped}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &fakeRunner{status: tc.status}
			m := New(context.Background(), runner)
			t.Cleanup(m.cancel)
			m.status = transfer.Status{Phase: transfer.PhaseDownloading}
			updated, cmd := m.Update(runFinishedMsg{err: tc.err})
			next := updated.(Model)
			if !next.finished || next.runErr != tc.err || next.status != tc.status {
				t.Errorf("completion model=%+v", next)
			}
			if cmd != nil {
				t.Error("completion scheduled more work")
			}
			updated, cmd = next.Update(refreshMsg{})
			if !updated.(Model).finished || cmd != nil {
				t.Error("late timer restarted refresh loop after completion")
			}
			if runner.runCalls.Load() != 0 {
				t.Error("completion restarted engine")
			}
		})
	}
}

func TestUpdateIgnoresUnknownMessages(t *testing.T) {
	m := Model{status: transfer.Status{Phase: transfer.PhaseStarting}}
	for _, msg := range []tea.Msg{nil, "unhandled", struct{}{}} {
		updated, cmd := m.Update(msg)
		if next, ok := updated.(Model); !ok || next.status != m.status || next.finished || next.stopping || next.runErr != nil || cmd != nil {
			t.Errorf("unknown message %T changed model or scheduled work", msg)
		}
	}
}

func TestQuitKeysWaitForTransferCompletion(t *testing.T) {
	for _, key := range []tea.KeyPressMsg{
		{Code: 'q', Text: "q"},
		{Code: 'c', Mod: tea.ModCtrl},
	} {
		t.Run(key.String(), func(t *testing.T) {
			runner := &fakeRunner{status: transfer.Status{Phase: transfer.PhaseDownloading}}
			m := New(context.Background(), runner)
			originalCancel := m.cancel
			t.Cleanup(originalCancel)
			cancelCalls := 0
			m.cancel = func() { cancelCalls++; originalCancel() }
			updated, cmd := m.Update(key)
			next := updated.(Model)
			if !next.stopping || next.finished || cmd != nil || next.ctx.Err() != context.Canceled {
				t.Fatalf("quit did not request cleanup without exiting: %+v, command=%v", next, cmd != nil)
			}
			updated, cmd = next.Update(key)
			next = updated.(Model)
			if cancelCalls != 1 || cmd != nil || !next.stopping {
				t.Errorf("repeated quit: cancellation calls=%d, command=%v", cancelCalls, cmd != nil)
			}
			runner.mu.Lock()
			runner.status = transfer.Status{Phase: transfer.PhaseStopped}
			runner.mu.Unlock()
			updated, cmd = next.Update(runFinishedMsg{err: context.Canceled})
			next = updated.(Model)
			if !next.finished || next.runErr != context.Canceled || next.status.Phase != transfer.PhaseStopped {
				t.Errorf("completion did not preserve terminal state: %+v", next)
			}
			if cmd == nil {
				t.Fatal("completion after quit did not schedule exit")
			}
			if _, ok := cmd().(tea.QuitMsg); !ok {
				t.Error("completion did not return quit command")
			}
		})
	}
}

func TestQuitAfterCompletionExitsImmediately(t *testing.T) {
	m := New(context.Background(), &fakeRunner{})
	t.Cleanup(m.cancel)
	m.finished = true
	updated, cmd := m.Update(tea.KeyPressMsg{Code: 'q', Text: "q"})
	if !updated.(Model).finished || cmd == nil {
		t.Fatal("quit after completion did not schedule exit")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Error("expected quit command")
	}
	if m.ctx.Err() != nil {
		t.Error("quit unnecessarily canceled already-finished transfer")
	}
}

func TestUpdateIgnoresUnrelatedKeys(t *testing.T) {
	m := Model{status: transfer.Status{Phase: transfer.PhaseStarting}}
	for _, key := range []tea.KeyPressMsg{{Code: 'x', Text: "x"}, {Code: tea.KeyEnter}} {
		updated, cmd := m.Update(key)
		next := updated.(Model)
		if next.stopping || next.finished || next.status != m.status || cmd != nil {
			t.Errorf("unrelated key %s changed model", key.String())
		}
	}
}

func TestUpdateWindowSizePreservesTransferState(t *testing.T) {
	failure := errors.New("output unavailable")
	status := transfer.Status{Phase: transfer.PhaseFailed, Err: failure, Progress: download.Progress{Name: "file.bin", TotalBytes: 8}}
	cancelCalls := 0
	m := Model{status: status, stopping: true, finished: true, runErr: failure,
		width: 120, height: 40, cancel: func() { cancelCalls++ }}
	for _, size := range []tea.WindowSizeMsg{{Width: 80, Height: 24}, {Width: 20, Height: 5}, {Width: 0, Height: 0}} {
		updated, cmd := m.Update(size)
		next := updated.(Model)
		if next.width != size.Width || next.height != size.Height {
			t.Errorf("dimensions=%dx%d, want %dx%d", next.width, next.height, size.Width, size.Height)
		}
		if next.status != status || !next.stopping || !next.finished || next.runErr != failure || cmd != nil || cancelCalls != 0 {
			t.Error("resize changed transfer lifecycle or scheduled work")
		}
		if m.width == next.width && m.height == next.height {
			t.Error("test did not exercise a size change")
		}
		m = next
	}
}

type fakeRunner struct {
	mu       sync.Mutex
	status   transfer.Status
	run      func(context.Context) error
	runCalls atomic.Int32
}

func (r *fakeRunner) Run(ctx context.Context) error {
	r.runCalls.Add(1)
	if r.run != nil {
		return r.run(ctx)
	}
	return nil
}
func (r *fakeRunner) Status() transfer.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

func TestNewSnapshotsStatusWithoutStartingTransfer(t *testing.T) {
	status := transfer.Status{Phase: transfer.PhaseDiscovering, Progress: download.Progress{Name: "file.bin", TotalBytes: 8}}
	runner := &fakeRunner{status: status}
	m := New(context.Background(), runner)
	t.Cleanup(m.cancel)
	if m.engine != runner || m.ctx == nil || m.cancel == nil {
		t.Fatal("constructor did not retain runner and child context")
	}
	if m.status != status || m.stopping || m.finished || m.runErr != nil {
		t.Errorf("unexpected initial model: %+v", m)
	}
	if runner.runCalls.Load() != 0 {
		t.Error("constructor started transfer")
	}
	m.status.Progress.Name = "changed"
	if got := runner.Status(); got != status {
		t.Error("model snapshot mutation changed runner status")
	}
}

func TestModelCancellationDoesNotCancelParent(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	t.Cleanup(cancelParent)
	m := New(parent, &fakeRunner{})
	t.Cleanup(m.cancel)
	m.cancel()
	if m.ctx.Err() != context.Canceled {
		t.Error("model cancel did not cancel child context")
	}
	if parent.Err() != nil {
		t.Error("model cancel canceled caller's context")
	}
}

func TestParentCancellationReachesModel(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	t.Cleanup(cancelParent)
	m := New(parent, &fakeRunner{})
	t.Cleanup(m.cancel)
	cancelParent()
	if m.ctx.Err() != context.Canceled {
		t.Error("parent cancellation did not reach model")
	}
}

func TestRunTransferCommandDefersWorkAndReturnsCompletion(t *testing.T) {
	failure := errors.New("transfer failed")
	for _, tc := range []struct {
		name   string
		err    error
		cancel bool
	}{
		{"success", nil, false},
		{"failure", failure, false},
		{"canceled before execution", context.Canceled, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &fakeRunner{}
			m := New(context.Background(), runner)
			t.Cleanup(m.cancel)
			runner.run = func(ctx context.Context) error {
				if ctx != m.ctx {
					t.Error("command did not use model's transfer context")
				}
				if tc.cancel {
					return ctx.Err()
				}
				return tc.err
			}
			cmd := m.runTransferCmd()
			if cmd == nil {
				t.Fatal("nil transfer command")
			}
			if runner.runCalls.Load() != 0 {
				t.Fatal("creating command started transfer")
			}
			if tc.cancel {
				m.cancel()
			}
			message := cmd()
			finished, ok := message.(runFinishedMsg)
			if !ok {
				t.Fatalf("message=%T, want runFinishedMsg", message)
			}
			if finished.err != tc.err {
				t.Errorf("completion error=%v, want %v", finished.err, tc.err)
			}
			if runner.runCalls.Load() != 1 {
				t.Error("command did not run transfer exactly once")
			}
			if m.finished || m.runErr != nil {
				t.Error("command mutated model instead of returning a message")
			}
		})
	}
}
