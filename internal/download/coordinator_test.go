package download

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

func TestWorkerStateOwnsAvailabilitySnapshot(t *testing.T) {
	advertised := peer.Bitfield{0x80}
	worker := newWorkerState(advertised)
	if !worker.available.HasPiece(0) {
		t.Fatal("initial availability was not retained")
	}
	if err := advertised.SetPiece(1); err != nil {
		t.Fatal(err)
	}
	if worker.available.HasPiece(1) {
		t.Fatal("peer mutation changed the coordinator's snapshot")
	}
	if err := worker.available.SetPiece(2); err != nil {
		t.Fatal(err)
	}
	if advertised.HasPiece(2) {
		t.Fatal("snapshot mutation changed the peer's bitfield")
	}
}

func TestWorkerStateDispatchReservesDistinctPieces(t *testing.T) {
	scheduler := newPieceScheduler(3)
	first := newWorkerState(peer.Bitfield{0xc0})
	second := newWorkerState(peer.Bitfield{0x60})
	for i, worker := range []*workerState{first, second} {
		if !worker.dispatch(scheduler) {
			t.Fatalf("worker %d did not receive eligible work", i)
		}
		if worker.phase != workerDownloading || worker.assignedIndex != i || scheduler.states[i] != pieceAssigned {
			t.Fatalf("worker %d did not retain a matching scheduler reservation", i)
		}
		select {
		case index := <-worker.jobs:
			if index.kind != commandDownload || index.index != i {
				t.Errorf("worker %d received command %+v", i, index)
			}
		default:
			t.Fatalf("worker %d has no queued job", i)
		}
	}
	before := slices.Clone(scheduler.states)
	if first.dispatch(scheduler) {
		t.Error("busy worker received another assignment")
	}
	if len(first.jobs) != 0 || !slices.Equal(before, scheduler.states) {
		t.Error("dispatch to a busy worker changed jobs or reservations")
	}
	// The second worker advertises an additional missing piece, but remains
	// busy even after its previous job has been consumed from the channel.
	if second.dispatch(scheduler) || len(second.jobs) != 0 || scheduler.states[2] != pieceMissing {
		t.Error("consuming a job incorrectly made its worker available for more work")
	}
}

func TestWorkerStateDispatchWithoutEligibleWork(t *testing.T) {
	for _, tt := range []struct {
		name      string
		states    []pieceState
		available peer.Bitfield
	}{
		{"empty scheduler", nil, nil},
		{"unavailable piece", []pieceState{pieceMissing}, peer.Bitfield{0}},
		{"already reserved", []pieceState{pieceAssigned}, peer.Bitfield{0x80}},
		{"already complete", []pieceState{pieceComplete}, peer.Bitfield{0x80}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scheduler := &pieceScheduler{states: slices.Clone(tt.states)}
			worker := newWorkerState(tt.available)
			if worker.dispatch(scheduler) {
				t.Fatal("dispatch succeeded without eligible work")
			}
			if worker.phase != workerIdle || len(worker.jobs) != 0 || !slices.Equal(scheduler.states, tt.states) {
				t.Error("unsuccessful dispatch changed worker or scheduling state")
			}
		})
	}
}

func TestCoordinatorRegisteredWorkersReceiveDistinctJobs(t *testing.T) {
	c := newCoordinator(2)
	channels := make([]<-chan workerCommand, 0, 2)
	for _, id := range []int{0, 7} {
		jobs, err := c.addWorker(id, peer.Bitfield{0xc0})
		if err != nil || jobs == nil {
			t.Fatalf("register worker %d: channel=%v, error=%v", id, jobs, err)
		}
		channels = append(channels, jobs)
		if !c.workers[id].dispatch(c.scheduler) {
			t.Fatalf("registered worker %d could not receive work", id)
		}
	}
	if channels[0] == channels[1] {
		t.Fatal("different workers share a job channel")
	}
	for index, jobs := range channels {
		select {
		case got := <-jobs:
			if got.kind != commandDownload || got.index != index {
				t.Errorf("worker %d got command %+v, want download of %d", index, got, index)
			}
		default:
			t.Fatalf("worker %d did not receive its dispatched job", index)
		}
	}
}

func TestCoordinatorDuplicateRegistrationPreservesWorker(t *testing.T) {
	c := newCoordinator(2)
	available := peer.Bitfield{0x80}
	jobs, err := c.addWorker(7, available)
	if err != nil {
		t.Fatal(err)
	}
	original := c.workers[7]
	if !original.dispatch(c.scheduler) {
		t.Fatal("original worker did not receive work")
	}
	duplicateJobs, err := c.addWorker(7, peer.Bitfield{0x40})
	if err == nil || duplicateJobs != nil {
		t.Fatal("duplicate worker registration did not return nil channel and error")
	}
	if len(c.workers) != 1 || c.workers[7] != original || original.phase != workerDownloading || original.assignedIndex != 0 {
		t.Fatal("duplicate registration replaced the active worker or its assignment")
	}
	if !slices.Equal(original.available, available) || c.scheduler.states[0] != pieceAssigned || c.scheduler.states[1] != pieceMissing {
		t.Error("duplicate registration changed availability or reservations")
	}
	select {
	case index, ok := <-jobs:
		if !ok || index.kind != commandDownload || index.index != 0 {
			t.Errorf("original job = (%+v, %t), want download of 0", index, ok)
		}
	default:
		t.Error("duplicate registration lost the original job")
	}
}

func TestCoordinatorRemoveWorkerReleasesOnlyItsAssignment(t *testing.T) {
	c := newCoordinator(2)
	channels := make(map[int]<-chan workerCommand)
	for _, id := range []int{1, 2} {
		jobs, err := c.addWorker(id, peer.Bitfield{0xc0})
		if err != nil {
			t.Fatal(err)
		}
		channels[id] = jobs
		if !c.workers[id].dispatch(c.scheduler) {
			t.Fatal("dispatch failed")
		}
		<-jobs // Model the worker consuming its assignment before reporting failure.
	}
	remaining := c.workers[2]
	if err := c.removeWorker(1); err != nil {
		t.Fatal(err)
	}
	if _, exists := c.workers[1]; exists {
		t.Error("removed worker remains registered")
	}
	if c.workers[2] != remaining || remaining.phase != workerDownloading || remaining.assignedIndex != 1 || c.scheduler.states[1] != pieceAssigned {
		t.Error("removal changed the other worker's active assignment")
	}
	select {
	case _, ok := <-channels[1]:
		if ok {
			t.Error("removed worker's jobs remain open")
		}
	default:
		t.Error("removed worker's jobs were not closed")
	}
	jobs, err := c.addWorker(3, peer.Bitfield{0x80})
	if err != nil {
		t.Fatal(err)
	}
	if !c.workers[3].dispatch(c.scheduler) {
		t.Fatal("replacement could not claim released work")
	}
	if command := <-jobs; command.kind != commandDownload || command.index != 0 {
		t.Errorf("replacement got command %+v, want download of 0", command)
	}
	if err := c.removeWorker(1); err == nil {
		t.Error("removing an unknown worker succeeded")
	}
}

func TestCoordinatorRemoveIdleWorker(t *testing.T) {
	c := newCoordinator(0)
	jobs, err := c.addWorker(0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.removeWorker(0); err != nil {
		t.Fatal(err)
	}
	if len(c.workers) != 0 || !c.scheduler.done() {
		t.Error("idle removal changed scheduler completion or retained worker")
	}
	select {
	case _, ok := <-jobs:
		if ok {
			t.Error("idle worker's job channel remains open")
		}
	default:
		t.Error("idle worker's job channel was not closed")
	}
}

func TestCoordinatorFailedRemovalPreservesWorker(t *testing.T) {
	c := newCoordinator(1)
	jobs, err := c.addWorker(1, peer.Bitfield{0x80})
	if err != nil {
		t.Fatal(err)
	}
	w := c.workers[1]
	if !w.dispatch(c.scheduler) {
		t.Fatal("dispatch failed")
	}
	<-jobs
	// Deliberately inconsistent bookkeeping: a busy worker must not own a
	// completed piece. A failed release should leave its registry entry intact.
	if err := c.scheduler.complete(0); err != nil {
		t.Fatal(err)
	}
	if err := c.removeWorker(1); err == nil {
		t.Fatal("expected scheduler release error")
	}
	if c.workers[1] != w || w.phase != workerDownloading || !c.scheduler.done() {
		t.Error("failed removal changed coordinator state")
	}
	select {
	case <-jobs:
		t.Error("failed removal closed the job channel")
	default:
	}
}

func TestCoordinatorWorkerForResult(t *testing.T) {
	for _, tt := range []struct {
		name      string
		result    pieceResult
		wantError string
	}{
		{"valid worker ID differs from piece index", pieceResult{workerID: 7, index: 0}, ""},
		{"failed download still has a valid assignment", pieceResult{workerID: 7, index: 0, err: errors.New("peer disconnected")}, ""},
		{"unknown worker", pieceResult{workerID: 99, index: 0}, "does not exist"},
		{"idle worker", pieceResult{workerID: 0, index: 0}, "not downloading"},
		{"wrong piece", pieceResult{workerID: 7, index: 1}, "expected 0, found 1"},
		{"negative piece", pieceResult{workerID: 7, index: -1}, "expected 0, found -1"},
		{"have result is not a download result", pieceResult{workerID: 7, index: 0, kind: commandHave}, "download"},
		{"unknown result kind", pieceResult{workerID: 7, index: 0, kind: workerCommandKind(255)}, "download"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newCoordinator(2)
			jobs, err := c.addWorker(7, peer.Bitfield{0xc0})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.addWorker(0, peer.Bitfield{0xc0}); err != nil {
				t.Fatal(err)
			}
			assigned := c.workers[7]
			if !assigned.dispatch(c.scheduler) {
				t.Fatal("dispatch failed")
			}
			<-jobs
			before := slices.Clone(c.scheduler.states)
			worker, err := c.workerForResult(tt.result)
			if tt.wantError == "" {
				if err != nil || worker != assigned {
					t.Fatalf("lookup = %p, %v; want assigned worker %p", worker, err, assigned)
				}
			} else if worker != nil || err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("lookup = %p, %v; want nil worker and error containing %q", worker, err, tt.wantError)
			}
			if len(c.workers) != 2 || assigned.phase != workerDownloading || assigned.assignedIndex != 0 || c.workers[0].phase != workerIdle || !slices.Equal(c.scheduler.states, before) {
				t.Error("result validation changed coordinator bookkeeping")
			}
		})
	}
}

func TestCoordinatorHandleFailure(t *testing.T) {
	fatal := errors.New("invalid torrent metadata")
	peerFailure := &PeerError{Op: "download block", Err: errors.New("peer disconnected")}
	for _, tt := range []struct {
		name      string
		result    pieceResult
		removed   bool
		wantCause error
	}{
		{"peer failure", pieceResult{workerID: 7, index: 0, err: peerFailure}, true, nil},
		{"wrapped peer failure", pieceResult{workerID: 7, index: 0, err: fmt.Errorf("transfer failed: %w", peerFailure)}, true, nil},
		{"fatal error", pieceResult{workerID: 7, index: 0, err: fatal}, false, fatal},
		{"success sent to failure handler", pieceResult{workerID: 7, index: 0}, false, nil},
		{"unknown worker", pieceResult{workerID: 99, index: 0, err: peerFailure}, false, nil},
		{"idle worker", pieceResult{workerID: 0, index: 0, err: peerFailure}, false, nil},
		{"mismatched piece", pieceResult{workerID: 7, index: 1, err: peerFailure}, false, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newCoordinator(2)
			jobs, err := c.addWorker(7, peer.Bitfield{0xc0})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.addWorker(0, nil); err != nil {
				t.Fatal(err)
			}
			original := c.workers[7]
			if !original.dispatch(c.scheduler) {
				t.Fatal("dispatch failed")
			}
			<-jobs
			err = c.handleFailure(tt.result)
			if tt.removed {
				if err != nil {
					t.Fatalf("retryable failure became fatal: %v", err)
				}
				if _, exists := c.workers[7]; exists {
					t.Fatal("failed worker remains registered")
				}
				select {
				case _, ok := <-jobs:
					if ok {
						t.Error("failed worker's jobs were not closed")
					}
				default:
					t.Error("failed worker's jobs were not closed")
				}
				if index, ok := c.scheduler.assign(peer.Bitfield{0x80}); !ok || index != 0 {
					t.Error("failed piece was not released for reassignment")
				}
			} else {
				if err == nil {
					t.Fatal("expected a fatal or invalid-result error")
				}
				if tt.wantCause != nil && !errors.Is(err, tt.wantCause) {
					t.Errorf("error = %v, want original cause %v", err, tt.wantCause)
				}
				if c.workers[7] != original || original.phase != workerDownloading || c.scheduler.states[0] != pieceAssigned {
					t.Error("fatal or invalid result changed the assignment")
				}
			}
			if c.workers[0] == nil || c.workers[0].phase != workerIdle || c.scheduler.completedCount != 0 {
				t.Error("failure handling changed another worker or counted completion")
			}
		})
	}
}

func TestCoordinatorHandleSuccessCommitsExactlyOnce(t *testing.T) {
	tf := &torrent.TorrentFile{Length: 6, PieceLength: 4, Pieces: make([][20]byte, 2)}
	dst := &downloadTestWriter{}
	s, err := NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	s.stats.Downloaded = 10 // Workers have already counted received bytes.
	c := newCoordinator(2)
	jobs, err := c.addWorker(7, peer.Bitfield{0xc0})
	if err != nil {
		t.Fatal(err)
	}
	w := c.workers[7]
	for index, data := range []string{"abcd", "ef"} {
		if !w.dispatch(c.scheduler) {
			t.Fatal("dispatch failed")
		}
		if got := <-jobs; got.kind != commandDownload || got.index != index {
			t.Fatalf("job = %+v, want download of %d", got, index)
		}
		result := pieceResult{workerID: 7, index: index, data: []byte(data)}
		if err := c.handleSuccess(s, result); err != nil {
			t.Fatal(err)
		}
		if w.phase != workerAnnouncing || !s.completed.HasPiece(index) || c.scheduler.states[index] != pieceComplete {
			t.Fatal("successful result did not commit progress and enter the announcing phase")
		}
		if s.completedCount != index+1 || c.scheduler.completedCount != index+1 {
			t.Errorf("completion counts = session %d, scheduler %d; want %d", s.completedCount, c.scheduler.completedCount, index+1)
		}
		wantLeft := int64(2)
		if index == 1 {
			wantLeft = 0
		}
		if stats := s.Stats(); stats.Left != wantLeft || stats.Downloaded != 10 {
			t.Errorf("stats = %+v, want left %d and downloaded 10", stats, wantLeft)
		}
		if len(dst.writes) != index+1 || dst.writes[index].offset != int64(index*4) || string(dst.writes[index].data) != data {
			t.Fatalf("unexpected storage writes: %+v", dst.writes)
		}
		before := s.Stats()
		if err := c.handleSuccess(s, result); err == nil {
			t.Fatal("duplicate result succeeded")
		}
		if len(dst.writes) != index+1 || s.Stats() != before || s.completedCount != index+1 || c.scheduler.completedCount != index+1 {
			t.Fatal("duplicate result changed output or completion accounting")
		}
		if w.dispatch(c.scheduler) {
			t.Fatal("worker received more work before acknowledging its have")
		}
		select {
		case command := <-jobs:
			if command.kind != commandHave || command.index != index {
				t.Fatalf("announcement command = %+v, want have for %d", command, index)
			}
		default:
			t.Fatal("successful result did not enqueue a have command")
		}
		if err := c.handleHaveResult(pieceResult{workerID: 7, index: index, kind: commandHave}); err != nil {
			t.Fatal(err)
		}
	}
	if !s.Complete() || !c.scheduler.done() {
		t.Error("completed session and scheduler disagree")
	}
}

func TestCoordinatorHandleSuccessRejectsBeforeCompletion(t *testing.T) {
	diskErr := errors.New("disk full")
	transferErr := &PeerError{Op: "download", Err: errors.New("disconnected")}
	for _, tt := range []struct {
		name       string
		result     pieceResult
		n          int
		writeErr   error
		wantCause  error
		wantWrites int
	}{
		{"write error", pieceResult{workerID: 7, index: 0, data: []byte("abcd")}, 2, diskErr, diskErr, 1},
		{"short write", pieceResult{workerID: 7, index: 0, data: []byte("abcd")}, 2, nil, io.ErrShortWrite, 1},
		{"invalid size", pieceResult{workerID: 7, index: 0, data: []byte("ab")}, 4, nil, nil, 0},
		{"failed result", pieceResult{workerID: 7, index: 0, err: transferErr}, 4, nil, transferErr, 0},
		{"unknown worker", pieceResult{workerID: 99, index: 0, data: []byte("abcd")}, 4, nil, nil, 0},
		{"wrong assignment", pieceResult{workerID: 7, index: 1, data: []byte("abcd")}, 4, nil, nil, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			writes := 0
			dst := completionTestWriterFunc(func([]byte, int64) (int, error) { writes++; return tt.n, tt.writeErr })
			tf := &torrent.TorrentFile{Length: 4, PieceLength: 4, Pieces: make([][20]byte, 1)}
			s, err := NewSession(tf, dst)
			if err != nil {
				t.Fatal(err)
			}
			c := newCoordinator(1)
			jobs, err := c.addWorker(7, peer.Bitfield{0x80})
			if err != nil {
				t.Fatal(err)
			}
			w := c.workers[7]
			if !w.dispatch(c.scheduler) {
				t.Fatal("dispatch failed")
			}
			<-jobs
			before := s.Stats()
			err = c.handleSuccess(s, tt.result)
			if err == nil {
				t.Fatal("failure was reported as success")
			}
			if tt.wantCause != nil && !errors.Is(err, tt.wantCause) {
				t.Errorf("error = %v, want cause %v", err, tt.wantCause)
			}
			if writes != tt.wantWrites || w.phase != workerDownloading || w.assignedIndex != 0 || len(w.jobs) != 0 || c.scheduler.states[0] != pieceAssigned || c.scheduler.completedCount != 0 || s.completedCount != 0 || s.completed.HasPiece(0) || s.Stats() != before {
				t.Error("failed result changed completion/assignment state or made unexpected writes")
			}
		})
	}
}

func TestCoordinatorHandleSuccessPropagatesBookkeepingErrors(t *testing.T) {
	for _, corrupt := range []string{"scheduler", "session bitfield"} {
		t.Run(corrupt, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: 4, PieceLength: 4, Pieces: make([][20]byte, 1)}
			s, err := NewSession(tf, &downloadTestWriter{})
			if err != nil {
				t.Fatal(err)
			}
			c := newCoordinator(1)
			jobs, err := c.addWorker(7, peer.Bitfield{0x80})
			if err != nil {
				t.Fatal(err)
			}
			w := c.workers[7]
			if !w.dispatch(c.scheduler) {
				t.Fatal("dispatch failed")
			}
			<-jobs
			// Simulate internal invariant violations to ensure errors are never
			// swallowed after writing. The coordinator must abort on these errors.
			if corrupt == "scheduler" {
				c.scheduler.states[0] = pieceMissing
			} else {
				s.completed = nil
			}
			err = c.handleSuccess(s, pieceResult{workerID: 7, index: 0, data: []byte("abcd")})
			if err == nil {
				t.Fatal("bookkeeping failure was reported as success")
			}
			if w.phase != workerDownloading || s.Stats().Left != 4 {
				t.Error("bookkeeping failure made worker idle or reduced remaining bytes")
			}
		})
	}
}

func TestCoordinatorAnnouncingWorkerPreservesCompletedAssignment(t *testing.T) {
	c := newCoordinator(2)
	jobs, err := c.addWorker(7, peer.Bitfield{0xc0})
	if err != nil {
		t.Fatal(err)
	}
	w := c.workers[7]
	if !w.dispatch(c.scheduler) {
		t.Fatal("dispatch failed")
	}
	<-jobs
	if err := c.scheduler.complete(0); err != nil {
		t.Fatal(err)
	}
	w.phase = workerAnnouncing
	if w.dispatch(c.scheduler) || len(w.jobs) != 0 || c.scheduler.states[1] != pieceMissing {
		t.Error("announcing worker received another download assignment")
	}
	if worker, err := c.workerForResult(pieceResult{workerID: 7, index: 0, kind: commandDownload}); err == nil || worker != nil {
		t.Error("download result accepted from an announcing worker")
	}
	if err := c.removeWorker(7); err != nil {
		t.Fatalf("remove announcing worker: %v", err)
	}
	if c.scheduler.states[0] != pieceComplete || c.scheduler.completedCount != 1 || c.scheduler.states[1] != pieceMissing {
		t.Error("removing announcing worker released or changed completed work")
	}
	if _, exists := c.workers[7]; exists {
		t.Error("announcing worker remains registered")
	}
	select {
	case _, ok := <-jobs:
		if ok {
			t.Error("removed worker's jobs remain open")
		}
	default:
		t.Error("removed worker's jobs were not closed")
	}
}

func TestCoordinatorHandleHaveResult(t *testing.T) {
	peerFailure := &PeerError{Op: "send have", Err: errors.New("connection closed")}
	fatal := errors.New("invalid announcement command")
	for _, tt := range []struct {
		name    string
		err     error
		removed bool
		fatal   bool
	}{
		{"success", nil, false, false},
		{"peer failure", peerFailure, true, false},
		{"wrapped peer failure", fmt.Errorf("announcement failed: %w", peerFailure), true, false},
		{"fatal failure", fatal, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dst := &downloadTestWriter{}
			tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: make([][20]byte, 2)}
			s, err := NewSession(tf, dst)
			if err != nil {
				t.Fatal(err)
			}
			s.stats.Downloaded = 4
			c := newCoordinator(2)
			jobs, err := c.addWorker(7, peer.Bitfield{0xc0})
			if err != nil {
				t.Fatal(err)
			}
			w := c.workers[7]
			if !w.dispatch(c.scheduler) {
				t.Fatal("dispatch failed")
			}
			<-jobs
			if err := c.handleSuccess(s, pieceResult{workerID: 7, index: 0, data: []byte("abcd")}); err != nil {
				t.Fatal(err)
			}
			select {
			case command := <-jobs:
				if command.kind != commandHave || command.index != 0 {
					t.Fatalf("unexpected announcement command: %+v", command)
				}
			default:
				t.Fatal("no have command was queued")
			}
			before := s.Stats()
			result := pieceResult{workerID: 7, index: 0, kind: commandHave, err: tt.err}
			err = c.handleHaveResult(result)
			if tt.fatal {
				if !errors.Is(err, tt.err) || c.workers[7] != w || w.phase != workerAnnouncing {
					t.Fatalf("fatal have result = %v; worker should remain announcing until shutdown", err)
				}
			} else if err != nil {
				t.Fatalf("recoverable have result became fatal: %v", err)
			}
			if tt.removed {
				if _, exists := c.workers[7]; exists {
					t.Fatal("failed announcing worker remains registered")
				}
				select {
				case _, ok := <-jobs:
					if ok {
						t.Error("failed worker's jobs remain open")
					}
				default:
					t.Error("failed worker's jobs were not closed")
				}
			} else if !tt.fatal && w.phase != workerIdle {
				t.Fatal("successful have acknowledgement did not make worker idle")
			}
			if s.Stats() != before || s.completedCount != 1 || !s.completed.HasPiece(0) || len(dst.writes) != 1 || c.scheduler.states[0] != pieceComplete || c.scheduler.completedCount != 1 || c.scheduler.states[1] != pieceMissing {
				t.Error("have result changed verified progress, storage, or scheduling completion")
			}
			if !tt.removed && !tt.fatal {
				if err := c.handleHaveResult(result); err == nil {
					t.Error("duplicate acknowledgement succeeded")
				}
				if !w.dispatch(c.scheduler) {
					t.Fatal("acknowledged worker could not receive its next piece")
				}
				if command := <-jobs; command.kind != commandDownload || command.index != 1 {
					t.Errorf("next command = %+v, want download of 1", command)
				}
			}
		})
	}
}

func TestCoordinatorHandleHaveResultRejectsInvalidResults(t *testing.T) {
	for _, tt := range []struct {
		name   string
		result pieceResult
		phase  workerPhase
	}{
		{"download result", pieceResult{workerID: 7, index: 0, kind: commandDownload}, workerAnnouncing},
		{"unknown kind", pieceResult{workerID: 7, index: 0, kind: workerCommandKind(255)}, workerAnnouncing},
		{"unknown worker", pieceResult{workerID: 99, index: 0, kind: commandHave}, workerAnnouncing},
		{"wrong index", pieceResult{workerID: 7, index: 1, kind: commandHave}, workerAnnouncing},
		{"idle worker", pieceResult{workerID: 7, index: 0, kind: commandHave}, workerIdle},
		{"downloading worker", pieceResult{workerID: 7, index: 0, kind: commandHave}, workerDownloading},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newCoordinator(2)
			jobs, err := c.addWorker(7, peer.Bitfield{0xc0})
			if err != nil {
				t.Fatal(err)
			}
			w := c.workers[7]
			if !w.dispatch(c.scheduler) {
				t.Fatal("dispatch failed")
			}
			<-jobs
			if err := c.scheduler.complete(0); err != nil {
				t.Fatal(err)
			}
			w.phase = tt.phase
			before := slices.Clone(c.scheduler.states)
			if err := c.handleHaveResult(tt.result); err == nil {
				t.Fatal("invalid have result succeeded")
			}
			if c.workers[7] != w || w.phase != tt.phase || len(w.jobs) != 0 || !slices.Equal(c.scheduler.states, before) || c.scheduler.completedCount != 1 {
				t.Error("invalid acknowledgement changed coordinator bookkeeping")
			}
		})
	}
}

func TestCoordinatorWorkerDownloadStoreHaveExchange(t *testing.T) {
	f := newWorkerTestFixture(t, -1)
	c := newCoordinator(2)
	jobs, err := c.addWorker(7, f.client.Bitfield)
	if err != nil {
		t.Fatal(err)
	}
	_, done := startPeerWorkerTest(t, f, jobs, c.results)
	w := c.workers[7]
	for index := 0; index < 2; index++ {
		if !w.dispatch(c.scheduler) {
			t.Fatal("dispatch failed")
		}
		var result pieceResult
		select {
		case result = <-c.results:
		case <-time.After(time.Second):
			t.Fatal("download result never arrived")
		}
		if result.kind != commandDownload || result.index != index || result.err != nil {
			t.Fatalf("unexpected download result: %+v", result)
		}
		if len(f.writer.writes) != index {
			t.Fatal("worker committed output before the coordinator handled its result")
		}
		if err := c.handleSuccess(f.session, result); err != nil {
			t.Fatal(err)
		}
		if w.phase != workerAnnouncing || len(f.writer.writes) != index+1 || w.dispatch(c.scheduler) {
			t.Fatal("worker became eligible for more work before its have acknowledgement")
		}
		if index == 1 && (!f.session.Complete() || !c.scheduler.done()) {
			t.Fatal("verified completion should precede the final have acknowledgement")
		}
		select {
		case result = <-c.results:
		case <-time.After(time.Second):
			t.Fatal("have acknowledgement never arrived")
		}
		if result.kind != commandHave || result.index != index || result.data != nil || result.err != nil {
			t.Fatalf("unexpected have result: %+v", result)
		}
		if err := c.handleHaveResult(result); err != nil {
			t.Fatal(err)
		}
		if w.phase != workerIdle {
			t.Fatal("worker did not become idle after acknowledgement")
		}
	}
	if err := c.removeWorker(7); err != nil {
		t.Fatal(err)
	}
	waitPeerWorkerTest(t, done)
	if stats := f.session.Stats(); stats.Downloaded != 8 || stats.Left != 0 {
		t.Errorf("final stats = %+v, want downloaded 8 and left 0", stats)
	}
	if len(f.writer.writes) != 2 || !bytes.Equal(f.writer.writes[0].data, f.data[:4]) || !bytes.Equal(f.writer.writes[1].data, f.data[4:]) {
		t.Fatal("stored output differs from verified piece results")
	}
	wire := bytes.NewReader(f.conn.outgoing.Bytes())
	for _, want := range []struct {
		id    peer.MessageID
		index uint32
	}{
		{peer.MsgRequest, 0}, {peer.MsgHave, 0}, {peer.MsgRequest, 1}, {peer.MsgHave, 1},
	} {
		message, err := peer.ReadMessage(wire)
		if err != nil {
			t.Fatal(err)
		}
		if message.ID != want.id || len(message.Payload) < 4 || binary.BigEndian.Uint32(message.Payload[:4]) != want.index {
			t.Fatalf("unexpected wire message: %+v, want ID %d index %d", message, want.id, want.index)
		}
	}
	if wire.Len() != 0 {
		t.Errorf("unexpected extra network bytes: %d", wire.Len())
	}
}
