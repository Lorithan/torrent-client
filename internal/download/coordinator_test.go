package download

import (
	"bytes"
	"context"
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
		result := pieceResult{workerID: 7, index: index, data: []byte(data), available: peer.Bitfield{0xc0}}
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
		if err := c.handleHaveResult(pieceResult{workerID: 7, index: index, kind: commandHave, available: peer.Bitfield{0xc0}}); err != nil {
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
		{"write error", pieceResult{workerID: 7, index: 0, data: []byte("abcd"), available: peer.Bitfield{0x80}}, 2, diskErr, diskErr, 1},
		{"short write", pieceResult{workerID: 7, index: 0, data: []byte("abcd"), available: peer.Bitfield{0x80}}, 2, nil, io.ErrShortWrite, 1},
		{"invalid size", pieceResult{workerID: 7, index: 0, data: []byte("ab"), available: peer.Bitfield{0x80}}, 4, nil, nil, 0},
		{"failed result", pieceResult{workerID: 7, index: 0, err: transferErr}, 4, nil, transferErr, 0},
		{"unknown worker", pieceResult{workerID: 99, index: 0, data: []byte("abcd"), available: peer.Bitfield{0x80}}, 4, nil, nil, 0},
		{"wrong assignment", pieceResult{workerID: 7, index: 1, data: []byte("abcd"), available: peer.Bitfield{0x80}}, 4, nil, nil, 0},
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
			err = c.handleSuccess(s, pieceResult{workerID: 7, index: 0, data: []byte("abcd"), available: peer.Bitfield{0x80}})
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
			if err := c.handleSuccess(s, pieceResult{workerID: 7, index: 0, data: []byte("abcd"), available: peer.Bitfield{0x80}}); err != nil {
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
			result := pieceResult{workerID: 7, index: 0, kind: commandHave, err: tt.err, available: peer.Bitfield{0xc0}}
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
		if err := c.handleResult(f.session, result); err != nil {
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
		if err := c.handleResult(f.session, result); err != nil {
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

func TestCoordinatorUsesLearnedAvailability(t *testing.T) {
	for _, learnedDuring := range []string{"download", "have"} {
		t.Run(learnedDuring, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: make([][20]byte, 2)}
			s, err := NewSession(tf, &downloadTestWriter{})
			if err != nil {
				t.Fatal(err)
			}
			c := newCoordinator(2)
			jobs, err := c.addWorker(7, peer.Bitfield{0x80})
			if err != nil {
				t.Fatal(err)
			}
			w := c.workers[7]
			if !w.dispatch(c.scheduler) {
				t.Fatal("initial dispatch failed")
			}
			<-jobs
			available := peer.Bitfield{0x80}
			if learnedDuring == "download" {
				available = peer.Bitfield{0xc0}
			}
			result := pieceResult{workerID: 7, index: 0, data: []byte("abcd"), available: available}
			if err := c.handleSuccess(s, result); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(w.available, available) {
				t.Fatal("download snapshot was not adopted")
			}
			<-jobs // Consume the have command before acknowledging it.
			if err := c.handleHaveResult(pieceResult{workerID: 7, index: 0, kind: commandHave, available: peer.Bitfield{0xc0}}); err != nil {
				t.Fatal(err)
			}
			if !w.available.HasPiece(1) || !w.dispatch(c.scheduler) {
				t.Fatal("newly advertised piece was not eligible for dispatch")
			}
			if command := <-jobs; command.kind != commandDownload || command.index != 1 {
				t.Errorf("next command = %+v, want download of 1", command)
			}
		})
	}
}

func TestCoordinatorRejectsInvalidAvailabilityBeforeMutation(t *testing.T) {
	for _, kind := range []workerCommandKind{commandDownload, commandHave} {
		for _, bad := range []struct {
			name      string
			available peer.Bitfield
		}{
			{"missing", nil}, {"too long", peer.Bitfield{0x80, 0}}, {"nonzero padding", peer.Bitfield{0x81}},
		} {
			t.Run(fmt.Sprintf("kind_%d/%s", kind, bad.name), func(t *testing.T) {
				dst := &downloadTestWriter{}
				tf := &torrent.TorrentFile{Length: 8, PieceLength: 4, Pieces: make([][20]byte, 2)}
				s, err := NewSession(tf, dst)
				if err != nil {
					t.Fatal(err)
				}
				c := newCoordinator(2)
				jobs, err := c.addWorker(7, peer.Bitfield{0x80})
				if err != nil {
					t.Fatal(err)
				}
				w := c.workers[7]
				if !w.dispatch(c.scheduler) {
					t.Fatal("dispatch failed")
				}
				<-jobs
				if kind == commandHave {
					if err := c.handleSuccess(s, pieceResult{workerID: 7, index: 0, data: []byte("abcd"), available: peer.Bitfield{0x80}}); err != nil {
						t.Fatal(err)
					}
					<-jobs
				}
				beforeStats, beforeCount, beforeWrites := s.Stats(), s.completedCount, len(dst.writes)
				beforeStates, beforeAvailable, beforePhase := slices.Clone(c.scheduler.states), slices.Clone(w.available), w.phase
				result := pieceResult{workerID: 7, index: 0, kind: kind, data: []byte("abcd"), available: bad.available}
				if kind == commandDownload {
					err = c.handleSuccess(s, result)
				} else {
					err = c.handleHaveResult(result)
				}
				if err == nil {
					t.Fatal("invalid snapshot was accepted")
				}
				if !strings.Contains(err.Error(), "worker 7") {
					t.Errorf("snapshot error lacks worker context: %v", err)
				}
				if s.Stats() != beforeStats || s.completedCount != beforeCount || c.scheduler.completedCount != beforeCount || len(dst.writes) != beforeWrites || c.workers[7] != w || w.phase != beforePhase || !slices.Equal(c.scheduler.states, beforeStates) || !slices.Equal(w.available, beforeAvailable) || len(w.jobs) != 0 {
					t.Error("invalid availability changed output, progress, scheduling, or worker state")
				}
			})
		}
	}
}

func TestWorkerStateRequestsAvailabilityWithoutReservingPiece(t *testing.T) {
	c := newCoordinator(2)
	jobs, err := c.addWorker(7, peer.Bitfield{0xc0})
	if err != nil {
		t.Fatal(err)
	}
	w := c.workers[7]
	w.assignedIndex = 1 // An idle record may retain a previous piece index.
	before := slices.Clone(c.scheduler.states)
	if !w.requestAvailability() {
		t.Fatal("idle worker rejected an availability request")
	}
	if w.phase != workerWaitingAvailability || w.assignedIndex != 1 || !slices.Equal(c.scheduler.states, before) {
		t.Fatal("availability request changed an assignment or failed to enter waiting phase")
	}
	select {
	case command := <-jobs:
		if command.kind != commandAvailability {
			t.Fatalf("queued command = %+v, want availability", command)
		}
	default:
		t.Fatal("availability request did not enqueue a command")
	}
	if w.requestAvailability() || w.dispatch(c.scheduler) || len(w.jobs) != 0 || !slices.Equal(c.scheduler.states, before) {
		t.Error("waiting worker accepted additional work or changed reservations")
	}
	if err := c.removeWorker(7); err != nil {
		t.Fatalf("remove waiting worker without an assignment: %v", err)
	}
	if !slices.Equal(c.scheduler.states, before) || c.scheduler.completedCount != 0 {
		t.Error("waiting-worker removal changed scheduling state")
	}
}

func TestWorkerStateAvailabilityRequestRejectsOccupiedPhases(t *testing.T) {
	for _, phase := range []workerPhase{workerDownloading, workerAnnouncing, workerWaitingAvailability} {
		w := newWorkerState(peer.Bitfield{0x80})
		w.phase, w.assignedIndex = phase, 7
		if w.requestAvailability() {
			t.Errorf("availability request accepted during phase %d", phase)
		}
		if w.phase != phase || w.assignedIndex != 7 || len(w.jobs) != 0 || !bytes.Equal(w.available, []byte{0x80}) {
			t.Errorf("rejected request mutated worker state during phase %d", phase)
		}
	}
}

func TestCoordinatorAvailabilityResultMakesNewPieceEligible(t *testing.T) {
	c := newCoordinator(2)
	jobs, err := c.addWorker(7, peer.Bitfield{0})
	if err != nil {
		t.Fatal(err)
	}
	w := c.workers[7]
	w.assignedIndex = 99 // Retained value is irrelevant to availability commands.
	if !w.requestAvailability() {
		t.Fatal("availability request failed")
	}
	<-jobs
	result := pieceResult{workerID: 7, kind: commandAvailability, available: peer.Bitfield{0x40}}
	if err := c.handleResult(nil, result); err != nil {
		t.Fatal(err)
	}
	if w.phase != workerIdle || w.assignedIndex != 99 || !bytes.Equal(w.available, []byte{0x40}) {
		t.Fatal("availability result did not update snapshot and phase independently of piece index")
	}
	if c.scheduler.states[0] != pieceMissing || c.scheduler.states[1] != pieceMissing || c.scheduler.completedCount != 0 {
		t.Fatal("availability result changed scheduling or completion")
	}
	if err := c.handleAvailabilityResult(result); err == nil {
		t.Error("duplicate availability result succeeded")
	}
	if !w.dispatch(c.scheduler) {
		t.Fatal("worker could not receive newly available work")
	}
	if command := <-jobs; command.kind != commandDownload || command.index != 1 {
		t.Errorf("next command = %+v, want download of 1", command)
	}
}

func TestCoordinatorAvailabilityResultFailures(t *testing.T) {
	peerFailure := &PeerError{Op: "wait for availability", Err: io.EOF}
	fatal := errors.New("invalid command")
	for _, tt := range []struct {
		name    string
		err     error
		removed bool
	}{
		{"peer failure", peerFailure, true},
		{"wrapped peer failure", fmt.Errorf("availability failed: %w", peerFailure), true},
		{"fatal error", fatal, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newCoordinator(2)
			jobs, err := c.addWorker(7, peer.Bitfield{0})
			if err != nil {
				t.Fatal(err)
			}
			w := c.workers[7]
			w.assignedIndex = 99
			if !w.requestAvailability() {
				t.Fatal("request failed")
			}
			<-jobs
			err = c.handleAvailabilityResult(pieceResult{workerID: 7, kind: commandAvailability, err: tt.err})
			if tt.removed {
				if err != nil {
					t.Fatal(err)
				}
				if _, exists := c.workers[7]; exists {
					t.Fatal("failed waiting worker remains registered")
				}
				select {
				case _, ok := <-jobs:
					if ok {
						t.Error("removed worker's jobs remain open")
					}
				default:
					t.Error("removed worker's jobs were not closed")
				}
			} else if !errors.Is(err, tt.err) || c.workers[7] != w || w.phase != workerWaitingAvailability {
				t.Fatalf("fatal result = %v, want original cause and retained waiting worker", err)
			}
			if !slices.Equal(c.scheduler.states, []pieceState{pieceMissing, pieceMissing}) || c.scheduler.completedCount != 0 {
				t.Error("failed availability wait changed scheduling or completion")
			}
		})
	}
}

func TestCoordinatorAvailabilityResultRejectsInvalidMessages(t *testing.T) {
	for _, tt := range []struct {
		name   string
		result pieceResult
		phase  workerPhase
	}{
		{"download result", pieceResult{workerID: 7, kind: commandDownload}, workerWaitingAvailability},
		{"have result", pieceResult{workerID: 7, kind: commandHave}, workerWaitingAvailability},
		{"unknown worker", pieceResult{workerID: 99, kind: commandAvailability}, workerWaitingAvailability},
		{"idle worker", pieceResult{workerID: 7, kind: commandAvailability}, workerIdle},
		{"downloading worker", pieceResult{workerID: 7, kind: commandAvailability}, workerDownloading},
		{"announcing worker", pieceResult{workerID: 7, kind: commandAvailability}, workerAnnouncing},
		{"missing snapshot", pieceResult{workerID: 7, kind: commandAvailability}, workerWaitingAvailability},
		{"oversized snapshot", pieceResult{workerID: 7, kind: commandAvailability, available: peer.Bitfield{0x80, 0}}, workerWaitingAvailability},
		{"nonzero padding", pieceResult{workerID: 7, kind: commandAvailability, available: peer.Bitfield{0x81}}, workerWaitingAvailability},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newCoordinator(2)
			if _, err := c.addWorker(7, peer.Bitfield{0}); err != nil {
				t.Fatal(err)
			}
			w := c.workers[7]
			w.phase = tt.phase
			err := c.handleAvailabilityResult(tt.result)
			if err == nil {
				t.Fatal("invalid availability result succeeded")
			}
			if tt.phase == workerWaitingAvailability && tt.result.kind == commandAvailability && tt.result.workerID == 7 && !strings.Contains(err.Error(), "worker 7") {
				t.Errorf("snapshot error lacks worker context: %v", err)
			}
			if c.workers[7] != w || w.phase != tt.phase || !bytes.Equal(w.available, []byte{0}) || len(w.jobs) != 0 || !slices.Equal(c.scheduler.states, []pieceState{pieceMissing, pieceMissing}) {
				t.Error("invalid message changed worker or scheduling state")
			}
		})
	}
}

func TestCoordinatorRoutesResultFailures(t *testing.T) {
	peerFailure := &PeerError{Op: "network operation", Err: io.EOF}
	fatal := errors.New("fatal operation failure")
	for _, kind := range []workerCommandKind{commandDownload, commandHave, commandAvailability} {
		for _, recoverable := range []bool{true, false} {
			t.Run(fmt.Sprintf("kind_%d/recoverable_%t", kind, recoverable), func(t *testing.T) {
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
				if kind == commandAvailability {
					if !w.requestAvailability() {
						t.Fatal("availability request failed")
					}
					<-jobs
				} else {
					if !w.dispatch(c.scheduler) {
						t.Fatal("dispatch failed")
					}
					<-jobs
					if kind == commandHave {
						if err := c.handleResult(s, pieceResult{workerID: 7, index: 0, available: peer.Bitfield{0x80}, data: []byte("abcd")}); err != nil {
							t.Fatal(err)
						}
						<-jobs
					}
				}
				cause := error(fatal)
				if recoverable {
					cause = fmt.Errorf("wrapped failure: %w", peerFailure)
				}
				beforeStats, beforePhase := s.Stats(), w.phase
				err = c.handleResult(s, pieceResult{workerID: 7, index: 0, kind: kind, err: cause})
				if recoverable {
					if err != nil {
						t.Fatalf("peer failure was not recovered: %v", err)
					}
					if _, exists := c.workers[7]; exists {
						t.Error("failed worker remains registered")
					}
				} else if !errors.Is(err, fatal) || c.workers[7] != w || w.phase != beforePhase {
					t.Errorf("fatal error was not propagated with state preserved: %v", err)
				}
				if s.Stats() != beforeStats {
					t.Error("routing failure changed progress statistics")
				}
				if kind == commandHave && (!s.Complete() || !c.scheduler.done()) {
					t.Error("have failure lost completed progress")
				}
			})
		}
	}
}

func TestCoordinatorRejectsUnknownResultKind(t *testing.T) {
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
	err = c.handleResult(nil, pieceResult{workerID: 7, kind: workerCommandKind(255), err: &PeerError{Op: "unexpected", Err: io.EOF}})
	if err == nil || !strings.Contains(err.Error(), "255") {
		t.Fatalf("error = %v, want unknown result kind 255", err)
	}
	if c.workers[7] != w || w.phase != workerDownloading || c.scheduler.states[0] != pieceAssigned || len(w.jobs) != 0 {
		t.Error("unknown result kind changed the worker or assignment")
	}
}

func TestCoordinatorReadyToFinish(t *testing.T) {
	for _, tt := range []struct {
		name     string
		pieces   int
		complete bool
		phases   []workerPhase
		want     bool
	}{
		{"empty torrent", 0, false, nil, true},
		{"incomplete without workers", 1, false, nil, false},
		{"incomplete idle worker", 1, false, []workerPhase{workerIdle}, false},
		{"incomplete waiting worker", 1, false, []workerPhase{workerWaitingAvailability}, false},
		{"complete without workers", 1, true, nil, true},
		{"complete idle and waiting workers", 1, true, []workerPhase{workerIdle, workerWaitingAvailability}, true},
		{"pending download", 1, true, []workerPhase{workerIdle, workerDownloading}, false},
		{"pending announcement", 1, true, []workerPhase{workerWaitingAvailability, workerAnnouncing}, false},
		{"unknown phase", 1, true, []workerPhase{workerPhase(255)}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newCoordinator(tt.pieces)
			if tt.complete {
				index, ok := c.scheduler.assign(peer.Bitfield{0x80})
				if !ok {
					t.Fatal("assignment failed")
				}
				if err := c.scheduler.complete(index); err != nil {
					t.Fatal(err)
				}
			}
			for id, phase := range tt.phases {
				c.workers[id] = &workerState{phase: phase}
			}
			if got := c.readyToFinish(); got != tt.want {
				t.Errorf("readyToFinish = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestCoordinatorWaitsForFinalHaveBeforeFinishing(t *testing.T) {
	c := newCoordinator(1)
	tf := &torrent.TorrentFile{Length: 4, PieceLength: 4, Pieces: make([][20]byte, 1)}
	s, err := NewSession(tf, &downloadTestWriter{})
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := c.addWorker(7, peer.Bitfield{0x80})
	if err != nil {
		t.Fatal(err)
	}
	if !c.workers[7].dispatch(c.scheduler) {
		t.Fatal("dispatch failed")
	}
	<-jobs
	if c.readyToFinish() {
		t.Fatal("coordinator finished before storing its piece")
	}
	if err := c.handleResult(s, pieceResult{workerID: 7, kind: commandDownload, index: 0, data: []byte("abcd"), available: peer.Bitfield{0x80}}); err != nil {
		t.Fatal(err)
	}
	if !s.Complete() || c.readyToFinish() {
		t.Fatal("coordinator did not distinguish committed output from pending announcement")
	}
	<-jobs
	if err := c.handleResult(s, pieceResult{workerID: 7, kind: commandHave, index: 0, available: peer.Bitfield{0x80}}); err != nil {
		t.Fatal(err)
	}
	if !c.readyToFinish() {
		t.Fatal("coordinator did not become ready after the final acknowledgement")
	}
	if _, err := c.addWorker(8, peer.Bitfield{0}); err != nil {
		t.Fatal(err)
	}
	if !c.workers[8].requestAvailability() {
		t.Fatal("availability request failed")
	}
	if !c.readyToFinish() {
		t.Fatal("completed transfer waits unnecessarily for availability reads instead of canceling them during cleanup")
	}
}

func TestCoordinatorDispatchWorkReservesBeforeRequestingAvailability(t *testing.T) {
	// Map iteration order varies. The property must hold for every order: two
	// workers claim the two pieces, and the spare stays idle for prompt recovery.
	for attempt := 0; attempt < 64; attempt++ {
		c := newCoordinator(2)
		for id, available := range []peer.Bitfield{{0x80}, {0x80}, {0x40}} {
			if _, err := c.addWorker(id, available); err != nil {
				t.Fatal(err)
			}
		}
		c.dispatchWork()
		assigned := make(map[int]int)
		spareID := -1
		for id, w := range c.workers {
			switch w.phase {
			case workerDownloading:
				select {
				case command := <-w.jobs:
					if command.kind != commandDownload || command.index != w.assignedIndex || !w.available.HasPiece(command.index) {
						t.Fatalf("invalid assignment: %+v", command)
					}
					if _, exists := assigned[command.index]; exists {
						t.Fatal("duplicate piece assignment")
					}
					assigned[command.index] = id
				default:
					t.Fatal("downloading worker has no job")
				}
			case workerIdle:
				if spareID != -1 || len(w.jobs) != 0 {
					t.Fatal("unexpected spare worker or queued job")
				}
				spareID = id
			default:
				t.Fatalf("worker %d entered phase %d although all missing pieces could be assigned", id, w.phase)
			}
		}
		if len(assigned) != 2 || spareID == -1 || c.scheduler.hasMissing() {
			t.Fatal("did not assign both pieces while retaining an idle spare")
		}
		if err := c.removeWorker(assigned[0]); err != nil {
			t.Fatal(err)
		}
		c.dispatchWork()
		spare := c.workers[spareID]
		if spare.phase != workerDownloading || spare.assignedIndex != 0 {
			t.Fatal("idle spare did not take the released assignment")
		}
	}
}

func TestCoordinatorDispatchWorkRequestsUnavailableMissingPieces(t *testing.T) {
	c := newCoordinator(2)
	jobs, err := c.addWorker(7, peer.Bitfield{0})
	if err != nil {
		t.Fatal(err)
	}
	c.dispatchWork()
	w := c.workers[7]
	if w.phase != workerWaitingAvailability {
		t.Fatal("idle worker with no eligible piece did not wait for availability")
	}
	select {
	case command := <-jobs:
		if command.kind != commandAvailability {
			t.Fatalf("command = %+v, want availability", command)
		}
	default:
		t.Fatal("availability request was not queued")
	}
	c.dispatchWork()
	if len(w.jobs) != 0 || !slices.Equal(c.scheduler.states, []pieceState{pieceMissing, pieceMissing}) {
		t.Error("repeated scheduling duplicated work or changed missing pieces")
	}
}

func TestCoordinatorRunCompletesRegisteredWorker(t *testing.T) {
	f := newWorkerTestFixture(t, -1)
	c := newCoordinator(2)
	jobs, err := c.addWorker(7, f.client.Bitfield)
	if err != nil {
		t.Fatal(err)
	}
	cancelWorker, done := startPeerWorkerTest(t, f, jobs, c.results)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = c.run(ctx, f.session)
	cancelWorker()
	waitPeerWorkerTest(t, done)
	if err != nil {
		t.Fatalf("coordinator stopped before its worker finished: %v", err)
	}
	if !c.readyToFinish() || !f.session.Complete() || len(f.writer.writes) != 2 || f.session.Stats().Downloaded != 8 || f.session.Stats().Left != 0 {
		t.Error("coordinator returned without complete verified output and acknowledged announcements")
	}
}

func TestCoordinatorRunWaitsForAvailabilityWorker(t *testing.T) {
	c := newCoordinator(1)
	jobs, err := c.addWorker(7, peer.Bitfield{0})
	if err != nil {
		t.Fatal(err)
	}
	if !c.workers[7].requestAvailability() {
		t.Fatal("request failed")
	}
	<-jobs
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err = c.run(ctx, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting worker treated as exhausted: %v", err)
	}
	if c.workers[7] == nil || c.workers[7].phase != workerWaitingAvailability {
		t.Error("waiting canceled without preserving registry for caller cleanup")
	}
}

func TestCoordinatorRunEmptyExhaustedAndCanceled(t *testing.T) {
	if err := newCoordinator(0).run(context.Background(), nil); err != nil {
		t.Errorf("empty torrent: %v", err)
	}
	err := newCoordinator(1).run(context.Background(), nil)
	var peerErr *PeerError
	if !errors.As(err, &peerErr) {
		t.Errorf("no workers error = %v, want retryable peer exhaustion", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := newCoordinator(0).run(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("cancellation error = %v, want context.Canceled", err)
	}
}

func TestCoordinatorRunRejectsClosedResults(t *testing.T) {
	c := newCoordinator(1)
	if _, err := c.addWorker(7, peer.Bitfield{0x80}); err != nil {
		t.Fatal(err)
	}
	close(c.results)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := c.run(ctx, nil)
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("error = %v, want explicitly closed result channel", err)
	}
}

func TestCoordinatorRunPropagatesStorageFailure(t *testing.T) {
	f := newWorkerTestFixture(t, -1)
	want := errors.New("disk full")
	f.writer.failAt, f.writer.err = 1, want
	c := newCoordinator(2)
	jobs, err := c.addWorker(7, f.client.Bitfield)
	if err != nil {
		t.Fatal(err)
	}
	cancelWorker, done := startPeerWorkerTest(t, f, jobs, c.results)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = c.run(ctx, f.session)
	cancelWorker()
	waitPeerWorkerTest(t, done)
	var peerErr *PeerError
	if !errors.Is(err, want) || errors.As(err, &peerErr) {
		t.Fatalf("storage error = %v, want original fatal storage cause", err)
	}
	if f.session.completedCount != 0 || c.scheduler.completedCount != 0 || f.session.Stats().Left != 8 || c.workers[7].phase != workerDownloading {
		t.Error("fatal storage failure marked progress or released the worker for more work")
	}
}

func TestNewCoordinatorForSessionPreservesVerifiedProgress(t *testing.T) {
	for _, tt := range []struct {
		name       string
		pieceCount int
		completed  []int
	}{
		{"empty torrent", 0, nil},
		{"fresh session", 10, nil},
		{"single completed piece", 1, []int{0}},
		{"most significant bit", 8, []int{0}},
		{"least significant bit", 8, []int{7}},
		{"mixed pieces", 10, []int{0, 3, 7, 8}},
		{"partial final byte", 10, []int{8, 9}},
		{"complete session", 3, []int{0, 1, 2}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if value := recover(); value != nil {
					t.Fatalf("constructor panicked on valid session progress: %v", value)
				}
			}()
			tf := &torrent.TorrentFile{Length: int64(tt.pieceCount) * 4, PieceLength: 4, Pieces: make([][20]byte, tt.pieceCount)}
			s, err := NewSession(tf, &downloadTestWriter{})
			if err != nil {
				t.Fatal(err)
			}
			completed := make(map[int]bool)
			for _, index := range tt.completed {
				if err := s.completed.SetPiece(index); err != nil {
					t.Fatal(err)
				}
				s.completedCount++
				s.stats.Left -= 4
				completed[index] = true
			}
			s.stats.Downloaded, s.stats.Uploaded = 100, 3
			beforeStats, beforeBits, beforeCount := s.Stats(), slices.Clone(s.completed), s.completedCount
			c := newCoordinatorForSession(s)
			if len(c.scheduler.states) != tt.pieceCount || c.scheduler.completedCount != len(tt.completed) {
				t.Fatalf("scheduler has %d states and %d completions; want %d and %d", len(c.scheduler.states), c.scheduler.completedCount, tt.pieceCount, len(tt.completed))
			}
			for index, state := range c.scheduler.states {
				want := pieceMissing
				if completed[index] {
					want = pieceComplete
				}
				if state != want {
					t.Errorf("piece %d state = %d, want %d", index, state, want)
				}
			}
			available := peer.NewBitfield(tt.pieceCount)
			for index := range tt.pieceCount {
				if err := available.SetPiece(index); err != nil {
					t.Fatal(err)
				}
			}
			index, found := c.scheduler.assign(available)
			if found && completed[index] {
				t.Error("restored completed piece was assigned again")
			}
			if found != (len(tt.completed) < tt.pieceCount) {
				t.Error("restored scheduler disagrees about remaining work")
			}
			if s.Stats() != beforeStats || !slices.Equal(s.completed, beforeBits) || s.completedCount != beforeCount || len(c.workers) != 0 {
				t.Error("initializing scheduling state changed session progress or started workers")
			}
		})
	}
}
