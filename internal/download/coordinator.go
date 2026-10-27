package download

import (
	"errors"
	"fmt"
	"log"
	"slices"

	"github.com/lorithan/torrent-client/internal/peer"
)

type workerPhase uint8

const (
	workerIdle workerPhase = iota
	workerDownloading
	workerAnnouncing
)

type workerState struct {
	jobs          chan workerCommand
	available     peer.Bitfield
	assignedIndex int
	phase         workerPhase
}

func newWorkerState(available peer.Bitfield) *workerState {
	return &workerState{
		jobs:      make(chan workerCommand, 1),
		available: slices.Clone(available),
	}
}

func (w *workerState) dispatch(scheduler *pieceScheduler) bool {
	if w.phase != workerIdle {
		return false
	}

	assignedIndex, assigned := scheduler.assign(w.available)
	if !assigned {
		return false
	}

	w.assignedIndex = assignedIndex
	w.phase = workerDownloading
	w.jobs <- workerCommand{
		index: assignedIndex,
		kind:  commandDownload,
	}

	return true
}

type coordinator struct {
	scheduler *pieceScheduler
	workers   map[int]*workerState
	results   chan pieceResult
}

func newCoordinator(pieceCount int) *coordinator {
	return &coordinator{
		scheduler: newPieceScheduler(pieceCount),
		workers:   make(map[int]*workerState),
		results:   make(chan pieceResult),
	}
}

func (c *coordinator) addWorker(
	workerID int,
	available peer.Bitfield,
) (<-chan workerCommand, error) {
	_, exists := c.workers[workerID]
	if exists {
		return nil, fmt.Errorf("worker %d already exists", workerID)
	}

	worker := newWorkerState(available)
	c.workers[workerID] = worker

	return worker.jobs, nil
}

func (c *coordinator) removeWorker(workerID int) error {
	worker, exists := c.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %d does not exist", workerID)
	}

	if worker.phase == workerDownloading {
		err := c.scheduler.release(worker.assignedIndex)
		if err != nil {
			return err
		}
	}

	close(worker.jobs)
	delete(c.workers, workerID)
	return nil
}

func (c *coordinator) workerForResult(
	result pieceResult,
) (*workerState, error) {
	if result.kind != commandDownload {
		return nil, fmt.Errorf("expected download result")
	}

	worker, exists := c.workers[result.workerID]
	if !exists {
		return nil, fmt.Errorf("worker %d does not exist", result.workerID)
	}

	if worker.phase != workerDownloading {
		return nil, fmt.Errorf("worker %d is not downloading", result.workerID)
	}

	if result.index != worker.assignedIndex {
		return nil, fmt.Errorf("assigned index mismatch. expected %d, found %d", worker.assignedIndex, result.index)
	}

	return worker, nil
}

func (c *coordinator) handleFailure(result pieceResult) error {
	_, err := c.workerForResult(result)
	if err != nil {
		return err
	}

	if result.err == nil {
		return fmt.Errorf("expected failure result for worker %d", result.workerID)
	}

	var peerErr *PeerError
	if errors.As(result.err, &peerErr) {
		log.Printf("peer error for worker %d (piece %d): %v", result.workerID, result.index, result.err)
		return c.removeWorker(result.workerID)

	} else {
		return result.err
	}
}

func (c *coordinator) handleSuccess(
	session *Session,
	result pieceResult,
) error {
	worker, err := c.workerForResult(result)
	if err != nil {
		return err
	}

	if result.err != nil {
		return result.err
	}
	if session.completed.HasPiece(result.index) {
		return fmt.Errorf("piece %d already complete", result.index)
	}

	err = session.storePiece(result.index, result.data)
	if err != nil {
		return err
	}

	err = c.scheduler.complete(result.index)
	if err != nil {
		return err
	}

	err = session.completed.SetPiece(result.index)
	if err != nil {
		return err
	}
	session.completedCount++

	session.statsMu.Lock()
	session.stats.Left -= int64(len(result.data))
	session.statsMu.Unlock()

	worker.phase = workerAnnouncing
	worker.jobs <- workerCommand{
		kind:  commandHave,
		index: worker.assignedIndex,
	}
	return nil
}

func (c *coordinator) handleHaveResult(result pieceResult) error {
	if result.kind != commandHave {
		return fmt.Errorf("expected have result")
	}

	worker, exists := c.workers[result.workerID]
	if !exists {
		return fmt.Errorf("worker %d does not exist", result.workerID)
	}

	if worker.phase != workerAnnouncing {
		return fmt.Errorf("worker %d is not announcing", result.workerID)
	}

	if result.index != worker.assignedIndex {
		return fmt.Errorf("assigned index mismatch. expected %d, found %d", worker.assignedIndex, result.index)
	}

	if result.err == nil {
		worker.phase = workerIdle
	}

	if _, ok := errors.AsType[*PeerError](result.err); ok {
		log.Printf("worker (id: %d) annouce error: %v", result.workerID, result.err)
		return c.removeWorker(result.workerID)
	}

	return result.err
}
