package download

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"

	"github.com/lorithan/torrent-client/internal/peer"
)

type workerPhase uint8

const (
	workerIdle workerPhase = iota
	workerDownloading
	workerAnnouncing
	workerWaitingAvailability
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
	workerWG  sync.WaitGroup
}

func newCoordinator(pieceCount int) *coordinator {
	return &coordinator{
		scheduler: newPieceScheduler(pieceCount),
		workers:   make(map[int]*workerState),
		results:   make(chan pieceResult),
	}
}

func newCoordinatorForSession(session *Session) *coordinator {
	c := newCoordinator(len(session.torrent.Pieces))

	for index := range session.torrent.Pieces {
		if session.completed.HasPiece(index) {
			c.scheduler.states[index] = pieceComplete
			c.scheduler.completedCount++
		}
	}

	return c
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

func (c *coordinator) startWorker(
	ctx context.Context,
	session *Session,
	workerID int,
	client *peer.Client,
) error {
	jobs, err := c.addWorker(workerID, client.Bitfield)
	if err != nil {
		return err
	}

	c.workerWG.Go(func() {
		session.runPeerWorker(ctx, workerID, client, jobs, c.results)
	})

	return nil
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

	err = result.available.Validate(len(c.scheduler.states))
	if err != nil {
		return fmt.Errorf("worker %d availability: %w", result.workerID, err)
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
	worker.available = result.available
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
		err := result.available.Validate(len(c.scheduler.states))
		if err != nil {
			return fmt.Errorf("worker %d availability: %w", result.workerID, err)
		}

		worker.available = result.available
		worker.phase = workerIdle
	}

	if _, ok := errors.AsType[*PeerError](result.err); ok {
		log.Printf("worker %d annouce error: %v", result.workerID, result.err)
		return c.removeWorker(result.workerID)
	}

	return result.err
}

func (w *workerState) requestAvailability() bool {
	if w.phase != workerIdle {
		return false
	}

	w.phase = workerWaitingAvailability
	w.jobs <- workerCommand{
		kind: commandAvailability,
	}

	return true
}

func (c *coordinator) handleAvailabilityResult(result pieceResult) error {
	if result.kind != commandAvailability {
		return fmt.Errorf("expected availability result")
	}

	worker, exists := c.workers[result.workerID]
	if !exists {
		return fmt.Errorf("worker %d does not exist", result.workerID)
	}

	if worker.phase != workerWaitingAvailability {
		return fmt.Errorf("worker %d is not currently waiting", result.workerID)
	}

	if result.err == nil {
		err := result.available.Validate(len(c.scheduler.states))
		if err != nil {
			return fmt.Errorf("worker %d availability: %w", result.workerID, err)
		}

		worker.available = result.available
		worker.phase = workerIdle
	}

	if _, ok := errors.AsType[*PeerError](result.err); ok {
		log.Printf("worker %d availability error: %v", result.workerID, result.err)
		return c.removeWorker(result.workerID)
	}

	return result.err
}

func (c *coordinator) handleResult(
	session *Session,
	result pieceResult,
) error {
	switch result.kind {
	case commandDownload:
		if result.err != nil {
			return c.handleFailure(result)
		} else {
			return c.handleSuccess(session, result)
		}
	case commandHave:
		return c.handleHaveResult(result)
	case commandAvailability:
		return c.handleAvailabilityResult(result)
	default:
		return fmt.Errorf("Unknown worker command kind: %d", result.kind)
	}
}

func (c *coordinator) readyToFinish() bool {
	if c.scheduler.done() {
		for _, worker := range c.workers {
			if worker.phase == workerIdle || worker.phase == workerWaitingAvailability {
				continue
			}
			return false
		}

		return true
	}

	return false
}

func (c *coordinator) dispatchWork() {
	for _, worker := range c.workers {
		if worker.phase != workerIdle {
			continue
		}

		dispatched := worker.dispatch(c.scheduler)
		if dispatched {
			continue
		}
	}

	if !c.scheduler.hasMissing() {
		return
	}

	for _, worker := range c.workers {
		if worker.phase != workerIdle {
			continue
		}

		worker.requestAvailability()
	}
}

func (c *coordinator) run(
	ctx context.Context,
	session *Session,
) error {
	for {
		err := ctx.Err()
		if err != nil {
			return err
		}

		if c.readyToFinish() {
			return nil
		}

		if len(c.workers) == 0 {
			return &PeerError{
				Op:  "running coordinator",
				Err: fmt.Errorf("workers currently exhausetd"),
			}
		}

		c.dispatchWork()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-c.results:
			if !ok {
				return fmt.Errorf("results channel closed")
			}

			err := c.handleResult(session, result)
			if err != nil {
				return err
			}
		}
	}
}
