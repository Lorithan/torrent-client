package download

import (
	"fmt"

	"github.com/lorithan/torrent-client/internal/peer"
)

type pieceState uint8

const (
	pieceMissing pieceState = iota
	pieceAssigned
	pieceComplete
)

type pieceScheduler struct {
	states         []pieceState
	completedCount int
}

func newPieceScheduler(pieceCount int) *pieceScheduler {
	return &pieceScheduler{
		states: make([]pieceState, pieceCount),
	}
}

func (s *pieceScheduler) assign(available peer.Bitfield) (int, bool) {
	for index, piece := range s.states {
		if piece == pieceMissing && available.HasPiece(index) {
			s.states[index] = pieceAssigned

			return index, true
		}
	}

	return 0, false
}

func (s *pieceScheduler) release(index int) error {
	if index < 0 || index >= len(s.states) {
		return fmt.Errorf("index out of bounds")
	}

	if s.states[index] != pieceAssigned {
		return fmt.Errorf("piece %d not assigned", index)
	}

	s.states[index] = pieceMissing
	return nil
}

func (s *pieceScheduler) complete(index int) error {
	if index < 0 || index >= len(s.states) {
		return fmt.Errorf("index out of bounds")
	}

	if s.states[index] != pieceAssigned {
		return fmt.Errorf("piece %d not assigned", index)
	}

	s.states[index] = pieceComplete
	s.completedCount++
	return nil
}

func (s *pieceScheduler) done() bool {
	return s.completedCount == len(s.states)
}
