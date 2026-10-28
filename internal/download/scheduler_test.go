package download

import (
	"slices"
	"testing"

	"github.com/lorithan/torrent-client/internal/peer"
)

func TestPieceSchedulerAssignReservesAcrossPeers(t *testing.T) {
	s := newPieceScheduler(10)
	first := peer.Bitfield{0xa0, 0x80}  // Pieces 0, 2, and 8.
	second := peer.Bitfield{0xc0, 0x40} // Pieces 0, 1, and 9.
	firstBefore, secondBefore := slices.Clone(first), slices.Clone(second)
	for _, step := range []struct {
		available peer.Bitfield
		want      int
	}{
		{first, 0},
		{second, 1}, // Piece 0 is advertised by both peers but already reserved.
		{first, 2},
		{second, 9},
		{first, 8},
	} {
		index, ok := s.assign(step.available)
		if !ok || index != step.want {
			t.Fatalf("assign = (%d, %t), want (%d, true)", index, ok, step.want)
		}
		if s.states[index] != pieceAssigned {
			t.Fatalf("returned piece %d was not reserved", index)
		}
	}
	for _, available := range []peer.Bitfield{first, second} {
		if index, ok := s.assign(available); ok || index != 0 {
			t.Errorf("exhausted peer assign = (%d, %t), want (0, false)", index, ok)
		}
	}
	if !slices.Equal(first, firstBefore) || !slices.Equal(second, secondBefore) {
		t.Error("assign modified a peer's advertised availability")
	}
	// Unadvertised pieces must remain available for other peers to claim.
	if index, ok := s.assign(peer.Bitfield{0x10, 0}); !ok || index != 3 {
		t.Errorf("new peer assign = (%d, %t), want (3, true)", index, ok)
	}
}

func TestPieceSchedulerAssignNoEligiblePiece(t *testing.T) {
	for _, tt := range []struct {
		name      string
		states    []pieceState
		available peer.Bitfield
	}{
		{"empty torrent", nil, nil},
		{"peer advertises nothing", []pieceState{pieceMissing, pieceMissing}, peer.Bitfield{0}},
		{"assigned and complete pieces", []pieceState{pieceAssigned, pieceComplete}, peer.Bitfield{0xc0}},
		{"advertisement beyond piece count", []pieceState{pieceMissing}, peer.Bitfield{0x40}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &pieceScheduler{states: slices.Clone(tt.states)}
			index, ok := s.assign(tt.available)
			if ok || index != 0 {
				t.Errorf("assign = (%d, %t), want (0, false)", index, ok)
			}
			if !slices.Equal(s.states, tt.states) {
				t.Error("unsuccessful assignment changed piece states")
			}
		})
	}
}

func TestPieceSchedulerReleaseAllowsReassignment(t *testing.T) {
	s := newPieceScheduler(2)
	available := peer.Bitfield{0xc0}
	first, ok := s.assign(available)
	if !ok || first != 0 {
		t.Fatalf("first assignment = (%d, %t), want (0, true)", first, ok)
	}
	second, ok := s.assign(available)
	if !ok || second != 1 {
		t.Fatalf("second assignment = (%d, %t), want (1, true)", second, ok)
	}
	if err := s.release(first); err != nil {
		t.Fatalf("release assigned piece: %v", err)
	}
	if err := s.release(first); err == nil {
		t.Fatal("releasing the same assignment twice succeeded")
	}
	index, ok := s.assign(available)
	if !ok || index != first {
		t.Fatalf("replacement assignment = (%d, %t), want (%d, true)", index, ok, first)
	}
	if s.states[second] != pieceAssigned {
		t.Error("releasing one piece changed another assignment")
	}
	if _, ok := s.assign(available); ok {
		t.Error("a piece was assigned twice after replacement")
	}
}

func TestPieceSchedulerReleaseRejectsInvalidTransitions(t *testing.T) {
	for _, tt := range []struct {
		name   string
		states []pieceState
		index  int
	}{
		{"negative index", []pieceState{pieceAssigned}, -1},
		{"index at length", []pieceState{pieceAssigned}, 1},
		{"index beyond length", []pieceState{pieceAssigned}, 99},
		{"empty scheduler", nil, 0},
		{"missing piece", []pieceState{pieceMissing, pieceAssigned}, 0},
		{"completed piece", []pieceState{pieceComplete, pieceAssigned}, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &pieceScheduler{states: slices.Clone(tt.states)}
			if err := s.release(tt.index); err == nil {
				t.Fatal("expected release to fail")
			}
			if !slices.Equal(s.states, tt.states) {
				t.Error("rejected release changed scheduling state")
			}
		})
	}
}

func TestPieceSchedulerCompleteRetiresAssignment(t *testing.T) {
	s := newPieceScheduler(2)
	available := peer.Bitfield{0xc0}
	first, ok := s.assign(available)
	if !ok || first != 0 {
		t.Fatalf("first assignment = (%d, %t), want (0, true)", first, ok)
	}
	if err := s.complete(first); err != nil {
		t.Fatalf("complete assigned piece: %v", err)
	}
	if s.completedCount != 1 || s.done() {
		t.Fatalf("after one completion: count=%d, done=%t; want 1, false", s.completedCount, s.done())
	}
	if err := s.complete(first); err == nil {
		t.Error("duplicate completion succeeded")
	}
	if err := s.release(first); err == nil {
		t.Error("completed piece could be released")
	}
	if s.completedCount != 1 || s.done() {
		t.Fatal("rejected transitions changed completion accounting")
	}
	second, ok := s.assign(available)
	if !ok || second != 1 {
		t.Fatalf("next assignment = (%d, %t), want (1, true)", second, ok)
	}
	if err := s.complete(second); err != nil {
		t.Fatal(err)
	}
	if s.completedCount != 2 || !s.done() {
		t.Fatalf("after both completions: count=%d, done=%t; want 2, true", s.completedCount, s.done())
	}
	if _, ok := s.assign(available); ok {
		t.Error("completed pieces were assigned again")
	}
}

func TestPieceSchedulerCompleteRejectsInvalidTransitions(t *testing.T) {
	for _, tt := range []struct {
		name   string
		states []pieceState
		index  int
	}{
		{"negative index", []pieceState{pieceAssigned}, -1},
		{"index at length", []pieceState{pieceAssigned}, 1},
		{"index beyond length", []pieceState{pieceAssigned}, 99},
		{"empty scheduler", nil, 0},
		{"missing piece", []pieceState{pieceMissing, pieceAssigned}, 0},
		{"completed piece", []pieceState{pieceComplete, pieceAssigned}, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &pieceScheduler{states: slices.Clone(tt.states)}
			if err := s.complete(tt.index); err == nil {
				t.Fatal("expected completion to fail")
			}
			if !slices.Equal(s.states, tt.states) {
				t.Error("rejected completion changed scheduling state")
			}
			if s.completedCount != 0 {
				t.Error("rejected completion incremented the completion counter")
			}
		})
	}
}

func TestPieceSchedulerDoneTracksCompletionAcrossRetry(t *testing.T) {
	if !newPieceScheduler(0).done() {
		t.Fatal("empty scheduler should be done immediately")
	}
	s := newPieceScheduler(1)
	if s.done() {
		t.Fatal("missing piece reported complete")
	}
	index, ok := s.assign(peer.Bitfield{0x80})
	if !ok {
		t.Fatal("piece was not assigned")
	}
	if s.done() {
		t.Fatal("assigned piece reported complete")
	}
	if err := s.release(index); err != nil {
		t.Fatal(err)
	}
	if s.completedCount != 0 || s.done() {
		t.Fatal("releasing failed work counted it as complete")
	}
	index, ok = s.assign(peer.Bitfield{0x80})
	if !ok {
		t.Fatal("released piece was not reassigned")
	}
	if err := s.complete(index); err != nil {
		t.Fatal(err)
	}
	if !s.done() || s.completedCount != 1 {
		t.Fatal("retried piece did not count as exactly one completion")
	}
}

func TestPieceSchedulerHasMissing(t *testing.T) {
	for _, tt := range []struct {
		name   string
		states []pieceState
		want   bool
	}{
		{"empty", nil, false},
		{"missing", []pieceState{pieceMissing}, true},
		{"all assigned", []pieceState{pieceAssigned, pieceAssigned}, false},
		{"all complete", []pieceState{pieceComplete}, false},
		{"assigned and complete", []pieceState{pieceAssigned, pieceComplete}, false},
		{"missing after other states", []pieceState{pieceAssigned, pieceComplete, pieceMissing}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &pieceScheduler{states: slices.Clone(tt.states)}
			if got := s.hasMissing(); got != tt.want {
				t.Errorf("hasMissing = %t, want %t", got, tt.want)
			}
			if !slices.Equal(s.states, tt.states) {
				t.Error("query changed scheduling state")
			}
		})
	}
}
