package download

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/lorithan/torrent-client/internal/peer"
)

func TestSessionClassifiesAvailabilityAndAnnouncementDeadlineFailures(t *testing.T) {
	for _, tt := range []struct {
		name      string
		failAt    int
		completed int
	}{
		{"set availability deadline", 1, 0},
		{"clear availability deadline", 2, 0},
		{"set announcement deadline", 5, 1},
		{"clear announcement deadline", 6, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, tf, conn := availabilityDeadlineFixture(t)
			wantErr := errors.New("deadline failed")
			conn.failAt, conn.err = tt.failAt, wantErr
			s, err := NewSession(tf, &downloadTestWriter{})
			if err != nil {
				t.Fatal(err)
			}
			err = s.DownloadFrom(client)
			var classified *PeerError
			if !errors.As(err, &classified) || !errors.Is(err, wantErr) {
				t.Fatalf("error = %v, want PeerError preserving deadline cause", err)
			}
			if classified.Op == "" {
				t.Error("missing operation context")
			}
			if s.completedCount != tt.completed {
				t.Errorf("completed = %d, want %d", s.completedCount, tt.completed)
			}
		})
	}
}

func TestSessionClassifiesAvailabilityMessageFailures(t *testing.T) {
	for _, tt := range []struct {
		name  string
		wire  []byte
		cause error
	}{
		{"disconnect", nil, io.EOF},
		{"malformed message", []byte{0, 0, 0, 2, byte(peer.MsgUnchoke), 42}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, tf, conn := availabilityDeadlineFixture(t)
			conn.incoming = bytes.NewReader(tt.wire)
			s, err := NewSession(tf, &downloadTestWriter{})
			if err != nil {
				t.Fatal(err)
			}
			err = s.DownloadFrom(client)
			var classified *PeerError
			if !errors.As(err, &classified) {
				t.Fatalf("error = %v, want PeerError", err)
			}
			if tt.cause != nil && !errors.Is(err, tt.cause) {
				t.Errorf("lost underlying error: %v", err)
			}
			if s.completedCount != 0 || conn.outgoing.Len() != 0 {
				t.Error("availability failure committed progress or sent a request")
			}
		})
	}
}
