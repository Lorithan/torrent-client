package download

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"net"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

func TestSessionDownloadFromRetainsProgressAfterInterruptedPiece(t *testing.T) {
	first, tf, firstConn, data := downloadFixture(t, 4, 10, -1)
	allReplies := make([]byte, firstConn.incoming.Len())
	if _, err := io.ReadFull(firstConn.incoming, allReplies); err != nil {
		t.Fatal(err)
	}
	// Complete piece zero, then disconnect after only two bytes of piece one's data.
	firstConn.incoming = bytes.NewReader(allReplies[:17+13+2])
	dst := &downloadTestWriter{}
	s, err := NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DownloadFrom(first); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("first attempt error = %v, want truncated piece error", err)
	}
	if s.completedCount != 1 || !s.completed.HasPiece(0) || s.completed.HasPiece(1) {
		t.Fatalf("progress after failure = count %d, bitfield %x; want only piece zero complete", s.completedCount, s.completed)
	}
	if s.Complete() {
		t.Error("session reported completion after an interrupted piece")
	}
	if got := s.Stats().Left; got != 6 {
		t.Errorf("Left after interrupted piece = %d, want 6", got)
	}
	second, _, secondConn, _ := downloadFixture(t, 4, 10, -1)
	secondConn.incoming = bytes.NewReader(allReplies[17:])
	if err := s.DownloadFrom(second); err != nil {
		t.Fatal(err)
	}
	// The new peer must restart piece one at offset zero, then request piece two.
	want := []byte{
		0, 0, 0, 13, 6, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4,
		0, 0, 0, 5, 4, 0, 0, 0, 1,
		0, 0, 0, 13, 6, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2,
		0, 0, 0, 5, 4, 0, 0, 0, 2,
	}
	if !bytes.Equal(secondConn.outgoing.Bytes(), want) {
		t.Errorf("second peer received %x, want %x", secondConn.outgoing.Bytes(), want)
	}
	if s.completedCount != 3 {
		t.Errorf("completed count = %d, want 3", s.completedCount)
	}
	if !s.Complete() {
		t.Error("session did not report completion after the second peer finished")
	}
	if got := s.Stats().Left; got != 0 {
		t.Errorf("Left after completing the short final piece = %d, want 0", got)
	}
	if len(dst.writes) != 3 {
		t.Fatalf("writes = %d, want 3", len(dst.writes))
	}
	for i, w := range dst.writes {
		if w.offset != int64(i*4) || !bytes.Equal(w.data, data[i*4:min(i*4+4, len(data))]) {
			t.Errorf("incorrect write %d: %+v", i, w)
		}
	}
	// A completed session should need neither another peer nor more output writes.
	if err := s.DownloadFrom(nil); err != nil {
		t.Errorf("completed session: %v", err)
	}
	if len(dst.writes) != 3 {
		t.Error("completed session wrote pieces again")
	}
	if got := s.Stats().Left; got != 0 {
		t.Errorf("reusing completed session changed Left to %d", got)
	}
}

func TestSessionDownloadFromRetainsCommittedPieceOnHaveFailure(t *testing.T) {
	first, tf, conn, _ := downloadFixture(t, 4, 4, -1)
	wantErr := errors.New("have write failed")
	first.Conn = &sessionHaveFailureConn{pieceTestConn: conn, err: wantErr}
	dst := &downloadTestWriter{}
	s, err := NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	err = s.DownloadFrom(first)
	var classified *PeerError
	if !errors.Is(err, wantErr) || !errors.As(err, &classified) {
		t.Fatalf("error = %v, want peer failure preserving the have-send cause", err)
	}
	if s.completedCount != 1 || !s.completed.HasPiece(0) || len(dst.writes) != 1 {
		t.Error("announcement failure lost the successfully stored piece")
	}
	if !s.Complete() {
		t.Error("final announcement failure hid successful file completion")
	}
	if got := s.Stats().Left; got != 0 {
		t.Errorf("announcement failure left %d bytes remaining for a committed file", got)
	}
	if got := s.Stats().Downloaded; got != tf.Length {
		t.Errorf("announcement failure lost received payload count: got %d, want %d", got, tf.Length)
	}
	if err := s.DownloadFrom(nil); err != nil {
		t.Errorf("completed session should not redownload: %v", err)
	}
}

func TestSessionStorePieceWritesExactOffsetWithoutCompleting(t *testing.T) {
	for _, tt := range []struct {
		index  int
		data   string
		offset int64
	}{
		{0, "abcd", 0},
		{1, "efgh", 4},
		{2, "ij", 8},
	} {
		t.Run(fmt.Sprintf("piece_%d", tt.index), func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: 10, PieceLength: 4, Pieces: make([][20]byte, 3)}
			dst := &downloadTestWriter{}
			s, err := NewSession(tf, dst)
			if err != nil {
				t.Fatal(err)
			}
			before := s.Stats()
			if err := s.storePiece(tt.index, []byte(tt.data)); err != nil {
				t.Fatal(err)
			}
			if len(dst.writes) != 1 || dst.writes[0].offset != tt.offset || !bytes.Equal(dst.writes[0].data, []byte(tt.data)) {
				t.Fatalf("unexpected output writes: %+v", dst.writes)
			}
			if s.completedCount != 0 || s.completed.HasPiece(tt.index) || s.Stats() != before {
				t.Error("storage helper changed completion or statistics")
			}
		})
	}
}

func TestSessionStorePieceRejectsInvalidDataBeforeWriting(t *testing.T) {
	for _, tt := range []struct {
		name  string
		index int
		data  string
	}{
		{"negative index", -1, "abcd"},
		{"index at piece count", 3, "abcd"},
		{"empty data", 0, ""},
		{"short data", 0, "abc"},
		{"oversized data", 0, "abcde"},
		{"final piece must be short", 2, "abcd"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dst := &downloadTestWriter{}
			tf := &torrent.TorrentFile{Length: 10, PieceLength: 4, Pieces: make([][20]byte, 3)}
			s, err := NewSession(tf, dst)
			if err != nil {
				t.Fatal(err)
			}
			before := s.Stats()
			if err := s.storePiece(tt.index, []byte(tt.data)); err == nil {
				t.Fatal("expected input validation error")
			}
			if len(dst.writes) != 0 || s.completedCount != 0 || s.Stats() != before {
				t.Error("invalid piece reached storage or changed session progress")
			}
		})
	}
}

func TestSessionStorePiecePreservesWriteErrors(t *testing.T) {
	diskErr := errors.New("disk full")
	for _, tt := range []struct {
		name string
		n    int
		err  error
		want error
	}{
		{"partial write without error", 2, nil, io.ErrShortWrite},
		{"zero write without error", 0, nil, io.ErrShortWrite},
		{"partial write with error", 2, diskErr, diskErr},
		{"full write with error", 4, diskErr, diskErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: 4, PieceLength: 4, Pieces: make([][20]byte, 1)}
			dst := completionTestWriterFunc(func([]byte, int64) (int, error) { return tt.n, tt.err })
			s, err := NewSession(tf, dst)
			if err != nil {
				t.Fatal(err)
			}
			err = s.storePiece(0, []byte("abcd"))
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want cause %v", err, tt.want)
			}
			var peerErr *PeerError
			if errors.As(err, &peerErr) {
				t.Error("storage error classified as retryable peer failure")
			}
			if s.completedCount != 0 || s.completed.HasPiece(0) || s.Stats().Left != 4 {
				t.Error("failed storage write changed completion")
			}
		})
	}
}

func TestSessionMarkCompleteCountsActualPieceSizeOnce(t *testing.T) {
	tf := &torrent.TorrentFile{Length: 10, PieceLength: 4, Pieces: make([][20]byte, 3)}
	dst := &downloadTestWriter{}
	s, err := NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	s.stats.Downloaded = 20
	if err := s.markComplete(2); err != nil {
		t.Fatal(err)
	}
	if s.completedCount != 1 || !s.completed.HasPiece(2) || s.Stats().Left != 8 || s.Stats().Downloaded != 20 {
		t.Fatalf("final-piece accounting = %+v, count %d; want left 8, downloaded 20, count 1", s.Stats(), s.completedCount)
	}
	beforeStats, beforeBits := s.Stats(), bytes.Clone(s.completed)
	for _, index := range []int{2, -1, 3} {
		if err := s.markComplete(index); err == nil {
			t.Errorf("invalid or duplicate completion %d succeeded", index)
		}
		if s.completedCount != 1 || s.Stats() != beforeStats || !bytes.Equal(s.completed, beforeBits) {
			t.Error("rejected completion changed progress")
		}
	}
	for _, index := range []int{0, 1} {
		if err := s.markComplete(index); err != nil {
			t.Fatal(err)
		}
	}
	if !s.Complete() || s.completedCount != 3 || s.Stats().Left != 0 || s.Stats().Downloaded != 20 || len(dst.writes) != 0 {
		t.Error("completion bookkeeping wrote data or failed to count all pieces once")
	}
}

func TestSessionMarkCompleteSerializesDuplicateUpdates(t *testing.T) {
	tf := &torrent.TorrentFile{Length: 1, PieceLength: 1, Pieces: make([][20]byte, 1)}
	s, err := NewSession(tf, &downloadTestWriter{})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 32
	start := make(chan struct{})
	results := make(chan error, callers)
	var workers sync.WaitGroup
	for range callers {
		workers.Go(func() { <-start; results <- s.markComplete(0) })
	}
	close(start)
	workers.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 || s.completedCount != 1 || s.Stats().Left != 0 || !s.Complete() {
		t.Errorf("duplicate updates: successes %d, count %d, stats %+v", successes, s.completedCount, s.Stats())
	}
}

func TestSessionAvailabilitySnapshotsAreIndependent(t *testing.T) {
	tf := &torrent.TorrentFile{Length: 36, PieceLength: 4, Pieces: make([][20]byte, 9)}
	s, err := NewSession(tf, &downloadTestWriter{})
	if err != nil {
		t.Fatal(err)
	}
	original := s.Availability()
	if err := s.markComplete(8); err != nil {
		t.Fatal(err)
	}
	current := s.Availability()
	if original.HasPiece(8) || !current.HasPiece(8) || !s.HasPiece(8) || s.CompletedPieces() != 1 {
		t.Fatal("progress queries did not distinguish old and new snapshots")
	}
	current[0], current[1] = 255, 255
	if s.HasPiece(0) || !s.HasPiece(8) || !bytes.Equal(s.Availability(), []byte{0, 0x80}) || s.CompletedPieces() != 1 {
		t.Error("caller mutation of a snapshot changed session state")
	}
	for _, index := range []int{-1, 9, 10, 15, 16, int(^uint(0) >> 1)} {
		if s.HasPiece(index) {
			t.Errorf("nonexistent piece %d reported available", index)
		}
	}
}

func TestSessionProgressQueriesDuringCompletion(t *testing.T) {
	const pieceCount = 65
	tf := &torrent.TorrentFile{Length: pieceCount, PieceLength: 1, Pieces: make([][20]byte, pieceCount)}
	s, err := NewSession(tf, &downloadTestWriter{})
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	errorsSeen := make(chan error, 4)
	var readers, ready sync.WaitGroup
	for range 4 {
		ready.Add(1)
		readers.Go(func() {
			ready.Done()
			lastCount, lastLeft := 0, int64(pieceCount)
			for {
				select {
				case <-stop:
					return
				default:
				}
				snapshot := s.Availability()
				if err := snapshot.Validate(pieceCount); err != nil {
					errorsSeen <- err
					return
				}
				count := s.CompletedPieces()
				stats := s.Stats()
				if count < lastCount || count > pieceCount || stats.Left > lastLeft || stats.Left < 0 {
					errorsSeen <- fmt.Errorf("progress regressed: count %d, stats %+v", count, stats)
					return
				}
				lastCount, lastLeft = count, stats.Left
				for index := range pieceCount {
					if snapshot.HasPiece(index) && !s.HasPiece(index) {
						errorsSeen <- fmt.Errorf("completed piece %d disappeared", index)
						return
					}
				}
				if s.Complete() && s.Stats().Left != 0 {
					errorsSeen <- fmt.Errorf("complete session retained remaining bytes")
					return
				}
			}
		})
	}
	ready.Wait()
	for index := range pieceCount {
		if err := s.markComplete(index); err != nil {
			t.Error(err)
		}
	}
	close(stop)
	readers.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Error(err)
	}
	snapshot := s.Availability()
	completed := 0
	for _, value := range snapshot {
		completed += bits.OnesCount8(value)
	}
	if completed != pieceCount || s.CompletedPieces() != pieceCount || !s.Complete() || s.Stats().Left != 0 {
		t.Error("final progress queries disagree after all pieces complete")
	}
}

func TestSessionDownloadFromDoesNotCommitFailedWrite(t *testing.T) {
	client, tf, conn, _ := downloadFixture(t, 4, 4, -1)
	wantErr := errors.New("disk full")
	dst := &downloadTestWriter{failAt: 1, err: wantErr}
	s, err := NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	err = s.DownloadFrom(client)
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want storage failure", err)
	}
	var classified *PeerError
	if errors.As(err, &classified) {
		t.Errorf("storage failure was incorrectly classified as retryable: %v", err)
	}
	if s.completedCount != 0 || s.completed.HasPiece(0) {
		t.Error("failed output write committed progress")
	}
	if s.Complete() {
		t.Error("failed storage write was reported as a completed download")
	}
	if got := s.Stats().Left; got != tf.Length {
		t.Errorf("failed storage write changed Left to %d, want %d", got, tf.Length)
	}
	if got := s.Stats().Downloaded; got != tf.Length {
		t.Errorf("failed storage write lost received payload count: got %d, want %d", got, tf.Length)
	}
	if conn.outgoing.Len() != 17 {
		t.Error("failed output write announced completion")
	}
}

func TestSessionLeftPreservesProgressAfterCorruption(t *testing.T) {
	bad, tf, _, _ := downloadFixture(t, 4, 10, 1)
	dst := &downloadTestWriter{}
	s, err := NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	var peerErr *PeerError
	err = s.DownloadFrom(bad)
	if !errors.As(err, &peerErr) || peerErr.Op != "verify piece 1" {
		t.Fatalf("error = %v, want corrupt second piece", err)
	}
	if got := s.Stats().Left; got != 6 {
		t.Errorf("corrupt piece reduced Left to %d, want 6", got)
	}
	if got := s.Stats().Downloaded; got != 8 {
		t.Errorf("corrupt piece was omitted from transferred bytes: got %d, want 8", got)
	}
	good, _, conn, _ := downloadFixture(t, 4, 10, -1)
	// The replacement supplies only pieces one and two; piece zero is committed.
	if _, err := conn.incoming.Seek(17, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := s.DownloadFrom(good); err != nil {
		t.Fatal(err)
	}
	if got := s.Stats().Left; got != 0 {
		t.Errorf("Left after replacement = %d, want 0", got)
	}
	if got := s.Stats().Downloaded; got != 14 {
		t.Errorf("received bytes after retry = %d, want 14 (including the corrupt attempt)", got)
	}
	if len(dst.writes) != 3 {
		t.Errorf("writes = %d, want each of three pieces committed once", len(dst.writes))
	}
}

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

func TestSessionRetainsProgressAfterTimeout(t *testing.T) {
	for _, phase := range []string{"availability", "block response"} {
		t.Run(phase, func(t *testing.T) {
			first, tf, fixtureConn, data := downloadFixture(t, 4, 10, -1)
			replies := make([]byte, fixtureConn.incoming.Len())
			if _, err := io.ReadFull(fixtureConn.incoming, replies); err != nil {
				t.Fatal(err)
			}
			beforeTimeout := append([]byte(nil), replies[:17+13+2]...)
			if phase == "availability" {
				first.Bitfield = []byte{0x80}
				beforeTimeout = append(append([]byte(nil), replies[:17]...), 0, 0, 0, 0)
			}
			timeout := &pieceTestTimeout{}
			// Inject a real net.Error shape after partial progress rather than waiting
			// 30 seconds. Separate deadline tests verify the actual timer placement.
			failedConn := &pieceErrorTestConn{reader: io.MultiReader(bytes.NewReader(beforeTimeout), iotest.ErrReader(timeout))}
			first.Conn = failedConn
			dst := &downloadTestWriter{}
			session, err := NewSession(tf, dst)
			if err != nil {
				t.Fatal(err)
			}
			err = session.DownloadFrom(first)
			var classified *PeerError
			var networkErr net.Error
			if !errors.As(err, &classified) || !errors.As(err, &networkErr) || !networkErr.Timeout() || !errors.Is(err, timeout) {
				t.Fatalf("error = %v, want peer failure preserving timeout", err)
			}
			if session.completedCount != 1 || !session.completed.HasPiece(0) || session.completed.HasPiece(1) || session.Complete() || len(dst.writes) != 1 {
				t.Fatal("timeout lost committed progress or committed unfinished data")
			}
			second, _, secondConn, _ := downloadFixture(t, 4, 10, -1)
			secondConn.incoming = bytes.NewReader(replies[17:])
			if err := session.DownloadFrom(second); err != nil {
				t.Fatal(err)
			}
			wantFirstRequest := []byte{0, 0, 0, 13, 6, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4}
			if sent := secondConn.outgoing.Bytes(); len(sent) < 17 || !bytes.Equal(sent[:17], wantFirstRequest) {
				t.Errorf("replacement did not restart unfinished piece one at offset zero: %x", sent)
			}
			if !session.Complete() || len(dst.writes) != 3 {
				t.Fatal("replacement did not complete each piece exactly once")
			}
			for i, write := range dst.writes {
				if write.offset != int64(i*4) || !bytes.Equal(write.data, data[i*4:min(i*4+4, len(data))]) {
					t.Errorf("incorrect verified output at write %d: %+v", i, write)
				}
			}
		})
	}
}

func TestNewSession(t *testing.T) {
	for _, tt := range []struct {
		name                  string
		length                int64
		pieces, bitfieldBytes int
	}{
		{"empty", 0, 0, 0},
		{"short only piece", 1, 1, 1},
		{"full only piece", 4, 1, 1},
		{"short final piece", 10, 3, 1},
		{"seven pieces", 28, 7, 1},
		{"eight pieces", 32, 8, 1},
		{"nine pieces", 36, 9, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: tt.length, PieceLength: 4, Pieces: make([][20]byte, tt.pieces)}
			dst := &downloadTestWriter{}
			s, err := NewSession(tf, dst)
			if err != nil || s == nil {
				t.Fatalf("NewSession() = (%v, %v), want session", s, err)
			}
			if s.torrent != tf || s.dest != dst {
				t.Error("constructor did not retain the supplied metadata and destination")
			}
			if s.completedCount != 0 || !bytes.Equal(s.completed, make([]byte, tt.bitfieldBytes)) {
				t.Errorf("initial progress = %d completed, bitfield %x; want zero completed and %d zero bytes", s.completedCount, s.completed, tt.bitfieldBytes)
			}
			if got := s.Complete(); got != (tt.pieces == 0) {
				t.Errorf("new session Complete() = %v, want %v", got, tt.pieces == 0)
			}
			if got, want := s.Stats(), (Stats{Left: tt.length}); got != want {
				t.Errorf("initial stats = %+v, want %+v", got, want)
			}
			if err := s.completed.Validate(tt.pieces); err != nil {
				t.Errorf("invalid initial completion bitfield: %v", err)
			}
			if len(dst.writes) != 0 {
				t.Error("constructor wrote output before downloading anything")
			}
		})
	}
}

func TestSessionStatsReturnsSnapshot(t *testing.T) {
	tf := &torrent.TorrentFile{Length: 4, PieceLength: 4, Pieces: make([][20]byte, 1)}
	s, err := NewSession(tf, &downloadTestWriter{})
	if err != nil {
		t.Fatal(err)
	}
	original := s.Stats()
	changed := original
	changed.Uploaded = 123
	changed.Left = 0
	if got := s.Stats(); got != original {
		t.Errorf("changing a snapshot changed the session: %+v", got)
	}
	// Snapshots already returned to a caller must not become live views of the
	// session when its counters change later.
	s.stateMu.Lock()
	s.stats.Uploaded = 10
	s.stateMu.Unlock()
	if original.Uploaded != 0 {
		t.Error("session changes mutated an earlier snapshot")
	}
	if got := s.Stats(); got.Uploaded != 10 {
		t.Error("accessor did not return current statistics")
	}
}

func TestSessionStatsDuringTransfer(t *testing.T) {
	client, tf, conn, _ := downloadFixture(t, 4096, 128*4096+17, -1)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	client.Conn = &statsGateConn{pieceTestConn: conn, entered: entered, release: release}
	s, err := NewSession(tf, &downloadTestWriter{})
	if err != nil {
		t.Fatal(err)
	}
	transferDone := make(chan error, 1)
	go func() { transferDone <- s.DownloadFrom(client) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("transfer did not reach the network read")
	}
	// A stalled network operation must not prevent a tracker worker from reading
	// statistics. This also catches holding the mutex across peer I/O.
	snapshot := make(chan Stats, 1)
	go func() { snapshot <- s.Stats() }()
	select {
	case got := <-snapshot:
		if got != (Stats{Left: tf.Length}) {
			t.Errorf("stats before first payload = %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Stats blocked behind network I/O")
	}
	stop, started := make(chan struct{}), make(chan struct{})
	var stopOnce sync.Once
	stopObserver := func() { stopOnce.Do(func() { close(stop) }) }
	defer stopObserver()
	observed := make(chan error, 1)
	go func() {
		previous := s.Stats()
		close(started)
		for {
			select {
			case <-stop:
				observed <- nil
				return
			default:
				current := s.Stats()
				if current.Downloaded < previous.Downloaded || current.Left > previous.Left || current.Left < 0 || current.Left > tf.Length || current.Uploaded != 0 {
					observed <- fmt.Errorf("inconsistent snapshots: previous %+v, current %+v", previous, current)
					return
				}
				previous = current
			}
		}
	}()
	<-started
	unblock()
	select {
	case err := <-transferDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("transfer blocked during concurrent statistics reads")
	}
	stopObserver()
	if err := <-observed; err != nil {
		t.Error(err)
	}
	if got, want := s.Stats(), (Stats{Downloaded: tf.Length}); got != want {
		t.Errorf("final stats = %+v, want %+v", got, want)
	}
}

func TestNewSessionRejectsInvalidMetadata(t *testing.T) {
	for _, tt := range []struct {
		name                string
		length, pieceLength int64
		hashes              int
	}{
		{"zero piece length", 4, 0, 1},
		{"negative piece length", 4, -1, 1},
		{"negative file length", -1, 4, 0},
		{"missing final hash", 10, 4, 2},
		{"extra hash", 10, 4, 4},
		{"extra hash at boundary", 8, 4, 3},
		{"hash for empty file", 0, 4, 1},
		{"large count", math.MaxInt64, 2, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: tt.length, PieceLength: tt.pieceLength, Pieces: make([][20]byte, tt.hashes)}
			dst := &downloadTestWriter{}
			if s, err := NewSession(tf, dst); s != nil || err == nil {
				t.Errorf("NewSession() = (%v, %v), want nil session and error", s, err)
			}
			if len(dst.writes) != 0 {
				t.Error("invalid metadata caused an output write")
			}
		})
	}
}

func TestNewSessionProgressIsIndependent(t *testing.T) {
	tf := &torrent.TorrentFile{Length: 36, PieceLength: 4, Pieces: make([][20]byte, 9)}
	sessions := make([]*Session, 2)
	for i := range sessions {
		s, err := NewSession(tf, &downloadTestWriter{})
		if err != nil {
			t.Fatal(err)
		}
		sessions[i] = s
	}
	if err := sessions[0].completed.SetPiece(8); err != nil {
		t.Fatal(err)
	}
	sessions[0].completedCount++
	if sessions[1].completed.HasPiece(8) || sessions[1].completedCount != 0 {
		t.Error("updating one session changed another session's progress")
	}
	for i, s := range sessions {
		if err := s.completed.Validate(9); err != nil {
			t.Error(fmt.Errorf("session %d: %w", i, err))
		}
	}
}
