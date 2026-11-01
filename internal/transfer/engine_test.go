package transfer

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/download"
	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

func TestCalculateRates(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		previous, current        download.Stats
		elapsed                  time.Duration
		downloadRate, uploadRate float64
	}{
		{"one second", download.Stats{}, download.Stats{Downloaded: 2048, Uploaded: 512}, time.Second, 2048, 512},
		{"fractional interval", download.Stats{Downloaded: 100, Uploaded: 50}, download.Stats{Downloaded: 103, Uploaded: 51}, 500 * time.Millisecond, 6, 2},
		{"multiple seconds", download.Stats{Downloaded: 10, Uploaded: 20}, download.Stats{Downloaded: 20, Uploaded: 24}, 2 * time.Second, 5, 2},
		{"idle counters", download.Stats{Downloaded: 10, Uploaded: 20}, download.Stats{Downloaded: 10, Uploaded: 20}, time.Second, 0, 0},
		{"retries without verified progress", download.Stats{Downloaded: 100, Uploaded: 50, Left: 100}, download.Stats{Downloaded: 612, Uploaded: 178, Left: 100}, 250 * time.Millisecond, 2048, 512},
		{"zero elapsed", download.Stats{}, download.Stats{Downloaded: 1, Uploaded: 1}, 0, 0, 0},
		{"negative elapsed", download.Stats{}, download.Stats{Downloaded: 1, Uploaded: 1}, -time.Second, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			downloadRate, uploadRate := calculateRates(tc.previous, tc.current, tc.elapsed)
			if downloadRate != tc.downloadRate || uploadRate != tc.uploadRate {
				t.Errorf("rates=(%g, %g), want (%g, %g) bytes/second", downloadRate, uploadRate, tc.downloadRate, tc.uploadRate)
			}
		})
	}
}

func TestEngineProgressUpdatesPublishAndStop(t *testing.T) {
	session, err := download.NewSession(&torrent.TorrentFile{Name: "sample.bin", Length: 4, PieceLength: 4, Pieces: make([][20]byte, 1)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{status: Status{Phase: PhaseDownloading, ActivePeers: 2}}
	stop := e.startProgressUpdates(context.Background(), session, time.Millisecond)
	t.Cleanup(stop)
	want := session.Progress()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for e.Status().Progress != want {
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatal("periodic updater did not publish session progress")
		}
	}
	if got := e.Status(); got.Phase != PhaseDownloading || got.ActivePeers != 2 {
		t.Errorf("progress update changed other status fields: %+v", got)
	}
	stop()
	// Repeated stops should return immediately, like tracker shutdown helpers.
	stoppedAgain := make(chan struct{})
	go func() { stop(); close(stoppedAgain) }()
	select {
	case <-stoppedAgain:
	case <-time.After(time.Second):
		t.Fatal("second stop blocked")
	}
	sentinel := Status{Phase: PhaseStopped, Progress: download.Progress{Name: "final snapshot"}}
	e.mu.Lock()
	e.status = sentinel
	e.mu.Unlock()
	checkUntil := time.NewTimer(10 * time.Millisecond)
	defer checkUntil.Stop()
	for {
		if got := e.Status(); got != sentinel {
			t.Fatalf("stopped updater overwrote final status: %+v", got)
		}
		select {
		case <-ticker.C:
		case <-checkUntil.C:
			return
		}
	}
}

func TestEngineProgressUpdatesAlreadyCanceled(t *testing.T) {
	session, err := download.NewSession(&torrent.TorrentFile{PieceLength: 4}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := &Engine{}
	stop := e.startProgressUpdates(ctx, session, time.Hour)
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("updater did not stop before first tick")
	}
}

func TestEnginePublishesInitialAndFinalProgress(t *testing.T) {
	for _, corruptSecond := range []bool{false, true} {
		t.Run(fmt.Sprintf("corruptSecond=%t", corruptSecond), func(t *testing.T) {
			firstHash, secondHash := sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))
			info := "d6:lengthi8e4:name8:file.bin12:piece lengthi4e6:pieces40:" + string(firstHash[:]) + string(secondHash[:]) + "e"
			infoHash := sha1.Sum([]byte(info))
			addr, peerDone := startFallbackTestPeerWithVerifier(t, peer.Handshake{InfoHash: infoHash}, func(conn net.Conn) error {
				for index, contents := range []string{"abcd", "efgh"} {
					if err := fallbackTestRequest(conn, uint32(index)); err != nil {
						return err
					}
					if corruptSecond && index == 1 {
						contents = "bad!"
					}
					if err := fallbackTestReply(conn, uint32(index), []byte(contents)); err != nil {
						return err
					}
					if !corruptSecond || index == 0 {
						if err := fallbackTestHave(conn, uint32(index)); err != nil {
							return err
						}
					}
				}
				return fallbackTestExpectClosed(conn)
			}, func(wire [68]byte) error {
				if !bytes.Equal(wire[28:48], infoHash[:]) {
					return errors.New("incorrect info hash")
				}
				return nil
			})
			initialSeen, releaseInitial := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseInitial) }) }
			t.Cleanup(release)
			compact := binary.BigEndian.AppendUint16([]byte{127, 0, 0, 1}, uint16(addr.Port))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("event") == "started" {
					close(initialSeen)
					<-releaseInitial
				}
				_, _ = fmt.Fprintf(w, "d8:intervali1800e5:peers6:%se", compact)
			}))
			t.Cleanup(func() { release(); server.Close() })
			dir := t.TempDir()
			input, output := filepath.Join(dir, "input.torrent"), filepath.Join(dir, "output.bin")
			meta := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(server.URL), server.URL, info)
			if err := os.WriteFile(input, meta, 0600); err != nil {
				t.Fatal(err)
			}
			e, err := New(Config{TorrentPath: input, OutputPath: output, Workers: 1, PeerWaitTimeout: 5 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var runErr error
			t.Cleanup(func() {
				release()
				cancel()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("engine did not terminate")
				}
			})
			go func() { defer close(done); runErr = e.Run(ctx) }()
			select {
			case <-initialSeen:
			case <-time.After(2 * time.Second):
				t.Fatal("initial announce did not start")
			}
			want := download.Progress{Name: "file.bin", TotalBytes: 8, TotalPieces: 2, Stats: download.Stats{Left: 8}}
			if got := e.Status().Progress; got != want {
				t.Errorf("initial progress=%+v, want %+v", got, want)
			}
			if got := e.Status().Phase; got != PhaseDiscovering {
				t.Errorf("phase during initial announce=%s, want discovering", got)
			}
			release()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("engine did not finish")
			}
			want.Stats.Downloaded = 8
			want.VerifiedBytes, want.CompletedPieces, want.Stats.Left = 8, 2, 0
			wantPhase := PhaseStopped
			wantData := "abcdefgh"
			if corruptSecond {
				want.VerifiedBytes, want.CompletedPieces, want.Stats.Left = 4, 1, 4
				wantPhase, wantData = PhaseFailed, "abcd"
				if runErr == nil {
					t.Error("corrupt transfer succeeded")
				}
			} else if runErr != nil {
				t.Fatal(runErr)
			}
			status := e.Status()
			if status.Progress != want || status.Phase != wantPhase || status.Err != runErr {
				t.Errorf("final status=%+v, want progress=%+v phase=%s err=%v", status, want, wantPhase, runErr)
			}
			if data, err := os.ReadFile(output); err != nil || string(data) != wantData {
				t.Errorf("output=%q, err=%v", data, err)
			}
			select {
			case err := <-peerDone:
				if err != nil {
					t.Errorf("peer: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("peer did not finish")
			}
		})
	}
}

func TestEngineFinish(t *testing.T) {
	failure := errors.New("output write failed")
	for _, tc := range []struct {
		name  string
		err   error
		phase Phase
	}{
		{"success", nil, PhaseStopped},
		{"canceled", context.Canceled, PhaseStopped},
		{"wrapped cancellation", fmt.Errorf("transfer: %w", context.Canceled), PhaseStopped},
		{"failure", failure, PhaseFailed},
		{"wrapped failure", fmt.Errorf("transfer: %w", failure), PhaseFailed},
		{"deadline exceeded", context.DeadlineExceeded, PhaseFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			progress := download.Progress{Name: "file.bin", TotalBytes: 10, VerifiedBytes: 4,
				CompletedPieces: 1, TotalPieces: 3, Stats: download.Stats{Downloaded: 8, Uploaded: 2, Left: 6}}
			e := &Engine{status: Status{Phase: PhaseDownloading, Progress: progress, ActivePeers: 3,
				DownloadRate: 100, UploadRate: 50, Err: errors.New("stale failure")}}
			e.finish(tc.err)
			got := e.Status()
			want := Status{Phase: tc.phase, Progress: progress}
			if tc.phase == PhaseFailed {
				want.Err = tc.err
			}
			if got != want {
				t.Errorf("finished status=%+v, want %+v", got, want)
			}
		})
	}
}

func TestEngineRunAlreadyCanceled(t *testing.T) {
	dir := t.TempDir()
	e, err := New(Config{TorrentPath: filepath.Join(dir, "missing.torrent"), OutputPath: filepath.Join(dir, "output.bin"), Workers: 1, PeerWaitTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run()=%v, want context.Canceled", err)
	}
	before := e.Status()
	if before != (Status{Phase: PhaseStopped}) {
		t.Errorf("canceled status=%+v", before)
	}
	if _, err := os.Stat(e.config.OutputPath); !os.IsNotExist(err) {
		t.Errorf("canceled engine created output: %v", err)
	}
	if err := e.Run(context.Background()); err == nil {
		t.Fatal("second Run succeeded")
	}
	if got := e.Status(); got != before {
		t.Error("rejected second Run changed terminal status")
	}
}

func TestEngineRunFailurePreservesErrorAndRejectsRestart(t *testing.T) {
	dir := t.TempDir()
	e, err := New(Config{TorrentPath: filepath.Join(dir, "missing.torrent"), OutputPath: filepath.Join(dir, "output.bin"), Workers: 1, PeerWaitTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	runErr := e.Run(context.Background())
	if !os.IsNotExist(runErr) {
		t.Fatalf("Run()=%v, want missing input error", runErr)
	}
	before := e.Status()
	if before.Phase != PhaseFailed || before.Err != runErr {
		t.Errorf("status=%+v, want failed with original error", before)
	}
	if err := e.Run(context.Background()); err == nil {
		t.Fatal("second Run succeeded")
	}
	if got := e.Status(); got != before {
		t.Error("rejected second Run overwrote first failure")
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config Config
	}{
		{"missing torrent", Config{OutputPath: "output", Workers: 1, PeerWaitTimeout: time.Second}},
		{"missing output", Config{TorrentPath: "input", Workers: 1, PeerWaitTimeout: time.Second}},
		{"zero workers", Config{TorrentPath: "input", OutputPath: "output", PeerWaitTimeout: time.Second}},
		{"negative workers", Config{TorrentPath: "input", OutputPath: "output", Workers: -1, PeerWaitTimeout: time.Second}},
		{"zero peer wait timeout", Config{TorrentPath: "input", OutputPath: "output", Workers: 1}},
		{"negative peer wait timeout", Config{TorrentPath: "input", OutputPath: "output", Workers: 1, PeerWaitTimeout: -time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, err := New(tc.config)
			if engine != nil || err == nil {
				t.Fatalf("New()=(%v, %v), want nil engine and error", engine, err)
			}
		})
	}
}

func TestEngineStatusSnapshotIsIndependent(t *testing.T) {
	want := Status{Phase: PhaseDownloading, ActivePeers: 2, DownloadRate: 100, UploadRate: 50,
		Progress: download.Progress{Name: "file.bin", TotalBytes: 8, VerifiedBytes: 4, Stats: download.Stats{Downloaded: 6, Left: 4}}}
	e := &Engine{status: want}
	snapshot := e.Status()
	if snapshot != want {
		t.Fatalf("Status()=%+v, want %+v", snapshot, want)
	}
	snapshot.Phase, snapshot.ActivePeers, snapshot.Err = PhaseFailed, 0, errors.New("caller error")
	snapshot.DownloadRate, snapshot.UploadRate = 0, 0
	snapshot.Progress.Name, snapshot.Progress.Stats.Left = "changed", 0
	if got := e.Status(); got != want {
		t.Errorf("caller changed engine status: %+v", got)
	}
}

func TestEngineStatusConcurrentSnapshots(t *testing.T) {
	e := &Engine{status: Status{Phase: PhaseDownloading}}
	stop := make(chan struct{})
	failures := make(chan Status, 4)
	var readers, ready sync.WaitGroup
	for range 4 {
		ready.Add(1)
		readers.Go(func() {
			_ = e.Status()
			ready.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s := e.Status()
				if s.Phase != PhaseDownloading || s.Progress.TotalBytes != s.Progress.VerifiedBytes || s.DownloadRate != float64(s.Progress.TotalBytes) {
					failures <- s
					return
				}
			}
		})
	}
	ready.Wait()
	for i := range 1000 {
		e.mu.Lock()
		e.status.Progress.TotalBytes = int64(i)
		e.status.Progress.VerifiedBytes = int64(i)
		e.status.DownloadRate = float64(i)
		e.mu.Unlock()
	}
	close(stop)
	readers.Wait()
	close(failures)
	for s := range failures {
		t.Errorf("inconsistent snapshot: %+v", s)
	}
}

func TestNewDefersIOAndPreservesConfiguration(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "existing.bin")
	if err := os.WriteFile(output, []byte("keep existing output"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, port := range []uint16{0, 65535} {
		for _, seed := range []bool{false, true} {
			config := Config{
				TorrentPath: filepath.Join(dir, "missing.torrent"),
				OutputPath:  output, Port: port, Workers: 1, Seed: seed,
				PeerWaitTimeout: time.Second,
			}
			engine, err := New(config)
			if err != nil || engine == nil {
				t.Fatalf("New(%+v)=(%v, %v), want configured engine without opening files", config, engine, err)
			}
			if engine.config != config {
				t.Errorf("stored config=%+v, want %+v", engine.config, config)
			}
			if engine.status != (Status{Phase: PhaseStarting}) {
				t.Errorf("initial status=%+v", engine.status)
			}
			config.Workers = 100
			if engine.config.Workers != 1 {
				t.Error("caller mutation changed engine configuration")
			}
		}
	}
	if contents, err := os.ReadFile(output); err != nil || string(contents) != "keep existing output" {
		t.Errorf("constructor changed existing output: %q, %v", contents, err)
	}
}
