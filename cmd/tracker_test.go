package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/tracker"
)

func startTrackerUpdatesTest(t *testing.T, interval time.Duration, announce func() (*tracker.TrackerResponse, error)) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); runTrackerUpdates(ctx, interval, announce) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("tracker update worker did not stop after cancellation")
		}
	})
	return cancel, done
}

func TestRunJoinsPeriodicAnnounceBeforeLifecycleEvents(t *testing.T) {
	firstHash, secondHash := sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))
	info := "d6:lengthi8e4:name8:file.bin12:piece lengthi4e6:pieces40:" + string(firstHash[:]) + string(secondHash[:]) + "e"
	infoHash := sha1.Sum([]byte(info))
	announcedID := make(chan string, 1)
	periodicStarted, transferFinished := make(chan struct{}), make(chan struct{})
	releaseResponse := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseResponse) }) }
	defer release()
	addr, peerDone := startFallbackTestPeerWithVerifier(t, peer.Handshake{InfoHash: infoHash}, func(conn net.Conn) error {
		if err := fallbackTestRequest(conn, 0); err != nil {
			return err
		}
		if err := fallbackTestReply(conn, 0, []byte("abcd")); err != nil {
			return err
		}
		if err := fallbackTestHave(conn, 0); err != nil {
			return err
		}
		if err := fallbackTestRequest(conn, 1); err != nil {
			return err
		}
		select {
		case <-periodicStarted:
		case <-time.After(3 * time.Second):
			return errors.New("periodic announce never started during transfer")
		}
		if err := fallbackTestReply(conn, 1, []byte("efgh")); err != nil {
			return err
		}
		if err := fallbackTestHave(conn, 1); err != nil {
			return err
		}
		if err := fallbackTestExpectClosed(conn); err != nil {
			return err
		}
		close(transferFinished)
		return nil
	}, func(wire [68]byte) error {
		select {
		case id := <-announcedID:
			if !bytes.Equal(wire[28:48], infoHash[:]) || string(wire[48:]) != id {
				return errors.New("peer identity differs from initial announce")
			}
		default:
			return errors.New("peer connected before initial announce")
		}
		return nil
	})
	compact := binary.BigEndian.AppendUint16([]byte{127, 0, 0, 1}, uint16(addr.Port))
	requests := make(chan url.Values, 8)
	var periodicOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		requests <- q
		switch q.Get("event") {
		case "started":
			announcedID <- q.Get("peer_id")
			_, _ = io.WriteString(w, "d8:intervali1e5:peers6:"+string(compact)+"e")
			return
		case "":
			periodicOnce.Do(func() { close(periodicStarted) })
			select {
			case <-releaseResponse:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = io.WriteString(w, "d8:intervali1800e5:peers0:e")
	}))
	t.Cleanup(func() { release(); server.Close() })
	dir := t.TempDir()
	torrentPath, outputPath := filepath.Join(dir, "input.torrent"), filepath.Join(dir, "output.bin")
	meta := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(server.URL), server.URL, info)
	if err := os.WriteFile(torrentPath, meta, 0600); err != nil {
		t.Fatal(err)
	}
	prepareRun(t, "-torrent", torrentPath, "-output", outputPath)
	runDone := make(chan error, 1)
	go func() { runDone <- run() }()
	// Join run before prepareRun restores the process-wide flags and logger.
	var result error
	joined := false
	t.Cleanup(func() {
		release()
		if !joined {
			select {
			case <-runDone:
			case <-time.After(5 * time.Second):
				t.Error("CLI did not finish during test cleanup")
			}
		}
	})
	select {
	case <-transferFinished:
	case err := <-peerDone:
		if err != nil {
			t.Fatalf("peer failed before completion: %v", err)
		}
		// Both channels may be ready after a successful peer finishes.
		select {
		case <-transferFinished:
		default:
			t.Fatal("peer exited without finishing the transfer")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("transfer never reached the held periodic request")
	}
	if len(requests) != 2 {
		t.Fatalf("requests while regular announce is held = %d, want only started and regular", len(requests))
	}
	started, regular := <-requests, <-requests
	if started.Get("event") != "started" {
		t.Errorf("first event = %q, want started", started.Get("event"))
	}
	for key, want := range map[string]string{"downloaded": "4", "uploaded": "0", "left": "4", "peer_id": started.Get("peer_id"), "port": started.Get("port"), "info_hash": started.Get("info_hash")} {
		if got := regular.Get(key); got != want {
			t.Errorf("regular query[%q] = %q, want %q", key, got, want)
		}
	}
	if _, exists := regular["event"]; exists {
		t.Error("regular announce must omit event")
	}
	select {
	case result = <-runDone:
		joined = true
		t.Errorf("CLI returned before joining the active announce: %v", result)
	default:
	}
	release()
	if !joined {
		select {
		case result = <-runDone:
			joined = true
		case <-time.After(5 * time.Second):
			t.Fatal("CLI did not finish after releasing the regular announce")
		}
	}
	if result != nil {
		t.Fatal(result)
	}
	if len(requests) != 2 {
		t.Fatalf("remaining announces = %d, want completed and stopped", len(requests))
	}
	for _, event := range []string{"completed", "stopped"} {
		q := <-requests
		if q.Get("event") != event || q.Get("downloaded") != "8" || q.Get("left") != "0" {
			t.Errorf("incorrect final announce: %v", q)
		}
	}
	got, err := os.ReadFile(outputPath)
	if err != nil || !bytes.Equal(got, []byte("abcdefgh")) {
		t.Errorf("output = %q, error %v", got, err)
	}
}

func TestTrackerUpdatesCancelBeforeInterval(t *testing.T) {
	calls := make(chan struct{}, 1)
	cancel, done := startTrackerUpdatesTest(t, time.Hour, func() (*tracker.TrackerResponse, error) {
		calls <- struct{}{}
		return &tracker.TrackerResponse{Interval: time.Hour}, nil
	})
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop while waiting")
	}
	select {
	case <-calls:
		t.Error("announced before the initial interval elapsed")
	default:
	}
}

func TestTrackerUpdatesRetryAfterFailure(t *testing.T) {
	calls := make(chan int, 4)
	attempt := 0
	startTrackerUpdatesTest(t, time.Millisecond, func() (*tracker.TrackerResponse, error) {
		attempt++
		calls <- attempt
		if attempt == 1 {
			return nil, errors.New("tracker temporarily unavailable")
		}
		return &tracker.TrackerResponse{Interval: time.Hour}, nil
	})
	for want := 1; want <= 2; want++ {
		select {
		case got := <-calls:
			if got != want {
				t.Errorf("attempt = %d, want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("announce attempt %d never ran; failures must schedule another wait", want)
		}
	}
}

func TestTrackerUpdatesUseReturnedInterval(t *testing.T) {
	calls := make(chan struct{}, 4)
	startTrackerUpdatesTest(t, time.Millisecond, func() (*tracker.TrackerResponse, error) {
		calls <- struct{}{}
		return &tracker.TrackerResponse{Interval: time.Hour}, nil
	})
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("initial interval never triggered an announce")
	}
	select {
	case <-calls:
		t.Error("worker reused the short initial interval instead of the returned hour")
	case <-time.After(20 * time.Millisecond):
	}
}
