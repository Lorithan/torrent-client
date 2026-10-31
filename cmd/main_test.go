package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
)

func TestRunContinuesWithSecondPeer(t *testing.T) {
	firstHash, secondHash := sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))
	info := "d6:lengthi8e4:name8:file.bin12:piece lengthi4e6:pieces40:" + string(firstHash[:]) + string(secondHash[:]) + "e"
	infoHash := sha1.Sum([]byte(info))
	local := peer.Handshake{InfoHash: infoHash}
	// The CLI creates a random session ID. Hand the announced value to each fake
	// peer through a channel to check identity reuse without shared-memory races.
	announcedIDs := make(chan string, 2)
	lifecycle := make(chan url.Values, 4)
	verify := func(wire [68]byte) error {
		var id string
		select {
		case id = <-announcedIDs:
		default:
			return fmt.Errorf("peer connected without an announce")
		}
		if wire[0] != 19 || string(wire[1:20]) != "BitTorrent protocol" || !bytes.Equal(wire[28:48], infoHash[:]) || string(wire[48:]) != id {
			return fmt.Errorf("handshake does not match torrent and announced session identity")
		}
		return nil
	}
	first, firstDone := startFallbackTestPeerWithVerifier(t, local, func(conn net.Conn) error {
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
		return fallbackTestReply(conn, 1, []byte("ef"))
	}, verify)
	second, secondDone := startFallbackTestPeerWithVerifier(t, local, func(conn net.Conn) error {
		if err := fallbackTestRequest(conn, 1); err != nil {
			return err
		}
		if err := fallbackTestReply(conn, 1, []byte("efgh")); err != nil {
			return err
		}
		if err := fallbackTestHave(conn, 1); err != nil {
			return err
		}
		var next [1]byte
		if _, err := conn.Read(next[:]); err != io.EOF {
			return fmt.Errorf("expected CLI cleanup, got %v", err)
		}
		return nil
	}, verify)
	var compact []byte
	for _, addr := range []net.TCPAddr{first, second} {
		compact = append(compact, 127, 0, 0, 1)
		compact = binary.BigEndian.AppendUint16(compact, uint16(addr.Port))
	}
	tracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		lifecycle <- query
		if query.Get("event") == "started" {
			id := query.Get("peer_id")
			announcedIDs <- id
			announcedIDs <- id
		}
		_, _ = io.WriteString(w, "d8:intervali1800e5:peers12:"+string(compact)+"e")
	}))
	t.Cleanup(tracker.Close)
	dir := t.TempDir()
	torrentPath, outputPath := filepath.Join(dir, "input.torrent"), filepath.Join(dir, "output.bin")
	meta := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(tracker.URL), tracker.URL, info)
	if err := os.WriteFile(torrentPath, meta, 0600); err != nil {
		t.Fatal(err)
	}
	prepareRun(t, "-torrent", torrentPath, "-output", outputPath, "-workers", "1")
	if err := run(context.Background(), 25*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := <-firstDone; err != nil {
		t.Errorf("first peer: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Errorf("second peer: %v", err)
	}
	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("abcdefgh")) {
		t.Errorf("saved %q, want abcdefgh", got)
	}
	if len(lifecycle) != 3 {
		t.Fatalf("announce count = %d, want started, completed, and stopped", len(lifecycle))
	}
	started, completed := <-lifecycle, <-lifecycle
	for key, want := range map[string]string{"event": "completed", "uploaded": "0", "downloaded": "8", "left": "0", "peer_id": started.Get("peer_id"), "port": started.Get("port"), "info_hash": started.Get("info_hash")} {
		if got := completed.Get(key); got != want {
			t.Errorf("completion query[%q] = %q, want %q", key, got, want)
		}
	}
	stopped := <-lifecycle
	for key, want := range map[string]string{"event": "stopped", "uploaded": "0", "downloaded": "8", "left": "0", "peer_id": started.Get("peer_id"), "port": started.Get("port"), "info_hash": started.Get("info_hash")} {
		if got := stopped.Get(key); got != want {
			t.Errorf("stopped query[%q] = %q, want %q", key, got, want)
		}
	}
}

// run uses the standard flag set and logger; isolate their process-wide state
// between tests. These tests must remain sequential.
func TestRunWorkerFlagEnablesOverlappingPeerRequests(t *testing.T) {
	firstHash, secondHash := sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))
	info := "d6:lengthi8e4:name8:file.bin12:piece lengthi4e6:pieces40:" + string(firstHash[:]) + string(secondHash[:]) + "e"
	local := peer.Handshake{InfoHash: sha1.Sum([]byte(info))}
	announcedIDs := make(chan string, 2)
	verify := func(wire [68]byte) error {
		select {
		case id := <-announcedIDs:
			if !bytes.Equal(wire[28:48], local.InfoHash[:]) || string(wire[48:]) != id {
				return errors.New("handshake identity differs from tracker announce")
			}
		default:
			return errors.New("peer connected before initial announce")
		}
		return nil
	}
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	requests := make(chan uint32, 2)
	serve := func(conn net.Conn) error {
		request, err := peer.ReadMessage(conn)
		if err != nil {
			return err
		}
		if request == nil || request.ID != peer.MsgRequest || len(request.Payload) != 12 || binary.BigEndian.Uint32(request.Payload[4:8]) != 0 || binary.BigEndian.Uint32(request.Payload[8:]) != 4 {
			return fmt.Errorf("unexpected request: %+v", request)
		}
		index := binary.BigEndian.Uint32(request.Payload[:4])
		if index > 1 {
			return fmt.Errorf("invalid piece index %d", index)
		}
		requests <- index
		select {
		case <-release:
		case <-time.After(2 * time.Second):
			return errors.New("CLI did not request work from both peers concurrently")
		}
		data := []byte("abcd")
		if index == 1 {
			data = []byte("efgh")
		}
		if err := fallbackTestReply(conn, index, data); err != nil {
			return err
		}
		if err := fallbackTestHave(conn, index); err != nil {
			return err
		}
		return fallbackTestExpectClosed(conn)
	}
	first, firstDone := startFallbackTestPeerWithVerifier(t, local, serve, verify)
	second, secondDone := startFallbackTestPeerWithVerifier(t, local, serve, verify)
	var compact []byte
	for _, addr := range []net.TCPAddr{first, second} {
		compact = append(compact, 127, 0, 0, 1)
		compact = binary.BigEndian.AppendUint16(compact, uint16(addr.Port))
	}
	announces := make(chan url.Values, 8)
	tracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		announces <- query
		if query.Get("event") == "started" {
			announcedIDs <- query.Get("peer_id")
			announcedIDs <- query.Get("peer_id")
		}
		_, _ = fmt.Fprintf(w, "d8:intervali1800e5:peers12:%se", string(compact))
	}))
	t.Cleanup(tracker.Close)
	dir := t.TempDir()
	torrentPath, outputPath := filepath.Join(dir, "input.torrent"), filepath.Join(dir, "output.bin")
	meta := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(tracker.URL), tracker.URL, info)
	if err := os.WriteFile(torrentPath, meta, 0600); err != nil {
		t.Fatal(err)
	}
	prepareRun(t, "-torrent", torrentPath, "-output", outputPath, "-workers", "2")
	done := make(chan struct{})
	var runErr error
	go func() { defer close(done); runErr = run(context.Background(), 25*time.Millisecond) }()
	// Join the CLI before prepareRun restores global flags and logging.
	t.Cleanup(func() {
		unblock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("CLI did not finish during cleanup")
		}
	})
	seen := make(map[uint32]bool)
	for len(seen) < 2 {
		select {
		case index := <-requests:
			if seen[index] {
				t.Fatal("CLI scheduled a piece twice")
			}
			seen[index] = true
		case <-done:
			t.Fatalf("CLI returned before both peers received work: %v", runErr)
		case <-time.After(time.Second):
			t.Fatal("CLI did not overlap requests to two prepared peers")
		}
	}
	unblock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("CLI did not finish after releasing both peers")
	}
	if runErr != nil {
		t.Fatal(runErr)
	}
	for _, peerDone := range []<-chan error{firstDone, secondDone} {
		if err := <-peerDone; err != nil {
			t.Error(err)
		}
	}
	output, err := os.ReadFile(outputPath)
	if err != nil || string(output) != "abcdefgh" {
		t.Errorf("output = %q, error %v", output, err)
	}
	if len(announces) != 3 {
		t.Fatalf("announces = %d, want started/completed/stopped", len(announces))
	}
	var started url.Values
	for _, event := range []string{"started", "completed", "stopped"} {
		query := <-announces
		if query.Get("event") != event {
			t.Errorf("event = %q, want %q", query.Get("event"), event)
		}
		if event == "started" {
			started = query
		}
		for _, key := range []string{"peer_id", "info_hash", "port"} {
			if query.Get(key) != started.Get(key) {
				t.Errorf("announce identity changed for %s", key)
			}
		}
		if event != "started" && (query.Get("left") != "0" || query.Get("downloaded") != "8") {
			t.Errorf("incorrect final statistics: %v", query)
		}
	}
}

func prepareRun(t *testing.T, args ...string) *bytes.Buffer {
	t.Helper()
	previousArgs, previousFlags := os.Args, flag.CommandLine
	previousWriter, previousLogFlags, previousPrefix := log.Writer(), log.Flags(), log.Prefix()
	t.Cleanup(func() {
		os.Args, flag.CommandLine = previousArgs, previousFlags
		log.SetOutput(previousWriter)
		log.SetFlags(previousLogFlags)
		log.SetPrefix(previousPrefix)
	})
	// Existing CLI tests exercise download-and-exit; seeding tests override this.
	os.Args = append([]string{"torrent-client", "-seed=false"}, args...)
	flag.CommandLine = flag.NewFlagSet("torrent-client", flag.ContinueOnError)
	var output bytes.Buffer
	flag.CommandLine.SetOutput(&output)
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")
	return &output
}

func TestRunDownload(t *testing.T) {
	for _, scenario := range []string{"success", "completion tracker failure", "stopped tracker failure", "corrupted data", "corrupt and stopped failures", "peer disconnect"} {
		t.Run(scenario, func(t *testing.T) { runDownloadScenario(t, scenario) })
	}
}

func runDownloadScenario(t *testing.T, scenario string) {
	t.Helper()
	successful := scenario == "success" || scenario == "completion tracker failure" || scenario == "stopped tracker failure"
	corrupted := scenario == "corrupted data" || scenario == "corrupt and stopped failures"
	stopFailure := scenario == "stopped tracker failure" || scenario == "corrupt and stopped failures"
	contents := []byte("data")
	pieceHash := sha1.Sum(contents)
	info := "d6:lengthi4e4:name8:file.bin12:piece lengthi4e6:pieces20:" + string(pieceHash[:]) + "e"
	hash := sha1.Sum([]byte(info))
	remoteID := [20]byte{'r', 'e', 'm', 'o', 't', 'e', 0xff}
	peerListener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peerListener.Close() })
	if err := peerListener.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	type peerResult struct {
		handshake [68]byte
		err       error
	}
	peerDone := make(chan peerResult, 1)
	go func() {
		var result peerResult
		conn, err := peerListener.AcceptTCP()
		if err != nil {
			if scenario != "existing output" || !errors.Is(err, net.ErrClosed) {
				result.err = err
			}
			peerDone <- result
			return
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			result.err = err
			peerDone <- result
			return
		}
		if _, err := io.ReadFull(conn, result.handshake[:]); err != nil {
			result.err = err
			peerDone <- result
			return
		}
		reply := append([]byte{19}, []byte("BitTorrent protocol")...)
		reply = append(reply, make([]byte, 8)...)
		reply = append(reply, hash[:]...)
		reply = append(reply, remoteID[:]...)
		if _, err := conn.Write(reply); err != nil {
			result.err = err
			peerDone <- result
			return
		}
		var interested [5]byte
		if _, err := io.ReadFull(conn, interested[:]); err != nil {
			result.err = fmt.Errorf("reading interested message: %w", err)
			peerDone <- result
			return
		}
		if !bytes.Equal(interested[:], []byte{0, 0, 0, 1, 2}) {
			result.err = fmt.Errorf("expected interested message, got %x", interested)
			peerDone <- result
			return
		}
		// Advertise piece zero, send a keep-alive, then grant permission to request.
		if _, err := conn.Write([]byte{0, 0, 0, 2, 5, 0x80, 0, 0, 0, 0, 0, 0, 0, 1, 1}); err != nil {
			result.err = err
			peerDone <- result
			return
		}
		if scenario != "existing output" {
			var request [17]byte
			if _, err := io.ReadFull(conn, request[:]); err != nil {
				result.err = fmt.Errorf("reading block request: %w", err)
				peerDone <- result
				return
			}
			if !bytes.Equal(request[:], []byte{0, 0, 0, 13, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}) {
				result.err = fmt.Errorf("incorrect block request: %x", request)
				peerDone <- result
				return
			}
			if scenario == "peer disconnect" {
				peerDone <- result
				return
			}
			blockData := append([]byte(nil), contents...)
			if corrupted {
				blockData[0] ^= 0xff
			}
			block := append([]byte{0, 0, 0, 13, 7, 0, 0, 0, 0, 0, 0, 0, 0}, blockData...)
			if _, err := conn.Write(block); err != nil {
				result.err = err
				peerDone <- result
				return
			}
		}
		if successful {
			var have [9]byte
			if _, err := io.ReadFull(conn, have[:]); err != nil {
				result.err = fmt.Errorf("reading completion announcement: %w", err)
				peerDone <- result
				return
			}
			if !bytes.Equal(have[:], []byte{0, 0, 0, 5, 4, 0, 0, 0, 0}) {
				result.err = fmt.Errorf("incorrect have message: %x", have)
				peerDone <- result
				return
			}
		}
		var next [1]byte
		if _, err := conn.Read(next[:]); err != io.EOF {
			result.err = fmt.Errorf("expected CLI to close peer connection, got %v", err)
		}
		peerDone <- result
	}()
	compactPeer := []byte{127, 0, 0, 1}
	compactPeer = binary.BigEndian.AppendUint16(compactPeer, uint16(peerListener.Addr().(*net.TCPAddr).Port))
	requests := make(chan url.Values, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Query()
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if scenario == "completion tracker failure" && r.URL.Query().Get("event") == "completed" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "completion tracker unavailable")
			return
		}
		if stopFailure && r.URL.Query().Get("event") == "stopped" {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "stopped tracker unavailable")
			return
		}
		_, _ = io.WriteString(w, "d8:intervali1800e5:peers6:"+string(compactPeer)+"e")
	}))
	t.Cleanup(server.Close)
	dir := t.TempDir()
	torrentPath := filepath.Join(dir, "input.torrent")
	outputPath := filepath.Join(dir, "output.bin")
	existing := []byte("preserve this existing file")
	if scenario == "existing output" {
		if err := os.WriteFile(outputPath, existing, 0600); err != nil {
			t.Fatal(err)
		}
	}
	data := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(server.URL), server.URL, info)
	if err := os.WriteFile(torrentPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	output := prepareRun(t, "-torrent", torrentPath, "-output", outputPath)
	runErr := run(context.Background(), 25*time.Millisecond)
	if scenario == "existing output" {
		// Output refusal now happens before connecting. Release the unused fake peer.
		peerListener.Close()
	}
	result := <-peerDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	if successful {
		if runErr != nil {
			t.Fatal(runErr)
		}
	} else {
		if runErr == nil {
			t.Fatal("expected error")
		}
		if scenario == "existing output" && !os.IsExist(runErr) {
			t.Errorf("expected file-exists error, got %v", runErr)
		}
		if corrupted && (!strings.Contains(runErr.Error(), "peer discovery attempts") || !strings.Contains(output.String(), "hash")) {
			t.Errorf("expected peer exhaustion with logged hash failure, got %v; logs %q", runErr, output.String())
		}
	}
	if scenario != "existing output" && (result.handshake[0] != 19 || string(result.handshake[1:20]) != "BitTorrent protocol" || !bytes.Equal(result.handshake[20:28], make([]byte, 8)) || !bytes.Equal(result.handshake[28:48], hash[:])) {
		t.Errorf("incorrect outgoing handshake: %x", result.handshake)
	}
	if scenario != "existing output" && !strings.Contains(output.String(), fmt.Sprintf("%x", remoteID)) {
		t.Error("remote peer ID was not displayed")
	}
	for _, want := range []string{"file.bin", "file length: 4", "piece count: 1", "127.0.0.1"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("output %q does not contain %q", output.String(), want)
		}
	}
	select {
	case query := <-requests:
		for key, want := range map[string]string{"info_hash": string(hash[:]), "left": "4", "compact": "1", "event": "started"} {
			if got := query.Get(key); got != want {
				t.Errorf("query[%q] = %q, want %q", key, got, want)
			}
		}
		if id := query.Get("peer_id"); len(id) != 20 || !strings.HasPrefix(id, "-CT0001-") {
			t.Errorf("invalid peer ID: %q", id)
		}
		if scenario != "existing output" && string(result.handshake[48:68]) != query.Get("peer_id") {
			t.Error("peer handshake used a different ID from the announce")
		}
		port, err := strconv.Atoi(query.Get("port"))
		if err != nil || port < 1 || port > 65535 {
			t.Errorf("invalid actual port: %q", query.Get("port"))
		}
		if !strings.Contains(output.String(), fmt.Sprintf("current addr: %d", port)) {
			t.Error("announced port differs from logged listening port")
		}
		if successful {
			select {
			case completed := <-requests:
				for key, want := range map[string]string{"event": "completed", "uploaded": "0", "downloaded": "4", "left": "0", "port": query.Get("port"), "peer_id": query.Get("peer_id"), "info_hash": query.Get("info_hash")} {
					if got := completed.Get(key); got != want {
						t.Errorf("completion query[%q] = %q, want %q", key, got, want)
					}
				}
			default:
				t.Error("tracker did not receive completion announce")
			}
			if !strings.Contains(output.String(), "download complete") {
				t.Error("verified output was not reported successful")
			}
			if scenario == "completion tracker failure" && !strings.Contains(output.String(), "503") {
				t.Error("completion announce failure was not logged with its cause")
			}
		}
		wantDownloaded, wantLeft := "4", "4"
		if successful {
			wantLeft = "0"
		}
		if scenario == "peer disconnect" {
			wantDownloaded = "0"
		}
		select {
		case stopped := <-requests:
			for key, want := range map[string]string{"event": "stopped", "uploaded": "0", "downloaded": wantDownloaded, "left": wantLeft, "port": query.Get("port"), "peer_id": query.Get("peer_id"), "info_hash": query.Get("info_hash")} {
				if got := stopped.Get(key); got != want {
					t.Errorf("stopped query[%q] = %q, want %q", key, got, want)
				}
			}
		default:
			t.Error("tracker did not receive stopped announce")
		}
		if stopFailure && !strings.Contains(output.String(), "502") {
			t.Error("stopped announce failure was not logged")
		}
		if len(requests) != 0 {
			t.Error("unexpected extra lifecycle announce")
		}
		// run must release the listener when it returns.
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			t.Errorf("listening port was not released: %v", err)
		} else {
			listener.Close()
		}
	default:
		t.Error("tracker did not receive an announce")
	}
	saved, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	switch scenario {
	case "success", "completion tracker failure", "stopped tracker failure":
		if !bytes.Equal(saved, contents) {
			t.Errorf("saved %q, want %q", saved, contents)
		}
		stat, err := os.Stat(outputPath)
		if err != nil {
			t.Fatal(err)
		}
		if stat.Mode().Perm() != 0600 {
			t.Errorf("output permissions = %o, want 600", stat.Mode().Perm())
		}
	case "existing output":
		if !bytes.Equal(saved, existing) {
			t.Error("existing output was altered")
		}
	default:
		if len(saved) != 0 {
			t.Errorf("failed first piece wrote %d unverified bytes", len(saved))
		}
	}
}

func TestRunTrackerFailures(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status int
		body   string
	}{
		{"HTTP failure", http.StatusServiceUnavailable, "unavailable"},
		{"tracker rejection", http.StatusOK, "d14:failure reason6:deniede"},
		{"invalid response", http.StatusOK, "x"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			announces := make(chan string, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				announces <- r.URL.Query().Get("event")
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			t.Cleanup(server.Close)
			dir := t.TempDir()
			path := filepath.Join(dir, "input.torrent")
			outputPath := filepath.Join(dir, "output.bin")
			data := fmt.Appendf(nil, "d8:announce%d:%s4:infod6:lengthi4e4:name8:file.bin12:piece lengthi4e6:pieces20:01234567890123456789ee", len(server.URL), server.URL)
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			prepareRun(t, "-torrent", path, "-output", outputPath)
			err := run(context.Background(), 25*time.Millisecond)
			if err == nil {
				t.Fatal("expected tracker error")
			}
			if tt.name == "tracker rejection" && !strings.Contains(err.Error(), "denied") {
				t.Errorf("lost failure reason: %v", err)
			}
			if len(announces) != 1 {
				t.Errorf("initial announce failure caused %d tracker requests, want 1", len(announces))
			}
			if data, err := os.ReadFile(outputPath); err != nil || len(data) != 0 {
				t.Errorf("tracker failure should retain the empty output created before announcing: bytes %d, error %v", len(data), err)
			}
		})
	}
}

func TestRunExistingOutputDoesNotAnnounce(t *testing.T) {
	requests := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- struct{}{}
		_, _ = io.WriteString(w, "d8:intervali1800e5:peers0:e")
	}))
	t.Cleanup(server.Close)
	dir := t.TempDir()
	torrentPath, outputPath := filepath.Join(dir, "input.torrent"), filepath.Join(dir, "output.bin")
	meta := fmt.Appendf(nil, "d8:announce%d:%s4:infod6:lengthi4e4:name8:file.bin12:piece lengthi4e6:pieces20:01234567890123456789ee", len(server.URL), server.URL)
	if err := os.WriteFile(torrentPath, meta, 0600); err != nil {
		t.Fatal(err)
	}
	want := []byte("preserve this existing file")
	if err := os.WriteFile(outputPath, want, 0600); err != nil {
		t.Fatal(err)
	}
	prepareRun(t, "-torrent", torrentPath, "-output", outputPath)
	if err := run(context.Background(), 25*time.Millisecond); !os.IsExist(err) {
		t.Errorf("error = %v, want file-exists error", err)
	}
	if len(requests) != 0 {
		t.Error("announced before validating output availability")
	}
	got, err := os.ReadFile(outputPath)
	if err != nil || !bytes.Equal(got, want) {
		t.Errorf("existing output changed: %q, error %v", got, err)
	}
}

func TestRunUploadsVerifiedStorageWhileDownloading(t *testing.T) {
	firstHash, secondHash := sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))
	info := "d6:lengthi8e4:name8:file.bin12:piece lengthi4e6:pieces40:" + string(firstHash[:]) + string(secondHash[:]) + "e"
	infoHash := sha1.Sum([]byte(info))
	secondRequested, releaseSecond := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSecond) }) }
	t.Cleanup(release)
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
		close(secondRequested)
		<-releaseSecond
		if err := fallbackTestReply(conn, 1, []byte("efgh")); err != nil {
			return err
		}
		if err := fallbackTestHave(conn, 1); err != nil {
			return err
		}
		return fallbackTestExpectClosed(conn)
	}, func(wire [68]byte) error {
		if !bytes.Equal(wire[28:48], infoHash[:]) {
			return errors.New("wrong download info hash")
		}
		return nil
	}, 10*time.Second)
	compact := binary.BigEndian.AppendUint16([]byte{127, 0, 0, 1}, uint16(addr.Port))
	announces := make(chan url.Values, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		announces <- r.URL.Query()
		_, _ = fmt.Fprintf(w, "d8:intervali1800e5:peers%d:%se", len(compact), compact)
	}))
	t.Cleanup(server.Close)
	dir := t.TempDir()
	torrentPath, outputPath := filepath.Join(dir, "input.torrent"), filepath.Join(dir, "output.bin")
	meta := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(server.URL), server.URL, info)
	if err := os.WriteFile(torrentPath, meta, 0600); err != nil {
		t.Fatal(err)
	}
	prepareRun(t, "-torrent", torrentPath, "-output", outputPath)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	t.Cleanup(func() {
		release()
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("run did not shut down")
		}
	})
	go func() { defer close(done); runErr = run(ctx, 25*time.Millisecond) }()
	var started url.Values
	select {
	case started = <-announces:
	case <-time.After(3 * time.Second):
		t.Fatal("missing started announce")
	}
	select {
	case <-secondRequested:
	case <-time.After(3 * time.Second):
		t.Fatal("download did not reach gated second piece")
	}
	requester := func() net.Conn {
		t.Helper()
		conn, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", started.Get("port")), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write((peer.Handshake{InfoHash: infoHash}).Serialize()); err != nil {
			t.Fatal(err)
		}
		reply, err := peer.ReadHandshake(conn)
		if err != nil || reply == nil || string(reply.PeerID[:]) != started.Get("peer_id") {
			t.Fatalf("incoming handshake=%+v, err=%v", reply, err)
		}
		msg, err := peer.ReadMessage(conn)
		if err != nil || msg == nil || msg.ID != peer.MsgBitfield || !bytes.Equal(msg.Payload, []byte{0x80}) {
			t.Fatalf("partial availability=%v, err=%v", msg, err)
		}
		if _, err := conn.Write([]byte{0, 0, 0, 1, 2}); err != nil {
			t.Fatal(err)
		}
		msg, err = peer.ReadMessage(conn)
		if err != nil || msg == nil || msg.ID != peer.MsgUnchoke {
			t.Fatalf("unchoke=%v, err=%v", msg, err)
		}
		return conn
	}
	conn := requester()
	request, err := peer.NewRequest(0, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(request.Serialize()); err != nil {
		t.Fatal(err)
	}
	block, err := peer.ReadMessage(conn)
	if err != nil || block == nil || block.ID != peer.MsgPiece || !bytes.Equal(block.Payload, []byte{0, 0, 0, 0, 0, 0, 0, 1, 'b', 'c'}) {
		t.Fatalf("uploaded verified data=%v, err=%v", block, err)
	}
	// The second piece is still being downloaded and must not be exposed.
	request, err = peer.NewRequest(1, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(request.Serialize()); err != nil {
		t.Fatal(err)
	}
	expectIncomingTestEOF(t, conn)
	for _, wire := range [][]byte{
		{0, 0, 0, 2, 6, 42}, // Malformed request payload.
		{0, 0, 0, 13, 6, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 2}, // Crosses piece boundary.
	} {
		bad := requester()
		if _, err := bad.Write(wire); err != nil {
			t.Fatal(err)
		}
		expectIncomingTestEOF(t, bad)
	}
	select {
	case <-done:
		t.Fatalf("peer rejection stopped active download: %v", runErr)
	default:
	}
	release()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("download did not finish")
	}
	if runErr != nil {
		t.Fatal(runErr)
	}
	if err := <-peerDone; err != nil {
		t.Errorf("download peer: %v", err)
	}
	for _, event := range []string{"completed", "stopped"} {
		select {
		case q := <-announces:
			if q.Get("event") != event || q.Get("uploaded") != "2" || q.Get("downloaded") != "8" || q.Get("left") != "0" {
				t.Errorf("%s announce=%v", event, q)
			}
		default:
			t.Errorf("missing %s announce", event)
		}
	}
	if data, err := os.ReadFile(outputPath); err != nil || string(data) != "abcdefgh" {
		t.Errorf("output=%q, err=%v", data, err)
	}
}

func TestRunSeedsCompletedFileUntilCanceled(t *testing.T) {
	firstHash, secondHash := sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))
	info := "d6:lengthi8e4:name8:file.bin12:piece lengthi4e6:pieces40:" + string(firstHash[:]) + string(secondHash[:]) + "e"
	infoHash := sha1.Sum([]byte(info))
	addr, peerDone := startFallbackTestPeerWithVerifier(t, peer.Handshake{InfoHash: infoHash}, func(conn net.Conn) error {
		for index, data := range []string{"abcd", "efgh"} {
			if err := fallbackTestRequest(conn, uint32(index)); err != nil {
				return err
			}
			if err := fallbackTestReply(conn, uint32(index), []byte(data)); err != nil {
				return err
			}
			if err := fallbackTestHave(conn, uint32(index)); err != nil {
				return err
			}
		}
		return fallbackTestExpectClosed(conn)
	}, func(wire [68]byte) error {
		if !bytes.Equal(wire[28:48], infoHash[:]) {
			return errors.New("wrong download info hash")
		}
		return nil
	})
	compact := binary.BigEndian.AppendUint16([]byte{127, 0, 0, 1}, uint16(addr.Port))
	announces := make(chan url.Values, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		announces <- r.URL.Query()
		_, _ = fmt.Fprintf(w, "d8:intervali1e5:peers%d:%se", len(compact), compact)
	}))
	t.Cleanup(server.Close)
	dir := t.TempDir()
	torrentPath, outputPath := filepath.Join(dir, "input.torrent"), filepath.Join(dir, "output.bin")
	meta := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(server.URL), server.URL, info)
	if err := os.WriteFile(torrentPath, meta, 0600); err != nil {
		t.Fatal(err)
	}
	prepareRun(t, "-torrent", torrentPath, "-output", outputPath, "-seed=true")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("run did not shut down")
		}
	})
	go func() { defer close(done); runErr = run(ctx, 25*time.Millisecond) }()
	nextAnnounce := func() url.Values {
		t.Helper()
		select {
		case q := <-announces:
			return q
		case <-done:
			t.Fatalf("run exited before expected announce: %v", runErr)
		case <-time.After(8 * time.Second):
			t.Fatal("timed out waiting for announce")
		}
		return nil
	}
	started := nextAnnounce()
	if started.Get("event") != "started" {
		t.Fatal("missing started announce")
	}
	completed := nextAnnounce()
	if completed.Get("event") != "completed" || completed.Get("left") != "0" {
		t.Fatalf("completion announce=%v", completed)
	}
	select {
	case <-done:
		t.Fatalf("seeding exited after completion: %v", runErr)
	default:
	}
	conn, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", started.Get("port")), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	remote := peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{'u'}}
	if _, err := conn.Write(remote.Serialize()); err != nil {
		t.Fatal(err)
	}
	reply, err := peer.ReadHandshake(conn)
	if err != nil || reply == nil || reply.InfoHash != infoHash || string(reply.PeerID[:]) != started.Get("peer_id") {
		t.Fatalf("incoming handshake=%+v, err=%v", reply, err)
	}
	availability, err := peer.ReadMessage(conn)
	if err != nil || availability == nil || availability.ID != peer.MsgBitfield || !bytes.Equal(availability.Payload, []byte{0xc0}) {
		t.Fatalf("availability=%v, err=%v", availability, err)
	}
	if _, err := conn.Write([]byte{0, 0, 0, 1, 2}); err != nil {
		t.Fatal(err)
	}
	unchoke, err := peer.ReadMessage(conn)
	if err != nil || unchoke == nil || unchoke.ID != peer.MsgUnchoke {
		t.Fatalf("unchoke=%v, err=%v", unchoke, err)
	}
	request, err := peer.NewRequest(1, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(request.Serialize()); err != nil {
		t.Fatal(err)
	}
	block, err := peer.ReadMessage(conn)
	if err != nil || block == nil || block.ID != peer.MsgPiece || !bytes.Equal(block.Payload, []byte{0, 0, 0, 1, 0, 0, 0, 1, 'f', 'g'}) {
		t.Fatalf("uploaded block=%v, err=%v", block, err)
	}
	regular := nextAnnounce()
	if regular.Get("event") != "" || regular.Get("left") != "0" || regular.Get("downloaded") != "8" || regular.Get("uploaded") != "2" {
		t.Fatalf("seeding update=%v", regular)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("seeding did not stop")
	}
	if !errors.Is(runErr, context.Canceled) {
		t.Errorf("run error=%v, want context.Canceled", runErr)
	}
	stopped := <-announces
	if stopped.Get("event") != "stopped" || stopped.Get("uploaded") != "2" {
		t.Errorf("stopped announce=%v", stopped)
	}
	expectIncomingTestEOF(t, conn)
	if err := <-peerDone; err != nil {
		t.Errorf("download peer: %v", err)
	}
	if data, err := os.ReadFile(outputPath); err != nil || string(data) != "abcdefgh" {
		t.Errorf("output=%q, err=%v", data, err)
	}
}

func TestRunParentCancellationStopsServices(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	events := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		event := r.URL.Query().Get("event")
		events <- event
		if event == "started" {
			cancel()
		}
		_, _ = io.WriteString(w, "d8:intervali1800e5:peers0:e")
	}))
	t.Cleanup(server.Close)
	dir := t.TempDir()
	torrentPath := filepath.Join(dir, "input.torrent")
	outputPath := filepath.Join(dir, "output.bin")
	info := "d6:lengthi4e4:name8:file.bin12:piece lengthi4e6:pieces20:01234567890123456789e"
	meta := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(server.URL), server.URL, info)
	if err := os.WriteFile(torrentPath, meta, 0600); err != nil {
		t.Fatal(err)
	}
	prepareRun(t, "-torrent", torrentPath, "-output", outputPath)
	if err := run(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("run() error=%v, want context.Canceled", err)
	}
	if len(events) != 2 {
		t.Fatalf("announce count=%d, want started and stopped", len(events))
	}
	if first, last := <-events, <-events; first != "started" || last != "stopped" {
		t.Errorf("events=%q, %q; want started, stopped", first, last)
	}
}

func TestRunNoPeers(t *testing.T) {
	for _, empty := range []bool{true, false} {
		name := "nonempty torrent"
		if empty {
			name = "empty torrent"
		}
		t.Run(name, func(t *testing.T) {
			announces := make(chan string, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				announces <- r.URL.Query().Get("event")
				_, _ = io.WriteString(w, "d8:intervali1800e5:peers0:e")
			}))
			t.Cleanup(server.Close)
			info := "d6:lengthi0e4:name8:file.bin12:piece lengthi4e6:pieces0:e"
			if !empty {
				info = "d6:lengthi4e4:name8:file.bin12:piece lengthi4e6:pieces20:01234567890123456789e"
			}
			dir := t.TempDir()
			torrentPath, outputPath := filepath.Join(dir, "input.torrent"), filepath.Join(dir, "output.bin")
			meta := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(server.URL), server.URL, info)
			if err := os.WriteFile(torrentPath, meta, 0600); err != nil {
				t.Fatal(err)
			}
			logs := prepareRun(t, "-torrent", torrentPath, "-output", outputPath)
			err := run(context.Background(), 25*time.Millisecond)
			if len(announces) != 2 {
				t.Errorf("announce count = %d, want started and stopped", len(announces))
			}
			select {
			case event := <-announces:
				if event != "started" {
					t.Errorf("initial event = %q, want started", event)
				}
			default:
				t.Error("missing initial announce")
			}
			select {
			case event := <-announces:
				if event != "stopped" {
					t.Errorf("final event = %q, want stopped", event)
				}
			default:
				t.Error("missing stopped announce")
			}
			if empty {
				if err != nil {
					t.Fatalf("empty torrent requires no peers: %v", err)
				}
				if !strings.Contains(logs.String(), "download complete") {
					t.Error("empty torrent did not report completion")
				}
				if strings.Contains(logs.String(), "announce error") {
					t.Error("initially complete torrent attempted an unnecessary completion announce")
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), "peer discovery attempts") {
					t.Errorf("error = %v, want peer exhaustion", err)
				}
				if strings.Contains(logs.String(), "download complete") {
					t.Error("incomplete torrent reported completion")
				}
			}
			// The output is now opened before the session checks for completion or
			// peer exhaustion. An incomplete attempt retains its empty partial file.
			data, readErr := os.ReadFile(outputPath)
			if readErr != nil || len(data) != 0 {
				t.Errorf("output has %d bytes, error %v; want empty file", len(data), readErr)
			}
		})
	}
}

func TestRunRequiresArguments(t *testing.T) {
	for _, tt := range []struct {
		name    string
		args    []string
		message string
	}{
		{"neither flag", nil, "torrent file not specified"},
		{"missing torrent", []string{"-output", "output.bin"}, "torrent file not specified"},
		{"missing output", []string{"-torrent", "input.torrent"}, "output destination not specified"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			prepareRun(t, tt.args...)
			err := run(context.Background(), 25*time.Millisecond)
			if err == nil || !strings.Contains(err.Error(), tt.message) {
				t.Errorf("error = %v, want %q", err, tt.message)
			}
		})
	}
}

func TestRunRejectsUnreadableOrInvalidTorrent(t *testing.T) {
	dir := t.TempDir()
	invalidPath := filepath.Join(dir, "invalid.torrent")
	if err := os.WriteFile(invalidPath, []byte("i1e"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, path string
	}{
		{"missing file", filepath.Join(dir, "missing.torrent")},
		{"directory", dir},
		{"invalid torrent", invalidPath},
	} {
		t.Run(tt.name, func(t *testing.T) {
			output := prepareRun(t, "-torrent", tt.path, "-output", filepath.Join(dir, "output.bin"))
			if err := run(context.Background(), 25*time.Millisecond); err == nil {
				t.Error("expected an error")
			}
			if strings.Contains(output.String(), "Torrent file read") {
				t.Error("reported success after failure")
			}
		})
	}
}

func TestRunPortRange(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "missing.torrent")
	for _, tt := range []struct {
		port  string
		valid bool
	}{
		{"0", true},
		{"1", true},
		{"65535", true},
		{"-1", false},
		{"65536", false},
	} {
		t.Run(tt.port, func(t *testing.T) {
			prepareRun(t, "-torrent", missingPath, "-output", "output.bin", "-port", tt.port)
			err := run(context.Background(), 25*time.Millisecond)
			if tt.valid {
				// An accepted port reaches the next step: opening the torrent file.
				if !os.IsNotExist(err) {
					t.Errorf("valid port %s should reach file loading, got %v", tt.port, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "port") || os.IsNotExist(err) {
				t.Errorf("invalid port %s should fail before file loading, got %v", tt.port, err)
			}
		})
	}
}

func TestRunWorkerLimit(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "missing.torrent")
	for _, tt := range []struct {
		name  string
		value string
		valid bool
	}{
		{"default", "", true},
		{"one worker", "1", true},
		{"four workers", "4", true},
		{"zero workers", "0", false},
		{"negative workers", "-1", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := []string{"-torrent", missingPath, "-output", "output.bin"}
			if tt.value != "" {
				args = append(args, "-workers", tt.value)
			}
			prepareRun(t, args...)
			err := run(context.Background(), 25*time.Millisecond)
			if tt.valid {
				if !os.IsNotExist(err) {
					t.Errorf("valid worker limit did not reach file loading: %v", err)
				}
			} else if err == nil || os.IsNotExist(err) || !strings.Contains(err.Error(), "workers") {
				t.Errorf("invalid worker limit did not fail before file loading: %v", err)
			}
		})
	}
}
