package main

import (
	"bytes"
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
		id := r.URL.Query().Get("peer_id")
		announcedIDs <- id
		announcedIDs <- id
		_, _ = io.WriteString(w, "d8:intervali1800e5:peers12:"+string(compact)+"e")
	}))
	t.Cleanup(tracker.Close)
	dir := t.TempDir()
	torrentPath, outputPath := filepath.Join(dir, "input.torrent"), filepath.Join(dir, "output.bin")
	meta := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(tracker.URL), tracker.URL, info)
	if err := os.WriteFile(torrentPath, meta, 0600); err != nil {
		t.Fatal(err)
	}
	prepareRun(t, "-torrent", torrentPath, "-output", outputPath)
	if err := run(); err != nil {
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
}

// run uses the standard flag set and logger; isolate their process-wide state
// between tests. These tests must remain sequential.
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
	os.Args = append([]string{"torrent-client"}, args...)
	flag.CommandLine = flag.NewFlagSet("torrent-client", flag.ContinueOnError)
	var output bytes.Buffer
	flag.CommandLine.SetOutput(&output)
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")
	return &output
}

func TestRunDownload(t *testing.T) {
	for _, scenario := range []string{"success", "existing output", "corrupted data", "peer disconnect"} {
		t.Run(scenario, func(t *testing.T) { runDownloadScenario(t, scenario) })
	}
}

func runDownloadScenario(t *testing.T, scenario string) {
	t.Helper()
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
			if scenario == "corrupted data" {
				blockData[0] ^= 0xff
			}
			block := append([]byte{0, 0, 0, 13, 7, 0, 0, 0, 0, 0, 0, 0, 0}, blockData...)
			if _, err := conn.Write(block); err != nil {
				result.err = err
				peerDone <- result
				return
			}
		}
		if scenario == "success" {
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
	requests := make(chan url.Values, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Query()
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
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
	runErr := run()
	if scenario == "existing output" {
		// Output refusal now happens before connecting. Release the unused fake peer.
		peerListener.Close()
	}
	result := <-peerDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	if scenario == "success" {
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
		if scenario == "corrupted data" && (!strings.Contains(runErr.Error(), "exhausted peers") || !strings.Contains(output.String(), "hash")) {
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
	case "success":
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
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			err := run()
			if err == nil {
				t.Fatal("expected tracker error")
			}
			if tt.name == "tracker rejection" && !strings.Contains(err.Error(), "denied") {
				t.Errorf("lost failure reason: %v", err)
			}
			if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
				t.Errorf("tracker failure created output or stat failed: %v", err)
			}
		})
	}
}

func TestRunNoPeers(t *testing.T) {
	for _, empty := range []bool{true, false} {
		name := "nonempty torrent"
		if empty {
			name = "empty torrent"
		}
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			err := run()
			if empty {
				if err != nil {
					t.Fatalf("empty torrent requires no peers: %v", err)
				}
				if !strings.Contains(logs.String(), "download complete") {
					t.Error("empty torrent did not report completion")
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), "exhausted peers") {
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
			err := run()
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
			if err := run(); err == nil {
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
			err := run()
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
