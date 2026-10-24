package main

import (
	"bytes"
	"crypto/sha1"
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
)

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

func TestRunLoadsTorrentMetadata(t *testing.T) {
	requests := make(chan url.Values, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Query()
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		_, _ = io.WriteString(w, "d8:intervali1800e5:peers6:"+string([]byte{127, 0, 0, 1, 26, 225})+"e")
	}))
	t.Cleanup(server.Close)
	dir := t.TempDir()
	torrentPath := filepath.Join(dir, "input.torrent")
	outputPath := filepath.Join(dir, "output.bin")
	info := "d6:lengthi4e4:name8:file.bin12:piece lengthi4e6:pieces20:01234567890123456789e"
	data := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(server.URL), server.URL, info)
	if err := os.WriteFile(torrentPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	output := prepareRun(t, "-torrent", torrentPath, "-output", outputPath)
	if err := run(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"file.bin", "file length: 4", "piece count: 1", "127.0.0.1"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("output %q does not contain %q", output.String(), want)
		}
	}
	select {
	case query := <-requests:
		hash := sha1.Sum([]byte(info))
		for key, want := range map[string]string{"info_hash": string(hash[:]), "left": "4", "compact": "1", "event": "started"} {
			if got := query.Get(key); got != want {
				t.Errorf("query[%q] = %q, want %q", key, got, want)
			}
		}
		if id := query.Get("peer_id"); len(id) != 20 || !strings.HasPrefix(id, "-CT0001-") {
			t.Errorf("invalid peer ID: %q", id)
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
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Errorf("metadata loading created output or stat failed: %v", err)
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
