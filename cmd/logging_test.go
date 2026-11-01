package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRedirectLogsAppendsAndRestoresWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "diagnostics.log")
	if err := os.WriteFile(path, []byte("existing diagnostic\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var previous bytes.Buffer
	original := log.Writer()
	log.SetOutput(&previous)
	t.Cleanup(func() { log.SetOutput(original) })
	cleanup, err := redirectLogs(path)
	if err != nil {
		t.Fatal(err)
	}
	cleaned := false
	t.Cleanup(func() {
		if !cleaned {
			_ = cleanup()
		}
	})
	log.Print("redirected diagnostic marker")
	if previous.Len() != 0 {
		t.Error("diagnostic escaped to previous writer")
	}
	err = cleanup()
	cleaned = true
	if err != nil {
		t.Fatal(err)
	}
	if log.Writer() != &previous {
		t.Error("cleanup did not restore previous log writer")
	}
	log.Print("restored writer marker")
	if !strings.Contains(previous.String(), "restored writer marker") {
		t.Error("restored writer did not receive later logs")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(contents, []byte("existing diagnostic\n")) || !bytes.Contains(contents, []byte("redirected diagnostic marker")) {
		t.Errorf("diagnostics were not appended: %q", contents)
	}
	if bytes.Contains(contents, []byte("restored writer marker")) {
		t.Error("logs continued writing to diagnostic file after cleanup")
	}
}

func TestRedirectLogsFailureLeavesWriterUnchanged(t *testing.T) {
	var previous bytes.Buffer
	original := log.Writer()
	log.SetOutput(&previous)
	t.Cleanup(func() { log.SetOutput(original) })
	cleanup, err := redirectLogs(filepath.Join(t.TempDir(), "missing-directory", "diagnostics.log"))
	if err == nil || cleanup != nil {
		t.Fatalf("redirectLogs()=(cleanup present: %t, %v), want no cleanup and an error", cleanup != nil, err)
	}
	if log.Writer() != &previous {
		t.Error("failed redirection changed log writer")
	}
	log.Print("original writer still works")
	if previous.Len() == 0 {
		t.Error("failed redirection lost later diagnostics")
	}
}

func TestRunLogFlagOnlyAppliesToTUIMode(t *testing.T) {
	for _, tuiMode := range []bool{false, true} {
		name := "default CLI"
		if tuiMode {
			name = "TUI"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			input, output := filepath.Join(dir, "missing.torrent"), filepath.Join(dir, "output.bin")
			logPath := filepath.Join(dir, "missing-directory", "diagnostics.log")
			args := []string{"-torrent", input, "-output", output, "-log", logPath}
			if tuiMode {
				args = append(args, "-tui")
			}
			prepareRun(t, args...)
			previous := log.Writer()
			err := run(context.Background(), time.Second)
			var pathErr *os.PathError
			if !errors.As(err, &pathErr) {
				t.Fatalf("run()=%v, want filesystem error", err)
			}
			wantPath := input
			if tuiMode {
				wantPath = logPath
			}
			if pathErr.Path != wantPath {
				t.Errorf("failed path=%q, want %q", pathErr.Path, wantPath)
			}
			if log.Writer() != previous {
				t.Error("mode setup failure changed log destination")
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Errorf("setup failure created output: %v", err)
			}
		})
	}
}
