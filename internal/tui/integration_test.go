package tui

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
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/transfer"
)

type tuiEngineFixture struct {
	config      transfer.Config
	infoHash    [20]byte
	announces   chan url.Values
	release     chan struct{}
	releaseOnce sync.Once
}

func (f *tuiEngineFixture) finishDownload() { f.releaseOnce.Do(func() { close(f.release) }) }

func newTUIEngineFixture(t *testing.T) *tuiEngineFixture {
	t.Helper()
	f := &tuiEngineFixture{announces: make(chan url.Values, 16), release: make(chan struct{})}
	first, second := sha1.Sum([]byte("abcd")), sha1.Sum([]byte("efgh"))
	info := "d6:lengthi8e4:name8:file.bin12:piece lengthi4e6:pieces40:" + string(first[:]) + string(second[:]) + "e"
	f.infoHash = sha1.Sum([]byte(info))
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixtureCtx, cancelFixture := context.WithCancel(context.Background())
	peerDone := make(chan error, 1)
	go func() {
		serve := func() error {
			conn, err := listener.Accept()
			if err != nil {
				return err
			}
			defer conn.Close()
			stop := context.AfterFunc(fixtureCtx, func() { conn.Close() })
			defer stop()
			if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				return err
			}
			remote, err := peer.ReadHandshake(conn)
			if err != nil {
				return err
			}
			if remote.InfoHash != f.infoHash {
				return errors.New("wrong torrent in outgoing handshake")
			}
			if _, err := conn.Write((peer.Handshake{InfoHash: f.infoHash, PeerID: [20]byte{'r'}}).Serialize()); err != nil {
				return err
			}
			interested, err := peer.ReadMessage(conn)
			if err != nil || interested == nil || interested.ID != peer.MsgInterested {
				return fmt.Errorf("expected interested: %v, %v", interested, err)
			}
			if _, err := conn.Write([]byte{0, 0, 0, 2, 5, 0xc0, 0, 0, 0, 1, 1}); err != nil {
				return err
			}
			for index, data := range []string{"abcd", "efgh"} {
				msg, err := peer.ReadMessage(conn)
				if err != nil || msg == nil {
					return fmt.Errorf("request missing: %v", err)
				}
				request, err := msg.BlockRequest()
				if err != nil || request.Index != uint32(index) || request.Begin != 0 || request.Length != 4 {
					return fmt.Errorf("unexpected request: %+v, %v", request, err)
				}
				if index == 1 {
					select {
					case <-f.release:
					case <-fixtureCtx.Done():
						return nil
					}
				}
				piece, err := peer.NewPiece(uint32(index), 0, []byte(data))
				if err != nil {
					return err
				}
				if _, err := conn.Write(piece.Serialize()); err != nil {
					return err
				}
				have, err := peer.ReadMessage(conn)
				if err != nil || have == nil || have.ID != peer.MsgHave || !bytes.Equal(have.Payload, []byte{0, 0, 0, byte(index)}) {
					return fmt.Errorf("expected completed-piece have: %v, %v", have, err)
				}
			}
			var next [1]byte
			if _, err := conn.Read(next[:]); !errors.Is(err, io.EOF) {
				return fmt.Errorf("download connection not closed: %v", err)
			}
			return nil
		}
		err := serve()
		if fixtureCtx.Err() != nil {
			err = nil
		}
		peerDone <- err
	}()
	compact := binary.BigEndian.AppendUint16([]byte{127, 0, 0, 1}, uint16(listener.Addr().(*net.TCPAddr).Port))
	tracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.announces <- r.URL.Query()
		_, _ = fmt.Fprintf(w, "d8:intervali1800e5:peers6:%se", compact)
	}))
	t.Cleanup(func() {
		cancelFixture()
		listener.Close()
		f.finishDownload()
		tracker.Close()
		select {
		case err := <-peerDone:
			if err != nil {
				t.Errorf("fixture peer: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("fixture peer did not stop")
		}
	})
	dir := t.TempDir()
	f.config = transfer.Config{TorrentPath: filepath.Join(dir, "input.torrent"), OutputPath: filepath.Join(dir, "output.bin"), Workers: 1, Seed: true, PeerWaitTimeout: 25 * time.Millisecond}
	meta := fmt.Appendf(nil, "d8:announce%d:%s4:info%se", len(tracker.URL), tracker.URL, info)
	if err := os.WriteFile(f.config.TorrentPath, meta, 0600); err != nil {
		t.Fatal(err)
	}
	return f
}

type inspectModelMsg struct{}

func startObservedTUI(t *testing.T, engine Runner) (*tea.Program, <-chan Model, <-chan struct{}, *error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	programs := make(chan *tea.Program, 1)
	observations := make(chan Model, 64)
	opts := append(headlessOptions(),
		func(p *tea.Program) { programs <- p },
		tea.WithFilter(func(model tea.Model, msg tea.Msg) tea.Msg {
			select {
			case observations <- model.(Model):
			default:
			}
			return msg
		}))
	done := make(chan struct{})
	var result error
	go func() { defer close(done); result = Run(ctx, engine, opts...) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("observed TUI did not stop")
		}
	})
	select {
	case program := <-programs:
		return program, observations, done, &result
	case <-time.After(time.Second):
		t.Fatal("program was not constructed")
	}
	return nil, nil, nil, nil
}

func waitForModel(t *testing.T, observations <-chan Model, done <-chan struct{}, predicate func(Model) bool) Model {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case model := <-observations:
			if predicate(model) {
				return model
			}
		case <-done:
			t.Fatal("TUI exited before expected state")
		case <-timer.C:
			t.Fatal("TUI did not publish expected state")
		}
	}
}

func TestTUIRealEngineTransferResizeSeedAndQuit(t *testing.T) {
	for _, stopDuringDownload := range []bool{false, true} {
		t.Run(fmt.Sprintf("stopDuringDownload=%t", stopDuringDownload), func(t *testing.T) {
			f := newTUIEngineFixture(t)
			engine, err := transfer.New(f.config)
			if err != nil {
				t.Fatal(err)
			}
			program, observations, done, result := startObservedTUI(t, engine)
			partial := waitForModel(t, observations, done, func(m Model) bool {
				return m.status.Phase == transfer.PhaseDownloading && m.status.Progress.VerifiedBytes == 4
			})
			if partial.status.ActivePeers != 1 || partial.status.Progress.Stats.Downloaded != 4 {
				t.Errorf("partial status=%+v", partial.status)
			}
			program.Send(tea.WindowSizeMsg{Width: 20, Height: 3})
			program.Send(inspectModelMsg{})
			small := waitForModel(t, observations, done, func(m Model) bool { return m.width == 20 && m.height == 3 })
			if view := ansi.Strip(small.View().Content); lipgloss.Height(view) > 3 || !strings.Contains(view, "q: stop/quit") {
				t.Errorf("small view hid controls: %q", view)
			}
			program.Send(tea.WindowSizeMsg{Width: 80, Height: 24})
			program.Send(inspectModelMsg{})
			large := waitForModel(t, observations, done, func(m Model) bool { return m.width == 80 && m.height == 24 })
			if view := ansi.Strip(large.View().Content); !strings.Contains(view, "50.0%") || !strings.Contains(view, "Peers: 1") {
				t.Errorf("expanded view lost progress: %q", view)
			}
			var started url.Values
			select {
			case started = <-f.announces:
			default:
				t.Fatal("missing started announce")
			}
			if started.Get("event") != "started" {
				t.Fatal("first announce was not started")
			}
			wantBytes, wantUploaded, wantLeft := "abcd", "0", "4"
			var upload net.Conn
			if !stopDuringDownload {
				f.finishDownload()
				waitForModel(t, observations, done, func(m Model) bool {
					return m.status.Phase == transfer.PhaseSeeding && m.status.Progress.VerifiedBytes == 8
				})
				upload, err = net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", started.Get("port")), time.Second)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { upload.Close() })
				if err := upload.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
					t.Fatal(err)
				}
				if _, err := upload.Write((peer.Handshake{InfoHash: f.infoHash}).Serialize()); err != nil {
					t.Fatal(err)
				}
				handshake, err := peer.ReadHandshake(upload)
				if err != nil || handshake == nil || string(handshake.PeerID[:]) != started.Get("peer_id") {
					t.Fatalf("seed handshake=%+v, %v", handshake, err)
				}
				available, err := peer.ReadMessage(upload)
				if err != nil || available == nil || available.ID != peer.MsgBitfield || !bytes.Equal(available.Payload, []byte{0xc0}) {
					t.Fatalf("seed availability=%v, %v", available, err)
				}
				if _, err := upload.Write([]byte{0, 0, 0, 1, 2}); err != nil {
					t.Fatal(err)
				}
				unchoke, err := peer.ReadMessage(upload)
				if err != nil || unchoke == nil || unchoke.ID != peer.MsgUnchoke {
					t.Fatalf("seed unchoke=%v, %v", unchoke, err)
				}
				request, _ := peer.NewRequest(1, 1, 2)
				if _, err := upload.Write(request.Serialize()); err != nil {
					t.Fatal(err)
				}
				piece, err := peer.ReadMessage(upload)
				if err != nil || piece == nil || piece.ID != peer.MsgPiece || !bytes.Equal(piece.Payload, []byte{0, 0, 0, 1, 0, 0, 0, 1, 'f', 'g'}) {
					t.Fatalf("uploaded data=%v, %v", piece, err)
				}
				waitForModel(t, observations, done, func(m Model) bool {
					return m.status.Phase == transfer.PhaseSeeding && m.status.Progress.Stats.Uploaded == 2 && m.status.ActivePeers == 1
				})
				wantBytes, wantUploaded, wantLeft = "abcdefgh", "2", "0"
			}
			key := tea.KeyPressMsg{Code: 'q', Text: "q"}
			if !stopDuringDownload {
				key = tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl}
			}
			program.Send(key)
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("quit did not finish engine cleanup")
			}
			if !errors.Is(*result, context.Canceled) {
				t.Errorf("TUI result=%v, want cancellation", *result)
			}
			status := engine.Status()
			if status.Phase != transfer.PhaseStopped || status.ActivePeers != 0 || status.DownloadRate != 0 || status.UploadRate != 0 {
				t.Errorf("final status=%+v", status)
			}
			if data, err := os.ReadFile(f.config.OutputPath); err != nil || string(data) != wantBytes {
				t.Errorf("output=%q, %v; want %q", data, err, wantBytes)
			}
			if upload != nil {
				var b [1]byte
				if _, err := upload.Read(b[:]); !errors.Is(err, io.EOF) {
					t.Errorf("incoming connection survived quit: %v", err)
				}
			}
			completed, stopped := 0, 0
			for len(f.announces) > 0 {
				q := <-f.announces
				switch q.Get("event") {
				case "completed":
					completed++
				case "stopped":
					stopped++
					if q.Get("uploaded") != wantUploaded || q.Get("left") != wantLeft || q.Get("downloaded") != fmt.Sprint(len(wantBytes)) {
						t.Errorf("stopped counters=%v", q)
					}
				}
			}
			wantCompleted := 1
			if stopDuringDownload {
				wantCompleted = 0
			}
			if completed != wantCompleted || stopped != 1 {
				t.Errorf("events: completed=%d stopped=%d", completed, stopped)
			}
		})
	}
}

func TestTUIRealEngineFailureRemainsVisibleUntilQuit(t *testing.T) {
	dir := t.TempDir()
	engine, err := transfer.New(transfer.Config{TorrentPath: filepath.Join(dir, "missing.torrent"), OutputPath: filepath.Join(dir, "output.bin"), Workers: 1, PeerWaitTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	program, observations, done, result := startObservedTUI(t, engine)
	failed := waitForModel(t, observations, done, func(m Model) bool { return m.finished && m.status.Phase == transfer.PhaseFailed })
	if text := ansi.Strip(failed.View().Content); !strings.Contains(text, "Error") || !strings.Contains(text, "q / Ctrl+C: quit") {
		t.Errorf("failure view=%q", text)
	}
	program.Send(tea.KeyPressMsg{Code: 'q', Text: "q"})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("failure screen did not quit")
	}
	if !os.IsNotExist(*result) {
		t.Errorf("TUI result=%v, want original input error", *result)
	}
}
