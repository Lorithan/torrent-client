package tracker

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/torrent"
)

func TestAnnounce(t *testing.T) {
	tf := &torrent.TorrentFile{
		Announce: "https://tracker.example/private/announce?passkey=secret",
		InfoHash: [20]byte{0, 255, 128, ' ', '+', '&', '%'},
		Length:   10485760,
	}
	peerID := [20]byte{'-', 'C', 'T', '0', '0', '0', '1', '-', 255, 0, '+', ' '}
	// Two compact peers: 127.0.0.1:6881 and 192.168.1.2:65535.
	wireBody := "d8:intervali1800e5:peers12:" + string([]byte{127, 0, 0, 1, 26, 225, 192, 168, 1, 2, 255, 255}) + "e"
	body := &announceTestBody{Reader: strings.NewReader(wireBody)}
	calls := 0
	client := &http.Client{Transport: announceTestTransport(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Method != http.MethodGet || req.URL.Host != "tracker.example" || req.URL.Path != "/private/announce" {
			t.Errorf("unexpected request: %s %s", req.Method, req.URL)
		}
		query := req.URL.Query()
		for key, want := range map[string]string{
			"passkey": "secret", "info_hash": string(tf.InfoHash[:]), "peer_id": string(peerID[:]),
			"port": "6881", "uploaded": "0", "downloaded": "0", "left": "10485760",
			"compact": "1", "event": "started",
		} {
			if values := query[key]; len(values) != 1 || values[0] != want {
				t.Errorf("query[%q] = %q, want single value %q", key, values, want)
			}
		}
		return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
	})}

	got, err := Announce(tf, peerID, 6881, client)
	if err != nil {
		t.Fatalf("Announce() error: %v", err)
	}
	if got == nil {
		t.Fatal("nil response without error")
	}
	if got.Interval != 30*time.Minute {
		t.Errorf("interval = %v, want 30m", got.Interval)
	}
	wantPeers := []string{"127.0.0.1:6881", "192.168.1.2:65535"}
	if len(got.Peers) != len(wantPeers) {
		t.Fatalf("got %d peers, want %d", len(got.Peers), len(wantPeers))
	}
	for i, want := range wantPeers {
		if addr := got.Peers[i].String(); addr != want {
			t.Errorf("peer %d = %q, want %q", i, addr, want)
		}
	}
	if calls != 1 || !body.closed {
		t.Errorf("requests = %d, body closed = %v; want 1 and true", calls, body.closed)
	}
}

func TestAnnounceErrors(t *testing.T) {
	connectionErr := errors.New("connection failed")
	tests := []struct {
		name       string
		announce   string
		status     int
		body       string
		requestErr error
		wantCalls  int
		message    string
	}{
		{name: "URL failure stops before request", announce: "udp://tracker.example/announce", wantCalls: 0},
		{name: "request failure", announce: "https://tracker.example/announce", requestErr: connectionErr, wantCalls: 1},
		{name: "HTTP failure", announce: "https://tracker.example/announce", status: 503, wantCalls: 1},
		{name: "invalid bencode", announce: "https://tracker.example/announce", status: 200, body: "x", wantCalls: 1},
		{name: "tracker rejection", announce: "https://tracker.example/announce", status: 200, body: "d14:failure reason6:deniede", wantCalls: 1, message: "denied"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: announceTestTransport(func(*http.Request) (*http.Response, error) {
				calls++
				if tt.requestErr != nil {
					return nil, tt.requestErr
				}
				return &http.Response{
					StatusCode: tt.status,
					Status:     http.StatusText(tt.status),
					Body:       io.NopCloser(strings.NewReader(tt.body)),
					Header:     make(http.Header),
				}, nil
			})}
			got, err := Announce(&torrent.TorrentFile{Announce: tt.announce, Length: 1024}, [20]byte{}, 6881, client)
			if err == nil || got != nil {
				t.Fatalf("got (%v, %v), want nil response and error", got, err)
			}
			if tt.requestErr != nil && !errors.Is(err, tt.requestErr) {
				t.Errorf("error = %v, want underlying error %v", err, tt.requestErr)
			}
			if !strings.Contains(err.Error(), tt.message) {
				t.Errorf("error %q should contain %q", err, tt.message)
			}
			if calls != tt.wantCalls {
				t.Errorf("requests = %d, want %d", calls, tt.wantCalls)
			}
		})
	}
}
