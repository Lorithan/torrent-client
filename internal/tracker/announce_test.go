package tracker

import (
	"bytes"
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
			"port": "6881", "uploaded": "321", "downloaded": "12345678", "left": "456",
			"compact": "1", "event": "stopped",
		} {
			if values := query[key]; len(values) != 1 || values[0] != want {
				t.Errorf("query[%q] = %q, want single value %q", key, values, want)
			}
		}
		return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
	})}

	got, err := Announce(tf, peerID, 6881, client, AnnounceParams{Uploaded: 321, Downloaded: 12345678, Left: 456, Event: AnnounceEventStopped})
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
			got, err := Announce(&torrent.TorrentFile{Announce: tt.announce, Length: 1024}, [20]byte{}, 6881, client, AnnounceParams{Left: 1024, Event: AnnounceEventStarted})
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

func TestAnnounceRejectsInvalidParamsBeforeRequest(t *testing.T) {
	for _, tt := range []struct {
		name   string
		params AnnounceParams
	}{
		{"negative uploaded", AnnounceParams{Uploaded: -1}},
		{"negative downloaded", AnnounceParams{Downloaded: -1}},
		{"negative remaining", AnnounceParams{Left: -1}},
		{"unsupported event", AnnounceParams{Event: AnnounceEvent("updated")}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: announceTestTransport(func(*http.Request) (*http.Response, error) {
				calls++
				return nil, errors.New("unexpected HTTP request")
			})}
			tf := &torrent.TorrentFile{Announce: "https://tracker.example/announce", Length: 1000}
			got, err := Announce(tf, [20]byte{}, 6881, client, tt.params)
			if got != nil || err == nil {
				t.Errorf("got (%v, %v), want nil response and validation error", got, err)
			}
			if calls != 0 {
				t.Errorf("made %d requests with invalid announce parameters", calls)
			}
		})
	}
}

// A custom transport lets these tests supply responses without network access.
type announceTestTransport func(*http.Request) (*http.Response, error)

func (f announceTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type announceTestBody struct {
	io.Reader
	closed bool
}

func (b *announceTestBody) Close() error {
	b.closed = true
	return nil
}

type announceErrorReader struct{ err error }

func (r announceErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestGetAnnounceResponse(t *testing.T) {
	readErr := errors.New("body read failed")
	tests := []struct {
		name    string
		status  int
		data    []byte
		readErr error
		wantErr bool
	}{
		{name: "response bytes", status: http.StatusOK, data: []byte{'d', 0, 0xff, 'e'}},
		{name: "empty body", status: http.StatusOK},
		{name: "exact maximum", status: http.StatusOK, data: bytes.Repeat([]byte{'x'}, MAX_RESPONSE_SIZE)},
		{name: "one byte over maximum", status: http.StatusOK, data: bytes.Repeat([]byte{'x'}, MAX_RESPONSE_SIZE+1), wantErr: true},
		{name: "well over maximum", status: http.StatusOK, data: bytes.Repeat([]byte{'x'}, MAX_RESPONSE_SIZE*2), wantErr: true},
		{name: "HTTP failure", status: http.StatusServiceUnavailable, wantErr: true},
		{name: "read failure", status: http.StatusOK, readErr: readErr, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := bytes.NewReader(tt.data)
			body := &announceTestBody{Reader: source}
			if tt.readErr != nil {
				body.Reader = announceErrorReader{err: tt.readErr}
			}
			client := &http.Client{Transport: announceTestTransport(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet || req.URL.String() != "https://tracker.example/announce?compact=1" {
					t.Errorf("unexpected request: %s %s", req.Method, req.URL)
				}
				return &http.Response{
					StatusCode: tt.status,
					Status:     http.StatusText(tt.status),
					Body:       body,
					Header:     make(http.Header),
				}, nil
			})}

			got, err := getAnnounceResponse("https://tracker.example/announce?compact=1", client)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.readErr != nil && !errors.Is(err, tt.readErr) {
				t.Errorf("error = %v, want underlying read error %v", err, tt.readErr)
			}
			if !tt.wantErr && !bytes.Equal(got, tt.data) {
				t.Errorf("response bytes differ: got %d bytes, want %d", len(got), len(tt.data))
			}
			if !body.closed {
				t.Error("response body was not closed")
			}
			if consumed := len(tt.data) - source.Len(); consumed > MAX_RESPONSE_SIZE+1 {
				t.Errorf("read %d bytes, exceeding detection limit", consumed)
			}
		})
	}
}

func TestGetAnnounceResponseRequestError(t *testing.T) {
	wantErr := errors.New("connection failed")
	client := &http.Client{Transport: announceTestTransport(func(*http.Request) (*http.Response, error) {
		return nil, wantErr
	})}
	got, err := getAnnounceResponse("https://tracker.example/announce", client)
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want underlying request error %v", err, wantErr)
	}
	if got != nil {
		t.Error("expected nil bytes on request failure")
	}
}
