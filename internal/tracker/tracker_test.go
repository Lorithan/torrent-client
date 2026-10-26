package tracker

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/torrent"
)

func TestParseTrackerResponseCompact(t *testing.T) {
	tests := []struct {
		name  string
		peers string
		want  []string
	}{
		{"no peers", "", nil},
		{"one peer", string([]byte{127, 0, 0, 1, 26, 225}), []string{"127.0.0.1:6881"}},
		{"two peers", string([]byte{127, 0, 0, 1, 26, 225, 192, 168, 1, 2, 255, 255}), []string{"127.0.0.1:6881", "192.168.1.2:65535"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Report a parser panic as a failed case so the remaining cases still run.
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("ParseTrackerResponse panicked: %v", r)
				}
			}()
			data := fmt.Appendf(nil, "d8:intervali1800e5:peers%d:%se", len(tt.peers), tt.peers)
			got, err := parseTrackerResponse(data)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil {
				t.Fatal("nil response without error")
			}
			if got.Interval != 30*time.Minute {
				t.Errorf("interval = %v, want 30m", got.Interval)
			}
			if len(got.Peers) != len(tt.want) {
				t.Fatalf("got %d peers, want %d", len(got.Peers), len(tt.want))
			}
			for i := range tt.want {
				if addr := got.Peers[i].String(); addr != tt.want[i] {
					t.Errorf("peer %d = %q, want %q", i, addr, tt.want[i])
				}
			}
		})
	}
}

func TestParseCompactPeersRejectsZeroPort(t *testing.T) {
	for _, tt := range []struct {
		name string
		data []byte
	}{
		{"first peer", []byte{127, 0, 0, 1, 0, 0}},
		{"after valid peer", []byte{127, 0, 0, 1, 26, 225, 192, 0, 2, 1, 0, 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			peers, err := parseCompactPeers(string(tt.data))
			if err == nil {
				t.Fatal("expected an error for port zero")
			}
			if len(peers) != 0 {
				t.Errorf("returned partial peers on failure: %v", peers)
			}
		})
	}
}

func TestParseTrackerResponseDictionary(t *testing.T) {
	for _, tt := range []struct {
		name string
		list string
		want []string
	}{
		{"empty list", "le", nil},
		{"one peer", "ld2:ip9:127.0.0.14:porti6881eee", []string{"127.0.0.1:6881"}},
		{"multiple peers", "ld2:ip9:192.0.2.14:porti1eed2:ip12:198.51.100.24:porti65535eee", []string{"192.0.2.1:1", "198.51.100.2:65535"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte("d8:intervali1800e5:peers" + tt.list + "e")
			got, err := parseTrackerResponse(data)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil {
				t.Fatal("nil response without error")
			}
			if got.Interval != 30*time.Minute || len(got.Peers) != len(tt.want) {
				t.Fatalf("response = %+v, want interval 30m and %d peers", got, len(tt.want))
			}
			for i, want := range tt.want {
				if got.Peers[i].String() != want {
					t.Errorf("peer %d = %s, want %s", i, got.Peers[i].String(), want)
				}
			}
		})
	}
}

func TestParseDictionaryPeers(t *testing.T) {
	for _, tt := range []struct {
		name string
		data []any
		want []string
	}{
		{"empty", nil, nil},
		{"one peer", []any{map[string]any{"ip": "127.0.0.1", "port": int64(6881)}}, []string{"127.0.0.1:6881"}},
		{"multiple peers and port boundaries", []any{
			map[string]any{"ip": "192.0.2.1", "port": int64(1), "peer id": "ignored"},
			map[string]any{"ip": "198.51.100.2", "port": int64(65535)},
		}, []string{"192.0.2.1:1", "198.51.100.2:65535"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDictionaryPeers(tt.data)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d peers, want %d", len(got), len(tt.want))
			}
			for i, want := range tt.want {
				if got[i].IP.To4() == nil || got[i].String() != want {
					t.Errorf("peer %d = %v, want IPv4 address %s", i, got[i], want)
				}
			}
		})
	}
}

func TestParseDictionaryPeersInvalid(t *testing.T) {
	for _, tt := range []struct {
		name  string
		entry any
	}{
		{"not a dictionary", "peer"},
		{"nil entry", nil},
		{"missing IP", map[string]any{"port": int64(6881)}},
		{"wrong IP type", map[string]any{"ip": int64(1), "port": int64(6881)}},
		{"empty IP", map[string]any{"ip": "", "port": int64(6881)}},
		{"malformed IP", map[string]any{"ip": "999.1.1.1", "port": int64(6881)}},
		{"unsupported IPv6", map[string]any{"ip": "2001:db8::1", "port": int64(6881)}},
		{"missing port", map[string]any{"ip": "192.0.2.1"}},
		{"wrong port type", map[string]any{"ip": "192.0.2.1", "port": "6881"}},
		{"zero port", map[string]any{"ip": "192.0.2.1", "port": int64(0)}},
		{"negative port", map[string]any{"ip": "192.0.2.1", "port": int64(-1)}},
		{"port too large", map[string]any{"ip": "192.0.2.1", "port": int64(65536)}},
		{"maximum int64 port", map[string]any{"ip": "192.0.2.1", "port": int64(9223372036854775807)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// A bad later entry must fail the entire response, not return partial peers.
			got, err := parseDictionaryPeers([]any{map[string]any{"ip": "192.0.2.1", "port": int64(6881)}, tt.entry})
			if err == nil {
				t.Fatal("expected an error")
			}
			if len(got) != 0 {
				t.Errorf("returned partial peers on failure: %v", got)
			}
		})
	}
}

func TestParseTrackerResponseInvalid(t *testing.T) {
	tests := []struct {
		name        string
		data        string
		wantMessage string
	}{
		{"invalid bencode", "x", ""},
		{"not a dictionary", "le", ""},
		{"tracker failure without success fields", "d14:failure reason6:deniede", "denied"},
		{"malformed failure reason", "d14:failure reasoni1ee", ""},
		{"missing interval", "d5:peers0:e", ""},
		{"wrong interval type", "d8:interval1:x5:peers0:e", ""},
		{"negative interval", "d8:intervali-1e5:peers0:e", ""},
		{"zero interval", "d8:intervali0e5:peers0:e", ""},
		{"interval overflow", "d8:intervali9223372037e5:peers0:e", ""},
		{"missing peers", "d8:intervali1800ee", ""},
		{"wrong peers type", "d8:intervali1800e5:peersi1ee", ""},
		{"incomplete peer record", "d8:intervali1800e5:peers5:abcdee", ""},
		{"compact peer with zero port", string(append([]byte("d8:intervali1800e5:peers6:"), 127, 0, 0, 1, 0, 0, 'e')), ""},
		{"dictionary instead of peer list", "d8:intervali1800e5:peersdee", ""},
		{"non-dictionary peer entry", "d8:intervali1800e5:peersli1eee", ""},
		{"dictionary peer without port", "d8:intervali1800e5:peersld2:ip9:127.0.0.1eee", ""},
		{"dictionary peer with zero port", "d8:intervali1800e5:peersld2:ip9:127.0.0.14:porti0eeee", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("ParseTrackerResponse panicked instead of returning an error: %v", r)
				}
			}()
			_, err := parseTrackerResponse([]byte(tt.data))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tt.wantMessage) {
				t.Errorf("error %q should contain %q", err, tt.wantMessage)
			}
		})
	}
}

func TestGeneratePeerID(t *testing.T) {
	result := GeneratePeerID()

	for i, c := range result[:8] {
		if PREFIX[i] != c {
			t.Fatalf("Mismatch prefix at %d. Expected '%c' found '%c'", i, PREFIX[i], c)
		}
	}

	// Make sure PeerID isn't all zeros
	passing := false
	for _, c := range result[8:] {
		if c != 0 {
			passing = true
			break
		}
	}
	if !passing {
		t.Fatalf("PeerID fully zeroed")
	}

	// Check if other PeerIDs are randomly generated
	other := GeneratePeerID()
	passing = false
	for i, c := range result {
		if c != other[i] {
			passing = true
			break
		}
	}
	if !passing {
		t.Fatalf("Generation of PeerID not random")
	}
}

func TestBuildAnnounceURL(t *testing.T) {
	// Include bytes that expose hex encoding, double escaping, and UTF-8 conversion bugs.
	infoHash := [20]byte{0x00, 0xff, 0x80, ' ', '+', '&', '=', '%', '?', '#', '/', 1, 2, 3, 4, 5, 6, 7, 8, 9}
	peerID := [20]byte{'-', 'C', 'T', '0', '0', '0', '1', '-', 0xff, 0x00, ' ', '+', '&', '=', '%', '?', '#', '/', 0x80, 0x01}

	tests := []struct {
		name     string
		announce string
		port     uint16
		length   int64
		wantLeft string
		wantPort string
	}{
		{"HTTP", "http://tracker.example/announce", 6881, 10485760, "10485760", "6881"},
		{"HTTPS with existing query", "https://tracker.example:8443/private/announce?passkey=a%2Bb%26c&tag=one&tag=two&port=1&left=1&info_hash=old&peer_id=old&compact=0&event=stopped&uploaded=9&downloaded=9", 65535, 1 << 40, "1099511627776", "65535"},
		{"empty file and lowest port", "https://tracker.example/announce", 1, 0, "0", "1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := torrent.TorrentFile{Announce: tt.announce, InfoHash: infoHash, Length: tt.length}
			before := tf
			got, err := buildAnnounceURL(&tf, peerID, tt.port, AnnounceParams{Left: tt.length, Event: AnnounceEventStarted})
			if err != nil {
				t.Fatalf("BuildAnnounceURL() error = %v", err)
			}
			u, err := url.Parse(got)
			if err != nil {
				t.Fatalf("parse returned URL: %v", err)
			}
			base, err := url.Parse(tt.announce)
			if err != nil {
				t.Fatal(err)
			}
			if u.Scheme != base.Scheme || u.Host != base.Host || u.EscapedPath() != base.EscapedPath() {
				t.Errorf("tracker endpoint changed: got %q, base %q", got, tt.announce)
			}
			query, err := url.ParseQuery(u.RawQuery)
			if err != nil {
				t.Fatalf("parse returned query: %v", err)
			}
			want := map[string]string{
				"info_hash":  string(infoHash[:]),
				"peer_id":    string(peerID[:]),
				"port":       tt.wantPort,
				"uploaded":   "0",
				"downloaded": "0",
				"left":       tt.wantLeft,
				"compact":    "1",
				"event":      "started",
			}
			for key, value := range want {
				values := query[key]
				if len(values) != 1 || values[0] != value {
					t.Errorf("query[%q] = %q, want single value %q", key, values, value)
				}
			}
			for key, values := range base.Query() {
				if _, replaced := want[key]; replaced {
					continue
				}
				gotValues := query[key]
				if len(gotValues) != len(values) {
					t.Errorf("query[%q] = %q, want %q", key, gotValues, values)
					continue
				}
				for i, value := range values {
					if gotValues[i] != value {
						t.Errorf("query[%q][%d] = %q, want %q", key, i, gotValues[i], value)
					}
				}
			}
			if tf.Announce != before.Announce || tf.InfoHash != before.InfoHash || tf.Length != before.Length {
				t.Error("BuildAnnounceURL mutated its torrent input")
			}
		})
	}
}

func TestBuildAnnounceURLRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		announce string
		port     uint16
	}{
		{"malformed URL escape", "https://tracker.example/%zz", 6881},
		{"missing hostname", "https:///announce", 6881},
		{"relative URL", "/announce", 6881},
		{"UDP tracker", "udp://tracker.example:6969/announce", 6881},
		{"unsupported scheme", "ftp://tracker.example/announce", 6881},
		{"zero port", "https://tracker.example/announce", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := torrent.TorrentFile{Announce: tt.announce, Length: 1024}
			if got, err := buildAnnounceURL(&tf, [20]byte{}, tt.port, AnnounceParams{Left: tf.Length, Event: AnnounceEventStarted}); err == nil {
				t.Errorf("BuildAnnounceURL() = %q, nil; want error", got)
			}
		})
	}
}

func TestBuildAnnounceURLTransferState(t *testing.T) {
	for _, tt := range []struct {
		name                              string
		params                            AnnounceParams
		uploaded, downloaded, left, event string
	}{
		{"initial", AnnounceParams{Left: 1000, Event: AnnounceEventStarted}, "0", "0", "1000", "started"},
		{"periodic with retries", AnnounceParams{Uploaded: 65, Downloaded: 1200, Left: 500}, "65", "1200", "500", ""},
		{"completed", AnnounceParams{Downloaded: 1200, Event: AnnounceEventCompleted}, "0", "1200", "0", "completed"},
		{"stopped while partial", AnnounceParams{Uploaded: 10, Downloaded: 800, Left: 200, Event: AnnounceEventStopped}, "10", "800", "200", "stopped"},
		{"large counters", AnnounceParams{Uploaded: 1 << 40, Downloaded: 1<<40 + 1, Left: 1<<40 + 2}, "1099511627776", "1099511627777", "1099511627778", ""},
		{"maximum counters", AnnounceParams{Uploaded: 9223372036854775807, Downloaded: 9223372036854775807, Left: 9223372036854775807}, "9223372036854775807", "9223372036854775807", "9223372036854775807", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Announce: "https://tracker.example/announce?passkey=a%2Bb&event=started&event=stopped&uploaded=old&downloaded=old&left=old", Length: 1000}
			before := tt.params
			got, err := buildAnnounceURL(tf, [20]byte{}, 6881, tt.params)
			if err != nil {
				t.Fatal(err)
			}
			u, err := url.Parse(got)
			if err != nil {
				t.Fatal(err)
			}
			query, err := url.ParseQuery(u.RawQuery)
			if err != nil {
				t.Fatal(err)
			}
			for key, want := range map[string]string{"uploaded": tt.uploaded, "downloaded": tt.downloaded, "left": tt.left, "passkey": "a+b"} {
				if values := query[key]; len(values) != 1 || values[0] != want {
					t.Errorf("query[%q] = %q, want single decimal/value %q", key, values, want)
				}
			}
			if tt.event == "" {
				if values, exists := query["event"]; exists {
					t.Errorf("regular announce retained event: %q", values)
				}
			} else if values := query["event"]; len(values) != 1 || values[0] != tt.event {
				t.Errorf("event = %q, want single value %q", values, tt.event)
			}
			if tt.params != before {
				t.Error("builder changed its parameters")
			}
		})
	}
}

func TestBuildAnnounceURLRejectsInvalidTransferState(t *testing.T) {
	for _, tt := range []struct {
		name   string
		params AnnounceParams
	}{
		{"negative uploaded", AnnounceParams{Uploaded: -1}},
		{"negative downloaded", AnnounceParams{Downloaded: -1}},
		{"negative left", AnnounceParams{Left: -1}},
		{"unknown event", AnnounceParams{Event: AnnounceEvent("updated")}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Announce: "https://tracker.example/announce", Length: 1000}
			if got, err := buildAnnounceURL(tf, [20]byte{}, 6881, tt.params); err == nil {
				t.Errorf("accepted invalid state: %q", got)
			}
		})
	}
}
