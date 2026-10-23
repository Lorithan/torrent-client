package tracker

import (
	"net/url"
	"testing"

	"github.com/lorithan/torrent-client/internal/torrent"
)

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
			got, err := buildAnnounceURL(&tf, peerID, tt.port)
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
			if got, err := buildAnnounceURL(&tf, [20]byte{}, tt.port); err == nil {
				t.Errorf("BuildAnnounceURL() = %q, nil; want error", got)
			}
		})
	}
}
