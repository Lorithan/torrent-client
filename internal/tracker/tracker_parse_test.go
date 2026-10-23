package tracker

import (
	"fmt"
	"strings"
	"testing"
	"time"
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
