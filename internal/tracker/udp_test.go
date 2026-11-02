package tracker

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"testing"
	"time"
)

func TestBuildUDPConnectRequest(t *testing.T) {
	for _, tt := range []struct {
		name          string
		transactionID uint32
		wantHex       string
	}{
		{"zero transaction", 0, "00000417271019800000000000000000"},
		{"distinct transaction bytes", 0x12345678, "00000417271019800000000012345678"},
		{"maximum transaction", 0xffffffff, "000004172710198000000000ffffffff"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Literal wire bytes independently check the protocol ID, connect
			// action, field offsets, packet length, and transaction byte order.
			want, err := hex.DecodeString(tt.wantHex)
			if err != nil {
				t.Fatal(err)
			}
			got := buildUDPConnectRequest(tt.transactionID)
			if !bytes.Equal(got, want) {
				t.Fatalf("packet = %x (%d bytes), want %x (%d bytes)", got, len(got), want, len(want))
			}
		})
	}
}

func TestUDPEventCode(t *testing.T) {
	// These are wire-protocol values, independent of declaration order in Go.
	for _, tt := range []struct {
		event AnnounceEvent
		want  uint32
	}{
		{AnnounceEventDefault, 0},
		{AnnounceEventCompleted, 1},
		{AnnounceEventStarted, 2},
		{AnnounceEventStopped, 3},
	} {
		t.Run(fmt.Sprintf("event_%q", tt.event), func(t *testing.T) {
			got, err := udpEventCode(tt.event)
			if err != nil || got != tt.want {
				t.Fatalf("udpEventCode(%q) = %d, %v; want %d, nil", tt.event, got, err, tt.want)
			}
		})
	}
	for _, event := range []AnnounceEvent{"unknown", "Started", "1"} {
		t.Run(fmt.Sprintf("reject_%q", event), func(t *testing.T) {
			if _, err := udpEventCode(event); err == nil {
				t.Fatalf("accepted unsupported event %q", event)
			}
		})
	}
}

func TestParseUDPConnectResponse(t *testing.T) {
	for _, tt := range []struct {
		name          string
		packetHex     string
		message       string
		wantID        uint64
		wantError     bool
		wantErrorText string
	}{
		{name: "connect", packetHex: "00000000123456781122334455667788", wantID: 0x1122334455667788},
		{name: "zero connection ID", packetHex: "00000000123456780000000000000000"},
		{name: "maximum connection ID", packetHex: "0000000012345678ffffffffffffffff", wantID: 0xffffffffffffffff},
		{name: "extension bytes", packetHex: "00000000123456781122334455667788abcdef", wantID: 0x1122334455667788},
		{name: "wrong transaction", packetHex: "00000000123456791122334455667788", wantError: true},
		{name: "unexpected action", packetHex: "0000000112345678", wantError: true},
		{name: "tracker error without message", packetHex: "0000000312345678", wantError: true},
		{name: "short tracker error", packetHex: "0000000312345678", message: "denied", wantError: true, wantErrorText: "denied"},
		{name: "long tracker error", packetHex: "0000000312345678", message: "tracker denied the request", wantError: true, wantErrorText: "tracker denied the request"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if value := recover(); value != nil {
					t.Fatalf("parser panicked: %v", value)
				}
			}()
			packet, err := hex.DecodeString(tt.packetHex)
			if err != nil {
				t.Fatal(err)
			}
			packet = append(packet, tt.message...)
			got, err := parseUDPConnectResponse(packet, 0x12345678)
			if tt.wantError {
				if err == nil {
					t.Fatalf("accepted invalid response with connection ID %x", got)
				}
				if !strings.Contains(err.Error(), tt.wantErrorText) {
					t.Errorf("error %q does not contain tracker message %q", err, tt.wantErrorText)
				}
				return
			}
			if err != nil || got != tt.wantID {
				t.Fatalf("connection ID = %x, error = %v; want %x, nil", got, err, tt.wantID)
			}
		})
	}
}

func TestParseUDPConnectResponseRejectsTruncation(t *testing.T) {
	packet, err := hex.DecodeString("00000000123456781122334455667788")
	if err != nil {
		t.Fatal(err)
	}
	for length := 0; length < len(packet); length++ {
		t.Run(fmt.Sprintf("length_%d", length), func(t *testing.T) {
			if _, err := parseUDPConnectResponse(packet[:length], 0x12345678); err == nil {
				t.Fatal("accepted truncated connect response")
			}
		})
	}
}

func TestBuildUDPAnnounceRequestWireFormat(t *testing.T) {
	infoHash := [20]byte{0x00, 0xff, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}
	peerID := [20]byte{0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f, 0x90, 0x91, 0x92, 0x93}
	for _, tt := range []struct {
		event   AnnounceEvent
		codeHex string
	}{
		{AnnounceEventDefault, "00000000"},
		{AnnounceEventCompleted, "00000001"},
		{AnnounceEventStarted, "00000002"},
		{AnnounceEventStopped, "00000003"},
	} {
		t.Run(fmt.Sprintf("event_%q", tt.event), func(t *testing.T) {
			params := AnnounceParams{
				Downloaded: 0x0102030405060708,
				Left:       0x1112131415161718,
				Uploaded:   0x2122232425262728,
				Event:      tt.event,
			}
			got, err := buildUDPAnnounceRequest(infoHash, peerID, 0xbeef, params, 0x1122334455667788, 0xa1b2c3d4, 0x55667788)
			if err != nil {
				t.Fatal(err)
			}
			// Independent wire fixture: binary identities, distinct 64-bit
			// counters, and distinct identifiers expose encoding/order mistakes.
			want, err := hex.DecodeString(
				"1122334455667788" + "00000001" + "a1b2c3d4" +
					"00ff0102030405060708090a0b0c0d0e0f101112" +
					"808182838485868788898a8b8c8d8e8f90919293" +
					"0102030405060708" + "1112131415161718" + "2122232425262728" +
					tt.codeHex + "00000000" + "55667788" + "ffffffff" + "beef")
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 98 || !bytes.Equal(got, want) {
				t.Fatalf("packet = %x (%d bytes), want %x (%d bytes)", got, len(got), want, len(want))
			}
		})
	}
}

func TestBuildUDPAnnounceRequestRejectsInvalidInput(t *testing.T) {
	for _, tt := range []struct {
		name   string
		port   uint16
		params AnnounceParams
	}{
		{name: "zero port"},
		{name: "negative downloaded", port: 6881, params: AnnounceParams{Downloaded: -1}},
		{name: "negative left", port: 6881, params: AnnounceParams{Left: -1}},
		{name: "negative uploaded", port: 6881, params: AnnounceParams{Uploaded: -1}},
		{name: "invalid event", port: 6881, params: AnnounceParams{Event: "unknown"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			packet, err := buildUDPAnnounceRequest([20]byte{}, [20]byte{}, tt.port, tt.params, 0, 0, 0)
			if err == nil {
				t.Fatal("accepted invalid announce input")
			}
			if len(packet) != 0 {
				t.Errorf("returned %d packet bytes on error", len(packet))
			}
		})
	}
}

func TestBuildUDPAnnounceRequestCounterLimits(t *testing.T) {
	for _, tt := range []struct {
		name    string
		counter int64
		port    uint16
		wantHex string
	}{
		{"zero", 0, 1, "0000000000000000"},
		{"maximum", math.MaxInt64, math.MaxUint16, "7fffffffffffffff"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			params := AnnounceParams{Downloaded: tt.counter, Left: tt.counter, Uploaded: tt.counter}
			packet, err := buildUDPAnnounceRequest([20]byte{}, [20]byte{}, tt.port, params, math.MaxUint64, math.MaxUint32, math.MaxUint32)
			if err != nil {
				t.Fatal(err)
			}
			if len(packet) != 98 {
				t.Fatalf("packet length = %d, want 98", len(packet))
			}
			if got := hex.EncodeToString(packet[56:80]); got != strings.Repeat(tt.wantHex, 3) {
				t.Errorf("counter bytes = %s, want %s", got, strings.Repeat(tt.wantHex, 3))
			}
			if got := hex.EncodeToString(packet[96:98]); got != fmt.Sprintf("%04x", tt.port) {
				t.Errorf("port bytes = %s, want %04x", got, tt.port)
			}
		})
	}
}

func TestParseUDPAnnounceResponse(t *testing.T) {
	for _, tt := range []struct {
		name          string
		packetHex     string
		message       string
		wantInterval  time.Duration
		wantPeers     []string
		wantError     bool
		wantErrorText string
	}{
		{name: "no peers", packetHex: "00000001123456780000001e0000000200000003", wantInterval: 30 * time.Second},
		{name: "two peers", packetHex: "00000001123456780000001e00000002000000037f0000011ae1c0000201ffff", wantInterval: 30 * time.Second, wantPeers: []string{"127.0.0.1:6881", "192.0.2.1:65535"}},
		{name: "maximum interval", packetHex: "0000000112345678ffffffff0000000000000000", wantInterval: time.Duration(math.MaxUint32) * time.Second},
		{name: "wrong transaction", packetHex: "00000001123456790000001e0000000000000000", wantError: true},
		{name: "wrong transaction in error", packetHex: "0000000312345679", message: "denied", wantError: true, wantErrorText: "mismatch"},
		{name: "unexpected action", packetHex: "0000000012345678", wantError: true},
		{name: "empty tracker error", packetHex: "0000000312345678", wantError: true},
		{name: "short tracker error", packetHex: "0000000312345678", message: "denied", wantError: true, wantErrorText: "denied"},
		{name: "zero interval", packetHex: "0000000112345678000000000000000000000000", wantError: true},
		{name: "incomplete peer", packetHex: "00000001123456780000001e000000000000000000", wantError: true},
		{name: "zero peer port", packetHex: "00000001123456780000001e0000000000000000c00002010000", wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if value := recover(); value != nil {
					t.Fatalf("parser panicked: %v", value)
				}
			}()
			packet, err := hex.DecodeString(tt.packetHex)
			if err != nil {
				t.Fatal(err)
			}
			packet = append(packet, tt.message...)
			got, err := parseUDPAnnounceResponse(packet, 0x12345678)
			if tt.wantError {
				if err == nil {
					t.Fatalf("accepted invalid response: %+v", got)
				}
				if !strings.Contains(err.Error(), tt.wantErrorText) {
					t.Errorf("error %q does not contain %q", err, tt.wantErrorText)
				}
				return
			}
			if err != nil || got == nil {
				t.Fatalf("response = %+v, error = %v", got, err)
			}
			if got.Interval != tt.wantInterval || len(got.Peers) != len(tt.wantPeers) {
				t.Fatalf("response = %+v; want interval %v and %d peers", got, tt.wantInterval, len(tt.wantPeers))
			}
			for i, want := range tt.wantPeers {
				if addr := got.Peers[i].String(); addr != want {
					t.Errorf("peer %d = %s, want %s", i, addr, want)
				}
			}
		})
	}
}

func TestParseUDPAnnounceResponseRejectsTruncation(t *testing.T) {
	packet, err := hex.DecodeString("00000001123456780000001e0000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	for length := 0; length < len(packet); length++ {
		t.Run(fmt.Sprintf("length_%d", length), func(t *testing.T) {
			defer func() {
				if value := recover(); value != nil {
					t.Fatalf("truncated input caused panic: %v", value)
				}
			}()
			if _, err := parseUDPAnnounceResponse(packet[:length:length], 0x12345678); err == nil {
				t.Fatal("accepted truncated announce response")
			}
		})
	}
}

func udpResponseTestPair(t *testing.T) (*net.UDPConn, *net.UDPConn) {
	t.Helper()
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	client, err := net.DialUDP("udp4", nil, server.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	if err := client.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := server.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return client, server
}

func TestReadUDPResponseIgnoresShortPackets(t *testing.T) {
	for length := 0; length < 8; length++ {
		t.Run(fmt.Sprintf("length_%d", length), func(t *testing.T) {
			client, server := udpResponseTestPair(t)
			want := []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}
			for _, packet := range [][]byte{make([]byte, length), want} {
				if _, err := server.WriteToUDP(packet, client.LocalAddr().(*net.UDPAddr)); err != nil {
					t.Fatal(err)
				}
			}
			got, err := readUDPResponse(client, 0)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("response = %x, error = %v; want %x, nil", got, err, want)
			}
		})
	}
}

func TestReadUDPResponseDoesNotUseStaleTransactionBytes(t *testing.T) {
	client, server := udpResponseTestPair(t)
	packets := []string{
		"0000000000345678aabbccddeeff0011", // Wrong transaction, but matching final byte.
		"00000000123456",                   // Only three transaction bytes: must be ignored.
		"00000000123456781122334455667788", // Complete matching packet.
	}
	var want []byte
	for _, packetHex := range packets {
		packet, err := hex.DecodeString(packetHex)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := server.WriteToUDP(packet, client.LocalAddr().(*net.UDPAddr)); err != nil {
			t.Fatal(err)
		}
		want = packet
	}
	got, err := readUDPResponse(client, 0x12345678)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("response = %x, error = %v; want %x, nil", got, err, want)
	}
}

func TestReadUDPResponseReturnsTrackerErrorPacket(t *testing.T) {
	client, server := udpResponseTestPair(t)
	want := []byte{0, 0, 0, 3, 0x12, 0x34, 0x56, 0x78, 'n', 'o'}
	if _, err := server.WriteToUDP(want, client.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	got, err := readUDPResponse(client, 0x12345678)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("response = %x, error = %v; want tracker error packet %x, nil", got, err, want)
	}
}

func TestReadUDPResponseReturnsReadErrors(t *testing.T) {
	t.Run("caller deadline", func(t *testing.T) {
		client, _ := udpResponseTestPair(t)
		if err := client.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		_, err := readUDPResponse(client, 0x12345678)
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("error = %v, want timeout from caller's deadline", err)
		}
	})
	t.Run("closed socket", func(t *testing.T) {
		client, _ := udpResponseTestPair(t)
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := readUDPResponse(client, 0); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("error = %v, want net.ErrClosed", err)
		}
	})
}

func TestExchangeUDPSendsRequestAndKeepsSocketOpen(t *testing.T) {
	client, server := udpResponseTestPair(t)
	// An earlier attempt's expired deadline must not prevent the next attempt.
	if err := client.SetDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, action := range []byte{0, 3} {
		request := []byte{0, 0, 4, 0x17, 0x27, 0x10, 0x19, 0x80, 0, 0, 0, 0, 0x12, 0x34, 0x56, 0x78}
		response := []byte{0, 0, 0, action, 0x12, 0x34, 0x56, 0x78, 1, 2, 3, 4, 5, 6, 7, 8}
		// Queue a reply so this test can inspect both directions without an
		// additional goroutine. Protocol parsing belongs to separate tests.
		if _, err := server.WriteToUDP(response, client.LocalAddr().(*net.UDPAddr)); err != nil {
			t.Fatal(err)
		}
		got, err := exchangeUDP(client, request, 0x12345678, time.Second)
		if err != nil || !bytes.Equal(got, response) {
			t.Fatalf("action %d: response = %x, error = %v; want %x, nil", action, got, err, response)
		}
		buffer := make([]byte, len(request)+1)
		n, err := server.Read(buffer)
		if err != nil || !bytes.Equal(buffer[:n], request) {
			t.Fatalf("request datagram = %x, error = %v; want %x, nil", buffer[:n], err, request)
		}
	}
}

func TestExchangeUDPRejectsNonpositiveTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			client, _ := udpResponseTestPair(t)
			if _, err := exchangeUDP(client, []byte("request"), 1, timeout); err == nil {
				t.Fatal("accepted nonpositive timeout")
			}
		})
	}
}

func TestExchangeUDPTimesOutWithoutMatchingReply(t *testing.T) {
	client, server := udpResponseTestPair(t)
	for _, unrelated := range [][]byte{{0}, {0, 0, 0, 0, 0x12, 0x34, 0x56, 0x79}} {
		if _, err := server.WriteToUDP(unrelated, client.LocalAddr().(*net.UDPAddr)); err != nil {
			t.Fatal(err)
		}
	}
	_, err := exchangeUDP(client, []byte("request"), 0x12345678, 50*time.Millisecond)
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("error = %v, want timeout while waiting for a matching response", err)
	}
	var packet [64]byte
	n, err := server.Read(packet[:])
	if err != nil || string(packet[:n]) != "request" {
		t.Fatalf("request = %q, error = %v", packet[:n], err)
	}
}

func TestExchangeUDPReturnsDeadlineAndWriteErrors(t *testing.T) {
	t.Run("closed socket", func(t *testing.T) {
		client, _ := udpResponseTestPair(t)
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := exchangeUDP(client, []byte("request"), 1, time.Second); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("error = %v, want net.ErrClosed", err)
		}
	})
	t.Run("datagram too large", func(t *testing.T) {
		client, _ := udpResponseTestPair(t)
		// IPv4 UDP payloads cannot exceed 65,507 bytes.
		_, err := exchangeUDP(client, make([]byte, 65508), 1, time.Second)
		var opErr *net.OpError
		if !errors.As(err, &opErr) || opErr.Op != "write" {
			t.Fatalf("error = %v, want UDP write error", err)
		}
	})
}

func TestConnectUDP(t *testing.T) {
	for _, tt := range []struct {
		name          string
		replies       []string
		wantID        uint64
		wantError     bool
		wantErrorText string
		wantTimeout   bool
	}{
		{name: "connect", replies: []string{"00000000123456781122334455667788"}, wantID: 0x1122334455667788},
		{name: "ignore unrelated transaction", replies: []string{"00000000123456790000000000000000", "00000000123456781122334455667788"}, wantID: 0x1122334455667788},
		{name: "tracker error", replies: []string{"000000031234567864656e696564"}, wantError: true, wantErrorText: "denied"},
		{name: "truncated connect", replies: []string{"0000000012345678"}, wantError: true},
		{name: "unexpected action", replies: []string{"0000000112345678"}, wantError: true},
		{name: "dropped response", wantError: true, wantTimeout: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, server := udpResponseTestPair(t)
			// Prepare immutable fixtures before starting the tracker goroutine.
			wantRequest, err := hex.DecodeString("00000417271019800000000012345678")
			if err != nil {
				t.Fatal(err)
			}
			var replies [][]byte
			for _, replyHex := range tt.replies {
				packet, err := hex.DecodeString(replyHex)
				if err != nil {
					t.Fatal(err)
				}
				replies = append(replies, packet)
			}
			done := make(chan error, 1)
			go func() {
				defer close(done)
				var packet [64]byte
				n, addr, err := server.ReadFromUDP(packet[:])
				if err != nil {
					done <- err
					return
				}
				if !bytes.Equal(packet[:n], wantRequest) {
					done <- fmt.Errorf("connect request = %x, want %x", packet[:n], wantRequest)
					return
				}
				for _, reply := range replies {
					if _, err := server.WriteToUDP(reply, addr); err != nil {
						done <- err
						return
					}
				}
			}()
			t.Cleanup(func() {
				server.Close()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("fake UDP tracker did not stop")
				}
			})
			timeout := time.Second
			if tt.wantTimeout {
				timeout = 50 * time.Millisecond
			}
			got, err := connectUDP(client, 0x12345678, timeout)
			if tt.wantError {
				if err == nil {
					t.Fatalf("connection ID = %x, want an error", got)
				}
				if !strings.Contains(err.Error(), tt.wantErrorText) {
					t.Errorf("error %q does not contain %q", err, tt.wantErrorText)
				}
				if tt.wantTimeout {
					var netErr net.Error
					if !errors.As(err, &netErr) || !netErr.Timeout() {
						t.Errorf("error = %v, want timeout", err)
					}
				}
			} else if err != nil || got != tt.wantID {
				t.Errorf("connection ID = %x, error = %v; want %x, nil", got, err, tt.wantID)
			}
			if err := <-done; err != nil {
				t.Errorf("fake UDP tracker: %v", err)
			}
		})
	}
}
