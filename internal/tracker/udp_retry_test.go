package tracker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/torrent"
)

func TestClientAnnounceUDPRetries(t *testing.T) {
	type step struct {
		action         uint32
		connectionID   uint64
		replyID        uint64
		reply          bool
		repeatPrevious bool
	}
	for _, tt := range []struct {
		name        string
		ttl         time.Duration
		steps       []step
		wantTimeout bool
	}{
		{
			name: "lost connect reply retains transaction",
			ttl:  time.Minute,
			steps: []step{
				{action: 0},
				{action: 0, reply: true, repeatPrevious: true},
				{action: 1, reply: true},
			},
		},
		{
			name: "lost announce reply reuses zero connection ID",
			ttl:  time.Minute,
			steps: []step{
				{action: 0, reply: true},
				{action: 1},
				{action: 1, reply: true, repeatPrevious: true},
			},
		},
		{
			name: "expired connection is renewed",
			ttl:  20 * time.Millisecond,
			steps: []step{
				{action: 0, reply: true, replyID: 0x1122334455667788},
				{action: 1, connectionID: 0x1122334455667788},
				{action: 0, reply: true, replyID: 0x8877665544332211},
				{action: 1, connectionID: 0x8877665544332211, reply: true},
			},
		},
		{
			name: "connect exhaustion preserves timeout",
			ttl:  time.Minute,
			steps: []step{
				{action: 0},
				{action: 0, repeatPrevious: true},
				{action: 0, repeatPrevious: true},
			},
			wantTimeout: true,
		},
		{
			name: "announce exhaustion preserves timeout",
			ttl:  time.Minute,
			steps: []step{
				{action: 0, reply: true},
				{action: 1},
				{action: 1, repeatPrevious: true},
				{action: 1, repeatPrevious: true},
			},
			wantTimeout: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, server := udpResponseTestPair(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			t.Cleanup(cancel)
			client := NewClient(http.DefaultClient)
			client.udpPolicy = udpRetryPolicy{initialTimeout: 50 * time.Millisecond, maxAttempts: 3, connectionTTL: tt.ttl}
			done := make(chan error, 1)
			go func() {
				defer close(done)
				done <- func() error {
					var previous []byte
					var firstAddr string
					for i, expected := range tt.steps {
						var buffer [256]byte
						n, addr, err := server.ReadFromUDP(buffer[:])
						if err != nil {
							return fmt.Errorf("step %d: %w", i, err)
						}
						packet := buffer[:n]
						if n < 16 || binary.BigEndian.Uint32(packet[8:12]) != expected.action {
							return fmt.Errorf("step %d: request %x, want action %d", i, packet, expected.action)
						}
						if firstAddr == "" {
							firstAddr = addr.String()
						} else if addr.String() != firstAddr {
							return fmt.Errorf("step %d: retry changed sockets", i)
						}
						if expected.repeatPrevious && !bytes.Equal(packet, previous) {
							return fmt.Errorf("step %d: retransmission changed request bytes: %x versus %x", i, packet, previous)
						}
						if expected.action == 1 && (n != 98 || binary.BigEndian.Uint64(packet[:8]) != expected.connectionID || binary.BigEndian.Uint32(packet[88:92]) != client.key) {
							return fmt.Errorf("step %d: announce used wrong connection ID or session key: %x", i, packet)
						}
						previous = append([]byte(nil), packet...)
						if !expected.reply {
							continue
						}
						reply := binary.BigEndian.AppendUint32(nil, expected.action)
						reply = append(reply, packet[12:16]...)
						if expected.action == 0 {
							reply = binary.BigEndian.AppendUint64(reply, expected.replyID)
						} else {
							reply = append(reply, 0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 0)
						}
						if _, err := server.WriteToUDP(reply, addr); err != nil {
							return err
						}
					}
					return nil
				}()
			}()
			t.Cleanup(func() {
				cancel()
				server.Close()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("fake retry tracker did not stop")
				}
			})
			tf := &torrent.TorrentFile{Announce: "udp://" + server.LocalAddr().String() + "/announce"}
			response, err := client.Announce(ctx, tf, [20]byte{}, 6881, AnnounceParams{})
			if tt.wantTimeout {
				var netErr net.Error
				if !errors.As(err, &netErr) || !netErr.Timeout() || errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("error = %v; want wrapped exchange timeout before the context backstop", err)
				}
			} else if err != nil || response == nil || response.Interval != 30*time.Second {
				t.Errorf("response = %+v, error = %v; want successful retry", response, err)
			}
			if trackerErr := <-done; trackerErr != nil {
				t.Errorf("fake retry tracker: %v", trackerErr)
			}
		})
	}
}
