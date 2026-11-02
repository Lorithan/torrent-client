package tracker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/lorithan/torrent-client/internal/torrent"
)

type Client struct {
	httpClient *http.Client
	key        uint32
	udpPolicy  udpRetryPolicy
}

func NewClient(httpClient *http.Client) *Client {
	key := make([]byte, 4)
	rand.Read(key)

	return &Client{
		httpClient: httpClient,
		key:        binary.BigEndian.Uint32(key),
		udpPolicy: udpRetryPolicy{
			initialTimeout: 15 * time.Second,
			maxAttempts:    3,
			connectionTTL:  time.Minute,
		},
	}
}

func (c *Client) Announce(
	ctx context.Context,
	tf *torrent.TorrentFile,
	peerID [20]byte,
	port uint16,
	params AnnounceParams,
) (*TrackerResponse, error) {
	cancelled := ctx.Err()
	if cancelled != nil {
		return nil, cancelled
	}

	err := validateAnnounceParams(port, params)
	if err != nil {
		return nil, err
	}

	parsed, err := url.Parse(tf.Announce)
	if err != nil {
		return nil, err
	}

	switch parsed.Scheme {
	case "http", "https":
		announceURL, err := buildAnnounceURL(tf, peerID, port, params)
		if err != nil {
			return nil, err
		}

		response, err := getAnnounceResponse(ctx, announceURL, c.httpClient)
		if err != nil {
			return nil, err
		}

		return parseTrackerResponse(response)
	case "udp":
		return c.announceUDP(ctx, tf, peerID, port, params)
	default:
		return nil, fmt.Errorf("unsupported url scheme: %s", parsed.Scheme)
	}
}
