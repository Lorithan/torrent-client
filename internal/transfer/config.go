package transfer

import "time"

type Config struct {
	TorrentPath     string
	OutputPath      string
	Port            uint16
	Workers         int
	Seed            bool
	PeerWaitTimeout time.Duration
}
