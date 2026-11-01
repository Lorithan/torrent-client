package transfer

import "github.com/lorithan/torrent-client/internal/download"

type Phase string

const (
	PhaseStarting    Phase = "starting"
	PhaseDiscovering Phase = "discovering"
	PhaseConnecting  Phase = "connecting"
	PhaseDownloading Phase = "downloading"
	PhaseWaiting     Phase = "waiting"
	PhaseSeeding     Phase = "seeding"
	PhaseStopped     Phase = "stopped"
	PhaseFailed      Phase = "failed"
)

type Status struct {
	Phase        Phase
	Progress     download.Progress
	ActivePeers  int
	DownloadRate float64
	UploadRate   float64
	Err          error
}
