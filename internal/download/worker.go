package download

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
)

type workerCommandKind uint8

const (
	commandDownload workerCommandKind = iota
	commandHave
	commandAvailability
)

type workerCommand struct {
	kind  workerCommandKind
	index int
}

type pieceResult struct {
	workerID  int
	index     int
	kind      workerCommandKind
	data      []byte
	available peer.Bitfield
	err       error
}

func (s *Session) runPeerWorker(
	ctx context.Context,
	workerID int,
	client *peer.Client,
	jobs <-chan workerCommand,
	results chan<- pieceResult,
) {
	defer client.Conn.Close()

	stopClient := context.AfterFunc(ctx, func() {
		client.Conn.Close()
	})

	defer stopClient()

	for {
		var job workerCommand
		select {
		case <-ctx.Done():
			return
		case newJob, ok := <-jobs:
			if !ok {
				return
			}
			job = newJob
		}

		if cancelled := ctx.Err(); cancelled != nil {
			return
		}

		result := pieceResult{
			workerID: workerID,
			index:    job.index,
			kind:     job.kind,
		}
		switch job.kind {
		case commandDownload:
			piece, err := s.DownloadPiece(client, job.index)
			result.data = piece
			result.err = err
		case commandHave:
			var err error
			if job.index >= len(s.torrent.Pieces) {
				err = fmt.Errorf("job index %d exeeds piece count", job.index)
			} else if job.index < 0 || job.index > math.MaxUint32 {
				err = fmt.Errorf("job index out of bounds")
			} else {
				err = sendHave(client, uint32(job.index))
			}

			result.err = err
		case commandAvailability:
			err := client.WaitForAvailabilityChange()
			if err != nil {
				result.err = &PeerError{
					Op:  "wait for availability",
					Err: err,
				}
			}
		default:
			result.err = fmt.Errorf("unsupported command type: %d", job.kind)
		}

		result.available = slices.Clone(client.Bitfield)

		select {
		case <-ctx.Done():
			return
		case results <- result:
		}

		if result.err != nil {
			return
		}
	}
}

func sendHave(client *peer.Client, index uint32) error {
	err := client.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		return &PeerError{
			Op:  fmt.Sprintf("set deadline to announce piece %d", index),
			Err: err,
		}
	}
	m := peer.NewHave(index)
	err = client.SendMessage(m)
	if err != nil {
		return &PeerError{
			Op:  fmt.Sprintf("announce piece %d", index),
			Err: err,
		}
	}
	err = client.Conn.SetDeadline(time.Time{})
	if err != nil {
		return &PeerError{
			Op:  fmt.Sprintf("clear deadline after announcing piece %d", index),
			Err: err,
		}
	}

	return nil
}
