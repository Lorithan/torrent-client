package download

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

type Stats struct {
	Uploaded   int64
	Downloaded int64
	Left       int64
}

// Session retains progress across peer attempts and worker groups. Stats may be
// called during a transfer; completion tracking and output have a single owner.
type Session struct {
	torrent        *torrent.TorrentFile
	dest           io.WriterAt
	completed      peer.Bitfield
	completedCount int
	stats          Stats
	statsMu        sync.Mutex // Protects stats, not the complete session state.
}

func NewSession(tf *torrent.TorrentFile, dst io.WriterAt) (*Session, error) {
	if tf.PieceLength < 1 {
		return nil, fmt.Errorf("piece length needs to be greater than 0")
	}
	if tf.Length < 0 {
		return nil, fmt.Errorf("file length cannot be negative")
	}

	expectedPieceCount := tf.Length / tf.PieceLength
	if tf.Length%tf.PieceLength != 0 {
		expectedPieceCount += 1
	}

	if expectedPieceCount != int64(len(tf.Pieces)) {
		return nil, fmt.Errorf("mismatch piece count, expected %d found %d", expectedPieceCount, len(tf.Pieces))
	}

	completed := peer.NewBitfield(int(expectedPieceCount))
	completedCount := 0

	stats := Stats{
		Left: tf.Length,
	}

	return &Session{
		torrent:        tf,
		dest:           dst,
		completed:      completed,
		completedCount: completedCount,
		stats:          stats,
	}, nil
}

func (s *Session) DownloadFrom(client *peer.Client) error {
	for s.completedCount < len(s.torrent.Pieces) {
		index, found := SelectPiece(client.Bitfield, s.completed, len(s.torrent.Pieces))
		if !found {
			err := client.Conn.SetDeadline(time.Now().Add(30 * time.Second))
			if err != nil {
				return &PeerError{
					Op:  "set availability deadline",
					Err: err,
				}
			}
			for !found {
				message, err := peer.ReadMessage(client.Conn)
				if err != nil {
					return &PeerError{
						Op:  "wait for available piece",
						Err: err,
					}
				}

				err = client.HandleMessage(message)
				if err != nil {
					return &PeerError{
						Op:  "handle availability message",
						Err: err,
					}
				}
				index, found = SelectPiece(client.Bitfield, s.completed, len(s.torrent.Pieces))
			}
			err = client.Conn.SetDeadline(time.Time{})
			if err != nil {
				return &PeerError{
					Op:  "clear availability deadline",
					Err: err,
				}
			}
		}

		piece, err := s.DownloadPiece(client, index)
		if err != nil {
			return err
		}

		err = s.storePiece(index, piece)
		if err != nil {
			return err
		}

		err = s.completed.SetPiece(index)
		if err != nil {
			return err
		}
		s.completedCount++

		s.statsMu.Lock()
		s.stats.Left -= int64(len(piece))
		s.statsMu.Unlock()

		err = sendHave(client, uint32(index))
		if err != nil {
			return err
		}

		log.Printf("Piece %d completed, %d/%d, %d bytes written", index, s.completedCount, len(s.torrent.Pieces), len(piece))
	}

	return nil
}

// DownloadFromPeers runs one bounded batch of already-connected peers. It takes
// ownership of every supplied connection, including on validation errors, and
// cancels and joins all started workers before returning. Callers must not run
// DownloadFrom or DownloadFromPeers concurrently on the same session.
func (s *Session) DownloadFromPeers(
	ctx context.Context,
	clients []*peer.Client,
	workerLimit int,
) error {
	coordinator := newCoordinatorForSession(s)

	childCtx, cancel := context.WithCancel(ctx)

	defer func() {
		cancel()

		for _, client := range clients {
			if client != nil && client.Conn != nil {
				client.Conn.Close()
			}
		}

		coordinator.workerWG.Wait()
	}()

	if workerLimit < 1 {
		return fmt.Errorf("worker limit must be positive")
	}

	if len(clients) > workerLimit {
		return fmt.Errorf("more clients than workers")
	}

	seen := make(map[*peer.Client]struct{})
	for _, client := range clients {
		if client == nil || client.Conn == nil {
			return fmt.Errorf("uninitialized client connection")
		}

		if _, exists := seen[client]; exists {
			return fmt.Errorf("duplicate client connection")
		}

		if client.PieceCount != len(s.torrent.Pieces) {
			return fmt.Errorf("mismatch piece count.")
		}

		seen[client] = struct{}{}
	}

	if cancelled := childCtx.Err(); cancelled != nil {
		return cancelled
	}

	if s.Complete() {
		return nil
	}

	for index, client := range clients {
		err := coordinator.startWorker(childCtx, s, index, client)
		if err != nil {
			return err
		}
	}

	return coordinator.run(childCtx, s)
}

func (s *Session) Complete() bool {
	return s.completedCount == len(s.torrent.Pieces)
}

// Stats returns an independent snapshot without holding the lock after return.
func (s *Session) Stats() Stats {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	return s.stats
}

func (s *Session) storePiece(index int, data []byte) error {
	expectedSize, err := PieceSize(s.torrent, index)
	if err != nil {
		return err
	}
	if expectedSize != int64(len(data)) {
		return fmt.Errorf("data size mismatch. expected %d bytes, found %d bytes", expectedSize, len(data))
	}

	n, err := s.dest.WriteAt(data, int64(index)*s.torrent.PieceLength)
	if err != nil {
		return fmt.Errorf("write error for piece %d: %w", index, err)
	}
	if n != int(expectedSize) {
		return fmt.Errorf("write error for piece %d: %w", index, io.ErrShortWrite)
	}

	return nil
}
