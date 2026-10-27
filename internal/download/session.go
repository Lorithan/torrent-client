package download

import (
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

// Session retains progress across sequential peer attempts. Stats may be called
// during a transfer; other session operations still require a single owner.
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
