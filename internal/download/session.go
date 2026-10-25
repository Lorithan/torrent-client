package download

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

type Session struct {
	torrent        *torrent.TorrentFile
	dest           io.WriterAt
	completed      peer.Bitfield
	completedCount int
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

	remainder := expectedPieceCount % 8

	// Each byte holds eight pieces; a partial group needs one additional byte.
	requiredBytes := expectedPieceCount / 8
	if remainder > 0 {
		requiredBytes += 1
	}

	completed := make(peer.Bitfield, requiredBytes)
	completedCount := 0

	return &Session{
		torrent:        tf,
		dest:           dst,
		completed:      completed,
		completedCount: completedCount,
	}, nil
}

func (s *Session) DownloadFrom(client *peer.Client) error {
	for s.completedCount < len(s.torrent.Pieces) {
		index, found := SelectPiece(client.Bitfield, s.completed, len(s.torrent.Pieces))
		if !found {
			err := client.Conn.SetDeadline(time.Now().Add(30 * time.Second))
			if err != nil {
				return err
			}
			for !found {
				message, err := peer.ReadMessage(client.Conn)
				if err != nil {
					return err
				}

				err = client.HandleMessage(message)
				if err != nil {
					return err
				}
				index, found = SelectPiece(client.Bitfield, s.completed, len(s.torrent.Pieces))
			}
			err = client.Conn.SetDeadline(time.Time{})
			if err != nil {
				return err
			}
		}

		piece, err := DownloadPiece(client, s.torrent, index)
		if err != nil {
			return err
		}

		n, err := s.dest.WriteAt(piece, int64(index)*s.torrent.PieceLength)
		if err != nil {
			return err
		}

		err = s.completed.SetPiece(index)
		if err != nil {
			return err
		}
		s.completedCount++

		message := peer.NewHave(uint32(index))
		err = client.Conn.SetDeadline(time.Now().Add(30 * time.Second))
		if err != nil {
			return err
		}
		err = client.SendMessage(message)
		if err != nil {
			return err
		}
		err = client.Conn.SetDeadline(time.Time{})
		if err != nil {
			return err
		}

		log.Printf("Piece %d completed, %d/%d, %d bytes written", index, s.completedCount, len(s.torrent.Pieces), n)
	}

	return nil
}
