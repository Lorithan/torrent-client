package download

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

func Download(client *peer.Client, tf *torrent.TorrentFile, dst io.WriterAt) error {
	if tf.PieceLength < 1 {
		return fmt.Errorf("piece length needs to be greater than 0")
	}
	if tf.Length < 0 {
		return fmt.Errorf("file length cannot be negative")
	}

	expectedPieceCount := tf.Length / tf.PieceLength
	if tf.Length%tf.PieceLength != 0 {
		expectedPieceCount += 1
	}

	if expectedPieceCount != int64(len(tf.Pieces)) {
		return fmt.Errorf("mismatch piece count, expected %d found %d", expectedPieceCount, len(tf.Pieces))
	}

	remainder := expectedPieceCount % 8

	// Each byte holds eight pieces; a partial group needs one additional byte.
	requiredBytes := expectedPieceCount / 8
	if remainder > 0 {
		requiredBytes += 1
	}

	completed := make(peer.Bitfield, requiredBytes)
	completedCount := 0

	for completedCount < len(tf.Pieces) {
		index, found := SelectPiece(client.Bitfield, completed, len(tf.Pieces))
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
				index, found = SelectPiece(client.Bitfield, completed, len(tf.Pieces))
			}
			err = client.Conn.SetDeadline(time.Time{})
			if err != nil {
				return err
			}
		}

		piece, err := DownloadPiece(client, tf, index)
		if err != nil {
			return err
		}

		n, err := dst.WriteAt(piece, int64(index)*tf.PieceLength)
		if err != nil {
			return err
		}

		err = completed.SetPiece(index)
		if err != nil {
			return err
		}
		completedCount++

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

		log.Printf("Piece %d completed, %d/%d, %d bytes written", index, completedCount, expectedPieceCount, n)
	}

	return nil
}
