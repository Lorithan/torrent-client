package download

import (
	"fmt"
	"io"

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

	for index := range len(tf.Pieces) {
		piece, err := DownloadPiece(client, tf, index)
		if err != nil {
			return err
		}

		_, err = dst.WriteAt(piece, int64(index)*tf.PieceLength)
		if err != nil {
			return err
		}
	}

	return nil
}
