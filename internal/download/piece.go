package download

import (
	"crypto/sha1"
	"fmt"
	"math"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

const maxBlockLength = 16 * 1024

func PieceSize(tf *torrent.TorrentFile, index int) (int64, error) {
	if index < 0 {
		return 0, fmt.Errorf("piece index out of range")
	}
	if index >= len(tf.Pieces) {
		return 0, fmt.Errorf("piece index out of range")
	}
	if tf.PieceLength <= 0 {
		return 0, fmt.Errorf("piece length must be positive")
	}
	if tf.Length < 0 {
		return 0, fmt.Errorf("file size cannot be negative")
	}

	if int64(index) > (math.MaxInt64 / tf.PieceLength) {
		return 0, fmt.Errorf("piece index out of range")
	}

	pieceStart := int64(index) * tf.PieceLength
	remaining := tf.Length - pieceStart

	if pieceStart >= tf.Length {
		return 0, fmt.Errorf("piece index out of range")
	}

	return min(remaining, tf.PieceLength), nil
}

func SelectPiece(available, completed peer.Bitfield, pieceCount int) (int, bool) {
	for index := range pieceCount {
		if available.HasPiece(index) && !completed.HasPiece(index) {
			return index, true
		}
	}

	return 0, false
}

func (s *Session) DownloadPiece(client *peer.Client, index int) ([]byte, error) {
	pieceSize, err := PieceSize(s.torrent, index)
	if err != nil {
		return nil, err
	}

	if index > math.MaxUint32 {
		return nil, fmt.Errorf("index out of range")
	}

	err = client.WaitForPiece(index)
	if err != nil {
		return nil, &PeerError{
			Op:  fmt.Sprintf("wait for piece %d", index),
			Err: err,
		}
	}

	piece := make([]byte, 0, pieceSize)

	var offset int64
	for offset < pieceSize {
		if offset > math.MaxUint32 {
			return nil, fmt.Errorf("offset out of range")
		}

		length := min(maxBlockLength, pieceSize-offset)

		block, err := client.DownloadBlock(uint32(index), uint32(offset), uint32(length))
		if err != nil {
			return nil, &PeerError{
				Op:  fmt.Sprintf("download piece %d block at offset %d", index, offset),
				Err: err,
			}
		}

		s.stateMu.Lock()
		s.stats.Downloaded += int64(len(block))
		s.stateMu.Unlock()

		piece = append(piece, block...)
		offset += int64(len(block))
	}

	hash := sha1.Sum(piece)

	if hash != s.torrent.Pieces[index] {
		return nil, &PeerError{
			Op:  fmt.Sprintf("verify piece %d", index),
			Err: fmt.Errorf("piece %d hash does not match expected hash", index),
		}
	}

	return piece, nil
}
