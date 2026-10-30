package download

import (
	"fmt"
	"io"

	"github.com/lorithan/torrent-client/internal/peer"
)

type Storage interface {
	io.ReaderAt
	io.WriterAt
}

func (s *Session) ReadBlock(request peer.BlockRequest) ([]byte, error) {
	if uint64(request.Index) >= uint64(len(s.torrent.Pieces)) {
		return nil, fmt.Errorf("index out of bounds")
	}
	index := int(request.Index)

	if request.Length < 1 || request.Length > maxBlockLength {
		return nil, fmt.Errorf("length out of bounds")
	}

	pieceSize, err := PieceSize(s.torrent, index)
	if err != nil {
		return nil, err
	}
	if int64(request.Begin)+int64(request.Length) > pieceSize {
		return nil, fmt.Errorf("block exceeds bounds of piece size")
	}

	if !s.HasPiece(index) {
		return nil, fmt.Errorf("piece %d is not available", index)
	}

	data := make([]byte, request.Length)

	n, err := s.dest.ReadAt(data, int64(index)*s.torrent.PieceLength+int64(request.Begin))
	if err != nil {
		return nil, fmt.Errorf("storage read error for piece %d offset %d: %w", index, request.Begin, err)
	}
	if n < int(request.Length) {
		return nil, fmt.Errorf("error reading block, expected %d bytes, read %d bytes", request.Length, n)
	}

	return data, nil
}
