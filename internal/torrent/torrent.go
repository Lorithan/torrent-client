package torrent

import (
	"crypto/sha1"
	"fmt"

	"github.com/lorithan/torrent-client/internal/bencode"
)

type TorrentFile struct {
	Announce    string
	InfoHash    [20]byte
	PieceLength int64
	Pieces      [][20]byte
	Name        string
	Length      int64
}

func ParseTorrentFile(data []byte) (*TorrentFile, error) {
	torrentMap, err := bencode.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("Torrent Parse error: %s", err)
	}

	// Type checking
	torrent, ok := torrentMap.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("Torrent file not in expected format")

	}

	// Extract the announce
	announce, ok := torrent["announce"].(string)
	if !ok {
		return nil, fmt.Errorf("Torrent file format error, could not find 'announce'")
	}

	// Extract the info dict
	info, ok := torrent["info"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("Torrent file format error, could not find 'announce'")
	}

	// We need the raw bytes of the info dict to hash to make sure the torrent is correct
	// We start at position 1 because we know from type checking earlier that data[0] == 'd'
	pos := 1
	var infoStart, infoEnd int
	for data[pos] != 'e' {
		key, newPos, err := bencode.DecodeStr(data, pos)
		if err != nil {
			return nil, err
		}
		pos = newPos

		if key == "info" {
			infoStart = pos
			_, newPos, err = bencode.DecodeAny(data, pos)
			if err != nil {
				return nil, err
			}
			infoEnd = newPos
			pos = newPos
			continue
		}

		// not the key we care about right now — just skip past its value
		_, newPos, err = bencode.DecodeAny(data, pos)
		if err != nil {
			return nil, err
		}
		pos = newPos
	}

	infoBytes := data[infoStart:infoEnd]
	infoHash := sha1.Sum(infoBytes)

	// Extract the name
	name, ok := info["name"].(string)
	if !ok {
		return nil, fmt.Errorf("Torrent file format error, could not find 'name'")
	}

	// Extract the length
	length, ok := info["length"].(int64)
	if !ok {
		return nil, fmt.Errorf("Torrent file format error, could not find 'length'")
	}

	pieceLen, ok := info["piece length"].(int64)
	if !ok {
		return nil, fmt.Errorf("Torrent file format error, could not find 'piece length'")
	}

	piecesbytes, ok := info["pieces"].(string)
	if !ok {
		return nil, fmt.Errorf("Torrent file format error, could not find 'pieces'")
	}

	if len(piecesbytes)%20 != 0 {
		return nil, fmt.Errorf("Pieces not valid")
	}

	var pieces [][20]byte
	for i := 0; i < len(piecesbytes)/20; i++ {
		byteChunk := []byte(piecesbytes[i*20 : (i*20)+20])
		pieceHash := [20]byte(byteChunk)
		pieces = append(pieces, pieceHash)
	}

	var torrentFile = TorrentFile{
		Announce:    announce,
		InfoHash:    infoHash,
		PieceLength: pieceLen,
		Pieces:      pieces,
		Name:        name,
		Length:      length,
	}

	return &torrentFile, nil
}
