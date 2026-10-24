package peer

import "fmt"

type Bitfield []byte

func (b Bitfield) HasPiece(index int) bool {
	if index < 0 {
		return false
	}

	byteIndex := index / 8

	if byteIndex >= len(b) {
		return false
	}

	bits := b[byteIndex]
	bitOffset := index % 8
	mask := byte(0x80 >> bitOffset)

	return bits&mask > 0
}

func (b Bitfield) SetPiece(index int) error {
	if index < 0 {
		return fmt.Errorf("Index out of bounds")
	}

	byteIndex := index / 8

	if byteIndex >= len(b) {
		return fmt.Errorf("Index out of bounds")
	}

	bits := b[byteIndex]
	bitOffset := index % 8
	mask := byte(0x80 >> bitOffset)

	b[byteIndex] = bits | mask

	return nil
}

// Validate checks the bitfield's length and padding against the torrent's piece
// count without modifying it. Unused bits must be zero.
func (b Bitfield) Validate(pieceCount int) error {
	if pieceCount < 0 {
		return fmt.Errorf("`pieceCount` cannot be negative")
	}

	remainder := pieceCount % 8

	// Each byte holds eight pieces; a partial group needs one additional byte.
	requiredBytes := pieceCount / 8
	if remainder > 0 {
		requiredBytes += 1
	}

	if requiredBytes != len(b) {
		return fmt.Errorf("incorrect byte count for bitfield. Expected %d, found %d", requiredBytes, len(b))
	}
	// Whole bytes have no padding. This also accepts an empty bitfield for zero
	// pieces without attempting to access a last byte.
	if remainder == 0 {
		return nil
	}

	// Pieces occupy bits from most to least significant. Select only the unused
	// low bits: for ten pieces, remainder is 2 and the mask becomes 00111111.
	lastByte := b[requiredBytes-1]
	var mask byte
	for i := remainder; i < 8; i++ {
		mask |= byte(0x80 >> i)
	}

	// Any selected bit being set advertises a piece beyond the torrent's count.
	if lastByte&mask > 0 {
		return fmt.Errorf("bits set beyond pieceCount")
	}

	return nil
}
