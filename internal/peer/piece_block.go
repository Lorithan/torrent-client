package peer

type PieceBlock struct {
	Index uint32
	Begin uint32
	Data  []byte
}
