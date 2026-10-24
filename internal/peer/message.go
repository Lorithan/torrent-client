package peer

import (
	"encoding/binary"
	"fmt"
	"io"
)

const maxMessageLength = 1 << 20
const maxBlockLength = 16 * 1024 // 16 KiB

type MessageID uint8

const (
	MsgChoke MessageID = iota
	MsgUnchoke
	MsgInterested
	MsgNotInterested
	MsgHave
	MsgBitfield
	MsgRequest
	MsgPiece
	MsgCancel
)

type Message struct {
	ID      MessageID
	Payload []byte
}

func NewRequest(index uint32, begin uint32, length uint32) (*Message, error) {
	if length > maxBlockLength || length == 0 {
		return nil, fmt.Errorf("length out of bounds")
	}

	payload := make([]byte, 0, 12)

	payload = binary.BigEndian.AppendUint32(payload, index)
	payload = binary.BigEndian.AppendUint32(payload, begin)
	payload = binary.BigEndian.AppendUint32(payload, length)

	return &Message{
		ID:      MsgRequest,
		Payload: payload,
	}, nil
}

func (m Message) Serialize() []byte {
	serialize := make([]byte, 4, len(m.Payload)+5)

	binary.BigEndian.PutUint32(serialize, uint32(1+len(m.Payload)))
	serialize = append(serialize, uint8(m.ID))
	serialize = append(serialize, m.Payload...)

	return serialize
}

func (m Message) PieceBlock() (*PieceBlock, error) {
	if m.ID != MsgPiece {
		return nil, fmt.Errorf("incorrect message ID. Expected %d, found %d", MsgPiece, m.ID)
	}

	if len(m.Payload) < 9 {
		return nil, fmt.Errorf("malformed payload, length needs to be greater than 8")
	}

	if len(m.Payload[8:]) > maxBlockLength {
		return nil, fmt.Errorf("payload data too large, largest block accepted is %d bytes", maxBlockLength)
	}

	index := binary.BigEndian.Uint32(m.Payload[0:4])
	begin := binary.BigEndian.Uint32(m.Payload[4:8])
	data := make([]byte, len(m.Payload[8:]))
	copy(data, m.Payload[8:])

	return &PieceBlock{
		Index: index,
		Begin: begin,
		Data:  data,
	}, nil
}

func (m Message) HaveIndex(pieceCount int) (int, error) {
	if m.ID != MsgHave {
		return 0, fmt.Errorf("incorrect message ID. Expected %d, found %d", MsgHave, m.ID)
	}

	if len(m.Payload) != 4 {
		return 0, fmt.Errorf("message payload should be exactly 4 bytes, found %d", len(m.Payload))
	}

	if pieceCount < 0 {
		return 0, fmt.Errorf("pieceCount cannot be negative")
	}

	index := binary.BigEndian.Uint32(m.Payload)
	if uint64(index) >= uint64(pieceCount) {
		return 0, fmt.Errorf("piece index out of range")
	}
	return int(index), nil
}

func ReadMessage(r io.Reader) (*Message, error) {
	var sizeBytes [4]byte
	_, err := io.ReadFull(r, sizeBytes[:])
	if err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(sizeBytes[:])
	if size == 0 {
		return nil, nil
	}
	if size > maxMessageLength {
		return nil, fmt.Errorf("message length too large. Max is %d", maxMessageLength)
	}

	messageBytes := make([]byte, size)
	_, err = io.ReadFull(r, messageBytes)
	if err != nil {
		return nil, err
	}

	id := messageBytes[0]
	payload := messageBytes[1:]

	message := Message{
		ID:      MessageID(id),
		Payload: payload,
	}

	return &message, nil
}
