package peer

import (
	"fmt"
	"io"
	"net"
)

type Handshake struct {
	ReservedFlags [8]byte
	InfoHash      [20]byte
	PeerID        [20]byte
}

func (h Handshake) Serialize() []byte {
	serialized := make([]byte, 0, 68)
	serialized = append(serialized, 19)
	serialized = append(serialized, []byte(PROTOCOL)...)
	serialized = append(serialized, h.ReservedFlags[:]...)
	serialized = append(serialized, h.InfoHash[:]...)
	serialized = append(serialized, h.PeerID[:]...)

	return serialized
}

func ReadHandshake(r io.Reader) (*Handshake, error) {
	// Write protocol length to validate first.
	var handshakeBytes [68]byte
	_, err := io.ReadFull(r, handshakeBytes[:1])

	if err != nil {
		return nil, err
	}

	if handshakeBytes[0] != 19 {
		return nil, fmt.Errorf("Invalid protocol length. Expected `19`, found `%d`", handshakeBytes[0])
	}

	// Write rest of the handshake infomation
	_, err = io.ReadFull(r, handshakeBytes[1:])

	if err != nil {
		return nil, err
	}

	// Validate correct protocol
	protocol := string(handshakeBytes[1:20])
	if protocol != PROTOCOL {
		return nil, fmt.Errorf("Invalid protocol `%s`", protocol)
	}

	reservedFlags := handshakeBytes[20:28]
	infoHash := handshakeBytes[28:48]
	peerID := handshakeBytes[48:68]

	handshake := Handshake{
		ReservedFlags: [8]byte(reservedFlags),
		InfoHash:      [20]byte(infoHash),
		PeerID:        [20]byte(peerID),
	}

	return &handshake, nil
}

func ExchangeHandshake(conn net.Conn, local Handshake) (*Handshake, error) {
	localData := local.Serialize()

	_, err := conn.Write(localData)
	if err != nil {
		return nil, err
	}

	received, err := ReadHandshake(conn)
	if err != nil {
		return nil, err
	}

	if received.InfoHash != local.InfoHash {
		return nil, fmt.Errorf("Received info hash does not match local info hash")
	}

	return received, nil
}
