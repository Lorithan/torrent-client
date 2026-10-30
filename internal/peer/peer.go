package peer

import (
	"fmt"
	"log"
	"net"
	"slices"
	"time"
)

const PROTOCOL = "BitTorrent protocol"

const handshakeTimeout = 10 * time.Second
const dialTimeout = 10 * time.Second

type Client struct {
	Conn           net.Conn
	Handshake      Handshake
	Bitfield       Bitfield
	Choked         bool
	AmChoking      bool
	PeerInterested bool
	PieceCount     int
}

func NewClient(conn net.Conn, remote Handshake, pieceCount int) (*Client, error) {
	if pieceCount < 0 {
		return nil, fmt.Errorf("pieceCount out of bounds")
	}

	// calculate size of bitfield
	remainder := pieceCount % 8
	// Each byte holds eight pieces; a partial group needs one additional byte.
	requiredBytes := pieceCount / 8
	if remainder > 0 {
		requiredBytes += 1
	}
	bitfield := make(Bitfield, requiredBytes)

	newClient := Client{
		Conn:       conn,
		Handshake:  remote,
		Bitfield:   bitfield,
		Choked:     true,
		AmChoking:  true,
		PieceCount: pieceCount,
	}

	return &newClient, nil
}

func (c *Client) HandleMessage(m *Message) error {
	if m == nil {
		return nil
	}

	log.Printf("msg id: %d, payload length: %d", m.ID, len(m.Payload))

	switch m.ID {
	case MsgChoke:
		if len(m.Payload) != 0 {
			return fmt.Errorf("choke message must have an empty payload")
		}
		c.Choked = true
	case MsgUnchoke:
		if len(m.Payload) != 0 {
			return fmt.Errorf("unchoke message must have an empty payload")
		}
		c.Choked = false

		log.Printf("has piece 0: %v", c.Bitfield.HasPiece(0))
	case MsgHave:
		index, err := m.HaveIndex(c.PieceCount)
		if err != nil {
			return err
		}

		log.Printf("has piece %d", index)
		return c.Bitfield.SetPiece(index)
	case MsgBitfield:
		bitfield := Bitfield(m.Payload)

		err := bitfield.Validate(c.PieceCount)
		if err != nil {
			return err
		}

		copy(c.Bitfield, bitfield)
		log.Printf("has piece 0: %v", c.Bitfield.HasPiece(0))
	case MsgInterested:
		if len(m.Payload) != 0 {
			return fmt.Errorf("interested msg expects empty paylooad")
		}
		c.PeerInterested = true
	case MsgNotInterested:
		if len(m.Payload) != 0 {
			return fmt.Errorf("not interested msg expects empty paylooad")
		}
		c.PeerInterested = false
	}

	return nil
}

func (c *Client) SendMessage(m Message) error {
	_, err := c.Conn.Write(m.Serialize())
	return err
}

func (c *Client) SendBitfield(available Bitfield) error {
	err := available.Validate(c.PieceCount)
	if err != nil {
		return err
	}

	msg := Message{
		ID:      MsgBitfield,
		Payload: available,
	}
	return c.SendMessage(msg)
}

func (c *Client) WaitForUnchoke() error {
	interested := Message{
		ID: MsgInterested,
	}
	err := c.SendMessage(interested)
	if err != nil {
		return err
	}

	for c.Choked {
		message, err := ReadMessage(c.Conn)
		if err != nil {
			return err
		}

		err = c.HandleMessage(message)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) WaitForPiece(index int) error {
	if index >= c.PieceCount || index < 0 {
		return fmt.Errorf("index out of bounds")
	}

	if !c.Choked && c.Bitfield.HasPiece(index) {
		return nil
	}

	err := c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		return err
	}

	for c.Choked || !c.Bitfield.HasPiece(index) {
		message, err := ReadMessage(c.Conn)
		if err != nil {
			return err
		}

		err = c.HandleMessage(message)
		if err != nil {
			return err
		}
	}

	err = c.Conn.SetDeadline(time.Time{})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) WaitForAvailabilityChange() error {
	availabilitySnapshot := slices.Clone(c.Bitfield)

	err := c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		return err
	}

	for slices.Equal(availabilitySnapshot, c.Bitfield) {
		m, err := ReadMessage(c.Conn)
		if err != nil {
			return err
		}

		err = c.HandleMessage(m)
		if err != nil {
			return err
		}
	}

	err = c.Conn.SetDeadline(time.Time{})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) DownloadBlock(index, begin, length uint32) ([]byte, error) {
	if c.Choked {
		return nil, fmt.Errorf("client is currently chocked")
	}

	// Validate index
	if uint64(index) >= uint64(c.PieceCount) {
		return nil, fmt.Errorf("index out of bounds.")
	}

	if !c.Bitfield.HasPiece(int(index)) {
		return nil, fmt.Errorf("peer does not have current block %d", index)
	}

	request, err := NewRequest(index, begin, length)
	if err != nil {
		return nil, err
	}

	err = c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		return nil, err
	}

	err = c.SendMessage(*request)
	if err != nil {
		return nil, err
	}

	for {
		returnMessage, err := ReadMessage(c.Conn)
		if err != nil {
			return nil, err
		}
		// Keep alive
		if returnMessage == nil {
			continue
		}

		if returnMessage.ID != MsgPiece {
			err := c.HandleMessage(returnMessage)
			if err != nil {
				return nil, err
			}

			if c.Choked {
				return nil, fmt.Errorf("peer has sent choked messae")
			}

			continue
		}

		pieceBlock, err := returnMessage.PieceBlock()
		if err != nil {
			return nil, err
		}

		if pieceBlock.Index != index {
			return nil, fmt.Errorf("recieved block index does not match sent index. Senc %d, recieve %d", index, pieceBlock.Index)
		}
		if pieceBlock.Begin != begin {
			return nil, fmt.Errorf("recieved block begin does not match sent begin. Senc %d, recieve %d", begin, pieceBlock.Begin)
		}
		if len(pieceBlock.Data) != int(length) {
			return nil, fmt.Errorf("recieved block length does not match sent length. Senc %d, recieve %d", length, len(pieceBlock.Data))
		}

		err = c.Conn.SetDeadline(time.Time{})
		if err != nil {
			return nil, err
		}

		return pieceBlock.Data, nil
	}
}

func (c *Client) SetChoking(choking bool) error {
	var msgID MessageID
	if choking {
		msgID = MsgChoke
	} else {
		msgID = MsgUnchoke
	}

	msg := Message{
		ID: msgID,
	}

	err := c.SendMessage(msg)
	if err != nil {
		return err
	}

	c.AmChoking = choking
	return nil
}

func Connect(addr net.TCPAddr, local Handshake) (net.Conn, *Handshake, error) {
	// Establish connectioon
	conn, err := net.DialTimeout("tcp", addr.String(), dialTimeout)
	if err != nil {
		return nil, nil, err
	}

	err = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	received, err := ExchangeHandshake(conn, local)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	// Clear deadline
	err = conn.SetDeadline(time.Time{})
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, received, nil
}
