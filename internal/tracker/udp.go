package tracker

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

const protocolID uint64 = 0x41727101980

func buildUDPConnectRequest(transactionID uint32) []byte {
	data := make([]byte, 0, 16)

	data = binary.BigEndian.AppendUint64(data, protocolID)
	data = binary.BigEndian.AppendUint32(data, 0)
	data = binary.BigEndian.AppendUint32(data, transactionID)

	return data
}

func parseUDPConnectResponse(data []byte, transactionID uint32) (uint64, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("expected data to be at least 8 bytes")
	}

	action := binary.BigEndian.Uint32(data[0:4])
	id := binary.BigEndian.Uint32(data[4:8])

	if id != transactionID {
		return 0, fmt.Errorf("id mismatch.")
	}

	switch action {
	case 0: // connect
		if len(data) < 16 {
			return 0, fmt.Errorf("connection missing id")
		}
		connectionID := binary.BigEndian.Uint64(data[8:16])
		return connectionID, nil
	case 3: // tracker error
		return 0, fmt.Errorf("%s", string(data[8:]))
	default:
		return 0, fmt.Errorf("unexpected action %d", action)
	}
}

func udpEventCode(event AnnounceEvent) (uint32, error) {
	switch event {
	case AnnounceEventDefault:
		return 0, nil
	case AnnounceEventCompleted:
		return 1, nil
	case AnnounceEventStarted:
		return 2, nil
	case AnnounceEventStopped:
		return 3, nil
	default:
		return 0, fmt.Errorf("unexpected event")
	}
}

func buildUDPAnnounceRequest(
	infoHash [20]byte,
	peerID [20]byte,
	port uint16,
	params AnnounceParams,
	connectionID uint64,
	transactionID uint32,
	key uint32,
) ([]byte, error) {
	if port == 0 {
		return nil, fmt.Errorf("port cannot be 0")
	}
	if params.Downloaded < 0 {
		return nil, fmt.Errorf("download cannot be negative")
	}
	if params.Uploaded < 0 {
		return nil, fmt.Errorf("upload cannot be negative")
	}
	if params.Left < 0 {
		return nil, fmt.Errorf("left cannot be negative")
	}
	event, err := udpEventCode(params.Event)
	if err != nil {
		return nil, err
	}

	data := make([]byte, 0, 98)

	data = binary.BigEndian.AppendUint64(data, connectionID)
	data = binary.BigEndian.AppendUint32(data, 1) // action
	data = binary.BigEndian.AppendUint32(data, transactionID)
	data = append(data, infoHash[:]...)
	data = append(data, peerID[:]...)
	data = binary.BigEndian.AppendUint64(data, uint64(params.Downloaded))
	data = binary.BigEndian.AppendUint64(data, uint64(params.Left))
	data = binary.BigEndian.AppendUint64(data, uint64(params.Uploaded))
	data = binary.BigEndian.AppendUint32(data, event)
	data = binary.BigEndian.AppendUint32(data, 0)
	data = binary.BigEndian.AppendUint32(data, key)
	data = binary.BigEndian.AppendUint32(data, 0xffffffff)
	data = binary.BigEndian.AppendUint16(data, port)

	return data, nil
}

func parseUDPAnnounceResponse(
	data []byte,
	transactionID uint32,
) (*TrackerResponse, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("expected data to be at least 8 bytes")
	}

	action := binary.BigEndian.Uint32(data[0:4])
	transaction := binary.BigEndian.Uint32(data[4:8])

	if transaction != transactionID {
		return nil, fmt.Errorf("transaction ID mismatch")
	}

	if action == 3 {
		return nil, fmt.Errorf("%s", data[8:])
	}

	if action != 1 {
		return nil, fmt.Errorf("unsupported action")
	}

	if len(data) < 20 {
		return nil, fmt.Errorf("expected data to be at least 20 bytes")
	}

	interval := binary.BigEndian.Uint32(data[8:12])
	// leacher := binary.BigEndian.Uint32(data[12:16])
	// seeder := binary.BigEndian.Uint32(data[16:20])
	peers := data[20:]

	if interval == 0 {
		return nil, fmt.Errorf("interval cannot be 0")
	}

	peerList, err := parseCompactPeers(string(peers))
	if err != nil {
		return nil, err
	}

	return &TrackerResponse{
		Interval: time.Duration(interval) * time.Second,
		Peers:    peerList,
	}, nil
}

func readUDPResponse(
	conn *net.UDPConn,
	transactionID uint32,
) ([]byte, error) {
	packet := make([]byte, 64*1024)

	for {
		n, err := conn.Read(packet)
		if err != nil {
			return nil, err
		}

		if n < 8 {
			continue
		}

		if binary.BigEndian.Uint32(packet[4:8]) != transactionID {
			continue
		}

		return packet[:n], nil
	}
}

func exchangeUDP(
	conn *net.UDPConn,
	request []byte,
	transactionID uint32,
	timeout time.Duration,
) ([]byte, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("timeout must be greater than 0")
	}

	err := conn.SetDeadline(time.Now().Add(timeout))
	if err != nil {
		return nil, err
	}

	_, err = conn.Write(request)
	if err != nil {
		return nil, err
	}

	return readUDPResponse(conn, transactionID)
}

func connectUDP(
	conn *net.UDPConn,
	transactionID uint32,
	timeout time.Duration,
) (uint64, error) {
	request := buildUDPConnectRequest(transactionID)
	response, err := exchangeUDP(conn, request, transactionID, timeout)
	if err != nil {
		return 0, err
	}

	return parseUDPConnectResponse(response, transactionID)
}
