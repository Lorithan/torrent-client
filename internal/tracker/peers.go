package tracker

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
)

func parseCompactPeers(data string) ([]net.TCPAddr, error) {
	if len(data)%6 != 0 {
		return nil, fmt.Errorf("malformed peers field")
	}

	peers := make([]net.TCPAddr, 0, len(data)/6)

	for i := 0; i < len(data); i += 6 {
		addrBytes := []byte(data[i : i+6])
		port := binary.BigEndian.Uint16(addrBytes[4:6])
		if port == 0 {
			return nil, fmt.Errorf("port out of range")
		}

		addr := net.TCPAddr{
			IP:   net.IPv4(addrBytes[0], addrBytes[1], addrBytes[2], addrBytes[3]),
			Port: int(port),
		}

		peers = append(peers, addr)
	}

	return peers, nil
}

func parseDictionaryPeers(data []any) ([]net.TCPAddr, error) {
	peers := make([]net.TCPAddr, 0, len(data))

	for _, peerDict := range data {
		dict, ok := peerDict.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("malformed peers field")
		}

		ipString, ok := dict["ip"].(string)
		if !ok {
			return nil, fmt.Errorf("malformed peers field")
		}
		ip := net.ParseIP(ipString).To4()
		if ip == nil {
			return nil, fmt.Errorf("invalid ipv4 address")
		}

		port, ok := dict["port"].(int64)
		if !ok {
			return nil, fmt.Errorf("malformed peers field")
		}
		if port <= 0 || port > math.MaxUint16 {
			return nil, fmt.Errorf("port out of range")
		}

		addr := net.TCPAddr{
			IP:   ip,
			Port: int(port),
		}

		peers = append(peers, addr)
	}

	return peers, nil
}
