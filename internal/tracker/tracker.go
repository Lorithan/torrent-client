package tracker

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/lorithan/torrent-client/internal/bencode"
	"github.com/lorithan/torrent-client/internal/torrent"
)

const PREFIX = "-CT0001-"
const MAX_RESPONSE_SIZE = 1024 * 1024 // 1 MiB

func GeneratePeerID() [20]byte {

	results := make([]byte, 20)

	// Fills the slice without returning an error
	rand.Read(results)

	for i, c := range PREFIX {
		results[i] = byte(c)
	}

	return [20]byte(results)
}

func buildAnnounceURL(tf *torrent.TorrentFile, peerID [20]byte, port uint16) (string, error) {
	// Parse the original announceURL
	announceURL, err := url.Parse(tf.Announce)

	// Error checking
	if err != nil {
		return "", err
	}

	if announceURL.Hostname() == "" {
		return "", fmt.Errorf("No host name")
	}

	if announceURL.Scheme != "http" && announceURL.Scheme != "https" {
		return "", fmt.Errorf("Expected scheme `http` or `https`, found `%s`", announceURL.Scheme)
	}

	if port == 0 {
		return "", fmt.Errorf("Port cannot be `0`")
	}

	// Set query params
	queries := announceURL.Query()
	infoHash := tf.InfoHash

	queries.Set("info_hash", string(infoHash[:]))
	queries.Set("peer_id", string(peerID[:]))
	queries.Set("port", fmt.Sprint(port))
	queries.Set("uploaded", "0")
	queries.Set("downloaded", "0")
	queries.Set("left", fmt.Sprint(tf.Length))
	queries.Set("compact", "1") // Requests peers in compact binary format
	queries.Set("event", "started")

	announceURL.RawQuery = queries.Encode()

	return announceURL.String(), nil
}

func getAnnounceResponse(announceURL string, httpClient *http.Client) ([]byte, error) {
	response, err := httpClient.Get(announceURL)

	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Expected status code `200`, found `%s`", response.Status)
	}

	r := io.LimitedReader{
		R: response.Body,
		N: MAX_RESPONSE_SIZE + 1,
	}

	responseBytes, err := io.ReadAll(&r)

	if err != nil {
		return nil, err
	}

	if r.N == 0 {
		return nil, fmt.Errorf("Response larger than 1MB")
	}

	return responseBytes, nil
}

type TrackerResponse struct {
	Interval time.Duration
	Peers    []net.TCPAddr
}

func parseTrackerResponse(data []byte) (*TrackerResponse, error) {
	decoded, err := bencode.Decode(data)

	if err != nil {
		return nil, err
	}

	decodedMap, ok := decoded.(map[string]any)

	if !ok {
		return nil, fmt.Errorf("Invalid response body")
	}

	failureReason, exists := decodedMap["failure reason"]

	if exists {
		failureReason, ok := failureReason.(string)

		if ok {
			return nil, fmt.Errorf("Response error: `%s`", failureReason)
		} else {
			return nil, fmt.Errorf("Malform response error")
		}
	}

	interval, ok := decodedMap["interval"].(int64)

	if !ok {
		return nil, fmt.Errorf("Expected `interval` from response")
	}

	if interval <= 0 {
		return nil, fmt.Errorf("Field `interval` must be positive")
	}

	if interval > math.MaxInt64/int64(time.Second) {
		return nil, fmt.Errorf("Field `interval` too large.")
	}

	peers, ok := decodedMap["peers"].(string)

	if !ok {
		return nil, fmt.Errorf("Expected `peers` from response")
	}

	if len(peers)%6 != 0 {
		return nil, fmt.Errorf("Field `peers` is malformed")
	}

	peersList := make([]net.TCPAddr, 0, len(peers)/6)
	i := 0

	for {
		if i+6 > len(peers) {
			break
		}

		addrBytes := []byte(peers[i : i+6])

		addr := net.TCPAddr{
			IP:   net.IPv4(addrBytes[0], addrBytes[1], addrBytes[2], addrBytes[3]),
			Port: int(binary.BigEndian.Uint16(addrBytes[4:6])),
		}

		peersList = append(peersList, addr)

		i += 6
	}

	response := TrackerResponse{
		Interval: time.Duration(interval) * time.Second,
		Peers:    peersList,
	}

	return &response, nil
}

func Announce(tf *torrent.TorrentFile, peerID [20]byte, port uint16, httpClient *http.Client) (*TrackerResponse, error) {
	announceURL, err := buildAnnounceURL(tf, peerID, port)

	if err != nil {
		return nil, err
	}

	trackerResponse, err := getAnnounceResponse(announceURL, httpClient)

	if err != nil {
		return nil, err
	}

	return parseTrackerResponse(trackerResponse)
}
