package tracker

import (
	"crypto/rand"
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

type AnnounceEvent string

const (
	AnnounceEventDefault   AnnounceEvent = ""
	AnnounceEventStarted   AnnounceEvent = "started"
	AnnounceEventCompleted AnnounceEvent = "completed"
	AnnounceEventStopped   AnnounceEvent = "stopped"
)

func (e AnnounceEvent) isValid() bool {
	validValues := []AnnounceEvent{AnnounceEventDefault, AnnounceEventStarted, AnnounceEventCompleted, AnnounceEventStopped}

	for _, val := range validValues {
		if e == val {
			return true
		}
	}

	return false
}

type AnnounceParams struct {
	Uploaded   int64
	Downloaded int64
	Left       int64
	Event      AnnounceEvent
}

func GeneratePeerID() [20]byte {

	results := make([]byte, 20)

	// Fills the slice without returning an error
	rand.Read(results)

	for i, c := range PREFIX {
		results[i] = byte(c)
	}

	return [20]byte(results)
}

func buildAnnounceURL(tf *torrent.TorrentFile, peerID [20]byte, port uint16, params AnnounceParams) (string, error) {
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
		return "", fmt.Errorf("expected scheme `http` or `https`, found `%s`", announceURL.Scheme)
	}

	if port == 0 {
		return "", fmt.Errorf("Port cannot be `0`")
	}

	if params.Downloaded < 0 {
		return "", fmt.Errorf("download cannot be negative")
	}
	if params.Uploaded < 0 {
		return "", fmt.Errorf("uploaded cannout be negative")
	}
	if params.Left < 0 {
		return "", fmt.Errorf("left cannot be negative")
	}
	if !params.Event.isValid() {
		return "", fmt.Errorf("event %s not valid", params.Event)
	}

	// Set query params
	queries := announceURL.Query()
	infoHash := tf.InfoHash

	queries.Set("info_hash", string(infoHash[:]))
	queries.Set("peer_id", string(peerID[:]))
	queries.Set("port", fmt.Sprint(port))
	queries.Set("uploaded", fmt.Sprint(params.Uploaded))
	queries.Set("downloaded", fmt.Sprint(params.Downloaded))
	queries.Set("left", fmt.Sprint(params.Left))
	queries.Set("compact", "1") // Requests peers in compact binary format
	if params.Event == AnnounceEventDefault {
		queries.Del("event")
	} else {
		queries.Set("event", string(params.Event))
	}

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

	var peers []net.TCPAddr
	switch peersData := decodedMap["peers"].(type) {
	case string:
		peers, err = parseCompactPeers(peersData)
	case []any:
		peers, err = parseDictionaryPeers(peersData)
	default:
		return nil, fmt.Errorf("malformed or missing peers field")
	}
	if err != nil {
		return nil, err
	}

	response := TrackerResponse{
		Interval: time.Duration(interval) * time.Second,
		Peers:    peers,
	}

	return &response, nil
}

func Announce(tf *torrent.TorrentFile, peerID [20]byte, port uint16, httpClient *http.Client, params AnnounceParams) (*TrackerResponse, error) {
	announceURL, err := buildAnnounceURL(tf, peerID, port, params)

	if err != nil {
		return nil, err
	}

	trackerResponse, err := getAnnounceResponse(announceURL, httpClient)

	if err != nil {
		return nil, err
	}

	return parseTrackerResponse(trackerResponse)
}
