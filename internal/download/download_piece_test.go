package download

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

type pieceTestConn struct {
	net.Conn
	incoming *bytes.Reader
	outgoing bytes.Buffer
}

func (c *pieceTestConn) SetDeadline(time.Time) error { return nil }

func TestDownloadPieceRejectsCorruptedData(t *testing.T) {
	for _, tt := range []struct {
		name      string
		corruptAt int
	}{
		{"first block", 0},
		{"final block", 19999},
	} {
		t.Run(tt.name, func(t *testing.T) {
			original := bytes.Repeat([]byte{0xab}, 20000)
			tf := &torrent.TorrentFile{
				Length: 40000, PieceLength: 20000,
				Pieces: [][20]byte{sha1.Sum([]byte("other piece")), sha1.Sum(original)},
			}
			corrupt := append([]byte(nil), original...)
			corrupt[tt.corruptAt] ^= 0xff
			var replies []byte
			for offset := 0; offset < len(corrupt); {
				length := min(16384, len(corrupt)-offset)
				replies = binary.BigEndian.AppendUint32(replies, uint32(9+length))
				replies = append(replies, byte(peer.MsgPiece))
				replies = binary.BigEndian.AppendUint32(replies, 1)
				replies = binary.BigEndian.AppendUint32(replies, uint32(offset))
				replies = append(replies, corrupt[offset:offset+length]...)
				offset += length
			}
			conn := &pieceTestConn{incoming: bytes.NewReader(replies)}
			client, err := peer.NewClient(conn, peer.Handshake{}, 2)
			if err != nil {
				t.Fatal(err)
			}
			client.Choked = false
			if err := client.Bitfield.SetPiece(1); err != nil {
				t.Fatal(err)
			}
			got, err := DownloadPiece(client, tf, 1)
			if err == nil || got != nil {
				t.Fatalf("got %d bytes and error %v, want nil data and hash error", len(got), err)
			}
			if !strings.Contains(err.Error(), "hash") {
				t.Errorf("expected hash failure, got %v", err)
			}
			if conn.incoming.Len() != 0 || conn.outgoing.Len() != 34 {
				t.Error("hash check did not follow the complete two-block transfer")
			}
		})
	}
}

func (c *pieceTestConn) Read(p []byte) (int, error)  { return c.incoming.Read(p) }
func (c *pieceTestConn) Write(p []byte) (int, error) { return c.outgoing.Write(p) }

func TestDownloadPieceBlocks(t *testing.T) {
	for _, tt := range []struct {
		name                    string
		index                   int
		pieceLength, fileLength int64
		dataLength              int
	}{
		{"single short block", 0, 16384, 100, 100},
		{"three blocks", 0, 40960, 40960, 40960},
		{"short final piece", 1, 32768, 52768, 20000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, tt.dataLength)
			for i := range data {
				data[i] = byte(i*31 + i/16384)
			}
			tf := &torrent.TorrentFile{Length: tt.fileLength, PieceLength: tt.pieceLength, Pieces: make([][20]byte, tt.index+1)}
			tf.Pieces[tt.index] = sha1.Sum(data)
			var replies, wantRequests []byte
			for offset := 0; offset < len(data); {
				length := min(16384, len(data)-offset)
				replies = binary.BigEndian.AppendUint32(replies, uint32(9+length))
				replies = append(replies, byte(peer.MsgPiece))
				replies = binary.BigEndian.AppendUint32(replies, uint32(tt.index))
				replies = binary.BigEndian.AppendUint32(replies, uint32(offset))
				replies = append(replies, data[offset:offset+length]...)
				wantRequests = append(wantRequests, 0, 0, 0, 13, byte(peer.MsgRequest))
				wantRequests = binary.BigEndian.AppendUint32(wantRequests, uint32(tt.index))
				wantRequests = binary.BigEndian.AppendUint32(wantRequests, uint32(offset))
				wantRequests = binary.BigEndian.AppendUint32(wantRequests, uint32(length))
				offset += length
			}
			conn := &pieceTestConn{incoming: bytes.NewReader(replies)}
			client, err := peer.NewClient(conn, peer.Handshake{}, len(tf.Pieces))
			if err != nil {
				t.Fatal(err)
			}
			client.Choked = false
			if err := client.Bitfield.SetPiece(tt.index); err != nil {
				t.Fatal(err)
			}
			got, err := DownloadPiece(client, tf, tt.index)
			if !bytes.Equal(conn.outgoing.Bytes(), wantRequests) {
				t.Errorf("requests = %x, want %x", conn.outgoing.Bytes(), wantRequests)
			}
			if err != nil {
				t.Fatalf("DownloadPiece() error: %v", err)
			}
			if !bytes.Equal(got, data) {
				t.Errorf("assembled data differs: got %d bytes, want %d", len(got), len(data))
			}
			if conn.incoming.Len() != 0 {
				t.Errorf("left %d response bytes unread", conn.incoming.Len())
			}
		})
	}
}
