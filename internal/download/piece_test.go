package download

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

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
			session, err := NewSession(tf, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := session.DownloadPiece(client, 1)
			if err == nil || got != nil {
				t.Fatalf("got %d bytes and error %v, want nil data and hash error", len(got), err)
			}
			if !strings.Contains(err.Error(), "hash") {
				t.Errorf("expected hash failure, got %v", err)
			}
			if conn.incoming.Len() != 0 || conn.outgoing.Len() != 34 {
				t.Error("hash check did not follow the complete two-block transfer")
			}
			if stats := session.Stats(); stats.Downloaded != int64(len(original)) || stats.Left != tf.Length {
				t.Errorf("corrupt-piece stats = %+v, want downloaded %d with Left unchanged", stats, len(original))
			}
		})
	}
}

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
			session, err := NewSession(tf, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := session.DownloadPiece(client, tt.index)
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
			if stats := session.Stats(); stats.Downloaded != int64(len(data)) || stats.Left != tf.Length {
				t.Errorf("piece stats = %+v, want downloaded %d and Left unchanged until written", stats, len(data))
			}
		})
	}
}

func TestDownloadPieceClassifiesPeerFailures(t *testing.T) {
	timeout := &pieceTestTimeout{}
	for _, tt := range []struct {
		name      string
		choked    bool
		reader    io.Reader
		operation string
		cause     error
	}{
		{"wait disconnect", true, iotest.ErrReader(io.EOF), "wait for piece 0", io.EOF},
		{"wait timeout", true, iotest.ErrReader(timeout), "wait for piece 0", timeout},
		{"wait malformed message", true, bytes.NewReader([]byte{0, 0, 0, 2, 1, 42}), "wait for piece 0", nil},
		{"block disconnect", false, iotest.ErrReader(io.EOF), "download piece 0 block at offset 0", io.EOF},
		{"block timeout", false, iotest.ErrReader(timeout), "download piece 0 block at offset 0", timeout},
		{"malformed piece", false, bytes.NewReader([]byte{0, 0, 0, 1, 7}), "download piece 0 block at offset 0", nil},
		{"hash mismatch", false, bytes.NewReader([]byte{0, 0, 0, 13, 7, 0, 0, 0, 0, 0, 0, 0, 0, 'D', 'a', 't', 'a'}), "verify piece 0", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := &pieceErrorTestConn{reader: tt.reader}
			client := &peer.Client{Conn: conn, Choked: tt.choked, PieceCount: 1, Bitfield: peer.Bitfield{0x80}}
			tf := &torrent.TorrentFile{Length: 4, PieceLength: 4, Pieces: [][20]byte{sha1.Sum([]byte("data"))}}
			session, err := NewSession(tf, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := session.DownloadPiece(client, 0)
			var classified *PeerError
			if got != nil || !errors.As(err, &classified) {
				t.Fatalf("got (%x, %v), want nil data and PeerError", got, err)
			}
			if classified.Op != tt.operation {
				t.Errorf("operation = %q, want %q", classified.Op, tt.operation)
			}
			if classified.Err == nil {
				t.Error("classification lost underlying cause")
			}
			if tt.cause != nil && !errors.Is(err, tt.cause) {
				t.Errorf("underlying error %v not preserved in %v", tt.cause, err)
			}
			if tt.cause == timeout {
				var networkErr net.Error
				if !errors.As(err, &networkErr) || !networkErr.Timeout() {
					t.Error("timeout is not inspectable through wrapping")
				}
			}
		})
	}
}

func TestDownloadPieceDoesNotClassifyInvalidMetadataAsPeerFailure(t *testing.T) {
	for _, tt := range []struct {
		name                string
		length, pieceLength int64
		index               int
	}{
		{"negative index", 4, 4, -1},
		{"index beyond hashes", 4, 4, 1},
		{"zero piece length", 4, 0, 0},
		{"negative file length", -1, 4, 0},
		{"piece beyond file", 0, 4, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: tt.length, PieceLength: tt.pieceLength, Pieces: make([][20]byte, 1)}
			// A nil client proves local validation fails before accessing a peer.
			// Bypass the constructor only to exercise the method's local validation
			// with deliberately invalid metadata, before any peer operations.
			session := &Session{torrent: tf}
			got, err := session.DownloadPiece(nil, tt.index)
			if got != nil || err == nil {
				t.Fatalf("got (%x, %v), want nil data and validation error", got, err)
			}
			var classified *PeerError
			if errors.As(err, &classified) {
				t.Errorf("invalid metadata became retryable: %v", err)
			}
		})
	}
}

func TestPieceSize(t *testing.T) {
	for _, tt := range []struct {
		name                string
		length, pieceLength int64
		pieceCount, index   int
		want                int64
	}{
		{"first piece", 40960, 16384, 3, 0, 16384},
		{"middle piece", 40960, 16384, 3, 1, 16384},
		{"short final piece", 40960, 16384, 3, 2, 8192},
		{"full final piece", 32768, 16384, 2, 1, 16384},
		{"single short piece", 100, 16384, 1, 0, 100},
		{"single byte final piece", 16385, 16384, 2, 1, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: tt.length, PieceLength: tt.pieceLength, Pieces: make([][20]byte, tt.pieceCount)}
			got, err := PieceSize(tf, tt.index)
			if err != nil || got != tt.want {
				t.Errorf("PieceSize(%d) = (%d, %v), want (%d, nil)", tt.index, got, err, tt.want)
			}
		})
	}
}

func TestPieceSizeRejectsInvalidInput(t *testing.T) {
	for _, tt := range []struct {
		name                string
		length, pieceLength int64
		pieceCount, index   int
	}{
		{"negative index", 40960, 16384, 3, -1},
		{"index equals count", 40960, 16384, 3, 3},
		{"no pieces", 0, 16384, 0, 0},
		{"zero piece length", 100, 0, 1, 0},
		{"negative piece length", 100, -1, 1, 0},
		{"negative file length", -1, 16384, 1, 0},
		{"piece starts at file end", 16384, 16384, 2, 1},
		{"piece starts beyond file end", 100, 16384, 2, 1},
		{"piece in empty file", 0, 16384, 1, 0},
		{"piece offset overflow", 100, math.MaxInt64, 3, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := &torrent.TorrentFile{Length: tt.length, PieceLength: tt.pieceLength, Pieces: make([][20]byte, tt.pieceCount)}
			got, err := PieceSize(tf, tt.index)
			if got != 0 || err == nil {
				t.Errorf("PieceSize(%d) = (%d, %v), want (0, error)", tt.index, got, err)
			}
		})
	}
}

func TestSelectPiece(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		available, completed peer.Bitfield
		count, want          int
		found                bool
	}{
		{"first piece", peer.Bitfield{0x80}, peer.Bitfield{0}, 8, 0, true},
		{"skip unavailable pieces", peer.Bitfield{0x20}, peer.Bitfield{0}, 8, 2, true},
		{"skip completed piece", peer.Bitfield{0xa0}, peer.Bitfield{0x80}, 8, 2, true},
		{"choose lowest qualifying index", peer.Bitfield{0x25}, peer.Bitfield{0}, 8, 2, true},
		{"across byte boundary", peer.Bitfield{0x01, 0x80}, peer.Bitfield{0x01, 0}, 9, 8, true},
		{"last valid piece", peer.Bitfield{0, 0x40}, peer.Bitfield{0, 0}, 10, 9, true},
		{"no advertised pieces", peer.Bitfield{0}, peer.Bitfield{0}, 8, 0, false},
		{"all available pieces completed", peer.Bitfield{0xa0}, peer.Bitfield{0xa0}, 8, 0, false},
		{"remaining pieces unavailable", peer.Bitfield{0x80}, peer.Bitfield{0xc0}, 8, 0, false},
		{"ignore padding flags", peer.Bitfield{0, 0x3f}, peer.Bitfield{0, 0}, 10, 0, false},
		{"zero pieces", nil, nil, 0, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			availableBefore := append([]byte(nil), tt.available...)
			completedBefore := append([]byte(nil), tt.completed...)
			got, found := SelectPiece(tt.available, tt.completed, tt.count)
			if got != tt.want || found != tt.found {
				t.Errorf("SelectPiece() = (%d, %v), want (%d, %v)", got, found, tt.want, tt.found)
			}
			if !bytes.Equal(tt.available, availableBefore) || !bytes.Equal(tt.completed, completedBefore) {
				t.Error("selection modified an input bitfield")
			}
		})
	}
}
