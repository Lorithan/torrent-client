package download

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

type blockTestStorage struct {
	data   []byte
	reads  int
	offset int64
	err    error
	short  bool
}

func (s *blockTestStorage) WriteAt([]byte, int64) (int, error) {
	return 0, errors.New("unexpected write while serving a block")
}

func (s *blockTestStorage) ReadAt(p []byte, offset int64) (int, error) {
	s.reads++
	s.offset = offset
	if s.err != nil {
		return 0, s.err
	}
	if s.short {
		return copy(p, []byte{42}), nil
	}
	return bytes.NewReader(s.data).ReadAt(p, offset)
}

func blockTestSession(t *testing.T, storage *blockTestStorage, pieceLength int64) *Session {
	t.Helper()
	length := int64(len(storage.data))
	count := (length + pieceLength - 1) / pieceLength
	s, err := NewSession(&torrent.TorrentFile{
		Length: length, PieceLength: pieceLength, Pieces: make([][20]byte, count),
	}, storage)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSessionReadBlockVerifiedData(t *testing.T) {
	for _, tc := range []struct {
		name    string
		request peer.BlockRequest
		want    string
		offset  int64
	}{
		{"whole piece", peer.BlockRequest{Index: 1, Length: 4}, "efgh", 4},
		{"interior block", peer.BlockRequest{Index: 1, Begin: 1, Length: 2}, "fg", 5},
		{"short final piece", peer.BlockRequest{Index: 2, Length: 2}, "ij", 8},
		{"last byte", peer.BlockRequest{Index: 2, Begin: 1, Length: 1}, "j", 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := &blockTestStorage{data: []byte("abcdefghij")}
			s := blockTestSession(t, dst, 4)
			if err := s.markComplete(int(tc.request.Index)); err != nil {
				t.Fatal(err)
			}
			before := s.Stats()
			data, err := s.ReadBlock(tc.request)
			if err != nil || string(data) != tc.want {
				t.Fatalf("ReadBlock() = (%q, %v), want (%q, nil)", data, err, tc.want)
			}
			if dst.reads != 1 || dst.offset != tc.offset {
				t.Errorf("reads=%d offset=%d", dst.reads, dst.offset)
			}
			if s.Stats() != before {
				t.Error("reading changed transfer counters")
			}
		})
	}
	t.Run("maximum block", func(t *testing.T) {
		dst := &blockTestStorage{data: bytes.Repeat([]byte{42}, maxBlockLength)}
		s := blockTestSession(t, dst, maxBlockLength)
		if err := s.markComplete(0); err != nil {
			t.Fatal(err)
		}
		got, err := s.ReadBlock(peer.BlockRequest{Length: maxBlockLength})
		if err != nil || !bytes.Equal(got, dst.data) {
			t.Fatalf("maximum block: length=%d err=%v", len(got), err)
		}
	})
}

func TestSessionReadBlockRejectsBeforeStorageRead(t *testing.T) {
	for _, tc := range []struct {
		name     string
		request  peer.BlockRequest
		complete bool
	}{
		{"unverified piece", peer.BlockRequest{Length: 1}, false},
		{"index at count", peer.BlockRequest{Index: 3, Length: 1}, true},
		{"maximum index", peer.BlockRequest{Index: math.MaxUint32, Length: 1}, true},
		{"zero length", peer.BlockRequest{}, true},
		{"oversized block", peer.BlockRequest{Length: maxBlockLength + 1}, true},
		{"begin at piece end", peer.BlockRequest{Begin: 4, Length: 1}, true},
		{"cross piece boundary", peer.BlockRequest{Begin: 3, Length: 2}, true},
		{"cross final boundary", peer.BlockRequest{Index: 2, Begin: 1, Length: 2}, true},
		{"overflowing uint32 sum", peer.BlockRequest{Begin: math.MaxUint32, Length: 2}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := &blockTestStorage{data: []byte("abcdefghij")}
			s := blockTestSession(t, dst, 4)
			if tc.complete {
				for i := range 3 {
					if err := s.markComplete(i); err != nil {
						t.Fatal(err)
					}
				}
			}
			data, err := s.ReadBlock(tc.request)
			if err == nil || data != nil {
				t.Errorf("ReadBlock() = (%v, %v), want nil data and error", data, err)
			}
			if dst.reads != 0 {
				t.Errorf("invalid request performed %d storage reads", dst.reads)
			}
		})
	}
}

func TestSessionReadBlockStorageFailures(t *testing.T) {
	sentinel := errors.New("storage unavailable")
	for _, tc := range []struct {
		name  string
		err   error
		short bool
	}{
		{"read error", sentinel, false},
		{"EOF", io.EOF, false},
		{"short read without error", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := &blockTestStorage{data: []byte("abcd"), err: tc.err, short: tc.short}
			s := blockTestSession(t, dst, 4)
			if err := s.markComplete(0); err != nil {
				t.Fatal(err)
			}
			data, err := s.ReadBlock(peer.BlockRequest{Length: 4})
			if err == nil || data != nil {
				t.Errorf("ReadBlock() = (%v, %v), want nil data and error", data, err)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Errorf("error=%v, want underlying %v", err, tc.err)
			}
			if dst.reads != 1 {
				t.Errorf("storage reads=%d, want 1", dst.reads)
			}
		})
	}
}
