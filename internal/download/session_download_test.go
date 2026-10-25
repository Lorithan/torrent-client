package download

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/lorithan/torrent-client/internal/peer"
)

func TestSessionDownloadFromRetainsProgressAfterInterruptedPiece(t *testing.T) {
	first, tf, firstConn, data := downloadFixture(t, 4, 10, -1)
	allReplies := make([]byte, firstConn.incoming.Len())
	if _, err := io.ReadFull(firstConn.incoming, allReplies); err != nil {
		t.Fatal(err)
	}
	// Complete piece zero, then disconnect after only two bytes of piece one's data.
	firstConn.incoming = bytes.NewReader(allReplies[:17+13+2])
	dst := &downloadTestWriter{}
	s, err := NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DownloadFrom(first); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("first attempt error = %v, want truncated piece error", err)
	}
	if s.completedCount != 1 || !s.completed.HasPiece(0) || s.completed.HasPiece(1) {
		t.Fatalf("progress after failure = count %d, bitfield %x; want only piece zero complete", s.completedCount, s.completed)
	}
	second, _, secondConn, _ := downloadFixture(t, 4, 10, -1)
	secondConn.incoming = bytes.NewReader(allReplies[17:])
	if err := s.DownloadFrom(second); err != nil {
		t.Fatal(err)
	}
	// The new peer must restart piece one at offset zero, then request piece two.
	want := []byte{
		0, 0, 0, 13, 6, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4,
		0, 0, 0, 5, 4, 0, 0, 0, 1,
		0, 0, 0, 13, 6, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2,
		0, 0, 0, 5, 4, 0, 0, 0, 2,
	}
	if !bytes.Equal(secondConn.outgoing.Bytes(), want) {
		t.Errorf("second peer received %x, want %x", secondConn.outgoing.Bytes(), want)
	}
	if s.completedCount != 3 {
		t.Errorf("completed count = %d, want 3", s.completedCount)
	}
	if len(dst.writes) != 3 {
		t.Fatalf("writes = %d, want 3", len(dst.writes))
	}
	for i, w := range dst.writes {
		if w.offset != int64(i*4) || !bytes.Equal(w.data, data[i*4:min(i*4+4, len(data))]) {
			t.Errorf("incorrect write %d: %+v", i, w)
		}
	}
	// A completed session should need neither another peer nor more output writes.
	if err := s.DownloadFrom(nil); err != nil {
		t.Errorf("completed session: %v", err)
	}
	if len(dst.writes) != 3 {
		t.Error("completed session wrote pieces again")
	}
}

type sessionHaveFailureConn struct {
	*pieceTestConn
	err error
}

func (c *sessionHaveFailureConn) Write(p []byte) (int, error) {
	if len(p) >= 5 && p[4] == byte(peer.MsgHave) {
		return 0, c.err
	}
	return c.pieceTestConn.Write(p)
}

func TestSessionDownloadFromRetainsCommittedPieceOnHaveFailure(t *testing.T) {
	first, tf, conn, _ := downloadFixture(t, 4, 4, -1)
	wantErr := errors.New("have write failed")
	first.Conn = &sessionHaveFailureConn{pieceTestConn: conn, err: wantErr}
	dst := &downloadTestWriter{}
	s, err := NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DownloadFrom(first); !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want have failure", err)
	}
	if s.completedCount != 1 || !s.completed.HasPiece(0) || len(dst.writes) != 1 {
		t.Error("announcement failure lost the successfully stored piece")
	}
	if err := s.DownloadFrom(nil); err != nil {
		t.Errorf("completed session should not redownload: %v", err)
	}
}

func TestSessionDownloadFromDoesNotCommitFailedWrite(t *testing.T) {
	client, tf, conn, _ := downloadFixture(t, 4, 4, -1)
	wantErr := errors.New("disk full")
	dst := &downloadTestWriter{failAt: 1, err: wantErr}
	s, err := NewSession(tf, dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DownloadFrom(client); !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want storage failure", err)
	}
	if s.completedCount != 0 || s.completed.HasPiece(0) {
		t.Error("failed output write committed progress")
	}
	if conn.outgoing.Len() != 17 {
		t.Error("failed output write announced completion")
	}
}
