package download

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"testing/iotest"
)

func TestSessionRetainsProgressAfterTimeout(t *testing.T) {
	for _, phase := range []string{"availability", "block response"} {
		t.Run(phase, func(t *testing.T) {
			first, tf, fixtureConn, data := downloadFixture(t, 4, 10, -1)
			replies := make([]byte, fixtureConn.incoming.Len())
			if _, err := io.ReadFull(fixtureConn.incoming, replies); err != nil {
				t.Fatal(err)
			}
			beforeTimeout := append([]byte(nil), replies[:17+13+2]...)
			if phase == "availability" {
				first.Bitfield = []byte{0x80}
				beforeTimeout = append(append([]byte(nil), replies[:17]...), 0, 0, 0, 0)
			}
			timeout := &pieceTestTimeout{}
			// Inject a real net.Error shape after partial progress rather than waiting
			// 30 seconds. Separate deadline tests verify the actual timer placement.
			failedConn := &pieceErrorTestConn{reader: io.MultiReader(bytes.NewReader(beforeTimeout), iotest.ErrReader(timeout))}
			first.Conn = failedConn
			dst := &downloadTestWriter{}
			session, err := NewSession(tf, dst)
			if err != nil {
				t.Fatal(err)
			}
			err = session.DownloadFrom(first)
			var classified *PeerError
			var networkErr net.Error
			if !errors.As(err, &classified) || !errors.As(err, &networkErr) || !networkErr.Timeout() || !errors.Is(err, timeout) {
				t.Fatalf("error = %v, want peer failure preserving timeout", err)
			}
			if session.completedCount != 1 || !session.completed.HasPiece(0) || session.completed.HasPiece(1) || session.Complete() || len(dst.writes) != 1 {
				t.Fatal("timeout lost committed progress or committed unfinished data")
			}
			second, _, secondConn, _ := downloadFixture(t, 4, 10, -1)
			secondConn.incoming = bytes.NewReader(replies[17:])
			if err := session.DownloadFrom(second); err != nil {
				t.Fatal(err)
			}
			wantFirstRequest := []byte{0, 0, 0, 13, 6, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4}
			if sent := secondConn.outgoing.Bytes(); len(sent) < 17 || !bytes.Equal(sent[:17], wantFirstRequest) {
				t.Errorf("replacement did not restart unfinished piece one at offset zero: %x", sent)
			}
			if !session.Complete() || len(dst.writes) != 3 {
				t.Fatal("replacement did not complete each piece exactly once")
			}
			for i, write := range dst.writes {
				if write.offset != int64(i*4) || !bytes.Equal(write.data, data[i*4:min(i*4+4, len(data))]) {
					t.Errorf("incorrect verified output at write %d: %+v", i, write)
				}
			}
		})
	}
}
