package download

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
)

type workerTestConn struct {
	*pieceTestConn
	closed chan struct{}
	once   sync.Once
}

func (c *workerTestConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

type workerTestFixture struct {
	session *Session
	client  *peer.Client
	conn    *workerTestConn
	data    []byte
	writer  *downloadTestWriter
}

func newWorkerTestFixture(t *testing.T, corruptIndex int) workerTestFixture {
	t.Helper()
	client, tf, conn, data := downloadFixture(t, 4, 8, corruptIndex)
	wrapped := &workerTestConn{pieceTestConn: conn, closed: make(chan struct{})}
	client.Conn = wrapped
	writer := &downloadTestWriter{}
	session, err := NewSession(tf, writer)
	if err != nil {
		t.Fatal(err)
	}
	return workerTestFixture{session, client, wrapped, data, writer}
}

func startPeerWorkerTest(t *testing.T, f workerTestFixture, jobs <-chan workerCommand, results chan<- pieceResult) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.session.runPeerWorker(ctx, 7, f.client, jobs, results)
	}()
	t.Cleanup(func() { cancel(); waitPeerWorkerTest(t, done) })
	return cancel, done
}

func waitPeerWorkerTest(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not exit")
	}
}

func TestPeerWorkerClosedJobsDoesNotDownload(t *testing.T) {
	f := newWorkerTestFixture(t, -1)
	jobs := make(chan workerCommand)
	close(jobs)
	results := make(chan pieceResult, 3)
	_, done := startPeerWorkerTest(t, f, jobs, results)
	waitPeerWorkerTest(t, done)
	if len(results) != 0 || f.conn.outgoing.Len() != 0 {
		t.Errorf("closed jobs produced %d results and %d request bytes; want none", len(results), f.conn.outgoing.Len())
	}
	select {
	case <-f.conn.closed:
	default:
		t.Error("worker did not close its connection")
	}
}

func TestPeerWorkerReportsVerifiedPieces(t *testing.T) {
	f := newWorkerTestFixture(t, -1)
	jobs := make(chan workerCommand, 2)
	jobs <- workerCommand{kind: commandDownload, index: 0}
	jobs <- workerCommand{kind: commandDownload, index: 1}
	results := make(chan pieceResult, 3)
	cancel, done := startPeerWorkerTest(t, f, jobs, results)
	for index := 0; index < 2; index++ {
		select {
		case result := <-results:
			if result.kind != commandDownload || result.workerID != 7 || result.index != index || result.err != nil || !bytes.Equal(result.data, f.data[index*4:(index+1)*4]) {
				t.Fatalf("unexpected result for piece %d: %+v", index, result)
			}
		case <-time.After(time.Second):
			t.Fatal("worker did not report a result")
		}
	}
	cancel()
	waitPeerWorkerTest(t, done)
	if len(f.writer.writes) != 0 || f.session.Complete() || f.session.Stats().Left != 8 {
		t.Error("worker wrote output or marked session progress complete")
	}
	if f.session.Stats().Downloaded != 8 {
		t.Error("worker did not retain downloaded-byte statistics")
	}
	select {
	case _, ok := <-results:
		if !ok {
			t.Error("worker closed the shared result channel")
		} else {
			t.Error("unexpected extra result")
		}
	default:
	}
}

func TestPeerWorkerReportsFailureAndExits(t *testing.T) {
	f := newWorkerTestFixture(t, 0)
	jobs := make(chan workerCommand, 2)
	jobs <- workerCommand{kind: commandDownload, index: 0}
	jobs <- workerCommand{kind: commandDownload, index: 1}
	results := make(chan pieceResult, 2)
	_, done := startPeerWorkerTest(t, f, jobs, results)
	waitPeerWorkerTest(t, done)
	select {
	case result := <-results:
		var peerErr *PeerError
		if result.kind != commandDownload || result.workerID != 7 || result.index != 0 || result.data != nil || !errors.As(result.err, &peerErr) {
			t.Fatalf("unexpected failure result: %+v", result)
		}
	default:
		t.Fatal("worker did not report its failure")
	}
	if len(jobs) != 1 || len(results) != 0 {
		t.Error("worker continued processing jobs after a download failure")
	}
	if len(f.writer.writes) != 0 || f.session.Stats().Left != 8 {
		t.Error("failed worker committed output or completion")
	}
	select {
	case <-f.conn.closed:
	default:
		t.Error("failed worker did not close its connection")
	}
}

func TestPeerWorkerRejectsUnsupportedCommandAndExits(t *testing.T) {
	f := newWorkerTestFixture(t, -1)
	jobs := make(chan workerCommand, 2)
	jobs <- workerCommand{kind: workerCommandKind(255), index: 1}
	jobs <- workerCommand{kind: commandDownload, index: 0}
	results := make(chan pieceResult, 2)
	cancel, done := startPeerWorkerTest(t, f, jobs, results)
	select {
	case result := <-results:
		if result.workerID != 7 || result.index != 1 || result.kind != workerCommandKind(255) || result.err == nil || result.data != nil {
			t.Fatalf("unexpected unsupported-command result: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("unsupported command was not reported")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		cancel()
		waitPeerWorkerTest(t, done)
		t.Fatal("worker continued after an unsupported command")
	}
	if len(jobs) != 1 || len(results) != 0 || f.conn.outgoing.Len() != 0 {
		t.Error("unsupported command did not stop subsequent work without network requests")
	}
}

func TestPeerWorkerCancellationUnblocksResultSend(t *testing.T) {
	f := newWorkerTestFixture(t, -1)
	jobs := make(chan workerCommand, 1)
	jobs <- workerCommand{kind: commandDownload, index: 0}
	results := make(chan pieceResult)
	// Hold the first read until a download is in flight, then allow it to finish
	// and cancel without ever receiving its result.
	readFinished := make(chan struct{})
	gate := &statsGateConn{pieceTestConn: f.conn.pieceTestConn, entered: make(chan struct{}), release: readFinished}
	f.client.Conn = &workerGateTestConn{statsGateConn: gate, closer: f.conn}
	cancel, done := startPeerWorkerTest(t, f, jobs, results)
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start downloading")
	}
	close(readFinished)
	cancel()
	waitPeerWorkerTest(t, done)
}

type workerGateTestConn struct {
	*statsGateConn
	closer *workerTestConn
}

func (c *workerGateTestConn) Close() error { return c.closer.Close() }

type workerIOTestConn struct {
	net.Conn
	readStarted  chan struct{}
	writeStarted chan struct{}
	readOnce     sync.Once
	writeOnce    sync.Once
}

func (c *workerIOTestConn) Read(p []byte) (int, error) {
	c.readOnce.Do(func() { close(c.readStarted) })
	return c.Conn.Read(p)
}

func (c *workerIOTestConn) Write(p []byte) (int, error) {
	c.writeOnce.Do(func() { close(c.writeStarted) })
	return c.Conn.Write(p)
}

func TestPeerWorkerCancellationInterruptsNetworkIO(t *testing.T) {
	for _, operation := range []string{"read", "write"} {
		t.Run(operation, func(t *testing.T) {
			f := newWorkerTestFixture(t, -1)
			local, remote := net.Pipe()
			t.Cleanup(func() { local.Close(); remote.Close() })
			observed := &workerIOTestConn{
				Conn:         local,
				readStarted:  make(chan struct{}),
				writeStarted: make(chan struct{}),
			}
			f.client.Conn = observed
			var remoteRead <-chan error
			if operation == "read" {
				// Consume the request but send no block response, leaving the worker
				// waiting in its network read rather than its job/result channels.
				done := make(chan error, 1)
				remoteRead = done
				go func() {
					var request [17]byte
					_, err := io.ReadFull(remote, request[:])
					done <- err
				}()
			}
			jobs := make(chan workerCommand, 1)
			jobs <- workerCommand{kind: commandDownload, index: 0}
			results := make(chan pieceResult)
			cancel, done := startPeerWorkerTest(t, f, jobs, results)
			started := observed.writeStarted
			if operation == "read" {
				started = observed.readStarted
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("worker never entered network operation")
			}
			cancel()
			// The block operation's normal deadline is thirty seconds. Cancellation
			// must close the connection and return promptly instead.
			waitPeerWorkerTest(t, done)
			if operation == "read" {
				select {
				case err := <-remoteRead:
					if err != nil {
						t.Fatalf("reading the request: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("fake peer did not finish reading the request")
				}
			}
			if len(f.writer.writes) != 0 || f.session.Stats().Left != 8 {
				t.Error("canceled network operation committed output or completion")
			}
		})
	}
}

func TestSendHaveEncodesIndexAndClearsDeadline(t *testing.T) {
	for _, index := range []uint32{0, 0x12345678, 0xffffffff} {
		conn := &availabilityDeadlineConn{pieceTestConn: pieceTestConn{incoming: bytes.NewReader(nil)}}
		client := &peer.Client{Conn: conn}
		before := time.Now()
		if err := sendHave(client, index); err != nil {
			t.Fatal(err)
		}
		after := time.Now()
		want := binary.BigEndian.AppendUint32([]byte{0, 0, 0, 5, 4}, index)
		if !bytes.Equal(conn.outgoing.Bytes(), want) {
			t.Errorf("have for %d = %x, want %x", index, conn.outgoing.Bytes(), want)
		}
		if len(conn.deadlines) != 2 || conn.deadlines[0].Before(before.Add(30*time.Second)) || conn.deadlines[0].After(after.Add(30*time.Second)) || !conn.deadlines[1].IsZero() {
			t.Errorf("deadlines = %v, want thirty seconds followed by cleared deadline", conn.deadlines)
		}
	}
}

func TestSendHavePreservesDeadlineFailures(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		want := errors.New("deadline failed")
		conn := &availabilityDeadlineConn{
			pieceTestConn: pieceTestConn{incoming: bytes.NewReader(nil)},
			failAt:        failAt,
			err:           want,
		}
		err := sendHave(&peer.Client{Conn: conn}, 3)
		var peerErr *PeerError
		if !errors.Is(err, want) || !errors.As(err, &peerErr) || !strings.Contains(peerErr.Op, "piece 3") {
			t.Fatalf("error = %v, want PeerError preserving cause and piece index", err)
		}
		wantBytes := 0
		if failAt == 2 {
			wantBytes = 9
		}
		if conn.outgoing.Len() != wantBytes || len(conn.deadlines) != failAt {
			t.Errorf("continued past failed deadline operation: bytes=%d, deadlines=%d", conn.outgoing.Len(), len(conn.deadlines))
		}
	}
}

func TestSendHavePreservesWriteFailure(t *testing.T) {
	want := errors.New("connection write failed")
	conn := &sessionHaveFailureConn{
		pieceTestConn: &pieceTestConn{incoming: bytes.NewReader(nil)},
		err:           want,
	}
	err := sendHave(&peer.Client{Conn: conn}, 3)
	var peerErr *PeerError
	if !errors.Is(err, want) || !errors.As(err, &peerErr) || !strings.Contains(peerErr.Op, "piece 3") {
		t.Fatalf("error = %v, want PeerError preserving write failure and piece index", err)
	}
}

func TestPeerWorkerHaveCommand(t *testing.T) {
	for _, index := range []int{0, 1} {
		f := newWorkerTestFixture(t, -1)
		before := f.session.Stats()
		jobs := make(chan workerCommand, 1)
		jobs <- workerCommand{kind: commandHave, index: index}
		close(jobs)
		results := make(chan pieceResult, 1)
		_, done := startPeerWorkerTest(t, f, jobs, results)
		waitPeerWorkerTest(t, done)
		select {
		case result := <-results:
			if result.workerID != 7 || result.kind != commandHave || result.index != index || result.data != nil || result.err != nil {
				t.Fatalf("unexpected have result: %+v", result)
			}
		default:
			t.Fatal("worker did not acknowledge have command")
		}
		want := binary.BigEndian.AppendUint32([]byte{0, 0, 0, 5, 4}, uint32(index))
		if !bytes.Equal(f.conn.outgoing.Bytes(), want) {
			t.Errorf("have bytes = %x, want %x", f.conn.outgoing.Bytes(), want)
		}
		if f.session.Stats() != before || f.session.completedCount != 0 || len(f.writer.writes) != 0 {
			t.Error("have command changed statistics, completion, or stored output")
		}
	}
}

func TestPeerWorkerHaveRejectsInvalidIndex(t *testing.T) {
	for _, index := range []int{-1, 2, 3} {
		f := newWorkerTestFixture(t, -1)
		jobs := make(chan workerCommand, 2)
		jobs <- workerCommand{kind: commandHave, index: index}
		jobs <- workerCommand{kind: commandDownload, index: 0}
		close(jobs)
		results := make(chan pieceResult, 2)
		_, done := startPeerWorkerTest(t, f, jobs, results)
		waitPeerWorkerTest(t, done)
		select {
		case result := <-results:
			if result.workerID != 7 || result.kind != commandHave || result.index != index || result.data != nil || result.err == nil {
				t.Fatalf("invalid index %d produced result %+v", index, result)
			}
		default:
			t.Fatal("invalid have index was not reported")
		}
		if len(jobs) != 1 || len(results) != 0 || f.conn.outgoing.Len() != 0 {
			t.Error("invalid have index reached the network or allowed further jobs")
		}
	}
}

type workerWriteFailureConn struct {
	*workerTestConn
	err error
}

func (c *workerWriteFailureConn) Write([]byte) (int, error) { return 0, c.err }

func TestPeerWorkerHaveReportsNetworkFailureAndExits(t *testing.T) {
	f := newWorkerTestFixture(t, -1)
	want := errors.New("have send failed")
	f.client.Conn = &workerWriteFailureConn{workerTestConn: f.conn, err: want}
	jobs := make(chan workerCommand, 2)
	jobs <- workerCommand{kind: commandHave, index: 1}
	jobs <- workerCommand{kind: commandDownload, index: 0}
	results := make(chan pieceResult, 2)
	_, done := startPeerWorkerTest(t, f, jobs, results)
	waitPeerWorkerTest(t, done)
	select {
	case result := <-results:
		var peerErr *PeerError
		if result.workerID != 7 || result.kind != commandHave || result.index != 1 || result.data != nil || !errors.Is(result.err, want) || !errors.As(result.err, &peerErr) {
			t.Fatalf("unexpected failed-have result: %+v", result)
		}
	default:
		t.Fatal("worker did not report have failure")
	}
	if len(jobs) != 1 || len(results) != 0 {
		t.Error("worker continued after have failure")
	}
	if f.session.Stats().Left != 8 || f.session.Stats().Downloaded != 0 || len(f.writer.writes) != 0 {
		t.Error("failed have command changed session progress")
	}
}
