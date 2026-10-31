package download

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
)

type uploadReadResult struct {
	message *peer.Message
	err     error
}

func (s *Session) UploadBlock(client *peer.Client, request peer.BlockRequest) error {
	if client.AmChoking {
		return fmt.Errorf("currently choking peer")
	}
	if !client.PeerInterested {
		return fmt.Errorf("peer currently uninterested")
	}

	data, err := s.ReadBlock(request)
	if err != nil {
		return err
	}

	msg, err := peer.NewPiece(request.Index, request.Begin, data)
	if err != nil {
		return err
	}

	err = client.SendMessage(*msg)
	if err != nil {
		return err
	}

	s.stateMu.Lock()
	s.stats.Uploaded += int64(len(data))
	s.stateMu.Unlock()

	return nil
}

func readUploadMessages(
	ctx context.Context,
	conn net.Conn,
	results chan<- uploadReadResult,
) {
	for {
		if cancelled := ctx.Err(); cancelled != nil {
			return
		}

		err := conn.SetReadDeadline(time.Now().Add(3 * time.Minute))
		if err != nil {
			uploadResult := uploadReadResult{
				err: err,
			}
			select {
			case <-ctx.Done():
				return
			case results <- uploadResult:
			}
			return
		}

		msg, err := peer.ReadMessage(conn)
		uploadResult := uploadReadResult{
			message: msg,
			err:     err,
		}
		select {
		case <-ctx.Done():
			return
		case results <- uploadResult:
			if err != nil {
				return
			}
		}
	}
}

func (s *Session) handleUploadMessage(client *peer.Client, msg *peer.Message) error {
	if msg == nil {
		return nil
	}

	switch msg.ID {
	case peer.MsgRequest:
		request, err := msg.BlockRequest()
		if err != nil {
			return err
		}
		return s.UploadBlock(client, *request)
	case peer.MsgInterested:
		err := client.HandleMessage(msg)
		if err != nil {
			return err
		}
		return client.SetChoking(false)
	case peer.MsgNotInterested:
		err := client.HandleMessage(msg)
		if err != nil {
			return err
		}
		return client.SetChoking(true)
	default:
		return client.HandleMessage(msg)
	}
}

func (s *Session) ServePeer(ctx context.Context, client *peer.Client) error {
	defer client.Conn.Close()

	stop := context.AfterFunc(ctx, func() {
		client.Conn.Close()
	})
	defer stop()

	cancelled := ctx.Err()
	if cancelled != nil {
		return cancelled
	}

	err := client.Conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		return cancelledOrErr(ctx, err)
	}
	availability, availabilityChan := s.availabilitySnapshot()
	err = client.SendBitfield(availability)
	if err != nil {
		return cancelledOrErr(ctx, err)
	}

	readerCtx, readerCancel := context.WithCancel(ctx)
	results := make(chan uploadReadResult)
	readerDone := make(chan struct{})

	go func() {
		defer close(readerDone)
		readUploadMessages(readerCtx, client.Conn, results)
	}()

	cleanup := func() {
		readerCancel()
		client.Conn.Close()
		<-readerDone
	}
	defer cleanup()

	for {
		cancelled := ctx.Err()
		if cancelled != nil {
			return cancelled
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-results:
			if result.err != nil {
				return cancelledOrErr(ctx, result.err)
			}

			err = client.Conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
			if err != nil {
				return cancelledOrErr(ctx, err)
			}

			err = s.handleUploadMessage(client, result.message)
			if err != nil {
				return cancelledOrErr(ctx, err)
			}
		case <-availabilityChan:
			cancelled := ctx.Err()
			if cancelled != nil {
				return cancelled
			}
			newAvailability, newChannel := s.availabilitySnapshot()

			err := s.sendAvailabilityUpdates(client, availability, newAvailability)
			if err != nil {
				return cancelledOrErr(ctx, err)
			}

			availability = newAvailability
			availabilityChan = newChannel
		}
	}
}

func (s *Session) sendAvailabilityUpdates(
	client *peer.Client,
	advertised peer.Bitfield,
	available peer.Bitfield,
) error {
	err := client.Conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		return err
	}

	for index := range len(s.torrent.Pieces) {
		if available.HasPiece(index) && !advertised.HasPiece(index) {
			msg := peer.NewHave(uint32(index))

			err = client.SendMessage(msg)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
