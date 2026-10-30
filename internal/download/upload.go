package download

import (
	"context"
	"fmt"
	"time"

	"github.com/lorithan/torrent-client/internal/peer"
)

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
		cancelled := ctx.Err()
		if cancelled != nil {
			return cancelled
		}
		return err
	}
	availability := s.Availability()
	err = client.SendBitfield(availability)
	if err != nil {
		cancelled := ctx.Err()
		if cancelled != nil {
			return cancelled
		}
		return err
	}

	for {
		cancelled := ctx.Err()
		if cancelled != nil {
			return cancelled
		}

		err := client.Conn.SetReadDeadline(time.Now().Add(3 * time.Minute))
		if err != nil {
			cancelled := ctx.Err()
			if cancelled != nil {
				return cancelled
			}
			return err
		}
		msg, err := peer.ReadMessage(client.Conn)
		if err != nil {
			cancelled := ctx.Err()
			if cancelled != nil {
				return cancelled
			}
			return err
		}

		err = client.Conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if err != nil {
			cancelled := ctx.Err()
			if cancelled != nil {
				return cancelled
			}
			return err
		}
		err = s.handleUploadMessage(client, msg)
		if err != nil {
			cancelled := ctx.Err()
			if cancelled != nil {
				return cancelled
			}
			return err
		}
	}
}
