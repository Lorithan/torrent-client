package download

import (
	"github.com/lorithan/torrent-client/internal/peer"
	"github.com/lorithan/torrent-client/internal/torrent"
)

func Download(client *peer.Client, tf *torrent.TorrentFile, dst Storage) error {
	session, err := NewSession(tf, dst)
	if err != nil {
		return err
	}

	return session.DownloadFrom(client)
}
