package download

type Progress struct {
	Name            string
	TotalBytes      int64
	VerifiedBytes   int64
	CompletedPieces int
	TotalPieces     int
	Stats           Stats
}

func (s *Session) Progress() Progress {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	return Progress{
		Name:            s.torrent.Name,
		TotalBytes:      s.torrent.Length,
		VerifiedBytes:   s.torrent.Length - s.stats.Left,
		CompletedPieces: s.completedCount,
		TotalPieces:     len(s.torrent.Pieces),
		Stats:           s.stats,
	}
}
