package torrent

import (
	"testing"
)

func TestParseTorrentFile(t *testing.T) {
	tests := []struct {
		name      string
		inputData []byte
		wantErr   bool
	}{
		{
			name:      "Valid simple .torrent file",
			inputData: []byte("d8:announce18:http://example.com13:creation datei1719008400e4:infod6:lengthi10485760e4:name10:sample.mp412:piece lengthi16384e6:pieces20:abcdefghijklmnopqrstee"),
			wantErr:   false,
		},
		{
			name:      "Missing piece length field",
			inputData: []byte("d8:announce18:http://example.com13:creation datei1719008400e4:infod6:lengthi10485760e4:name10:sample.mp46:pieces20:abcdefghijklmnopqrstee"),
			wantErr:   true,
		},
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseTorrentFile(tt.inputData)
			if (err != nil && !tt.wantErr) || (err == nil && tt.wantErr) {
				t.Errorf("ParseTorrentFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
