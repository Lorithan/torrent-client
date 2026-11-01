package tui

import "fmt"

var byteLabels = [...]string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

func formatBytes(value float64) string {
	if value <= 0 {
		return "0 B"
	}

	label := 0

	for value >= 1024 && label < len(byteLabels)-1 {
		value = value / 1024
		label++
	}

	if label == 0 {
		return fmt.Sprintf("%.0f %s", value, byteLabels[label])
	}

	return fmt.Sprintf("%.1f %s", value, byteLabels[label])
}

func progressPercent(verified, total int64) float64 {
	if total <= 0 {
		return 0
	}

	percent := float64(verified) / float64(total) * 100

	return max(0, min(100, percent))
}
