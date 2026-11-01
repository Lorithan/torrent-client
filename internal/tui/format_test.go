package tui

import (
	"math"
	"testing"
)

func TestProgressPercent(t *testing.T) {
	for _, tc := range []struct {
		name            string
		verified, total int64
		want            float64
	}{
		{"not started", 0, 8, 0},
		{"quarter", 1, 4, 25},
		{"fractional", 1, 3, 100.0 / 3},
		{"complete", 8, 8, 100},
		{"zero total", 0, 0, 0},
		{"positive bytes with zero total", 4, 0, 0},
		{"negative total", 4, -1, 0},
		{"negative verified", -1, 8, 0},
		{"over complete", 9, 8, 100},
		{"large counters", math.MaxInt64 / 2, math.MaxInt64, 50},
		{"largest complete counters", math.MaxInt64, math.MaxInt64, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := progressPercent(tc.verified, tc.total); math.IsNaN(got) || math.IsInf(got, 0) || math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("progressPercent(%d, %d)=%g, want %g", tc.verified, tc.total, got, tc.want)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value float64
		want  string
	}{
		{"zero", 0, "0 B"},
		{"negative", -1024, "0 B"},
		{"bytes", 512, "512 B"},
		{"rounded bytes", 512.6, "513 B"},
		{"below KiB", 1023, "1023 B"},
		{"KiB", 1024, "1.0 KiB"},
		{"fractional KiB", 1536, "1.5 KiB"},
		{"MiB", 1 << 20, "1.0 MiB"},
		{"GiB", 1 << 30, "1.0 GiB"},
		{"TiB", 1 << 40, "1.0 TiB"},
		{"PiB", 1 << 50, "1.0 PiB"},
		{"EiB", 1 << 60, "1.0 EiB"},
		{"largest unit is capped", 1 << 70, "1024.0 EiB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if err := recover(); err != nil {
					t.Errorf("formatBytes(%g) panicked: %v", tc.value, err)
				}
			}()
			if got := formatBytes(tc.value); got != tc.want {
				t.Errorf("formatBytes(%g)=%q, want %q", tc.value, got, tc.want)
			}
		})
	}
}
