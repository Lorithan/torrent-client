package peer

import (
	"bytes"
	"testing"
)

func TestNewHave(t *testing.T) {
	for _, tt := range []struct {
		name    string
		index   uint32
		payload []byte
	}{
		{"zero", 0, []byte{0, 0, 0, 0}},
		{"multibyte", 0x01020304, []byte{1, 2, 3, 4}},
		{"maximum", ^uint32(0), []byte{255, 255, 255, 255}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := NewHave(tt.index)
			if m.ID != MsgHave || !bytes.Equal(m.Payload, tt.payload) {
				t.Errorf("message = %+v, want have with payload %x", m, tt.payload)
			}
			want := append([]byte{0, 0, 0, 5, 4}, tt.payload...)
			if got := m.Serialize(); !bytes.Equal(got, want) {
				t.Errorf("wire = %x, want %x", got, want)
			}
		})
	}
}
