package peer

import (
	"bytes"
	"testing"
)

func TestClientHandleMessage(t *testing.T) {
	for _, tt := range []struct {
		name          string
		message       *Message
		initialChoked bool
		wantChoked    bool
		wantBits      Bitfield
		wantErr       bool
	}{
		{"keep alive", nil, true, true, Bitfield{0x80, 0}, false},
		{"choke", &Message{ID: MsgChoke}, false, true, Bitfield{0x80, 0}, false},
		{"unchoke", &Message{ID: MsgUnchoke}, true, false, Bitfield{0x80, 0}, false},
		{"have", &Message{ID: MsgHave, Payload: []byte{0, 0, 0, 9}}, true, true, Bitfield{0x80, 0x40}, false},
		{"bitfield", &Message{ID: MsgBitfield, Payload: []byte{0x20, 0x80}}, true, true, Bitfield{0x20, 0x80}, false},
		{"malformed choke", &Message{ID: MsgChoke, Payload: []byte{1}}, false, false, Bitfield{0x80, 0}, true},
		{"malformed unchoke", &Message{ID: MsgUnchoke, Payload: []byte{1}}, true, true, Bitfield{0x80, 0}, true},
		{"short have", &Message{ID: MsgHave, Payload: []byte{0}}, true, true, Bitfield{0x80, 0}, true},
		{"out of range have", &Message{ID: MsgHave, Payload: []byte{0, 0, 0, 10}}, true, true, Bitfield{0x80, 0}, true},
		{"wrong bitfield length", &Message{ID: MsgBitfield, Payload: []byte{0x80}}, true, true, Bitfield{0x80, 0}, true},
		{"nonzero padding", &Message{ID: MsgBitfield, Payload: []byte{0x80, 1}}, true, true, Bitfield{0x80, 0}, true},
		{"unhandled message", &Message{ID: MsgInterested}, true, true, Bitfield{0x80, 0}, false},
		{"unknown ID", &Message{ID: MessageID(255)}, true, true, Bitfield{0x80, 0}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{PieceCount: 10, Choked: tt.initialChoked, Bitfield: Bitfield{0x80, 0}}
			var before []byte
			if tt.message != nil {
				before = append([]byte(nil), tt.message.Payload...)
			}
			err := c.HandleMessage(tt.message)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if c.Choked != tt.wantChoked || !bytes.Equal(c.Bitfield, tt.wantBits) {
				t.Errorf("state: choked %v, bits %x; want choked %v, bits %x", c.Choked, c.Bitfield, tt.wantChoked, tt.wantBits)
			}
			if tt.message != nil && !bytes.Equal(tt.message.Payload, before) {
				t.Error("handler modified message payload")
			}
		})
	}
}

func TestClientHandleBitfieldCopiesPayload(t *testing.T) {
	for _, name := range []string{"payload mutation", "client mutation"} {
		t.Run(name, func(t *testing.T) {
			c := &Client{PieceCount: 10, Bitfield: make(Bitfield, 2)}
			m := &Message{ID: MsgBitfield, Payload: []byte{0x80, 0}}
			if err := c.HandleMessage(m); err != nil {
				t.Fatal(err)
			}
			if name == "payload mutation" {
				m.Payload[0] = 0
				if !c.Bitfield.HasPiece(0) {
					t.Error("message payload mutation changed client bitfield")
				}
			} else {
				if err := c.HandleMessage(&Message{ID: MsgHave, Payload: []byte{0, 0, 0, 9}}); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(m.Payload, []byte{0x80, 0}) {
					t.Error("have update changed the earlier bitfield message payload")
				}
			}
		})
	}
}

func TestClientHandleHaveReportsSetPieceFailure(t *testing.T) {
	// Client fields are exported, so inconsistent state can currently be constructed.
	c := &Client{PieceCount: 10, Bitfield: Bitfield{0x80}}
	err := c.HandleMessage(&Message{ID: MsgHave, Payload: []byte{0, 0, 0, 9}})
	if err == nil {
		t.Error("handler hid SetPiece's out-of-bounds error")
	}
	if !bytes.Equal(c.Bitfield, []byte{0x80}) {
		t.Error("failed update changed the bitfield")
	}
}
