package bencode

import "testing"

func TestDecodeInt(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantVal int64
		wantPos int
		wantErr bool
	}{
		{name: "positive integer", input: "i42e", wantVal: 42, wantPos: 4},
		{name: "negative integer", input: "i-42e", wantVal: -42, wantPos: 5},
		{name: "zero integer", input: "i0e", wantVal: 0, wantPos: 3},
		{name: "invalid integer terminator", input: "i2be", wantErr: true},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, pos, err := DecodeInt([]byte(tt.input), 0)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error for input %q", tt.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("decodeInt(%q) returned unexpected error: %v", tt.input, err)
			}
			if got != tt.wantVal {
				t.Fatalf("decodeInt(%q) = %d, want %d", tt.input, got, tt.wantVal)
			}
			if pos != tt.wantPos {
				t.Fatalf("decodeInt(%q) end position = %d, want %d", tt.input, pos, tt.wantPos)
			}
		})
	}
}

func TestDecodeStr(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantVal string
		wantPos int
		wantErr bool
	}{
		{name: "regular string", input: "4:spam", wantVal: "spam", wantPos: 6},
		{name: "empty string", input: "0:", wantVal: "", wantPos: 2},
		{name: "truncated string payload", input: "5:spam", wantErr: true},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, pos, err := DecodeStr([]byte(tt.input), 0)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error for input %q", tt.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("decodeStr(%q) returned unexpected error: %v", tt.input, err)
			}
			if got != tt.wantVal {
				t.Fatalf("decodeStr(%q) = %q, want %q", tt.input, got, tt.wantVal)
			}
			if pos != tt.wantPos {
				t.Fatalf("decodeStr(%q) end position = %d, want %d", tt.input, pos, tt.wantPos)
			}
		})
	}
}

func TestDecodeList(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantLen int
		wantPos int
		assert  func(t *testing.T, got []any)
	}{
		{
			name:    "empty list",
			input:   "le",
			wantLen: 0,
			wantPos: 2,
			assert: func(t *testing.T, got []any) {
				if len(got) != 0 {
					t.Fatalf("got %v", got)
				}
			},
		},
		{
			name:    "string list",
			input:   "l4:spam4:eggse",
			wantLen: 2,
			wantPos: 14,
			assert: func(t *testing.T, got []any) {
				if len(got) != 2 {
					t.Fatalf("len = %d, want 2", len(got))
				}
				first, ok := got[0].(string)
				if !ok || first != "spam" {
					t.Fatalf("first element = %#v, want %q", got[0], "spam")
				}
				second, ok := got[1].(string)
				if !ok || second != "eggs" {
					t.Fatalf("second element = %#v, want %q", got[1], "eggs")
				}
			},
		},
		{
			name:    "nested list",
			input:   "ll4:spamee",
			wantLen: 1,
			wantPos: 10,
			assert: func(t *testing.T, got []any) {
				if len(got) != 1 {
					t.Fatalf("len = %d, want 1", len(got))
				}
				inner, ok := got[0].([]any)
				if !ok || len(inner) != 1 {
					t.Fatalf("inner = %#v, want a single-element list", got[0])
				}
				innerVal, ok := inner[0].(string)
				if !ok || innerVal != "spam" {
					t.Fatalf("inner[0] = %#v, want %q", inner[0], "spam")
				}
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, pos, err := DecodeList([]byte(tt.input), 0)
			if err != nil {
				t.Fatalf("decodeList(%q) error: %v", tt.input, err)
			}
			if pos != tt.wantPos {
				t.Fatalf("decodeList(%q) pos = %d, want %d", tt.input, pos, tt.wantPos)
			}
			if len(got) != tt.wantLen {
				t.Fatalf("decodeList(%q) len = %d, want %d", tt.input, len(got), tt.wantLen)
			}
			tt.assert(t, got)
		})
	}

	_, _, err := DecodeList([]byte("l4:spam"), 0)
	if err == nil {
		t.Fatal("expected malformed list to error")
	}
}

func TestDecodeDict(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantLen int
		wantPos int
		assert  func(t *testing.T, got map[string]any)
	}{
		{
			name:    "empty dict",
			input:   "de",
			wantLen: 0,
			wantPos: 2,
			assert: func(t *testing.T, got map[string]any) {
				if len(got) != 0 {
					t.Fatalf("got %v", got)
				}
			},
		},
		{
			name:    "simple dict",
			input:   "d3:cow3:moo4:spam4:eggse",
			wantLen: 2,
			wantPos: 24,
			assert: func(t *testing.T, got map[string]any) {
				if len(got) != 2 {
					t.Fatalf("len = %d, want 2", len(got))
				}
				cow, ok := got["cow"].(string)
				if !ok || cow != "moo" {
					t.Fatalf("got cow = %#v, want %q", got["cow"], "moo")
				}
				spam, ok := got["spam"].(string)
				if !ok || spam != "eggs" {
					t.Fatalf("got spam = %#v, want %q", got["spam"], "eggs")
				}
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, pos, err := DecodeDict([]byte(tt.input), 0)
			if err != nil {
				t.Fatalf("decodeDict(%q) error: %v", tt.input, err)
			}
			if pos != tt.wantPos {
				t.Fatalf("decodeDict(%q) pos = %d, want %d", tt.input, pos, tt.wantPos)
			}
			if len(got) != tt.wantLen {
				t.Fatalf("decodeDict(%q) len = %d, want %d", tt.input, len(got), tt.wantLen)
			}
			tt.assert(t, got)
		})
	}

	_, _, err := DecodeDict([]byte("d3:abce"), 0)
	if err == nil {
		t.Fatal("expected malformed dict to error")
	}
}
