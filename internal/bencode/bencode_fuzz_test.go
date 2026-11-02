package bencode

import (
	"reflect"
	"strconv"
	"testing"
)

func FuzzDecode(f *testing.F) {
	for _, seed := range []string{
		"", "i0e", "i-42e", "i9223372036854775807e", "i-9223372036854775808e",
		"0:", "4:spam", "3:\x00\xffe", "le", "de", "li42e4:spame",
		"d8:intervali30e5:peers6:\x7f\x00\x00\x01\x1a\xe1e",
		"lld1:ali1eeeeee", "i", "i-", "i42", "4", "5:spam",
		"9223372036854775807:", "d8:intervali30", "d5:peers6",
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Panics fail the fuzz target automatically, including on malformed input.
		value, err := Decode(data)
		if err != nil {
			return
		}

		decoded, end, err := DecodeAny(data, 0)
		if err != nil || !reflect.DeepEqual(decoded, value) {
			t.Fatalf("Decode and DecodeAny disagree: value=%#v, decoded=%#v, err=%v", value, decoded, err)
		}
		if end <= 0 || end > len(data) {
			t.Fatalf("successful decode ended at %d for %d bytes", end, len(data))
		}

		// A value must be self-contained within the bytes the decoder consumed.
		decoded, consumed, err := DecodeAny(data[:end], 0)
		if err != nil || consumed != end || !reflect.DeepEqual(decoded, value) {
			t.Fatalf("consumed prefix does not decode independently: end=%d, consumed=%d, err=%v", end, consumed, err)
		}

		// Recursive callers start at nonzero positions; preceding bytes must not
		// affect either the decoded value or the number of bytes consumed.
		prefix := []byte{0xff, 'e', 0, ':'}
		prefixed := append(prefix, data...)
		decoded, shiftedEnd, err := DecodeAny(prefixed, len(prefix))
		if err != nil || shiftedEnd != len(prefix)+end || !reflect.DeepEqual(decoded, value) {
			t.Fatalf("nonzero starting position changed the decode: end=%d, shiftedEnd=%d, err=%v", end, shiftedEnd, err)
		}
	})
}

func FuzzDecodeNestedValues(f *testing.F) {
	f.Add([]byte("spam"), int64(42))
	f.Add([]byte{}, int64(0))
	f.Add([]byte{0, 0xff, ':', 'i', 'e', 'l', 'd'}, int64(-42))
	f.Add([]byte("binary"), int64(9223372036854775807))
	f.Add([]byte("binary"), int64(-9223372036854775808))

	f.Fuzz(func(t *testing.T, payload []byte, number int64) {
		// Construct a valid list containing a dictionary. Payload bytes may look
		// like delimiters or contain invalid UTF-8; their declared length rules.
		encoded := []byte("ld4:data" + strconv.Itoa(len(payload)) + ":")
		encoded = append(encoded, payload...)
		encoded = append(encoded, []byte("6:numberi"+strconv.FormatInt(number, 10)+"eee")...)
		want := []any{map[string]any{"data": string(payload), "number": number}}

		got, end, err := DecodeAny(encoded, 0)
		if err != nil {
			t.Fatalf("valid nested input rejected: %v", err)
		}
		if end != len(encoded) || !reflect.DeepEqual(got, want) {
			t.Fatalf("decoded %#v ending at %d; want %#v ending at %d", got, end, want, len(encoded))
		}
	})
}
