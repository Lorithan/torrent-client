package bencode

import (
	"fmt"
	"strconv"
	"strings"
)

func Decode(input []byte) (any, error) {
	val, _, err := DecodeAny(input, 0)
	return val, err
}

func DecodeAny(data []byte, pos int) (any, int, error) {
	if pos >= len(data) {
		return nil, pos, fmt.Errorf("Unexpected end of data")
	}

	switch {
	case data[pos] == 'i':
		return DecodeInt(data, pos)
	case data[pos] >= '0' && data[pos] <= '9':
		return DecodeStr(data, pos)
	case data[pos] == 'l':
		return DecodeList(data, pos)
	case data[pos] == 'd':
		return DecodeDict(data, pos)
	default:
		return nil, pos, fmt.Errorf("unknown type at position %d", pos)
	}
}

func DecodeInt(data []byte, pos int) (int64, int, error) {
	// move past the 'i'
	pos += 1

	var strBuilder strings.Builder
	// check for leading '-' for negative values
	if data[pos] == '-' {
		strBuilder.WriteByte(data[pos])
		pos += 1
	}
	// Loop through while there are numbers to add
	for {
		if data[pos] < '0' || data[pos] > '9' {
			break
		}

		strBuilder.WriteByte(data[pos])
		pos += 1
	}

	// check if the int terminated correctly, then move past the 'e'
	if data[pos] != 'e' {
		return 0, pos, fmt.Errorf("integer types must terminate with a 'e', found %c", data[pos])
	}
	pos += 1

	// Parse int, pass along the error if needed
	var val, err = strconv.ParseInt(strBuilder.String(), 10, 64)
	if err != nil {
		return 0, pos, err
	}

	return val, pos, nil
}

func DecodeStr(data []byte, pos int) (string, int, error) {
	// Obtain the length of the string
	var strBuilder strings.Builder
	for {
		if data[pos] < '0' || data[pos] > '9' {
			break
		}

		strBuilder.WriteByte(data[pos])
		pos += 1
	}

	if data[pos] != ':' {
		return "", pos, fmt.Errorf("expected ':', found %c", data[pos])
	}
	pos += 1

	var length, err = strconv.ParseInt(strBuilder.String(), 10, 64)
	if err != nil {
		return "", pos, err
	}

	if pos+int(length) > len(data) {
		return "", pos, fmt.Errorf("unexpected end of data")
	}

	var newPos = int64(pos) + length

	var s = string(data[pos:newPos])

	return s, int(newPos), nil
}

func DecodeList(data []byte, pos int) ([]any, int, error) {
	var result []any
	pos += 1

	for {
		if pos >= len(data) {
			return nil, pos, fmt.Errorf("unexpected end of data")
		}
		if data[pos] == 'e' {
			pos += 1
			break
		}
		val, newPos, err := DecodeAny(data, pos)

		if err != nil {
			return nil, newPos, err
		}

		if newPos > len(data) {
			return nil, newPos, fmt.Errorf("unexpected end of data")
		}

		result = append(result, val)
		pos = newPos
	}

	return result, pos, nil
}

func DecodeDict(data []byte, pos int) (map[string]any, int, error) {
	result := make(map[string]any)
	pos += 1

	for {
		if pos >= len(data) {
			return nil, pos, fmt.Errorf("unexpected end of data")
		}
		if data[pos] == 'e' {
			pos += 1
			break
		}

		if data[pos] < '0' || data[pos] > '9' {
			return nil, pos, fmt.Errorf("dictionary keys must be strings")
		}

		key, newPos, err := DecodeStr(data, pos)
		if err != nil {
			return nil, newPos, err
		}

		val, newPos, err := DecodeAny(data, newPos)
		if err != nil {
			return nil, newPos, err
		}

		if newPos > len(data) {
			return nil, newPos, fmt.Errorf("unexpected termination of dictionary")
		}

		result[key] = val
		pos = newPos
	}

	return result, pos, nil
}
