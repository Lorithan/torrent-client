package tracker

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"
)

// A custom transport lets these tests supply responses without network access.
type announceTestTransport func(*http.Request) (*http.Response, error)

func (f announceTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type announceTestBody struct {
	io.Reader
	closed bool
}

func (b *announceTestBody) Close() error {
	b.closed = true
	return nil
}

type announceErrorReader struct{ err error }

func (r announceErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestGetAnnounceResponse(t *testing.T) {
	readErr := errors.New("body read failed")
	tests := []struct {
		name    string
		status  int
		data    []byte
		readErr error
		wantErr bool
	}{
		{name: "response bytes", status: http.StatusOK, data: []byte{'d', 0, 0xff, 'e'}},
		{name: "empty body", status: http.StatusOK},
		{name: "exact maximum", status: http.StatusOK, data: bytes.Repeat([]byte{'x'}, MAX_RESPONSE_SIZE)},
		{name: "one byte over maximum", status: http.StatusOK, data: bytes.Repeat([]byte{'x'}, MAX_RESPONSE_SIZE+1), wantErr: true},
		{name: "well over maximum", status: http.StatusOK, data: bytes.Repeat([]byte{'x'}, MAX_RESPONSE_SIZE*2), wantErr: true},
		{name: "HTTP failure", status: http.StatusServiceUnavailable, wantErr: true},
		{name: "read failure", status: http.StatusOK, readErr: readErr, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := bytes.NewReader(tt.data)
			body := &announceTestBody{Reader: source}
			if tt.readErr != nil {
				body.Reader = announceErrorReader{err: tt.readErr}
			}
			client := &http.Client{Transport: announceTestTransport(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet || req.URL.String() != "https://tracker.example/announce?compact=1" {
					t.Errorf("unexpected request: %s %s", req.Method, req.URL)
				}
				return &http.Response{
					StatusCode: tt.status,
					Status:     http.StatusText(tt.status),
					Body:       body,
					Header:     make(http.Header),
				}, nil
			})}

			got, err := getAnnounceResponse("https://tracker.example/announce?compact=1", client)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.readErr != nil && !errors.Is(err, tt.readErr) {
				t.Errorf("error = %v, want underlying read error %v", err, tt.readErr)
			}
			if !tt.wantErr && !bytes.Equal(got, tt.data) {
				t.Errorf("response bytes differ: got %d bytes, want %d", len(got), len(tt.data))
			}
			if !body.closed {
				t.Error("response body was not closed")
			}
			if consumed := len(tt.data) - source.Len(); consumed > MAX_RESPONSE_SIZE+1 {
				t.Errorf("read %d bytes, exceeding detection limit", consumed)
			}
		})
	}
}

func TestGetAnnounceResponseRequestError(t *testing.T) {
	wantErr := errors.New("connection failed")
	client := &http.Client{Transport: announceTestTransport(func(*http.Request) (*http.Response, error) {
		return nil, wantErr
	})}
	got, err := getAnnounceResponse("https://tracker.example/announce", client)
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want underlying request error %v", err, wantErr)
	}
	if got != nil {
		t.Error("expected nil bytes on request failure")
	}
}
