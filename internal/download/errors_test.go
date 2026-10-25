package download

import (
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestPeerError(t *testing.T) {
	cause := io.ErrUnexpectedEOF
	err := &PeerError{Op: "read block for piece 12 at offset 16384", Err: cause}
	want := "read block for piece 12 at offset 16384: unexpected EOF"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if got := err.Unwrap(); got != cause {
		t.Errorf("Unwrap() = %v, want original cause", got)
	}
	if !errors.Is(err, cause) {
		t.Error("errors.Is could not find the underlying cause")
	}
	// Additional context must preserve both the classification and the cause.
	outer := fmt.Errorf("download attempt: %w", err)
	var classified *PeerError
	if !errors.As(outer, &classified) || classified != err {
		t.Error("errors.As could not find the original PeerError through wrapping")
	}
	if !errors.Is(outer, cause) {
		t.Error("additional wrapping hid the underlying cause")
	}
}

func TestPeerErrorDistinguishesClassificationFromText(t *testing.T) {
	err := errors.New("read block: unexpected EOF")
	var classified *PeerError
	if errors.As(err, &classified) {
		t.Error("ordinary error was classified as a PeerError based on its text")
	}
}
