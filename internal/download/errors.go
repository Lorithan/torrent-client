package download

import (
	"context"
	"fmt"
)

type PeerError struct {
	Op  string
	Err error
}

func (e *PeerError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *PeerError) Unwrap() error {
	return e.Err
}

func cancelledOrErr(ctx context.Context, err error) error {
	cancelled := ctx.Err()
	if cancelled != nil {
		return cancelled
	}
	return err
}
