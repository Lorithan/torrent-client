package download

import "fmt"

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
