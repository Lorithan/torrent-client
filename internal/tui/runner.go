package tui

import (
	"context"
	"sync"
)

type managedRunner struct {
	Runner
	once   sync.Once
	runErr error
}

func (r *managedRunner) Run(ctx context.Context) error {
	r.once.Do(func() {
		r.runErr = r.Runner.Run(ctx)
	})

	return r.runErr
}
