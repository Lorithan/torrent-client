package tui

import (
	"context"

	tea "charm.land/bubbletea/v2"
)

func Run(
	ctx context.Context,
	runner Runner,
	opts ...tea.ProgramOption,
) error {
	runner = &managedRunner{
		Runner: runner,
	}

	model := New(ctx, runner)
	defer model.cancel()

	opts = append(opts, tea.WithoutSignalHandler())
	program := tea.NewProgram(model, opts...)
	_, programErr := program.Run()

	model.cancel()
	err := runner.Run(model.ctx)

	if programErr != nil {
		return programErr
	}

	return err
}
