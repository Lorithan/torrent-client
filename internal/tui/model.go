package tui

import (
	"context"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/lorithan/torrent-client/internal/transfer"
)

type Runner interface {
	Run(context.Context) error
	Status() transfer.Status
}

type Model struct {
	engine   Runner
	ctx      context.Context
	cancel   context.CancelFunc
	status   transfer.Status
	stopping bool
	finished bool
	runErr   error
	width    int
	height   int
}

func New(ctx context.Context, engine Runner) Model {
	childCtx, childCancel := context.WithCancel(ctx)

	return Model{
		engine: engine,
		ctx:    childCtx,
		cancel: childCancel,
		status: engine.Status(),
	}
}

type runFinishedMsg struct {
	err error
}

type refreshMsg struct{}

func (m Model) runTransferCmd() tea.Cmd {
	return func() tea.Msg {
		err := m.engine.Run(m.ctx)
		return runFinishedMsg{
			err: err,
		}
	}
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(m.runTransferCmd(), refreshCmd())
}

func refreshCmd() tea.Cmd {
	return tea.Tick(250*time.Millisecond, func(t time.Time) tea.Msg {
		return refreshMsg{}
	})
}
