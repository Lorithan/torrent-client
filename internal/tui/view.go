package tui

import (
	"context"
	"errors"
	"fmt"

	tea "charm.land/bubbletea/v2"
	lipgloss "charm.land/lipgloss/v2"
)

func (m Model) View() tea.View {
	result := ""

	if m.status.Progress.Name == "" {
		result += "Loading torrent...\n"
	} else {
		title := lipgloss.NewStyle().Bold(true).Render(m.status.Progress.Name)
		result += fmt.Sprintf("Torrent: %s\n", title)
	}

	result += fmt.Sprintf("Phase: %s\n\n", m.status.Phase)
	if m.status.Progress.TotalBytes > 0 {
		result += fmt.Sprintf("Verified: %s / %s (%.1f%%)\n", formatBytes(float64(m.status.Progress.VerifiedBytes)), formatBytes(float64(m.status.Progress.TotalBytes)), progressPercent(m.status.Progress.VerifiedBytes, m.status.Progress.TotalBytes))
	} else {
		result += fmt.Sprintf("Verified: %s / %s\n", formatBytes(float64(m.status.Progress.VerifiedBytes)), formatBytes(float64(m.status.Progress.TotalBytes)))
	}
	result += fmt.Sprintf("Peers: %d\n\n", m.status.ActivePeers)
	result += fmt.Sprintf("Download: %s/s\n", formatBytes(float64(m.status.DownloadRate)))
	result += fmt.Sprintf("Upload: %s/s\n\n", formatBytes(float64(m.status.UploadRate)))

	if m.status.Err != nil {
		errMsg := fmt.Sprintf("Error %s\n", m.status.Err)
		result += lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render(errMsg)
	} else {
		if m.runErr != nil && !errors.Is(m.runErr, context.Canceled) {
			errMsg := fmt.Sprintf("Error %s\n", m.runErr)
			result += lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render(errMsg)
		}
	}

	footer := ""
	if !m.finished && !m.stopping {
		footer += "q / Ctrl+C: stop and quit"
	} else if m.finished {
		footer += "q / Ctrl+C: quit"
	} else {
		footer += "Stopping transfer; waiting for cleanup..."
	}
	result += lipgloss.NewStyle().Faint(true).Render(footer)

	var width = m.width
	if width == 0 {
		width = 80
	} else {
		width = max(1, min(width, 80))
	}

	style := lipgloss.NewStyle().Width(width).MaxWidth(width)
	result = style.Render(result)

	if width < 20 || (m.height > 0 && lipgloss.Height(result) > m.height) {
		return tea.NewView(m.compactView(width, m.height))
	}

	return tea.NewView(result)
}

func (m Model) compactView(width, height int) string {
	if height < 1 {
		height = 3
	}

	footer := ""
	if !m.finished && !m.stopping {
		footer += "q: stop/quit"
	} else if m.finished {
		footer += "q: quit"
	} else {
		footer += "Stopping..."
	}

	result := ""

	if height > 1 {
		result += "Resize terminal\n"
	}
	if height > 2 {
		result += fmt.Sprintf("Phase %s\n", m.status.Phase)
	}

	result += footer

	style := lipgloss.NewStyle().MaxWidth(width)

	return style.Render(result)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case refreshMsg:
		m.status = m.engine.Status()
		if m.finished {
			return m, nil
		} else {
			return m, refreshCmd()
		}
	case runFinishedMsg:
		m.finished = true
		m.runErr = msg.err
		m.status = m.engine.Status()

		cancelled := m.ctx.Err()
		if m.stopping || cancelled != nil {
			return m, tea.Quit
		}
		return m, nil
	case tea.KeyPressMsg:
		if msg.String() == "q" || msg.String() == "ctrl+c" {
			if m.finished {
				return m, tea.Quit
			} else {
				if !m.stopping {
					m.stopping = true
					m.cancel()
				}
				return m, nil
			}
		}
		return m, nil
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	default:
		return m, nil
	}
}
