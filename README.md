# torrent-client

A Go BitTorrent client built as a portfolio and learning project.

The CLI and terminal UI share a transfer engine that discovers peers through an
HTTP/HTTPS tracker and downloads from bounded
groups of peers concurrently. It verifies piece SHA-1 hashes before writing,
reassigns failed work, and preserves verified progress across peer groups. Tracker
updates report session statistics and refresh discovery when peers are exhausted.
Incoming peers receive verified pieces from storage, including availability updates
as downloading progresses. Completed downloads continue seeding until stopped.
The Bubble Tea/Lip Gloss TUI displays live progress, rates, peers, phases, and
failures, with keyboard cancellation and a compact layout for small terminals.

See [Project scope](docs/SCOPE.md) for the current baseline, confirmed release
decisions, agreed feature boundaries, acceptance criteria, and milestone order.

## Usage

```sh
go run ./cmd -torrent ./input.torrent -output ./output.bin
```

Add `-tui` for the terminal UI:

```sh
go run ./cmd -tui -torrent ./input.torrent -output ./output.bin -log ./torrent-client.log
```

CLI mode is the default. In TUI mode, diagnostic logs are appended to the `-log`
path (default `torrent-client.log`); newly created log files use permissions
`0600`. The log flag does not redirect diagnostics in CLI mode. A log-open failure
stops TUI startup before transfer work begins.

Use `-port` to request a listening port; its default of `0` lets the OS select one.
Use `-workers` to set the maximum concurrent peers per batch (default `4`, minimum
`1`). Peer connections are prepared sequentially before each batch starts; new
tracker peers become candidates for subsequent batches.

Seeding after completion is enabled by default. Use `-seed=false` to stop the
transfer after downloading: the CLI exits, while the TUI retains its final screen
until dismissed. In the TUI, press `q` or Ctrl+C to stop and quit; it waits for
transfer cleanup before exiting. SIGTERM also stops the session. CLI Ctrl+C or
SIGTERM stops the session and cleans up its background
services. Incoming connections are limited to eight, including handshakes; excess
connections are closed. Interested incoming peers are unchoked under this limit.

The display shows verified bytes and percentage separately from transport
counters, which can include corrupt-piece retries. Rates use data bytes per
second and binary units such as KiB and MiB. Active peers include prepared
download connections and incoming connections after successful setup; handshakes
still count toward the incoming limit, but are not displayed as active peers.

Phases describe the session: starting reads metadata and sets up resources;
discovering announces to the tracker; connecting prepares a peer batch;
downloading runs that batch; waiting awaits tracker-discovered peers; seeding
serves verified output after completion. Failed peers can cause another
connecting/waiting cycle without ending the session. Stopped and failed states
are published after cleanup.

Resize the terminal to fit long names and errors. If the full display cannot fit,
a compact view preserves stop/quit controls and asks for a larger terminal. Final
failure details remain visible in the full view and are reported after the TUI is
dismissed. Recoverable peer diagnostics remain in the log.

Single-file torrents with HTTP/HTTPS trackers and compact or dictionary IPv4 peer
lists are supported. Existing output files are not overwritten. Verified progress
survives peer failures within the running session; restarting the application does
not resume partial output. The listening port accepts incoming TCP peers; public
inbound reachability may require manual router configuration. Uploads are currently
served through incoming connections. In-flight tracker HTTP requests and outbound
peer preparation use bounded timeouts rather than immediate context cancellation.

## Tests

```sh
go test ./...
go test -race ./...
```

Some tests open loopback TCP connections for local fake trackers and peers.
They do not require public trackers or a real swarm.
Real-engine TUI tests cover live partial progress, resizing, seeding uploads,
failure display, and quit during a stalled transfer. Other tests cover terminal
startup failures and joining transfer cleanup when the terminal program exits.
