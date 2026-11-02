# torrent-client

A Go BitTorrent client built as a portfolio and learning project.

The CLI and terminal UI share a transfer engine that discovers peers through an
HTTP/HTTPS tracker and downloads from bounded groups of peers concurrently. It
verifies piece SHA-1 hashes before writing,
reassigns failed work, and preserves verified progress across peer groups. Tracker
updates report session statistics and refresh discovery when peers are exhausted.
Incoming peers receive verified pieces from storage, including availability updates
as downloading progresses. Completed downloads continue seeding until stopped.
The Bubble Tea/Lip Gloss TUI displays live progress, rates, peers, phases, and
failures, with keyboard cancellation and a compact layout for small terminals.

See [Project scope](docs/SCOPE.md) for the current baseline, confirmed release
decisions, agreed feature boundaries, acceptance criteria, and milestone order.

## Build

Use Go 1.26.4 or later. Release checks currently target Linux amd64; other
platforms are not verified. TUI mode requires an interactive terminal. Tests need
permission to open loopback TCP sockets. Initial dependency downloads require
network access; the controlled swarm scenarios themselves run locally.

```sh
git clone https://github.com/Lorithan/torrent-client.git
cd torrent-client
mkdir -p bin
go build -trimpath -o ./bin/torrent-client ./cmd
./bin/torrent-client -h
```

Run the built binary with the same flags shown below, or use `go run ./cmd` from
the checkout. No public tracker or torrent is needed for the reproducible
[local verification procedure](docs/VERIFYING.md).

## Usage

```sh
go run ./cmd -torrent ./input.torrent -output ./output.bin
```

Add `-tui` for the terminal UI:

```sh
go run ./cmd -tui -torrent ./input.torrent -output ./output.bin -log ./torrent-client.log
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-torrent` | Required | Local single-file v1 `.torrent` path |
| `-output` | Required | Explicit output path; existing files are refused |
| `-port` | `0` | Listening port, `0` through `65535`; `0` asks the OS |
| `-workers` | `4` | Maximum peers in each download batch; minimum `1` |
| `-seed` | `true` | Continue uploading after download completion |
| `-tui` | `false` | Enable the terminal UI |
| `-log` | `torrent-client.log` | Append-only diagnostic log in TUI mode |

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
SIGTERM stops the session and cleans up its background services. Incoming
connections are limited to eight, including handshakes; excess
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

Partial output is retained after a stop or failure. There is no restart/resume:
another invocation refuses that existing path, even if the file is incomplete.
Choose a new output path or manage the partial file yourself. Parent output
directories must already exist. Keep downloaded data outside the checkout;
personal torrent files, logs, and ISO downloads are excluded from commits.

UDP trackers, magnet links, DHT/PEX, announce tiers, multi-file and v2/hybrid
torrents, IPv6 peers, automatic port mapping, pause/resume, and request pipelining
are deferred. The supported discovery path is HTTP/HTTPS with compact or
dictionary IPv4 peers. Detailed release limits and scenario commands are in
[VERIFYING.md](docs/VERIFYING.md) and [SCOPE.md](docs/SCOPE.md).

## Tests

```sh
go test ./...
go test -race ./... -count=1 -timeout=90s
go vet ./...
```

Bencode fuzz tests check arbitrary input for panics and invalid consumed positions,
and verify that generated valid nested values decode correctly, including binary
strings and integer limits. Their seed cases run with ordinary `go test`.
To generate additional inputs locally:

```sh
go test ./internal/bencode -run='^$' -fuzz='^FuzzDecode$' -fuzztime=30s -parallel=2 -timeout=90s
go test ./internal/bencode -run='^$' -fuzz='^FuzzDecodeNestedValues$' -fuzztime=30s -parallel=2 -timeout=90s
```

GitHub Actions builds, vets, and race-tests the project on pushes and pull
requests, then runs each fuzz target for ten seconds. These short runs supplement
the regression tests; they do not prove that all malformed inputs are handled.

Some tests open loopback TCP connections for local fake trackers and peers.
They do not require public trackers or a real swarm.
Real-engine TUI tests cover live partial progress, resizing, seeding uploads,
failure display, and quit during a stalled transfer. Other tests cover terminal
startup failures and joining transfer cleanup when the terminal program exits.
See [VERIFYING.md](docs/VERIFYING.md) for the acceptance matrix and an opt-in local
swarm that generates synthetic data in a directory you choose.

## AI assistance

The application implementation was written by Hunter Marshall. AI tools were
used for consultation, architecture guidance, code review, and writing and running
tests. AI also assisted with project documentation.

## License

This project is available under the [MIT License](LICENSE).
