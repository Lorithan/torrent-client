# torrent-client

A Go BitTorrent client built as a portfolio and learning project.

The CLI discovers peers through an HTTP/HTTPS tracker and downloads from bounded
groups of peers concurrently. It verifies piece SHA-1 hashes before writing,
reassigns failed work, and preserves verified progress across peer groups. Tracker
updates report session statistics and refresh discovery when peers are exhausted.
Incoming peers receive verified pieces from storage, including availability updates
as downloading progresses. Completed downloads continue seeding until stopped.
The TUI remains planned work.

See [Project scope](docs/SCOPE.md) for the current baseline, confirmed release
decisions, agreed feature boundaries, acceptance criteria, and milestone order.

## Current CLI

```sh
go run ./cmd -torrent ./input.torrent -output ./output.bin
```

Use `-port` to request a listening port; its default of `0` lets the OS select one.
Use `-workers` to set the maximum concurrent peers per batch (default `4`, minimum
`1`). Peer connections are prepared sequentially before each batch starts; new
tracker peers become candidates for subsequent batches.

Seeding after completion is enabled by default. Use `-seed=false` to exit after
downloading. Ctrl+C or SIGTERM stops the session and cleans up its background
services. Incoming connections are limited to eight, including handshakes; excess
connections are closed. Interested incoming peers are unchoked under this limit.

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
