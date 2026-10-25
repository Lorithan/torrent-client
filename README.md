# torrent-client

A Go BitTorrent client built as a portfolio and learning project.

The current CLI can discover peers through an HTTP/HTTPS tracker, connect to a
peer, download advertised pieces, verify their SHA-1 hashes, and write the output
file. It currently stops on transfer failures rather than replacing the failed
peer. Incoming uploads, seeding, simultaneous peer downloads, and the TUI remain
planned work.

See [Project scope](docs/SCOPE.md) for the current baseline, confirmed release
decisions, agreed feature boundaries, acceptance criteria, and milestone order.

## Current CLI

```sh
go run ./cmd -torrent ./input.torrent -output ./output.bin
```

Use `-port` to request a listening port; its default of `0` lets the OS select one.
Only single-file torrents with HTTP/HTTPS trackers and compact IPv4 peer lists are
currently supported. Existing output files are not overwritten. Partial downloads
are not resumed after restarting the application.

## Tests

```sh
go test ./...
```

Some tests open loopback TCP connections for local fake trackers and peers.
They do not require public trackers or a real swarm.
