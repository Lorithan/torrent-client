# Release verification

This document describes evidence for the supported first-release subset. It does
not add deferred features or publish a GitHub release/tag. Tests generate small
synthetic torrents and data; no personal torrent files belong in the repository.

## Environment and complete checks

The development/release-check environment is Go 1.26.4 on Linux amd64. Use an
interactive terminal for manual TUI checks and allow loopback TCP for local fake
trackers and peers. Download Go modules before attempting offline verification.

From the repository root:

```sh
go version
go env GOOS GOARCH
go mod verify
go vet ./...
go test ./... -count=1 -timeout=90s
go test -race ./... -count=1 -timeout=90s
mkdir -p bin
go build -trimpath -o ./bin/torrent-client ./cmd
./bin/torrent-client -h
```

The manual swarm test is skipped unless explicitly enabled. Ordinary tests never
need a public tracker, real swarm, or user-provided torrent. Some integration tests
deliberately exercise five-second tracker cooldowns, so the full suite is not
instantaneous. These checks are meant for the documented development platform.

## Automated acceptance matrix

| Requirement | Main reproducible evidence |
| --- | --- |
| Complete output and short final piece | `cmd/TestRunWorkerFlagEnablesOverlappingPeerRequests` runs full and two-byte final-piece cases, gates both peers before replying, verifies exact output and final tracker counters |
| Peer replacement and preserved progress | `cmd/TestRunContinuesWithSecondPeer`, `cmd/TestRunRediscoversPeerAfterTransferFailure` |
| Corruption recovery while another transfer remains active | `download/TestSessionDownloadFromPeersRecoversDuringOtherTransfer`, `transfer/TestDownloadFromPeersRetriesCorruptPiece` |
| Output protection and fatal storage errors | `cmd/TestRunExistingOutputDoesNotAnnounce`, `download/TestSessionDownloadFromPeersStorageFailureStopsWholeBatch`, session short-write tests |
| Uploading during downloading | `cmd/TestRunUploadsVerifiedStorageWhileDownloading` verifies real stored bytes and rejects malformed, unavailable, and cross-piece requests |
| Seeding and tracker lifecycle | `cmd/TestRunSeedsCompletedFileUntilCanceled` checks seed uploads, periodic updates, counters, stopped announce, and connection closure |
| TUI over the real engine | `tui/TestTUIRealEngineTransferResizeSeedAndQuit` and `tui/TestTUIRealEngineFailureRemainsVisibleUntilQuit` |
| Cancellation and cleanup | `download/TestSessionDownloadFromPeersCancellationJoinsStalledWorkers`, transfer incoming tests, TUI program startup/termination tests |
| Availability broadcasts and reader shutdown | Download session notification tests and `download/TestSessionServePeerBroadcastsAvailabilityAndServesNewPieces` |
| Diagnostics separated from the TUI | CLI logging tests, TUI lifecycle tests, and the manual terminal procedure below |

Focused checks, if needed after a change:

```sh
go test -race ./cmd -run '^TestRunWorkerFlagEnablesOverlappingPeerRequests$' -count=1
go test -race ./internal/download -run '^TestSessionDownloadFromPeers(RecoversDuringOtherTransfer|StorageFailureStopsWholeBatch|CancellationJoinsStalledWorkers)$' -count=1
go test -race ./internal/tui -run '^TestTUIRealEngine' -count=1
```

## Reproducible local CLI/TUI and seed check

Use two terminals, both in the repository root. The opt-in fixture lives in
`internal/tui/manual_test.go` and is test infrastructure, not a production tracker.
It starts one HTTP tracker and two TCP seed peers on loopback. Its synthetic file
is 33,501 bytes: two 16 KiB pieces and a 733-byte final piece. Responses are delayed
slightly so progress and overlapping transfers are visible.

In terminal A, create a fresh directory and keep the fixture running:

```sh
swarm_dir=$(mktemp -d)
printf '%s\n' "$swarm_dir"
go test ./internal/tui -run '^TestManualSwarm$' -v -timeout=0 -args -manual-swarm-dir "$swarm_dir"
```

The fixture prints `LOCAL SWARM READY`, the torrent/source paths, and the source
SHA-256. It refuses to overwrite `local.torrent` or `expected.bin`, so use a fresh
directory when restarting the fixture. All generated files stay outside the
checkout.

In terminal B, set `swarm_dir` to the exact directory printed by terminal A:

```sh
swarm_dir=/tmp/the-directory-printed-in-terminal-A
go run ./cmd -tui -torrent "$swarm_dir/local.torrent" -output "$swarm_dir/tui-output.bin" -log "$swarm_dir/tui.log" -workers 2
```

Check the following:

1. The display reports connecting/downloading, verified progress, and active
   peers without raw protocol logs appearing over it.
2. Shrink the terminal (for example, to 20 columns by three rows). The compact
   view retains stop/quit controls. Expand it and confirm the full view returns.
3. Completion reaches 100% and transitions to seeding. Leave default seeding
   enabled for this procedure.
4. Terminal A automatically connects as a downloader, requests the final five
   bytes of the short final piece, and prints `UPLOAD VERIFIED` after comparing
   them with the source. This exercises disk-backed upload after completion.
5. Once upload verification appears, press `q` in the TUI. Cleanup finishes and
   the client exits normally. Terminal A reports a stopped announce with
   `downloaded=33501`, `uploaded=5`, and `left=0`.

Then compare the entire output:

```sh
cmp "$swarm_dir/expected.bin" "$swarm_dir/tui-output.bin"
sha256sum "$swarm_dir/expected.bin" "$swarm_dir/tui-output.bin"
```

Repeat the plain CLI path with a new output filename while the fixture stays up:

```sh
go run ./cmd -torrent "$swarm_dir/local.torrent" -output "$swarm_dir/cli-output.bin" -workers 2
```

Wait for `UPLOAD VERIFIED` again, then stop the CLI with Ctrl+C and compare:

```sh
cmp "$swarm_dir/expected.bin" "$swarm_dir/cli-output.bin"
sha256sum "$swarm_dir/expected.bin" "$swarm_dir/cli-output.bin"
```

Stop the fixture in terminal A with Ctrl+C only after the clients have stopped.
An early client stop preserves partial output and intentionally cannot pass the
full-file comparison. Repeat with another output filename; restart/resume and
overwriting existing outputs are unsupported.

## Verified evidence and practical limits

On 2026-11-01, the built CLI and TUI both completed the fixture above and served
its seed probe. Both outputs matched the 33,501-byte source with SHA-256:

```text
c86a091f3e64efbceb52b31413d17baccc34493a743e98db7685a3dd3738c00a
```

The terminal check resized to 20x3 and back to 80x24, kept diagnostics in the log,
and exited with status 0 on quit. The CLI exited with status 0 on Ctrl+C. Both
sessions reported started, completed, and stopped with the expected counters.

These local scenarios cover the supported subset; they are not a claim of
compatibility with every public swarm. Earlier public-swarm testing demonstrated
sustained downloading but did not establish complete or parallel public-swarm
verification. Public swarms supplement these deterministic fixtures.

Supported: one BitTorrent v1 single-file torrent; HTTP/HTTPS tracker discovery;
compact/dictionary IPv4 peer lists; bounded concurrent TCP downloads; verified
disk-backed uploads through incoming connections; in-process seeding; CLI/TUI.

Important limits:

- No restart/resume, seed-only restart, pause/resume, or overwrite support.
- No UDP trackers, magnet links, DHT/PEX, announce tiers, multi-file/v2/hybrid
  torrents, IPv6 discovery, uTP/encryption, or automatic router port mapping.
- Download peers are prepared sequentially into bounded batches. New tracker
  peers join later batches; requests are not pipelined and advanced selection,
  endgame, fairness, or bandwidth controls are deferred.
- Incoming connections are limited to eight including handshakes. Active-peer
  status counts successfully prepared outgoing and incoming connections.
- Active transfer/incoming cancellation closes sockets and joins workers.
  In-flight tracker HTTP requests and outbound peer preparation still have
  bounded timeouts rather than immediate cancellation. Shutdown may wait for
  those timeouts and the final bounded stopped announce.
- Output is an explicit user path and existing files are refused. Failed/stopped
  attempts keep partial output; parent directories must already exist.
- Linux amd64 is verified. Installation instructions and fixture commands use
  that environment; other platforms remain unverified.

See [SCOPE.md](SCOPE.md) for the full deferred-feature list.
