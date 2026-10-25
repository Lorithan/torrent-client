# Project scope

Status: agreed first-release scope. Deferred features remain future goals rather
than first-release requirements. The implementation milestones are a working
sequence and can be adjusted as the project develops.

## Purpose and working agreement

Build a Go BitTorrent client as a portfolio and learning project, with a Bubble
Tea/Lip Gloss terminal UI. The first release handles one single-file torrent at a
time. Multi-file support comes later.

Hunter writes implementation code. The assistant helps with planning, code
review, tests, explanatory comments, and documentation. Implementation changes
remain Hunter's responsibility, even when implementation is requested.

The release should work reliably within an explicit supported subset of
BitTorrent. It does not need feature parity with mature torrent clients.

## Confirmed decisions

| Decision | First release |
| --- | --- |
| Download from several peers simultaneously | Required |
| Upload verified pieces and seed after completion | Required |
| Resume partial downloads after restarting the application | Deferred |

Preserving verified progress while replacing a failed connection is required.
This is different from restart/resume: the running download keeps its progress;
a new application process does not reconstruct it from disk.

## Current baseline

The implementation currently provides:

- Bencode decoding and single-file torrent parsing, including an info hash derived
  from the original encoded info dictionary bytes.
- Initial HTTP/HTTPS tracker announces and compact IPv4 peer-list parsing.
- A session peer ID and an OS-selected or explicitly requested listening port.
- Outgoing TCP connections, handshakes, and trying another address when initial
  connection or handshake attempts fail.
- Peer message framing, bitfield validation, have messages, and choke state.
- One outstanding block request at a time on one peer connection.
- Selection of advertised, unfinished pieces and writes at their file offsets.
- Piece SHA-1 verification before writing or announcing completion.
- Separate deadlines for waiting for availability, requesting blocks, and sending
  completion announcements.
- A CLI with explicit torrent/output paths and refusal to overwrite existing
  output files.

Tests exercise complete downloads using local fake trackers and peers, output
protection, invalid data, and several failure paths. Manual testing has shown
sustained downloads from a real swarm; that does not establish successful
completion of the large real-world torrent tested.

The current listener reserves an incoming port but does not accept and serve
incoming peers. Uploading, seeding, periodic tracker updates, a TUI, and replacing
a peer after a transfer failure remain unfinished. Current progress tracking is
local to the single-peer download operation.

## First-release features and acceptance criteria

### 1. Supported inputs and discovery

- Load a local BitTorrent v1, single-file `.torrent` and validate required metadata.
- Support HTTP and HTTPS tracker announces with both compact IPv4 peer lists and
  dictionary peer lists containing supported IPv4 endpoints.
- Retain the same peer ID and actual listening port for the session.
- Reannounce at the returned interval with current uploaded, downloaded, and
  remaining counts; send appropriate started, completed, and stopped events.
- Refresh peer discovery when the current peer set is exhausted, using bounded
  retries and delays rather than a tight retry loop.
- Explain unsupported torrent layouts, tracker transports, and peer addresses.

Acceptance: a local tracker can supply peers, observe updated transfer state and
lifecycle events, and supply a replacement peer after earlier peers become
unusable. Invalid or unsupported inputs fail with useful errors.

The initial discovery boundary is the torrent's primary announce URL.
UDP trackers and announce-list tiers are deferred; this deliberately limits which
public torrents the first release can use.

### 2. Download progress and peer recovery

- Keep completed-piece state for the lifetime of the download, independently of
  any particular peer connection.
- Preserve verified, successfully written pieces when a peer disconnects, times
  out, sends malformed messages, or supplies corrupt piece data.
- Release unfinished work for retry. Restart an interrupted piece from its
  beginning initially; retaining individual unverified blocks is unnecessary.
- Treat choking as peer state: stop issuing requests to a choking peer and allow
  other peers to continue.
- Distinguish peer failures from fatal output or metadata errors. A disk error
  must not trigger repeated peer replacements.
- Bound connection attempts, stalled requests, and retry frequency. A long
  download that continues making progress must not expire just because of total
  elapsed time.

Acceptance: after peer A fails, peer B finishes the file without requesting
already completed pieces again. Corrupt data is retried without being marked
complete. An output error stops the operation and remains visible to the user.

### 3. Simultaneous peer downloads

- Keep a bounded number of active peer connections, with concurrent transfers.
- Assign unfinished pieces to peers advertising them. Track pieces as missing,
  assigned, or complete so normal scheduling does not duplicate assignments.
- Return assignments to the missing set if their worker fails.
- Give completion tracking and output coordination a clear owner to prevent
  races and double counting.
- Begin with a simple eligible-piece strategy; rarest-first and endgame requests
  are later optimizations. One outstanding block request per connection is
  acceptable initially.

Acceptance: two fake peers with complementary piece availability jointly produce
the correct file. Their transfers overlap, each completed piece is counted once,
and peer failure does not strand assigned pieces. Concurrent tests pass the Go
race detector on the development platform.

### 4. Uploading, incoming peers, and seeding

- Accept incoming TCP connections and validate their torrent handshakes.
- Keep local availability distinct from each remote peer's availability.
- Advertise local verified pieces through bitfield/have messages.
- Handle interest and choking in both directions, with a simple bounded upload
  policy. Detailed fairness algorithms are outside this release.
- Serve valid requests only for locally completed pieces. Validate piece index,
  offset, and requested length before reading the output file.
- Read upload data from storage; do not keep an entire torrent in memory.
- Continue serving peers after download completion until the user stops seeding
  or exits. No automatic ratio or duration target is required.

Acceptance: a fake downloader connects, observes the right availability, and
receives exactly the requested bytes from verified storage. Invalid requests do
not expose unrelated file contents or crash the process. Uploading works while
other pieces are downloading and after the download completes.

Restarting the application in seed-only mode is deferred with restart/resume.
Seeding in this release continues in the process that completed the download.
Manual router configuration may be necessary for public inbound reachability;
automatic port mapping is deferred.

### 5. Output and lifecycle

- Keep an explicit user-selected output path and refuse to overwrite an existing
  file by default. Do not derive a filesystem path directly from torrent names.
- Mark pieces complete only after hash verification and successful writes.
- Preserve partial output when an attempt is stopped or fails, and explain that
  restart/resume is not supported yet. Report failures to finalize the output.
- Support a clean stop/quit path: cancel network work, stop accepting peers,
  attempt the stopped announce within a bounded interval, and close resources.
- Separate transfer completion from stopping the session, because completion
  transitions into seeding.

Acceptance: existing output remains unchanged; failed pieces are not announced as
complete; stop/quit releases connections and files without hanging. A successful
download matches the expected full-file contents.

Overwrite prompts and an automated overwrite flag are deferred. They were
identified as useful future behavior, not prerequisites for this release.

### 6. Terminal UI and CLI

- Keep a CLI path for diagnostics, local integration tests, and use without the
  TUI. Both entry points use the same transfer logic.
- Build the terminal UI with Bubble Tea and Lip Gloss.
- Show torrent name, total and verified bytes, progress, active peers, download
  and upload rates, and the current phase: discovering, connecting, downloading,
  waiting, seeding, stopped, or failed.
- Let users start one torrent using supplied paths, stop the session, and quit.
  A file picker is not required; CLI arguments may supply the initial paths.
- Keep network and disk operations out of the UI update loop. Use status updates
  from the transfer layer; raw diagnostic logs must not corrupt the display.
- Explain waits, peer replacements, output errors, and unsupported inputs without
  requiring users to interpret protocol message IDs.

Acceptance: the UI remains responsive during discovery and transfers, reports
progress and seeding accurately, and stops the transfer cleanly when requested.
Failure details are accessible without replacing the UI with debug logging.

## Deferred features

These features are deferred beyond the first release, rather than permanently
excluded. Revisit them after the first-release acceptance criteria are met:

- Multi-file torrents and several torrents running at once.
- Restart/resume, persisted sessions, and seed-only startup from existing files.
- Pause/resume controls; stop/quit are sufficient for the first UI.
- UDP trackers, announce-list tiers, magnet links, DHT, and peer exchange.
- BitTorrent v2/hybrid torrents, uTP transport, and protocol encryption.
- IPv6 peer discovery/connections and automatic NAT port mapping.
- Request pipelining, rarest-first selection, endgame mode, and advanced peer
  scoring or upload fairness.
- Bandwidth controls, file selection, streaming, watch folders, and a remote API.
- Overwrite prompts/flags, graphical file pickers, and UI customization.

Local tracker/peer fixtures can verify a small complete transfer without waiting
for UDP support or relying on a particular public swarm.

## Implementation milestones

Each milestone should preserve a working CLI and end with review, meaningful
tests, and a focused commit. These are learning steps, not fixed dates.

| Order | Milestone | Evidence needed before moving on |
| --- | --- | --- |
| 1 | Separate session progress from a peer; sequential fallback | Peer A fails; B finishes while completed pieces remain intact; fatal storage failures stop retries |
| 2 | Tracker lifecycle and rediscovery | Correct current statistics/events and replacement peer discovery with bounded retries |
| 3 | Concurrent peer scheduling | Complementary peers transfer concurrently; assignments recover on failure; race checks pass |
| 4 | Incoming connections, uploading, and seeding | Valid requests receive verified stored data; completion transitions to seeding |
| 5 | Shared status/cancellation and TUI | CLI and responsive TUI share the transfer engine; clean stop/quit |
| 6 | Release verification and user documentation | Controlled complete transfers, recovery/seeding scenarios, installation/use instructions, and explicit limitations |

Implement cancellation support when workers first need it; milestone 5 exposes
and integrates it with the UI rather than postponing worker shutdown design.
Revisit package boundaries when a milestone makes the need concrete. Public API
signatures, worker counts, and timeout defaults are not locked by this document.

## Testing and release evidence

- Prefer deterministic local fake peers/trackers for protocol ordering, timeout,
  corruption, recovery, concurrency, and upload tests.
- Verify complete file contents, short final pieces, output protection, and
  progress retained after peer replacement.
- Test timeout boundaries without long sleeps wherever practical, and exercise
  cancellation so failed workers do not outlive their session.
- Use temporary files for storage checks and the race detector for concurrent
  paths. Linux amd64 is the current development environment; broader platform
  support is not a first-release promise.
- Keep a small reproducible complete-download scenario and a seed/upload scenario
  for manual verification. Public swarm tests supplement these fixtures.
- Exclude personal torrent files and downloaded payloads from commits.

## Scope changes

Before adding a deferred feature, explicitly decide whether it is required for
this release or belongs to a later milestone, and update this document. Avoid
expanding the current task merely because an extension would be useful.

The immediate next implementation milestone is session progress plus sequential
peer fallback. This is a stepping
stone toward simultaneous peers, not the final release architecture.
