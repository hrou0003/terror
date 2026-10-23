# Terror

An actor-based Rust BitTorrent engine, originally started as a CodeCrafters project.
The Cargo workspace runs the engine in `src/crates/terror`; the older frontend in
`terror/` is separate and is not a workspace member.

## Run

From the repository root:

```sh
cargo run -- path/to/file.torrent
cargo test --workspace
```

The CLI writes through the existing torrent storage implementation, reports
completion, and shuts down its actors before exiting. Ctrl-C cancels a download
and waits for its peer tasks to exit; it also works while a peer is waiting for a
handshake response.

On macOS, an installed standalone Command Line Tools toolchain can be selected
for an individual command without changing the system setting:

```sh
DEVELOPER_DIR=/Library/Developer/CommandLineTools cargo test --workspace
```

## Actor ownership

- `TorrentManager` owns downloader handles, not a second copy of download state.
- A torrent actor owns piece/block state and processes commands and completions in
  the same event loop. Downloading does not monopolize its command mailbox.
- A peer pool owns its peer tasks and bounds outstanding requests per peer. Each
  peer actor exclusively owns its framed socket and polls both reads and writes.
- Handles retain task ownership. Explicit shutdown joins workers; dropping an
  owner cancels remaining tasks rather than detaching them.

`TorrentDownloaderHandle::new(torrent).await?` creates a paused downloader.
`start`, `pause`, and `resume` acknowledge command handling, not merely enqueueing.
A pause acknowledgment means the pool has stopped dispatching new requests.
Requests already handed to peer actors may still be sent and completed; their
results are retained, so resuming does not discard downloaded blocks.

`get_state` and `get_progress` read the torrent actor's published status. Progress
counts verified, saved pieces. Terminal states are `Completed`, `Error`, and
`Stopped`; start/resume cannot restart a terminal downloader. Explicit shutdown
transitions a successfully stopped actor to `Stopped`.

`remove_torrent` and manager `shutdown` wait for owned workers to terminate.
Unknown torrent IDs and failed command delivery return errors instead of silently
succeeding. Use explicit shutdown when the caller needs confirmation that socket
I/O has ended; drop cancellation is an ownership safety net, not an acknowledgment.

## Verification and limits

Regression tests use local TCP peers and an HTTP tracker, with no public-network
or working-directory dependency. They exercise pause/resume under backpressure,
out-of-order blocks, exact and partial piece boundaries, verified file output,
peer exhaustion, hash mismatch, mailbox closure, handshake/read cancellation,
and task-tree cleanup.

This is still an experimental engine, not a general-purpose torrent client. The
existing tracker support expects HTTP dictionary-style peer lists, and the
initial peer exchange expects a bitfield followed by unchoke. This cleanup adds
no compact-peer decoding, DHT, magnet support, retry policy, or network timeout
policy. Local end-to-end storage verification covers single-file torrents with
32 KiB pieces; multi-file layout and broader protocol compatibility are unchanged.
