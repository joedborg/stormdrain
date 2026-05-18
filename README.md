# stormdrain

A command-line BitTorrent client modelled after `curl` and `wget`. Pass it a
`.torrent` file, an HTTP(S) URL to one, or a magnet link and it downloads the
content to disk.

## Features

- `.torrent` files, HTTP(S) `.torrent` URLs, and magnet links
- DHT (BEP-5) for tracker-less and magnet-link operation
- MSE/PE encryption (BEP-6) with plaintext fallback
- BEP-9 metadata exchange (fetches the info dict from peers for magnet links)
- Resume interrupted downloads automatically on the next run
- Optional seeding after download completes, with ratio and time limits
- Progress bar showing speed, ETA, and peer count
- Structured logging via `RUST_LOG` / `--verbose`

## Installation

Requires Rust 1.85 or later (edition 2024).

```
git clone https://github.com/you/stormdrain
cd stormdrain
cargo install --path .
```

Or build without installing:

```
cargo build --release
# binary at target/release/stormdrain
```

## Usage

```
stormdrain [OPTIONS] <SOURCE>
```

`SOURCE` is a path to a `.torrent` file, an HTTP(S) URL to a `.torrent`, or a
magnet link.

### Options

| Flag                    | Short | Description                                                      |
| ----------------------- | ----- | ---------------------------------------------------------------- |
| `--download-dir <DIR>`  | `-w`  | Directory to save files (default: current directory)             |
| `--port <PORT>`         | `-p`  | Peer port advertised to trackers (default: 6881)                 |
| `--max-download <KBPS>` | `-d`  | Download speed cap in KB/s (0 = unlimited)                       |
| `--max-upload <KBPS>`   | `-u`  | Upload speed cap in KB/s (0 = unlimited)                         |
| `--sequential`          |       | Download pieces in order (useful for media streaming)            |
| `--seed`                | `-s`  | Seed after the download finishes                                 |
| `--seed-ratio <RATIO>`  |       | Stop seeding when upload/download ratio reaches this value       |
| `--seed-time <MINUTES>` |       | Stop seeding after this many minutes                             |
| `--quiet`               | `-q`  | Suppress progress bar; print errors only                         |
| `--verbose`             | `-v`  | Enable debug logging (equivalent to `RUST_LOG=stormdrain=debug`) |

### Examples

Download a torrent to the current directory:

```
stormdrain ubuntu-24.04.torrent
```

Download to a specific directory:

```
stormdrain -w ~/Downloads ubuntu-24.04.torrent
```

Download from a URL:

```
stormdrain https://releases.ubuntu.com/24.04/ubuntu-24.04-desktop-amd64.iso.torrent
```

Download from a magnet link:

```
stormdrain "magnet:?xt=urn:btih:..."
```

Download and seed until ratio 2.0:

```
stormdrain --seed --seed-ratio 2.0 ubuntu-24.04.torrent
```

## Resume

If a download is interrupted (Ctrl-C, crash, lost connection), the progress is
checkpointed to disk after each completed piece. Re-running the same command
will verify already-downloaded pieces and continue from where it left off.

## Library

The `stormdrain` crate doubles as a library. Add it as a dependency to embed
the engine in your own application:

```toml
[dependencies]
stormdrain = { path = "." }
```

```rust
use std::sync::Arc;
use stormdrain::{download, download::Config, metainfo::Metainfo};

#[tokio::main]
async fn main() {
    let data = std::fs::read("example.torrent").unwrap();
    let meta = Arc::new(Metainfo::from_bytes(&data).unwrap());
    let config = Config::default();
    download::download(meta, config, |stats| {
        println!("{:.1}%", stats.progress * 100.0);
    })
    .await
    .unwrap();
}
```

## Project layout

```
src/
  lib.rs               library crate root + public re-exports
  bencode.rs           bencode encoder/decoder
  dht.rs               DHT client (BEP-5)
  download.rs          download orchestrator
  error.rs             error types
  file_writer.rs       async multi-file writer
  magnet.rs            magnet link parser
  metainfo.rs          .torrent parser
  piece_manager.rs     piece scheduling and verification
  resume.rs            resume state persistence
  types.rs             shared types (InfoHash, PeerId, PeerAddr)
  peer/
    conn.rs            async peer connection (read/write messages)
    handshake.rs       BEP-3 handshake
    id.rs              peer ID generation
    message.rs         peer wire message codec
    metadata_exchange.rs  BEP-9 info dict fetch
    mse.rs             MSE/PE encryption (BEP-6)
  tracker/
    http.rs            HTTP tracker announce (BEP-3)
    udp.rs             UDP tracker announce (BEP-15)
  bin/
    stormdrain/
      main.rs          entry point + run()
      args.rs          CLI argument definitions (clap)
      display.rs       progress bar + human_size()
```

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
