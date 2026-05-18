//! stormdrain — a BitTorrent client library.
//!
//! # Overview
//!
//! The library exposes a core engine that can be embedded in any async
//! application. A built-in CLI binary (`stormdrain`) is included in the same
//! crate, making it straightforward to add a server, TUI, or GUI on top later.
//!
//! # Quick-start
//!
//! ```no_run
//! use std::sync::Arc;
//! use stormdrain::{download, Config, Metainfo};
//!
//! #[tokio::main]
//! async fn main() {
//!     let data = std::fs::read("example.torrent").unwrap();
//!     let meta = Arc::new(Metainfo::from_bytes(&data).unwrap());
//!     let config = Config::default();
//!     download::download(meta, config, |stats| {
//!         println!("{:.1}% @ {:.1} KB/s", stats.progress * 100.0, stats.download_speed / 1024.0);
//!     })
//!     .await
//!     .unwrap();
//! }
//! ```

pub mod bencode;
pub mod dht;
pub mod download;
pub mod error;
pub mod file_writer;
pub mod magnet;
pub mod metainfo;
pub mod peer;
pub mod piece_manager;
pub mod resume;
pub mod tracker;
pub mod types;

// Convenience re-exports.
pub use download::{Config, DownloadState, DownloadStats};
pub use error::{Error, Result};
pub use metainfo::Metainfo;

/// Resolve a source string (local path / HTTP URL / magnet link) to a
/// [`Metainfo`].
///
/// - Local `.torrent` files are parsed directly.
/// - `http://` and `https://` URLs are fetched and parsed.
/// - `magnet:` URIs are parsed into a placeholder [`Metainfo`]; callers must
///   resolve the full metadata via BEP-9 before starting a download.
///
/// # Errors
///
/// Returns an error if the path cannot be read, the URL cannot be fetched, the
/// torrent file is malformed, or the magnet URI is missing its info hash.
pub async fn resolve_source(source: &str) -> Result<(Metainfo, Option<magnet::MagnetLink>)> {
    if source.starts_with("magnet:") {
        let link = magnet::MagnetLink::parse(source)?;
        return Ok((Metainfo::placeholder_magnet(&link), Some(link)));
    }

    if source.starts_with("http://") || source.starts_with("https://") {
        let bytes = fetch_torrent_url(source).await?;
        let meta = Metainfo::from_bytes(&bytes)?;
        return Ok((meta, None));
    }

    // Local file.
    let bytes = tokio::fs::read(source).await?;
    let meta = Metainfo::from_bytes(&bytes)?;
    Ok((meta, None))
}

async fn fetch_torrent_url(url: &str) -> Result<bytes::Bytes> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .map_err(Error::Http)?;

    let resp = client
        .get(url)
        .header("User-Agent", "stormdrain/0.1.0")
        .send()
        .await
        .map_err(Error::Http)?;

    if !resp.status().is_success() {
        return Err(Error::Http(resp.error_for_status().unwrap_err()));
    }

    Ok(resp.bytes().await.map_err(Error::Http)?)
}
