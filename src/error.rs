//! Error types for the stormdrain library.

use thiserror::Error;

/// The error type for all stormdrain operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Bencode parse error: {0}")]
    Bencode(String),

    #[error("Invalid torrent: {0}")]
    InvalidTorrent(String),

    #[error("Invalid magnet link: {0}")]
    InvalidMagnet(String),

    #[error("Tracker error: {0}")]
    Tracker(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Peer error: {0}")]
    Peer(String),

    #[error("Piece {0} failed SHA-1 verification")]
    PieceVerification(u32),

    #[error("Download stalled: no peers could provide data")]
    Stalled,

    #[error("No trackers responded")]
    NoTrackers,

    #[error(
        "Trackers responded but no peers are available; the swarm may be empty or this torrent is new"
    )]
    EmptySwarm,

    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Convenience alias for `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;
