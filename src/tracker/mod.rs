//! Tracker announce: HTTP (BEP-3) and UDP (BEP-15).

pub mod http;
pub mod udp;

pub use http::announce;

use crate::types::{InfoHash, PeerAddr, PeerId};

/// Tracker announce event types (BEP-3 / BEP-15).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Started,
    Stopped,
    Completed,
}

impl Event {
    fn as_str(self) -> &'static str {
        match self {
            Event::Started => "started",
            Event::Stopped => "stopped",
            Event::Completed => "completed",
        }
    }
}

/// Parameters for a tracker announce request.
pub struct AnnounceRequest<'a> {
    pub tracker_url: &'a str,
    pub info_hash: InfoHash,
    pub peer_id: PeerId,
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: Option<Event>,
    pub num_want: i32,
}

/// Data returned by a successful tracker announce.
pub struct AnnounceResponse {
    pub peers: Vec<PeerAddr>,
    pub interval: u64,
    pub warning: Option<String>,
}
