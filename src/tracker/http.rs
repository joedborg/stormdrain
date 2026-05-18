//! HTTP tracker announce (BEP-3) with compact peer response parsing.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::OnceLock,
};

use crate::{
    bencode,
    error::{Error, Result},
    types::PeerAddr,
};

use super::{AnnounceRequest, AnnounceResponse};

/// Send an HTTP announce to the tracker described in `req`.
///
/// # Errors
///
/// Returns an error if the HTTP request fails or the response cannot be decoded.
pub async fn announce(req: &AnnounceRequest<'_>) -> Result<AnnounceResponse> {
    let url = build_url(req);
    tracing::debug!("HTTP announce → {}", req.tracker_url);

    let resp = http_client().get(&url).send().await.map_err(Error::Http)?;

    if !resp.status().is_success() {
        return Err(Error::Tracker(format!(
            "HTTP {}: {}",
            resp.status().as_u16(),
            req.tracker_url
        )));
    }

    let body = resp.bytes().await.map_err(Error::Http)?;
    parse_response(&body)
}

// Shared HTTP client — built once and reused across all announce calls.
static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn http_client() -> &'static reqwest::Client {
    HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .user_agent("stormdrain/0.1.0")
            .build()
            .expect("failed to build HTTP client")
    })
}

fn build_url(req: &AnnounceRequest<'_>) -> String {
    let sep = if req.tracker_url.contains('?') {
        '&'
    } else {
        '?'
    };
    let mut url = format!(
        "{}{sep}info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1&numwant={}",
        req.tracker_url,
        req.info_hash.url_encode(),
        req.peer_id.url_encode(),
        req.port,
        req.uploaded,
        req.downloaded,
        req.left,
        req.num_want,
    );
    if let Some(ev) = req.event {
        url.push_str("&event=");
        url.push_str(ev.as_str());
    }
    url
}

fn parse_response(body: &[u8]) -> Result<AnnounceResponse> {
    let val = bencode::decode(body)?;

    if let Some(reason) = val.get(b"failure reason").and_then(|v| v.as_str()) {
        return Err(Error::Tracker(format!("tracker: {reason}")));
    }

    let warning = val
        .get(b"warning message")
        .and_then(|v| v.as_str())
        .map(str::to_owned);

    let interval = val
        .get(b"interval")
        .and_then(|v| v.as_int())
        .unwrap_or(1800) as u64;

    let mut peers = Vec::new();

    // Compact IPv4 peers: 6 bytes each (4 IP + 2 port).
    if let Some(raw) = val.get(b"peers").and_then(|v| v.as_bytes()) {
        if raw.len() % 6 != 0 {
            return Err(Error::Tracker(format!(
                "compact peers length {} not a multiple of 6",
                raw.len()
            )));
        }
        for chunk in raw.chunks_exact(6) {
            let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            peers.push(PeerAddr {
                ip: IpAddr::V4(ip),
                port,
            });
        }
    }

    // Compact IPv6 peers: 18 bytes each (16 IP + 2 port).
    if let Some(raw) = val.get(b"peers6").and_then(|v| v.as_bytes()) {
        if raw.len() % 18 == 0 {
            for chunk in raw.chunks_exact(18) {
                let ip_bytes: [u8; 16] = chunk[..16].try_into().unwrap();
                let ip = Ipv6Addr::from(ip_bytes);
                let port = u16::from_be_bytes([chunk[16], chunk[17]]);
                peers.push(PeerAddr {
                    ip: IpAddr::V6(ip),
                    port,
                });
            }
        }
    }

    // Dict-format peers (non-compact fallback).
    if let Some(list) = val.get(b"peers").and_then(|v| v.as_list()) {
        for entry in list {
            let ip_str = entry.get(b"ip").and_then(|v| v.as_str()).unwrap_or("");
            let port = entry.get(b"port").and_then(|v| v.as_int()).unwrap_or(0) as u16;
            if port != 0 {
                if let Ok(ip) = ip_str.parse::<IpAddr>() {
                    peers.push(PeerAddr { ip, port });
                }
            }
        }
    }

    Ok(AnnounceResponse {
        peers,
        interval,
        warning,
    })
}
