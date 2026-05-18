//! UDP tracker protocol (BEP-15).
//!
//! Flow:
//!   1. Send connect request  (action=0)
//!   2. Receive connect response → get connection_id
//!   3. Send announce request (action=1) with connection_id
//!   4. Receive announce response → peer list

use std::net::SocketAddr;

use tokio::net::UdpSocket;
use tokio::time::{timeout, Duration};

use crate::{
    error::{Error, Result},
    types::PeerAddr,
};

use super::{AnnounceRequest, AnnounceResponse, Event};

// Magic protocol ID for the connect request
const PROTOCOL_MAGIC: u64 = 0x0000041727101980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;

/// Send a UDP announce to `url` using the BEP-15 protocol.
///
/// # Errors
///
/// Returns an error if the socket cannot be bound, the tracker does not
/// respond within the timeout, or the response is malformed.
pub async fn announce(req: &AnnounceRequest<'_>, url: &str) -> Result<AnnounceResponse> {
    // Parse udp://host:port/...
    let addr = parse_udp_addr(url)?;
    tracing::debug!("UDP announce → {addr}");

    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(&addr).await?;

    // Step 1: connect
    let transaction_id: u32 = rand::random();
    let mut connect_req = [0u8; 16];
    connect_req[..8].copy_from_slice(&PROTOCOL_MAGIC.to_be_bytes());
    connect_req[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    connect_req[12..16].copy_from_slice(&transaction_id.to_be_bytes());

    socket.send(&connect_req).await?;

    let mut buf = [0u8; 1024];
    let n = recv_timeout(&socket, &mut buf, Duration::from_secs(15)).await
        .map_err(|_| Error::Tracker("UDP connect timeout".into()))?;

    if n < 16 {
        return Err(Error::Tracker("UDP connect response too short".into()));
    }
    let resp_action = u32_be(&buf, 0);
    let resp_txid   = u32_be(&buf, 4);
    if resp_action != ACTION_CONNECT {
        return Err(Error::Tracker(format!("UDP connect: unexpected action {resp_action}")));
    }
    if resp_txid != transaction_id {
        return Err(Error::Tracker("UDP connect: transaction ID mismatch".into()));
    }
    let connection_id = u64::from_be_bytes(buf[8..16].try_into().unwrap());

    // Step 2: announce
    let transaction_id2: u32 = rand::random();
    let mut ann = [0u8; 98];
    ann[..8].copy_from_slice(&connection_id.to_be_bytes());
    ann[8..12].copy_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
    ann[12..16].copy_from_slice(&transaction_id2.to_be_bytes());
    ann[16..36].copy_from_slice(req.info_hash.as_bytes());
    ann[36..56].copy_from_slice(req.peer_id.as_bytes());
    ann[56..64].copy_from_slice(&req.downloaded.to_be_bytes());
    ann[64..72].copy_from_slice(&req.left.to_be_bytes());
    ann[72..80].copy_from_slice(&req.uploaded.to_be_bytes());
    let event_code: u32 = match req.event {
        None | Some(Event::Started)   => 2, // "started" = 2 in UDP
        Some(Event::Completed)        => 1,
        Some(Event::Stopped)          => 3,
    };
    ann[80..84].copy_from_slice(&event_code.to_be_bytes());
    ann[84..88].fill(0); // IP (0 = default)
    ann[88..92].copy_from_slice(&rand::random::<u32>().to_be_bytes()); // key
    // Num_want = -1 (default)
    ann[92..96].copy_from_slice(&(-1i32).to_be_bytes());
    ann[96..98].copy_from_slice(&req.port.to_be_bytes());

    socket.send(&ann).await?;

    let mut rbuf = [0u8; 65536];
    let n = recv_timeout(&socket, &mut rbuf, Duration::from_secs(15)).await
        .map_err(|_| Error::Tracker("UDP announce timeout".into()))?;

    if n < 20 {
        return Err(Error::Tracker(format!("UDP announce response too short: {n}")));
    }
    let resp_action = u32_be(&rbuf, 0);
    let resp_txid   = u32_be(&rbuf, 4);

    if resp_action == 3 {
        // Error response
        let msg = std::str::from_utf8(&rbuf[8..n]).unwrap_or("unknown error");
        return Err(Error::Tracker(format!("UDP tracker error: {msg}")));
    }
    if resp_action != ACTION_ANNOUNCE {
        return Err(Error::Tracker(format!("UDP announce: unexpected action {resp_action}")));
    }
    if resp_txid != transaction_id2 {
        return Err(Error::Tracker("UDP announce: transaction ID mismatch".into()));
    }

    let interval = u32_be(&rbuf, 8) as u64;
    // Leechers @ 12, seeders @ 16 — unused for now
    let peers = parse_compact_peers(&rbuf[20..n]);

    Ok(AnnounceResponse { peers, interval, warning: None })
}

fn parse_compact_peers(data: &[u8]) -> Vec<PeerAddr> {
    use std::net::{IpAddr, Ipv4Addr};
    let mut out = Vec::new();
    for chunk in data.chunks_exact(6) {
        let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        out.push(PeerAddr { ip: IpAddr::V4(ip), port });
    }
    out
}

fn parse_udp_addr(url: &str) -> Result<SocketAddr> {
    // Strip scheme: udp:// or UDP://
    let rest = url
        .strip_prefix("udp://")
        .or_else(|| url.strip_prefix("UDP://"))
        .ok_or_else(|| Error::Tracker(format!("not a UDP tracker URL: {url}")))?;
    // Drop any /announce path component
    let host_port = rest.split('/').next().unwrap_or(rest);
    host_port
        .parse::<SocketAddr>()
        .map_err(|e| Error::Tracker(format!("invalid UDP tracker address '{host_port}': {e}")))
}

async fn recv_timeout(socket: &UdpSocket, buf: &mut [u8], dur: Duration) -> Result<usize> {
    timeout(dur, socket.recv(buf))
        .await
        .map_err(|_| Error::Tracker("timeout".into()))?
        .map_err(Error::Io)
}

fn u32_be(buf: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(buf[offset..offset + 4].try_into().unwrap())
}
