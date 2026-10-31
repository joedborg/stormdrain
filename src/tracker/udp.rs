//! UDP tracker protocol (BEP-15).
//!
//! Flow:
//!   1. Send connect request  (action=0)
//!   2. Receive connect response → get connection_id
//!   3. Send announce request (action=1) with connection_id
//!   4. Receive announce response → peer list

use std::net::SocketAddr;

use tokio::net::UdpSocket;
use tokio::time::{Duration, timeout};

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
    let addr = parse_udp_addr(url).await?;
    tracing::debug!("UDP announce → {addr}");

    let bind_addr = if addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind_addr).await?;
    socket.connect(&addr).await?;

    // Step 1: connect
    let transaction_id: u32 = rand::random();
    let mut connect_req = [0u8; 16];
    connect_req[..8].copy_from_slice(&PROTOCOL_MAGIC.to_be_bytes());
    connect_req[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    connect_req[12..16].copy_from_slice(&transaction_id.to_be_bytes());

    socket.send(&connect_req).await?;

    let mut buf = [0u8; 1024];
    let n = recv_timeout(&socket, &mut buf, Duration::from_secs(15), "connect").await?;
    validate_response(&buf[..n], ACTION_CONNECT, transaction_id)?;
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
        None | Some(Event::Started) => 2, // "started" = 2 in UDP
        Some(Event::Completed) => 1,
        Some(Event::Stopped) => 3,
    };
    ann[80..84].copy_from_slice(&event_code.to_be_bytes());
    ann[84..88].fill(0); // IP (0 = default)
    ann[88..92].copy_from_slice(&rand::random::<u32>().to_be_bytes()); // key
    // Num_want = -1 (default)
    ann[92..96].copy_from_slice(&(-1i32).to_be_bytes());
    ann[96..98].copy_from_slice(&req.port.to_be_bytes());

    socket.send(&ann).await?;

    let mut rbuf = [0u8; 65536];
    let n = recv_timeout(&socket, &mut rbuf, Duration::from_secs(15), "announce").await?;
    validate_response(&rbuf[..n], ACTION_ANNOUNCE, transaction_id2)?;

    let interval = u32_be(&rbuf, 8) as u64;
    // Leechers @ 12, seeders @ 16 — unused for now
    let peers = parse_compact_peers(&rbuf[20..n], addr.is_ipv6());

    Ok(AnnounceResponse {
        peers,
        interval,
        warning: None,
    })
}

fn parse_compact_peers(data: &[u8], ipv6: bool) -> Vec<PeerAddr> {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    let mut out = Vec::new();
    let ip_len = if ipv6 { 16 } else { 4 };
    for chunk in data.chunks_exact(ip_len + 2) {
        let ip = if ipv6 {
            IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(&chunk[..16]).unwrap()))
        } else {
            IpAddr::V4(Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]))
        };
        let port = u16::from_be_bytes([chunk[ip_len], chunk[ip_len + 1]]);
        out.push(PeerAddr { ip, port });
    }
    out
}

async fn parse_udp_addr(tracker_url: &str) -> Result<SocketAddr> {
    let url = url::Url::parse(tracker_url)
        .map_err(|e| Error::Tracker(format!("invalid UDP tracker URL '{tracker_url}': {e}")))?;
    if url.scheme() != "udp" {
        return Err(Error::Tracker(format!(
            "not a UDP tracker URL: {tracker_url}"
        )));
    }
    let port = url
        .port()
        .filter(|&port| port != 0)
        .ok_or_else(|| Error::Tracker("UDP tracker URL must have a nonzero port".into()))?;
    match url.host() {
        Some(url::Host::Ipv4(ip)) => Ok(SocketAddr::new(ip.into(), port)),
        Some(url::Host::Ipv6(ip)) => Ok(SocketAddr::new(ip.into(), port)),
        Some(url::Host::Domain(host)) => {
            let addrs: Vec<_> = timeout(
                Duration::from_secs(15),
                tokio::net::lookup_host((host, port)),
            )
            .await
            .map_err(|_| Error::Tracker(format!("DNS lookup timed out for '{host}'")))?
            .map_err(|e| Error::Tracker(format!("DNS lookup failed for '{host}': {e}")))?
            .collect();
            // Prefer IPv4, but support trackers that only resolve to IPv6.
            addrs
                .iter()
                .find(|addr| addr.is_ipv4())
                .or_else(|| addrs.first())
                .copied()
                .ok_or_else(|| {
                    Error::Tracker(format!("DNS lookup returned no addresses for '{host}'"))
                })
        }
        None => Err(Error::Tracker("UDP tracker URL must have a host".into())),
    }
}

fn validate_response(data: &[u8], action: u32, transaction_id: u32) -> Result<()> {
    let stage = if action == ACTION_CONNECT {
        "connect"
    } else {
        "announce"
    };
    if data.len() < 8 {
        return Err(Error::Tracker(format!(
            "UDP {stage} response too short: {}",
            data.len()
        )));
    }
    if u32_be(data, 4) != transaction_id {
        return Err(Error::Tracker(format!(
            "UDP {stage}: transaction ID mismatch"
        )));
    }
    let response_action = u32_be(data, 0);
    if response_action == 3 {
        let message = String::from_utf8_lossy(&data[8..]);
        return Err(Error::Tracker(format!("UDP tracker error: {message}")));
    }
    if response_action != action {
        return Err(Error::Tracker(format!(
            "UDP {stage}: unexpected action {response_action}"
        )));
    }
    let minimum = if action == ACTION_CONNECT { 16 } else { 20 };
    if data.len() < minimum {
        return Err(Error::Tracker(format!(
            "UDP {stage} response too short: {}",
            data.len()
        )));
    }
    Ok(())
}

async fn recv_timeout(
    socket: &UdpSocket,
    buf: &mut [u8],
    dur: Duration,
    stage: &str,
) -> Result<usize> {
    timeout(dur, socket.recv(buf))
        .await
        .map_err(|_| Error::Tracker(format!("UDP {stage} timeout")))?
        .map_err(Error::Io)
}

fn u32_be(buf: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(buf[offset..offset + 4].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_short_tracker_error_responses_for_both_stages() {
        let mut response = 3u32.to_be_bytes().to_vec();
        response.extend_from_slice(&42u32.to_be_bytes());
        response.extend_from_slice(b"denied");
        for action in [ACTION_CONNECT, ACTION_ANNOUNCE] {
            assert!(matches!(
                validate_response(&response, action, 42),
                Err(Error::Tracker(message)) if message == "UDP tracker error: denied"
            ));
            assert!(matches!(
                validate_response(&response, action, 43),
                Err(Error::Tracker(message)) if message.contains("transaction ID mismatch")
            ));
        }
    }

    #[test]
    fn rejects_truncated_udp_responses_without_panicking() {
        for action in [ACTION_CONNECT, ACTION_ANNOUNCE] {
            let mut response = action.to_be_bytes().to_vec();
            response.extend_from_slice(&42u32.to_be_bytes());
            for len in 0..=response.len() {
                assert!(matches!(
                    validate_response(&response[..len], action, 42),
                    Err(Error::Tracker(message)) if message.contains("too short")
                ));
            }
        }
    }

    #[tokio::test]
    async fn receive_timeout_identifies_the_protocol_stage() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert!(matches!(
            recv_timeout(&socket, &mut [0u8; 1024], Duration::from_millis(10), "connect").await,
            Err(Error::Tracker(message)) if message == "UDP connect timeout"
        ));
    }

    #[test]
    fn parses_compact_peers_for_tracker_address_family() {
        let ipv4 = parse_compact_peers(&[127, 0, 0, 1, 0x1a, 0xe1], false);
        assert_eq!(SocketAddr::from(ipv4[0]), "127.0.0.1:6881".parse().unwrap());

        let mut data = std::net::Ipv6Addr::LOCALHOST.octets().to_vec();
        data.extend_from_slice(&6881u16.to_be_bytes());
        let ipv6 = parse_compact_peers(&data, true);
        assert_eq!(SocketAddr::from(ipv6[0]), "[::1]:6881".parse().unwrap());
    }

    #[tokio::test]
    async fn resolves_hostname_with_and_without_announce_path() {
        for url in ["udp://localhost:1337", "udp://localhost:1337/announce"] {
            let addr = parse_udp_addr(url).await.unwrap();
            assert!(addr.ip().is_loopback());
            assert_eq!(addr.port(), 1337);
        }
    }

    #[tokio::test]
    async fn parses_ip_literals_and_uppercase_scheme() {
        assert_eq!(
            parse_udp_addr("UDP://127.0.0.1:6969/announce")
                .await
                .unwrap(),
            "127.0.0.1:6969".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            parse_udp_addr("udp://[::1]:6969/announce").await.unwrap(),
            "[::1]:6969".parse::<SocketAddr>().unwrap()
        );
    }

    #[tokio::test]
    async fn rejects_invalid_tracker_urls() {
        for url in [
            "http://localhost:1337/announce",
            "udp://localhost/announce",
            "udp://localhost:0/announce",
            "udp://localhost:invalid/announce",
            "udp:///announce",
        ] {
            assert!(matches!(parse_udp_addr(url).await, Err(Error::Tracker(_))));
        }
    }
}
