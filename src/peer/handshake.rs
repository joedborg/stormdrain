//! BitTorrent handshake (BEP-3) with optional extension protocol flag (BEP-10).

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    error::{Error, Result},
    types::{InfoHash, PeerId},
};

const PROTOCOL: &[u8] = b"BitTorrent protocol";

/// Which BEP-10 / BEP-5 / BEP-6 capabilities a peer advertised.
#[derive(Debug, Clone, Default)]
pub struct PeerCapabilities {
    /// Extension protocol (BEP-10): `reserved[5]` & 0x10
    pub extension_protocol: bool,
    /// DHT (BEP-5):           `reserved[7]` & 0x01
    pub dht: bool,
    /// Fast extension (BEP-6): `reserved[7]` & 0x04
    pub fast: bool,
}

/// The peer ID and advertised capabilities returned after a successful handshake.
pub struct HandshakeResult {
    pub peer_id: [u8; 20],
    pub capabilities: PeerCapabilities,
}

/// Send our handshake and receive the peer's, returning their peer-id and
/// advertised capabilities.  The info-hash in their response must match ours.
pub async fn perform<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    info_hash: InfoHash,
    our_peer_id: PeerId,
) -> Result<HandshakeResult> {
    // Build: pstrlen(1) + pstr(19) + reserved(8) + info_hash(20) + peer_id(20) = 68
    let mut msg = [0u8; 68];
    msg[0] = PROTOCOL.len() as u8;
    msg[1..20].copy_from_slice(PROTOCOL);
    // Reserved bytes – advertise BEP-10 extension protocol.
    msg[25] |= 0x10; // byte[5] bit 4 from right
    msg[28..48].copy_from_slice(info_hash.as_bytes());
    msg[48..68].copy_from_slice(our_peer_id.as_bytes());

    stream.write_all(&msg).await?;

    let mut resp = [0u8; 68];
    stream.read_exact(&mut resp).await?;

    // Validate protocol string.
    let pstrlen = resp[0] as usize;
    if pstrlen != PROTOCOL.len() || &resp[1..20] != PROTOCOL {
        return Err(Error::Peer(format!(
            "unexpected protocol string ({pstrlen} bytes)"
        )));
    }

    // Validate info-hash.
    if &resp[28..48] != info_hash.as_bytes() {
        return Err(Error::Peer("info_hash mismatch in handshake".into()));
    }

    let reserved = &resp[20..28];
    let capabilities = PeerCapabilities {
        extension_protocol: reserved[5] & 0x10 != 0,
        dht: reserved[7] & 0x01 != 0,
        fast: reserved[7] & 0x04 != 0,
    };

    let mut peer_id = [0u8; 20];
    peer_id.copy_from_slice(&resp[48..68]);

    Ok(HandshakeResult {
        peer_id,
        capabilities,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    fn make_handshake_response(info_hash: &InfoHash, peer_id: [u8; 20], reserved: [u8; 8]) -> [u8; 68] {
        let mut resp = [0u8; 68];
        resp[0] = 19;
        resp[1..20].copy_from_slice(b"BitTorrent protocol");
        resp[20..28].copy_from_slice(&reserved);
        resp[28..48].copy_from_slice(info_hash.as_bytes());
        resp[48..68].copy_from_slice(&peer_id);
        resp
    }

    #[tokio::test]
    async fn perform_valid_handshake() {
        let info_hash = InfoHash([1u8; 20]);
        let our_id = PeerId([2u8; 20]);
        let peer_id = [3u8; 20];

        let (mut client, mut server) = duplex(4096);

        // Simulate the remote peer: read our handshake and send a valid response.
        let server_task = tokio::spawn(async move {
            let mut buf = [0u8; 68];
            server.read_exact(&mut buf).await.unwrap();
            let resp = make_handshake_response(&info_hash, peer_id, [0u8; 8]);
            server.write_all(&resp).await.unwrap();
        });

        let result = perform(&mut client, info_hash, our_id).await.unwrap();
        assert_eq!(result.peer_id, peer_id);
        assert!(!result.capabilities.extension_protocol);
        assert!(!result.capabilities.dht);
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn perform_detects_info_hash_mismatch() {
        let info_hash = InfoHash([1u8; 20]);
        let wrong_hash = InfoHash([9u8; 20]);
        let our_id = PeerId([2u8; 20]);

        let (mut client, mut server) = duplex(4096);

        let server_task = tokio::spawn(async move {
            let mut buf = [0u8; 68];
            server.read_exact(&mut buf).await.unwrap();
            // Respond with a *different* info_hash.
            let resp = make_handshake_response(&wrong_hash, [5u8; 20], [0u8; 8]);
            server.write_all(&resp).await.unwrap();
        });

        let result = perform(&mut client, info_hash, our_id).await;
        assert!(result.is_err());
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn perform_parses_extension_protocol_capability() {
        let info_hash = InfoHash([1u8; 20]);
        let our_id = PeerId([2u8; 20]);

        let (mut client, mut server) = duplex(4096);

        let server_task = tokio::spawn(async move {
            let mut buf = [0u8; 68];
            server.read_exact(&mut buf).await.unwrap();
            let mut reserved = [0u8; 8];
            reserved[5] = 0x10; // BEP-10 extension protocol
            reserved[7] = 0x01; // DHT
            let resp = make_handshake_response(&info_hash, [7u8; 20], reserved);
            server.write_all(&resp).await.unwrap();
        });

        let result = perform(&mut client, info_hash, our_id).await.unwrap();
        assert!(result.capabilities.extension_protocol);
        assert!(result.capabilities.dht);
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn perform_rejects_wrong_protocol_string() {
        let info_hash = InfoHash([1u8; 20]);
        let our_id = PeerId([2u8; 20]);

        let (mut client, mut server) = duplex(4096);

        let server_task = tokio::spawn(async move {
            let mut buf = [0u8; 68];
            server.read_exact(&mut buf).await.unwrap();
            let mut resp = [0u8; 68];
            resp[0] = 19;
            resp[1..20].copy_from_slice(b"WrongProtocol!!!   "); // wrong protocol string
            resp[28..48].copy_from_slice(info_hash.as_bytes());
            server.write_all(&resp).await.unwrap();
        });

        let result = perform(&mut client, info_hash, our_id).await;
        assert!(result.is_err());
        server_task.await.unwrap();
    }
}
