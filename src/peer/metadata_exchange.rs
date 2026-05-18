//! BEP-9 / BEP-10 — Extension protocol handshake + ut_metadata exchange.
//!
//! This allows fetching torrent metadata (the "info" dict) from peers using
//! only a magnet link (info_hash + optional tracker list).
//!
//! Protocol summary:
//!   1. During BEP-10 handshake (ext_id=0) both sides advertise extensions.
//!      We advertise `{"m":{"ut_metadata":1},"metadata_size":0}`.
//!   2. If the peer supports ut_metadata we request pieces one at a time:
//!      `{"msg_type":0, "piece":N}` (sent as an Extension message with ext_id=1)
//!   3. Peer replies with `{"msg_type":1, "piece":N, "total_size":T}` followed
//!      immediately by the raw metadata bytes for that piece (16 KiB each, last
//!      may be shorter).
//!   4. After all pieces are received we SHA-1 hash the entire metadata block
//!      and verify against the info_hash.
//!   5. We then bencode-parse the info dict and construct a `Metainfo`.

use bytes::Bytes;
use sha1::{Digest as _, Sha1};
use tokio::time::{Duration, timeout};

use crate::{
    bencode,
    error::{Error, Result},
    metainfo::Metainfo,
    peer::{conn::PeerConn, handshake, message::Message},
    types::{InfoHash, PeerAddr, PeerId},
};

const METADATA_PIECE_SIZE: usize = 16384;
const UT_METADATA_EXT_ID: u8 = 1;

/// Attempt to fetch the info dict from one peer using BEP-9.
///
/// Returns the complete `Metainfo` on success.
pub async fn fetch_from_peer(
    addr: &PeerAddr,
    info_hash: &InfoHash,
    peer_id: &PeerId,
) -> Result<Metainfo> {
    use tokio::net::TcpStream;

    let socket_addr = std::net::SocketAddr::new(addr.ip, addr.port);
    let mut stream = timeout(Duration::from_secs(10), TcpStream::connect(socket_addr))
        .await
        .map_err(|_| Error::Peer("connect timeout".into()))??;

    // BEP-3 handshake (with BEP-10 extension bit set).
    let hs = handshake::perform(&mut stream, *info_hash, *peer_id).await?;

    if !hs.capabilities.extension_protocol {
        return Err(Error::Peer(
            "peer does not support extension protocol".into(),
        ));
    }

    let mut conn = PeerConn::new(stream);

    // Send BEP-10 extension handshake
    // ext_id=0 means "extension handshake message"
    // payload: {"m":{"ut_metadata":1},"reqq":500}
    let ext_handshake = build_ext_handshake();
    conn.send(&Message::Extension {
        ext_id: 0,
        payload: Bytes::from(ext_handshake),
    })
    .await?;

    // Wait for the peer's extension handshake.
    let peer_ut_metadata_id;
    let metadata_size;
    loop {
        let msg = timeout(Duration::from_secs(15), conn.read_message())
            .await
            .map_err(|_| Error::Peer("extension handshake timeout".into()))??;
        match msg {
            Some(Message::Extension {
                ext_id: 0,
                ref payload,
            }) => {
                let (ut_id, size) = parse_ext_handshake(payload)?;
                peer_ut_metadata_id = ut_id;
                metadata_size = size;
                break;
            }
            // Ignore non-extension messages while waiting for handshake.
            _ => {}
        }
    }

    if peer_ut_metadata_id == 0 {
        return Err(Error::Peer("peer does not support ut_metadata".into()));
    }
    if metadata_size == 0 {
        return Err(Error::Peer("peer sent metadata_size=0".into()));
    }

    // Request all metadata pieces
    let num_pieces = (metadata_size + METADATA_PIECE_SIZE - 1) / METADATA_PIECE_SIZE;
    let mut pieces: Vec<Option<Vec<u8>>> = vec![None; num_pieces];
    let mut received = 0usize;

    // Request all pieces up front.
    for i in 0..num_pieces {
        let req = build_metadata_request(i as u32);
        conn.send(&Message::Extension {
            ext_id: peer_ut_metadata_id,
            payload: Bytes::from(req),
        })
        .await?;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    while received < num_pieces {
        // checked_duration_since returns None if now > deadline; passing
        // Duration::ZERO causes timeout() to fire immediately, returning the
        // timeout error rather than panicking on a negative subtraction.
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or(Duration::ZERO);
        let msg = timeout(remaining, conn.read_message())
            .await
            .map_err(|_| Error::Peer("metadata exchange timeout".into()))??;

        if let Some(Message::Extension {
            ext_id,
            ref payload,
        }) = msg
        {
            if ext_id == UT_METADATA_EXT_ID {
                if let Some((piece_idx, data)) = parse_metadata_piece(payload, metadata_size)? {
                    if pieces[piece_idx].is_none() {
                        pieces[piece_idx] = Some(data);
                        received += 1;
                    }
                }
            }
        }
    }

    // Assemble + verify
    let mut full: Vec<u8> = Vec::with_capacity(metadata_size);
    for piece in pieces.into_iter().flatten() {
        full.extend_from_slice(&piece);
    }

    let hash: [u8; 20] = Sha1::digest(&full).into();
    if hash != info_hash.0 {
        return Err(Error::Peer("metadata SHA-1 mismatch".into()));
    }

    Metainfo::from_info_bytes(&full, info_hash)
}

// Helpers
fn build_ext_handshake() -> Vec<u8> {
    // {"m":{"ut_metadata":1},"reqq":500}
    // We need proper bencode, build manually:
    // d 1:m d 11:ut_metadata i1e e 4:reqq i500e e
    b"d1:md11:ut_metadatai1ee4:reqqi500ee".to_vec()
}

fn parse_ext_handshake(payload: &[u8]) -> Result<(u8, usize)> {
    let val = bencode::decode(payload)?;
    let ut_id = val
        .get(b"m")
        .and_then(|m| m.get(b"ut_metadata"))
        .and_then(|v| v.as_int())
        .unwrap_or(0) as u8;
    let size = val
        .get(b"metadata_size")
        .and_then(|v| v.as_int())
        .unwrap_or(0) as usize;
    Ok((ut_id, size))
}

fn build_metadata_request(piece: u32) -> Vec<u8> {
    // {"msg_type":0,"piece":N}
    format!("d8:msg_typei0e5:piecei{}ee", piece).into_bytes()
}

fn parse_metadata_piece(payload: &[u8], total_size: usize) -> Result<Option<(usize, Vec<u8>)>> {
    // The payload is: bencoded dict || raw metadata bytes
    // We need to find where the dict ends and the data begins.
    let dict_end = find_bencode_end(payload)?;
    let dict_bytes = &payload[..dict_end];
    let data_bytes = &payload[dict_end..];

    let val = bencode::decode(dict_bytes)?;
    let msg_type = val.get(b"msg_type").and_then(|v| v.as_int()).unwrap_or(-1);
    if msg_type != 1 {
        return Ok(None); // Not a data message (could be reject=2)
    }
    let piece = val.get(b"piece").and_then(|v| v.as_int()).unwrap_or(-1);
    if piece < 0 {
        return Ok(None);
    }
    let piece = piece as usize;
    let num_pieces = (total_size + METADATA_PIECE_SIZE - 1) / METADATA_PIECE_SIZE;
    if piece >= num_pieces {
        return Ok(None);
    }
    Ok(Some((piece, data_bytes.to_vec())))
}

/// Find the byte length of the first complete bencoded value in `data`.
fn find_bencode_end(data: &[u8]) -> Result<usize> {
    if data.is_empty() {
        return Err(Error::Bencode("empty payload".into()));
    }
    match data[0] {
        b'd' => {
            let mut i = 1;
            while i < data.len() && data[i] != b'e' {
                // Key
                let key_end = find_bencode_end(&data[i..])?;
                i += key_end;
                // Value
                let val_end = find_bencode_end(&data[i..])?;
                i += val_end;
            }
            if i >= data.len() {
                return Err(Error::Bencode("unterminated dict".into()));
            }
            Ok(i + 1) // skip 'e'
        }
        b'l' => {
            let mut i = 1;
            while i < data.len() && data[i] != b'e' {
                let item_end = find_bencode_end(&data[i..])?;
                i += item_end;
            }
            if i >= data.len() {
                return Err(Error::Bencode("unterminated list".into()));
            }
            Ok(i + 1)
        }
        b'i' => {
            let end = data
                .iter()
                .position(|&b| b == b'e')
                .ok_or_else(|| Error::Bencode("unterminated int".into()))?;
            Ok(end + 1)
        }
        c if c.is_ascii_digit() => {
            let colon = data
                .iter()
                .position(|&b| b == b':')
                .ok_or_else(|| Error::Bencode("no colon in string".into()))?;
            let len: usize = std::str::from_utf8(&data[..colon])
                .map_err(|_| Error::Bencode("invalid string length".into()))?
                .parse()
                .map_err(|_| Error::Bencode("invalid string length".into()))?;
            Ok(colon + 1 + len)
        }
        _ => Err(Error::Bencode("unexpected byte in bencode".into())),
    }
}
