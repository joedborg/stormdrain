//! BitTorrent peer wire protocol messages (BEP-3) and extension protocol (BEP-10).

use crate::error::{Error, Result};
use bytes::{BufMut, Bytes, BytesMut};

/// Standard block size used in REQUEST messages (2^14 = 16 KiB).
pub const BLOCK_SIZE: u32 = 16_384;

// Message type IDs
pub const MSG_CHOKE: u8 = 0;
pub const MSG_UNCHOKE: u8 = 1;
pub const MSG_INTERESTED: u8 = 2;
pub const MSG_NOT_INTERESTED: u8 = 3;
pub const MSG_HAVE: u8 = 4;
pub const MSG_BITFIELD: u8 = 5;
pub const MSG_REQUEST: u8 = 6;
pub const MSG_PIECE: u8 = 7;
pub const MSG_CANCEL: u8 = 8;
pub const MSG_PORT: u8 = 9;
pub const MSG_EXTENSION: u8 = 20; // BEP-10

/// Parsed peer wire message.
#[derive(Debug, Clone)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request { index: u32, begin: u32, length: u32 },
    Piece { index: u32, begin: u32, data: Bytes },
    Cancel { index: u32, begin: u32, length: u32 },
    Port(u16),
    Extension { ext_id: u8, payload: Bytes },
    Unknown(u8),
}

impl Message {
    /// Encode a message into its wire representation.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        match self {
            Message::KeepAlive => {
                buf.put_u32(0);
            }
            Message::Choke => {
                buf.put_u32(1);
                buf.put_u8(MSG_CHOKE);
            }
            Message::Unchoke => {
                buf.put_u32(1);
                buf.put_u8(MSG_UNCHOKE);
            }
            Message::Interested => {
                buf.put_u32(1);
                buf.put_u8(MSG_INTERESTED);
            }
            Message::NotInterested => {
                buf.put_u32(1);
                buf.put_u8(MSG_NOT_INTERESTED);
            }
            Message::Have(index) => {
                buf.put_u32(5);
                buf.put_u8(MSG_HAVE);
                buf.put_u32(*index);
            }
            Message::Bitfield(bits) => {
                buf.put_u32(1 + bits.len() as u32);
                buf.put_u8(MSG_BITFIELD);
                buf.extend_from_slice(bits);
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                buf.put_u32(13);
                buf.put_u8(MSG_REQUEST);
                buf.put_u32(*index);
                buf.put_u32(*begin);
                buf.put_u32(*length);
            }
            Message::Piece { index, begin, data } => {
                buf.put_u32(9 + data.len() as u32);
                buf.put_u8(MSG_PIECE);
                buf.put_u32(*index);
                buf.put_u32(*begin);
                buf.extend_from_slice(data);
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                buf.put_u32(13);
                buf.put_u8(MSG_CANCEL);
                buf.put_u32(*index);
                buf.put_u32(*begin);
                buf.put_u32(*length);
            }
            Message::Port(port) => {
                buf.put_u32(3);
                buf.put_u8(MSG_PORT);
                buf.put_u16(*port);
            }
            Message::Extension { ext_id, payload } => {
                buf.put_u32(2 + payload.len() as u32);
                buf.put_u8(MSG_EXTENSION);
                buf.put_u8(*ext_id);
                buf.extend_from_slice(payload);
            }
            Message::Unknown(_) => {}
        }
        buf.freeze()
    }

    /// Decode from a length-prefixed payload already read off the wire.
    /// `id` is the first byte; `payload` is the rest.
    pub fn decode(id: u8, payload: &[u8]) -> Result<Message> {
        match id {
            MSG_CHOKE => Ok(Message::Choke),
            MSG_UNCHOKE => Ok(Message::Unchoke),
            MSG_INTERESTED => Ok(Message::Interested),
            MSG_NOT_INTERESTED => Ok(Message::NotInterested),
            MSG_HAVE => {
                ensure_len(payload, 4, "have")?;
                Ok(Message::Have(u32_be(payload, 0)))
            }
            MSG_BITFIELD => Ok(Message::Bitfield(payload.to_vec())),
            MSG_REQUEST => {
                ensure_len(payload, 12, "request")?;
                Ok(Message::Request {
                    index: u32_be(payload, 0),
                    begin: u32_be(payload, 4),
                    length: u32_be(payload, 8),
                })
            }
            MSG_PIECE => {
                ensure_len(payload, 8, "piece")?;
                Ok(Message::Piece {
                    index: u32_be(payload, 0),
                    begin: u32_be(payload, 4),
                    data: Bytes::copy_from_slice(&payload[8..]),
                })
            }
            MSG_CANCEL => {
                ensure_len(payload, 12, "cancel")?;
                Ok(Message::Cancel {
                    index: u32_be(payload, 0),
                    begin: u32_be(payload, 4),
                    length: u32_be(payload, 8),
                })
            }
            MSG_PORT => {
                ensure_len(payload, 2, "port")?;
                Ok(Message::Port(u16::from_be_bytes([payload[0], payload[1]])))
            }
            MSG_EXTENSION => {
                ensure_len(payload, 1, "extension")?;
                Ok(Message::Extension {
                    ext_id: payload[0],
                    payload: Bytes::copy_from_slice(&payload[1..]),
                })
            }
            id => Ok(Message::Unknown(id)),
        }
    }
}

fn ensure_len(payload: &[u8], min: usize, name: &str) -> Result<()> {
    if payload.len() < min {
        Err(Error::Peer(format!(
            "'{name}' message too short: {} < {min}",
            payload.len()
        )))
    } else {
        Ok(())
    }
}

fn u32_be(buf: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(buf[offset..offset + 4].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode a message and decode the payload back, returning the decoded message.
    fn roundtrip(msg: &Message) -> Message {
        let encoded = msg.encode();
        // encoded = 4-byte length prefix + 1-byte id + payload
        // For KeepAlive: only 4 bytes (length=0), no id byte.
        let len = u32::from_be_bytes(encoded[..4].try_into().unwrap());
        if len == 0 {
            return Message::KeepAlive;
        }
        let id = encoded[4];
        let payload = &encoded[5..];
        Message::decode(id, payload).unwrap()
    }

    #[test]
    fn keepalive_encode_is_four_zero_bytes() {
        let enc = Message::KeepAlive.encode();
        assert_eq!(&enc[..], &[0, 0, 0, 0]);
    }

    #[test]
    fn choke_roundtrip() {
        assert!(matches!(roundtrip(&Message::Choke), Message::Choke));
    }

    #[test]
    fn unchoke_roundtrip() {
        assert!(matches!(roundtrip(&Message::Unchoke), Message::Unchoke));
    }

    #[test]
    fn interested_roundtrip() {
        assert!(matches!(
            roundtrip(&Message::Interested),
            Message::Interested
        ));
    }

    #[test]
    fn not_interested_roundtrip() {
        assert!(matches!(
            roundtrip(&Message::NotInterested),
            Message::NotInterested
        ));
    }

    #[test]
    fn have_roundtrip() {
        let msg = Message::Have(1234567);
        if let Message::Have(idx) = roundtrip(&msg) {
            assert_eq!(idx, 1234567);
        } else {
            panic!("expected Have");
        }
    }

    #[test]
    fn bitfield_roundtrip() {
        let bits = vec![0b10101010, 0b01010101];
        let msg = Message::Bitfield(bits.clone());
        if let Message::Bitfield(decoded) = roundtrip(&msg) {
            assert_eq!(decoded, bits);
        } else {
            panic!("expected Bitfield");
        }
    }

    #[test]
    fn request_roundtrip() {
        let msg = Message::Request {
            index: 10,
            begin: 0,
            length: 16384,
        };
        if let Message::Request {
            index,
            begin,
            length,
        } = roundtrip(&msg)
        {
            assert_eq!(index, 10);
            assert_eq!(begin, 0);
            assert_eq!(length, 16384);
        } else {
            panic!("expected Request");
        }
    }

    #[test]
    fn piece_roundtrip() {
        let data = Bytes::from(vec![0xABu8; 32]);
        let msg = Message::Piece {
            index: 5,
            begin: 512,
            data: data.clone(),
        };
        if let Message::Piece {
            index,
            begin,
            data: d,
        } = roundtrip(&msg)
        {
            assert_eq!(index, 5);
            assert_eq!(begin, 512);
            assert_eq!(&d[..], &data[..]);
        } else {
            panic!("expected Piece");
        }
    }

    #[test]
    fn cancel_roundtrip() {
        let msg = Message::Cancel {
            index: 3,
            begin: 0,
            length: 16384,
        };
        if let Message::Cancel {
            index,
            begin,
            length,
        } = roundtrip(&msg)
        {
            assert_eq!(index, 3);
            assert_eq!(begin, 0);
            assert_eq!(length, 16384);
        } else {
            panic!("expected Cancel");
        }
    }

    #[test]
    fn port_roundtrip() {
        let msg = Message::Port(6881);
        if let Message::Port(p) = roundtrip(&msg) {
            assert_eq!(p, 6881);
        } else {
            panic!("expected Port");
        }
    }

    #[test]
    fn extension_roundtrip() {
        let payload = Bytes::from(vec![0x01, 0x02, 0x03]);
        let msg = Message::Extension {
            ext_id: 7,
            payload: payload.clone(),
        };
        if let Message::Extension { ext_id, payload: p } = roundtrip(&msg) {
            assert_eq!(ext_id, 7);
            assert_eq!(&p[..], &payload[..]);
        } else {
            panic!("expected Extension");
        }
    }

    #[test]
    fn unknown_id_decodes_as_unknown() {
        let decoded = Message::decode(99, &[]).unwrap();
        assert!(matches!(decoded, Message::Unknown(99)));
    }

    #[test]
    fn have_payload_too_short_errors() {
        assert!(Message::decode(MSG_HAVE, &[0, 0]).is_err());
    }

    #[test]
    fn request_payload_too_short_errors() {
        assert!(Message::decode(MSG_REQUEST, &[0u8; 4]).is_err());
    }

    #[test]
    fn block_size_constant() {
        assert_eq!(BLOCK_SIZE, 16_384);
    }
}
