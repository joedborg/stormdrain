//! Core value types shared across the library.

use std::{fmt, fmt::Write as _, net::IpAddr};

/// 20-byte SHA-1 info hash identifying a torrent.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct InfoHash(pub [u8; 20]);

impl InfoHash {
    /// Return the raw 20 bytes of the info hash.
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// Percent-encode each byte for use in tracker announce URLs.
    pub fn url_encode(&self) -> String {
        let mut s = String::with_capacity(60);
        for b in &self.0 {
            write!(s, "%{b:02x}").unwrap();
        }
        s
    }

    /// Return the info hash as a lowercase hex string.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InfoHash({})", self.to_hex())
    }
}

impl fmt::Display for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// 20-byte Azureus-style peer ID in the format `-SD0100-XXXXXXXXXXXX`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PeerId(pub [u8; 20]);

impl PeerId {
    /// Return the raw 20 bytes of the peer ID.
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// Percent-encode for tracker URLs (unreserved ASCII chars pass through).
    pub fn url_encode(&self) -> String {
        let mut s = String::with_capacity(60);
        for b in &self.0 {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
                s.push(*b as char);
            } else {
                write!(s, "%{b:02x}").unwrap();
            }
        }
        s
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PeerId({})", String::from_utf8_lossy(&self.0))
    }
}

/// A remote peer's network address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerAddr {
    pub ip: IpAddr,
    pub port: u16,
}

impl fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.ip, self.port)
    }
}

impl From<PeerAddr> for std::net::SocketAddr {
    fn from(p: PeerAddr) -> Self {
        std::net::SocketAddr::new(p.ip, p.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn info_hash_to_hex_all_zeros() {
        let h = InfoHash([0u8; 20]);
        assert_eq!(h.to_hex(), "0000000000000000000000000000000000000000");
    }

    #[test]
    fn info_hash_to_hex_known_value() {
        let mut bytes = [0u8; 20];
        bytes[0] = 0xde;
        bytes[1] = 0xad;
        bytes[2] = 0xbe;
        bytes[3] = 0xef;
        let h = InfoHash(bytes);
        assert!(h.to_hex().starts_with("deadbeef"));
        assert_eq!(h.to_hex().len(), 40);
    }

    #[test]
    fn info_hash_url_encode_length() {
        let h = InfoHash([0u8; 20]);
        // Each byte becomes %XX (3 chars); 20 bytes = 60 chars total.
        assert_eq!(h.url_encode().len(), 60);
    }

    #[test]
    fn info_hash_url_encode_format() {
        let mut bytes = [0u8; 20];
        bytes[0] = 0xab;
        bytes[1] = 0xcd;
        let h = InfoHash(bytes);
        let enc = h.url_encode();
        assert!(enc.starts_with("%ab%cd"));
    }

    #[test]
    fn info_hash_as_bytes_roundtrip() {
        let bytes: [u8; 20] = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let h = InfoHash(bytes);
        assert_eq!(h.as_bytes(), &bytes);
    }

    #[test]
    fn info_hash_display() {
        let h = InfoHash([0u8; 20]);
        assert_eq!(h.to_string(), "0000000000000000000000000000000000000000");
    }

    #[test]
    fn peer_id_url_encode_unreserved_passthrough() {
        // Alphanumeric and '-', '.', '_', '~' pass through un-encoded.
        let mut bytes = [b'a'; 20];
        bytes[0] = b'Z';
        bytes[1] = b'-';
        bytes[2] = b'.';
        let p = PeerId(bytes);
        let enc = p.url_encode();
        assert!(enc.starts_with("Z-.a"));
        assert!(!enc.contains('%'));
    }

    #[test]
    fn peer_id_url_encode_non_ascii_percent_encoded() {
        let mut bytes = [0u8; 20];
        bytes[0] = b'a'; // passes through
        bytes[1] = 0xff; // percent-encoded
        bytes[2] = 0x00; // percent-encoded
        let p = PeerId(bytes);
        let enc = p.url_encode();
        assert!(enc.starts_with("a%ff%00"));
    }

    #[test]
    fn peer_id_as_bytes_roundtrip() {
        let bytes = [42u8; 20];
        let p = PeerId(bytes);
        assert_eq!(p.as_bytes(), &bytes);
    }

    #[test]
    fn peer_addr_into_socket_addr() {
        let addr = PeerAddr {
            ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            port: 6881,
        };
        let sa: SocketAddr = addr.into();
        assert_eq!(sa.port(), 6881);
        assert_eq!(sa.ip().to_string(), "192.168.1.1");
    }

    #[test]
    fn info_hash_equality() {
        let a = InfoHash([7u8; 20]);
        let b = InfoHash([7u8; 20]);
        let c = InfoHash([8u8; 20]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
