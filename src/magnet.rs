//! Magnet link parser (BEP-9 `magnet:` URI scheme).
//!
//! A magnet link contains:
//! - `xt=urn:btih:<info_hash>` — 40-hex or 32-base32 info hash (required)
//! - `dn=<name>`               — display name (optional)
//! - `tr=<url>`                — tracker URLs (may repeat)
//! - `ws=<url>`                — web seed URLs (ignored for now)

use crate::{
    error::{Error, Result},
    types::InfoHash,
};

/// A parsed `magnet:` URI.
#[derive(Debug, Clone)]
pub struct MagnetLink {
    pub info_hash: InfoHash,
    pub name: Option<String>,
    pub trackers: Vec<String>,
}

impl MagnetLink {
    /// Parse a `magnet:?…` URI into a [`MagnetLink`].
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::InvalidMagnet`] if the URI is missing the
    /// required `xt=urn:btih:` parameter or the info hash cannot be decoded.
    pub fn parse(uri: &str) -> Result<Self> {
        let uri = uri.trim();
        let rest = uri
            .strip_prefix("magnet:?")
            .ok_or_else(|| Error::InvalidMagnet("must start with 'magnet:?'".into()))?;

        let mut info_hash: Option<InfoHash> = None;
        let mut name: Option<String> = None;
        let mut trackers: Vec<String> = Vec::new();

        for param in rest.split('&') {
            let (key, value) = match param.split_once('=') {
                Some(pair) => pair,
                None => continue,
            };
            let value = percent_decode(value);
            match key {
                "xt" => {
                    if let Some(hash_str) = value.strip_prefix("urn:btih:") {
                        info_hash = Some(parse_info_hash(hash_str)?);
                    }
                }
                "dn" => {
                    name = Some(value);
                }
                "tr" => {
                    trackers.push(value);
                }
                _ => {}
            }
        }

        let info_hash = info_hash
            .ok_or_else(|| Error::InvalidMagnet("missing 'xt=urn:btih:' parameter".into()))?;

        Ok(MagnetLink {
            info_hash,
            name,
            trackers,
        })
    }
}

fn parse_info_hash(s: &str) -> Result<InfoHash> {
    match s.len() {
        40 => {
            // Hex-encoded
            let bytes = hex::decode(s)
                .map_err(|e| Error::InvalidMagnet(format!("invalid hex info_hash: {e}")))?;
            let arr: [u8; 20] = bytes
                .try_into()
                .map_err(|_| Error::InvalidMagnet("info_hash must be 20 bytes".into()))?;
            Ok(InfoHash(arr))
        }
        32 => {
            // Base32-encoded (case-insensitive)
            let upper = s.to_uppercase();
            let bytes = base32_decode(&upper)
                .ok_or_else(|| Error::InvalidMagnet("invalid base32 info_hash".into()))?;
            let arr: [u8; 20] = bytes
                .try_into()
                .map_err(|_| Error::InvalidMagnet("info_hash must be 20 bytes".into()))?;
            Ok(InfoHash(arr))
        }
        _ => Err(Error::InvalidMagnet(format!(
            "info_hash must be 40-char hex or 32-char base32, got {} chars",
            s.len()
        ))),
    }
}

/// Simple percent-decoding (%XX → byte).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b);
                    i += 3;
                    continue;
                }
            }
        } else if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// RFC 4648 base32 decoder (A-Z + 2-7, no padding required).
fn base32_decode(s: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut buf: u64 = 0;
    let mut bits: u32 = 0;
    let mut out = Vec::new();
    for &c in s.as_bytes() {
        let val = ALPHABET.iter().position(|&a| a == c)? as u64;
        buf = (buf << 5) | val;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX_HASH: &str = "aabbccddaabbccddaabbccddaabbccddaabbccdd";

    #[test]
    fn parse_hex_info_hash() {
        let uri = format!("magnet:?xt=urn:btih:{HEX_HASH}");
        let m = MagnetLink::parse(&uri).unwrap();
        assert_eq!(m.info_hash.to_hex(), HEX_HASH);
        assert!(m.name.is_none());
        assert!(m.trackers.is_empty());
    }

    #[test]
    fn parse_with_display_name_plus_decoding() {
        let uri = format!("magnet:?xt=urn:btih:{HEX_HASH}&dn=My+File");
        let m = MagnetLink::parse(&uri).unwrap();
        // percent_decode treats '+' as space.
        assert_eq!(m.name.as_deref(), Some("My File"));
    }

    #[test]
    fn parse_with_display_name_percent_decoding() {
        let uri = format!("magnet:?xt=urn:btih:{HEX_HASH}&dn=My%20Torrent");
        let m = MagnetLink::parse(&uri).unwrap();
        assert_eq!(m.name.as_deref(), Some("My Torrent"));
    }

    #[test]
    fn parse_with_single_tracker() {
        let uri = format!(
            "magnet:?xt=urn:btih:{HEX_HASH}&tr=http%3A%2F%2Ftracker.example.com%2Fannounce"
        );
        let m = MagnetLink::parse(&uri).unwrap();
        assert_eq!(m.trackers.len(), 1);
        assert_eq!(m.trackers[0], "http://tracker.example.com/announce");
    }

    #[test]
    fn parse_with_multiple_trackers() {
        let uri = format!(
            "magnet:?xt=urn:btih:{HEX_HASH}&tr=http%3A%2F%2Fa.example%2Fann&tr=udp%3A%2F%2Fb.example%3A1337"
        );
        let m = MagnetLink::parse(&uri).unwrap();
        assert_eq!(m.trackers.len(), 2);
    }

    #[test]
    fn parse_base32_info_hash_all_zeros() {
        // 32 'A' chars in RFC-4648 base32 = 20 zero bytes.
        let uri = "magnet:?xt=urn:btih:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let m = MagnetLink::parse(uri).unwrap();
        assert_eq!(m.info_hash.as_bytes(), &[0u8; 20]);
    }

    #[test]
    fn error_missing_magnet_scheme() {
        assert!(MagnetLink::parse("http://example.com").is_err());
    }

    #[test]
    fn error_missing_xt_parameter() {
        assert!(MagnetLink::parse("magnet:?dn=foo").is_err());
    }

    #[test]
    fn error_invalid_hex_hash() {
        // Contains 'Z' which is not valid hex.
        let uri = "magnet:?xt=urn:btih:ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ";
        assert!(MagnetLink::parse(uri).is_err());
    }

    #[test]
    fn error_wrong_hash_length() {
        assert!(MagnetLink::parse("magnet:?xt=urn:btih:aabbcc").is_err());
    }

    #[test]
    fn unknown_params_are_ignored() {
        let uri = format!("magnet:?xt=urn:btih:{HEX_HASH}&ws=http%3A%2F%2Fexample.com%2Fseed&xl=65536");
        let m = MagnetLink::parse(&uri).unwrap();
        // ws and xl are ignored; the parse should still succeed.
        assert_eq!(m.info_hash.to_hex(), HEX_HASH);
    }
}
