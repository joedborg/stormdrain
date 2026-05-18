//! Peer ID generation.

use crate::types::PeerId;
use rand::prelude::*;

/// Generate a fresh Azureus-style peer ID: `-SD0100-XXXXXXXXXXXX`
pub fn generate() -> PeerId {
    let mut rng = rand::rng();
    let mut id = [0u8; 20];

    // Fixed prefix: -SD0100-
    id[..8].copy_from_slice(b"-SD0100-");

    // 12 random decimal digits
    for byte in &mut id[8..] {
        *byte = rng.random_range(b'0'..=b'9');
    }

    PeerId(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_has_correct_length() {
        let p = generate();
        assert_eq!(p.as_bytes().len(), 20);
    }

    #[test]
    fn generate_has_azureus_prefix() {
        let p = generate();
        assert_eq!(&p.as_bytes()[..8], b"-SD0100-");
    }

    #[test]
    fn generate_suffix_is_decimal_digits() {
        let p = generate();
        for &b in &p.as_bytes()[8..] {
            assert!(b.is_ascii_digit(), "byte {b} is not an ASCII digit");
        }
    }

    #[test]
    fn generate_produces_unique_ids() {
        // Two independently generated IDs should differ (with overwhelming probability).
        let a = generate();
        let b = generate();
        assert_ne!(a.as_bytes(), b.as_bytes());
    }
}
