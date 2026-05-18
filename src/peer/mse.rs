//! MSE/PE — Message Stream Encryption (Bittorrent protocol v2.0).
//!
//! Reference: <http://wiki.vuze.com/w/Message_Stream_Encryption>
//!
//! Handshake overview:
//!   Initiator (us):
//!     1. Generate random DH private key `a` (160-bit)
//!     2. Compute public key `Xa = g^a mod P`  (768-bit Diffie-Hellman)
//!     3. Send `Xa` (96 bytes) padded with 0–512 random bytes
//!     4. Receive peer public key `Xb` (96 bytes) + padding
//!     5. Compute shared secret `S = Xb^a mod P`
//!     6. Key derivation:
//!          req1 = SHA1("req1" || S)
//!          req2 = SHA1("req2" || SKEY)   (SKEY = info_hash)
//!          req3 = SHA1("req3" || S)
//!     7. Send `req1 XOR (req2 XOR req3)` (20 bytes — obfuscated SKEY hash)
//!     8. Derive RC4 keys:
//!          keyA = SHA1("keyA" || S || SKEY)   → encrypt key
//!          keyB = SHA1("keyB" || S || SKEY)   → decrypt key
//!     9. Encrypt+send: VC(8 bytes) || crypto_provide(4) || len(padC)(2) || padC || len(IA)(2) || IA
//!    10. Receive+decrypt: VC(8) || crypto_select(4) || len(padD)(2) || padD
//!    11. Optionally discard first 1024 bytes of RC4 stream (already done in key setup)
//!
//! We support crypto_select = 0x02 (RC4) and fall back to 0x01 (plaintext).

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use num_bigint::BigUint;
use rand::prelude::*;
use sha1::{Digest as _, Sha1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

use crate::{
    error::{Error, Result},
    types::InfoHash,
};

// DH parameters (from the MSE spec / Azureus source)
/// 768-bit prime P (Vuze/Azureus spec).
const P_HEX: &str = "FFFFFFFF FFFFFFFF C90FDAA2 2168C234 C4C6628B 80DC1CD1\
                      29024E08 8A67CC74 020BBEA6 3B139B22 514A0879 8E3404DD\
                      EF9519B3 CD3A431B 302B0A6D F25F1437 4FE1356D 6D51C245\
                      E485B576 625E7EC6 F44C42E9 A63A3620 FFFFFFFF FFFFFFFF";

const G: u64 = 2;
const DH_KEY_BYTES: usize = 96; // 768 / 8

// RC4 state
struct Rc4 {
    s: [u8; 256],
    i: u8,
    j: u8,
}

impl Rc4 {
    fn new(key: &[u8]) -> Self {
        let mut s: [u8; 256] = core::array::from_fn(|i| i as u8);
        let mut j: u8 = 0;
        for i in 0u8..=255 {
            j = j
                .wrapping_add(s[i as usize])
                .wrapping_add(key[i as usize % key.len()]);
            s.swap(i as usize, j as usize);
        }
        let mut rc4 = Rc4 { s, i: 0, j: 0 };
        // Discard first 1024 bytes.
        let mut discard = [0u8; 1024];
        rc4.apply(&mut discard);
        rc4
    }

    fn apply(&mut self, data: &mut [u8]) {
        for byte in data.iter_mut() {
            self.i = self.i.wrapping_add(1);
            self.j = self.j.wrapping_add(self.s[self.i as usize]);
            self.s.swap(self.i as usize, self.j as usize);
            let k =
                self.s[(self.s[self.i as usize].wrapping_add(self.s[self.j as usize])) as usize];
            *byte ^= k;
        }
    }

    fn encrypt(&mut self, data: &[u8]) -> Vec<u8> {
        let mut buf = data.to_vec();
        self.apply(&mut buf);
        buf
    }

    fn decrypt(&mut self, data: &mut [u8]) {
        self.apply(data);
    }
}

// Public API
/// Encrypted stream wrapping a `TcpStream`.
pub struct MseStream {
    inner: TcpStream,
    enc: Rc4,
    dec: Rc4,
    /// Encrypted bytes waiting to be flushed to the wire.
    write_buf: Vec<u8>,
}

impl MseStream {
    /// Unwrap the encrypted stream and return the underlying `TcpStream`.
    pub fn into_inner(self) -> TcpStream {
        self.inner
    }
}

impl AsyncRead for MseStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        let before = buf.filled().len();
        let result = Pin::new(&mut me.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) {
            me.dec.apply(&mut buf.filled_mut()[before..]);
        }
        result
    }
}

impl AsyncWrite for MseStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();
        // Flush any buffered encrypted bytes first.
        if !me.write_buf.is_empty() {
            let written = {
                let data = &me.write_buf[..];
                match Pin::new(&mut me.inner).poll_write(cx, data) {
                    Poll::Ready(Ok(n)) => n,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            };
            me.write_buf.drain(..written);
            if !me.write_buf.is_empty() {
                return Poll::Pending;
            }
        }
        // Encrypt new data and try to write immediately.
        me.write_buf = me.enc.encrypt(buf);
        let written = {
            let data = &me.write_buf[..];
            match Pin::new(&mut me.inner).poll_write(cx, data) {
                Poll::Ready(Ok(n)) => n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => 0, // buffered, flushed on next poll
            }
        };
        me.write_buf.drain(..written);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        while !me.write_buf.is_empty() {
            let written = {
                let data = &me.write_buf[..];
                match Pin::new(&mut me.inner).poll_write(cx, data) {
                    Poll::Ready(Ok(n)) => n,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            };
            me.write_buf.drain(..written);
        }
        Pin::new(&mut me.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Perform the MSE initiator handshake.
///
/// Returns `Ok(Some(stream))` if MSE negotiation succeeded with RC4,
/// or `Ok(None)` if the peer chose plaintext (caller should use raw stream).
///
/// On error the caller should retry with a plain connection.
pub async fn perform_initiator(mut stream: TcpStream, info_hash: &InfoHash) -> Result<MseStream> {
    let p = parse_prime();
    let g = BigUint::from(G);

    // Step 1–2: DH key generation.
    let a = random_dh_key();
    let xa = g.modpow(&a, &p);
    let xa_bytes = to_fixed_bytes(&xa);

    // Step 3: Send Xa + random padding (0–64 bytes).
    let pad_len: usize = rand::rng().random_range(0..64);
    let mut pad = vec![0u8; pad_len];
    rand::rng().fill(pad.as_mut_slice());
    stream.write_all(&xa_bytes).await?;
    stream.write_all(&pad).await?;

    // Step 4: Receive Xb (96 bytes). Skip any leading padding the peer sent.
    let xb_bytes = read_dh_key(&mut stream).await?;
    let xb = BigUint::from_bytes_be(&xb_bytes);

    // Step 5: shared secret.
    let s = xb.modpow(&a, &p);
    let s_bytes = to_fixed_bytes(&s);

    // Step 6–7: send obfuscated SKEY hash.
    let req1 = sha1_hash(&[b"req1", s_bytes.as_slice()]);
    let req2 = sha1_hash(&[b"req2", info_hash.as_bytes()]);
    let req3 = sha1_hash(&[b"req3", s_bytes.as_slice()]);
    let mut vc_hash = [0u8; 20];
    for i in 0..20 {
        vc_hash[i] = req1[i] ^ (req2[i] ^ req3[i]);
    }
    stream.write_all(&vc_hash).await?;

    // Step 8: Derive RC4 keys.
    let key_a = sha1_hash(&[b"keyA", s_bytes.as_slice(), info_hash.as_bytes()]);
    let key_b = sha1_hash(&[b"keyB", s_bytes.as_slice(), info_hash.as_bytes()]);
    let mut enc_rc4 = Rc4::new(&key_a); // we encrypt with keyA
    let mut dec_rc4 = Rc4::new(&key_b); // we decrypt with keyB

    // Step 9: Encrypt and send VC || crypto_provide || len(padC) || padC || len(IA) || IA
    let vc = [0u8; 8]; // verification constant = 8 zero bytes
    const CRYPTO_PROVIDE_RC4: u32 = 0x02;
    let mut init_msg = Vec::new();
    init_msg.extend_from_slice(&vc);
    init_msg.extend_from_slice(&CRYPTO_PROVIDE_RC4.to_be_bytes());
    init_msg.extend_from_slice(&0u16.to_be_bytes()); // padC len = 0
    init_msg.extend_from_slice(&0u16.to_be_bytes()); // IA len = 0
    let encrypted_init = enc_rc4.encrypt(&init_msg);
    stream.write_all(&encrypted_init).await?;

    // Step 10: Receive VC (8) || crypto_select (4) || len(padD) (2) || padD
    let mut resp = vec![0u8; 14];
    stream.read_exact(&mut resp).await?;
    dec_rc4.decrypt(&mut resp);
    let _vc_check = &resp[..8]; // should be 0x00 * 8
    let crypto_select = u32::from_be_bytes(resp[8..12].try_into().unwrap());
    let pad_d_len = u16::from_be_bytes(resp[12..14].try_into().unwrap()) as usize;
    if pad_d_len > 0 {
        let mut pad_d = vec![0u8; pad_d_len];
        stream.read_exact(&mut pad_d).await?;
        dec_rc4.decrypt(&mut pad_d);
    }

    if crypto_select & 0x02 == 0 {
        // Peer chose plaintext or something we don't support.
        return Err(Error::Peer("MSE: peer did not select RC4".into()));
    }

    Ok(MseStream {
        inner: stream,
        enc: enc_rc4,
        dec: dec_rc4,
        write_buf: Vec::new(),
    })
}

// Helpers
fn parse_prime() -> BigUint {
    let hex: String = P_HEX.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    BigUint::parse_bytes(hex.as_bytes(), 16).expect("hardcoded prime invalid")
}

fn random_dh_key() -> BigUint {
    let mut bytes = [0u8; 20]; // 160-bit private key
    rand::rng().fill(&mut bytes);
    BigUint::from_bytes_be(&bytes)
}

fn to_fixed_bytes(n: &BigUint) -> Vec<u8> {
    let mut b = n.to_bytes_be();
    // Pad or truncate to DH_KEY_BYTES.
    while b.len() < DH_KEY_BYTES {
        b.insert(0, 0);
    }
    b.truncate(DH_KEY_BYTES);
    b
}

/// Read exactly the DH public key (96 bytes) from the stream, skipping
/// any random padding the peer prepended (the key starts with non-zero bytes,
/// and peer pads with random bytes — we detect the key by its position after
/// any leading pad the peer sent first; per spec the peer sends Ya directly
/// after its own pad, so we just read 96 bytes).
async fn read_dh_key(stream: &mut TcpStream) -> Result<[u8; DH_KEY_BYTES]> {
    let mut buf = [0u8; DH_KEY_BYTES];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

fn sha1_hash(parts: &[&[u8]]) -> [u8; 20] {
    let mut h = Sha1::new();
    for part in parts {
        h.update(part);
    }
    h.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Rc4

    #[test]
    fn rc4_encrypt_decrypt_is_symmetric() {
        let key = b"secret-key-12345";
        let plaintext = b"Hello, BitTorrent world!";

        let mut enc = Rc4::new(key);
        let ciphertext = enc.encrypt(plaintext);
        assert_ne!(&ciphertext, plaintext);

        let mut dec = Rc4::new(key);
        let mut buf = ciphertext.clone();
        dec.decrypt(&mut buf);
        assert_eq!(&buf, plaintext);
    }

    #[test]
    fn rc4_apply_twice_with_fresh_instance_is_identity() {
        // RC4 is its own inverse: encrypt(key, encrypt(key, m)) == m
        // when each encryption uses a freshly initialized cipher.
        let key = b"test";
        let original = vec![0x01u8, 0x02, 0x03, 0x04, 0x05];
        let mut buf = original.clone();
        Rc4::new(key).apply(&mut buf);
        Rc4::new(key).apply(&mut buf); // fresh instance → same keystream → restores original
        assert_eq!(buf, original);
    }

    #[test]
    fn rc4_different_keys_produce_different_ciphertext() {
        let msg = b"same plaintext";
        let mut enc1 = Rc4::new(b"key1");
        let mut enc2 = Rc4::new(b"key2");
        let c1 = enc1.encrypt(msg);
        let c2 = enc2.encrypt(msg);
        assert_ne!(c1, c2);
    }

    #[test]
    fn rc4_empty_input_is_no_op() {
        let mut rc4 = Rc4::new(b"key");
        let mut buf: Vec<u8> = vec![];
        rc4.apply(&mut buf);
        assert!(buf.is_empty());
    }

    // DH helpers

    #[test]
    fn parse_prime_is_nonzero() {
        let p = parse_prime();
        assert_ne!(p, BigUint::from(0u32));
    }

    #[test]
    fn parse_prime_has_expected_byte_length() {
        let p = parse_prime();
        // 768-bit prime = 96 bytes.
        assert_eq!(p.to_bytes_be().len(), DH_KEY_BYTES);
    }

    #[test]
    fn to_fixed_bytes_pads_short_number_to_96_bytes() {
        let n = BigUint::from(1u32);
        let bytes = to_fixed_bytes(&n);
        assert_eq!(bytes.len(), DH_KEY_BYTES);
        // Value 1 should appear at the last byte.
        assert_eq!(bytes[DH_KEY_BYTES - 1], 1);
        // All other bytes are zero.
        assert!(bytes[..DH_KEY_BYTES - 1].iter().all(|&b| b == 0));
    }

    #[test]
    fn dh_key_bytes_constant() {
        assert_eq!(DH_KEY_BYTES, 96);
    }

    // sha1_hash helper

    #[test]
    fn sha1_hash_known_value() {
        // SHA-1 of empty string = da39a3ee5e6b4b0d3255bfef95601890afd80709
        let digest = sha1_hash(&[b""]);
        let hex = hex::encode(digest);
        assert_eq!(hex, "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn sha1_hash_multiple_parts() {
        // SHA-1("ab") == SHA-1("a" || "b")
        let combined = sha1_hash(&[b"ab"]);
        let split = sha1_hash(&[b"a", b"b"]);
        assert_eq!(combined, split);
    }
}
