//! `.torrent` metainfo parsing (BEP-3 + multi-tracker BEP-12).

use sha1::{Digest, Sha1};
use std::path::PathBuf;

use crate::{
    bencode::{self, Value},
    error::{Error, Result},
    types::InfoHash,
};

/// A single file within the torrent.
#[derive(Debug, Clone)]
pub struct FileInfo {
    /// Path relative to the download directory (already sanitised).
    pub path: PathBuf,
    /// Length in bytes.
    pub length: u64,
    /// Byte offset within the concatenated virtual data stream.
    pub offset: u64,
}

/// Parsed torrent metadata.
#[derive(Debug, Clone)]
pub struct Metainfo {
    pub info_hash: InfoHash,
    /// Torrent display name (sanitised for use as a file/directory name).
    pub name: String,
    /// Number of bytes per piece (all pieces except possibly the last).
    pub piece_length: u64,
    /// SHA-1 hash for each piece (20 bytes each).
    pub piece_hashes: Vec<[u8; 20]>,
    pub files: Vec<FileInfo>,
    pub total_length: u64,
    /// Tracker URL tiers (BEP-12). Outer = tier, inner = URLs within tier.
    pub trackers: Vec<Vec<String>>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub is_private: bool,
}

impl Metainfo {
    /// Parse raw `.torrent` bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let (root, info_range) = bencode::decode_torrent(data)?;

        // Compute info_hash from the raw bencoded "info" bytes.
        let info_hash = match info_range {
            Some((start, end)) => {
                let mut hasher = Sha1::new();
                hasher.update(&data[start..end]);
                let digest = hasher.finalize();
                let mut bytes = [0u8; 20];
                bytes.copy_from_slice(&digest);
                InfoHash(bytes)
            }
            None => return Err(Error::InvalidTorrent("missing 'info' key".into())),
        };

        let info = root
            .get(b"info")
            .ok_or_else(|| Error::InvalidTorrent("missing 'info' key".into()))?;

        let name = info
            .get(b"name")
            .and_then(|v| v.as_str())
            .unwrap_or("download")
            .to_owned();
        let name = sanitize_path_component(&name);

        let piece_length = info
            .get(b"piece length")
            .and_then(|v| v.as_int())
            .ok_or_else(|| Error::InvalidTorrent("missing 'piece length'".into()))?
            as u64;

        let pieces_raw = info
            .get(b"pieces")
            .and_then(|v| v.as_bytes())
            .ok_or_else(|| Error::InvalidTorrent("missing 'pieces'".into()))?;

        if pieces_raw.len() % 20 != 0 {
            return Err(Error::InvalidTorrent(format!(
                "'pieces' length {} is not a multiple of 20",
                pieces_raw.len()
            )));
        }
        let piece_hashes: Vec<[u8; 20]> = pieces_raw
            .chunks_exact(20)
            .map(|c| c.try_into().unwrap())
            .collect();

        // Single-file vs multi-file mode.
        let (files, total_length) = if let Some(len_val) = info.get(b"length") {
            let length = len_val
                .as_int()
                .ok_or_else(|| Error::InvalidTorrent("'length' must be an integer".into()))?
                as u64;
            let file = FileInfo {
                path: PathBuf::from(&name),
                length,
                offset: 0,
            };
            (vec![file], length)
        } else {
            let files_list = info
                .get(b"files")
                .and_then(|v| v.as_list())
                .ok_or_else(|| {
                    Error::InvalidTorrent("missing 'files' in multi-file torrent".into())
                })?;

            let mut files = Vec::new();
            let mut offset = 0u64;
            for entry in files_list {
                let length = entry
                    .get(b"length")
                    .and_then(|v| v.as_int())
                    .ok_or_else(|| Error::InvalidTorrent("file entry missing 'length'".into()))?
                    as u64;

                let path_list = entry
                    .get(b"path")
                    .and_then(|v| v.as_list())
                    .ok_or_else(|| Error::InvalidTorrent("file entry missing 'path'".into()))?;

                let mut path = PathBuf::from(&name);
                for component in path_list {
                    let c = component.as_str().ok_or_else(|| {
                        Error::InvalidTorrent("path component must be a string".into())
                    })?;
                    path.push(sanitize_path_component(c));
                }

                files.push(FileInfo {
                    path,
                    length,
                    offset,
                });
                offset = offset
                    .checked_add(length)
                    .ok_or_else(|| Error::InvalidTorrent("total size overflow".into()))?;
            }
            (files, offset)
        };

        let trackers = parse_trackers(&root);

        let comment = root
            .get(b"comment")
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        let created_by = root
            .get(b"created by")
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        let is_private = info.get(b"private").and_then(|v| v.as_int()).unwrap_or(0) != 0;

        Ok(Metainfo {
            info_hash,
            name,
            piece_length,
            piece_hashes,
            files,
            total_length,
            trackers,
            comment,
            created_by,
            is_private,
        })
    }

    pub fn piece_count(&self) -> u32 {
        self.piece_hashes.len() as u32
    }

    /// Length of piece `index` (accounts for shorter last piece).
    pub fn piece_len(&self, index: u32) -> u64 {
        let last = self.piece_count().saturating_sub(1);
        if index == last {
            let rem = self.total_length % self.piece_length;
            if rem == 0 { self.piece_length } else { rem }
        } else {
            self.piece_length
        }
    }

    /// Construct a `Metainfo` from a raw bencoded info-dict (BEP-9).
    /// `info_hash` must already be verified by the caller.
    pub fn from_info_bytes(data: &[u8], info_hash: &InfoHash) -> Result<Self> {
        // Wrap the info-dict in a fake root dict and reuse from_bytes logic.
        let mut wrapped = b"d4:info".to_vec();
        wrapped.extend_from_slice(data);
        wrapped.push(b'e');
        let mut meta = Self::from_bytes(&wrapped)?;
        // Override the info_hash with the pre-verified one.
        meta.info_hash = *info_hash;
        Ok(meta)
    }

    /// Create a placeholder `Metainfo` from a magnet link (before BEP-9 exchange).
    /// The metadata fields (files, pieces, etc.) are empty; download logic must
    /// complete BEP-9 before proceeding.
    pub fn placeholder_magnet(link: &crate::magnet::MagnetLink) -> Self {
        Metainfo {
            info_hash: link.info_hash,
            name: link.name.clone().unwrap_or_else(|| link.info_hash.to_hex()),
            piece_length: 0,
            piece_hashes: Vec::new(),
            files: Vec::new(),
            total_length: 0,
            trackers: link.trackers.iter().map(|u| vec![u.clone()]).collect(),
            comment: None,
            created_by: None,
            is_private: false,
        }
    }
}

// Helpers
/// Prevent path traversal by stripping/replacing dangerous characters.
fn sanitize_path_component(s: &str) -> String {
    // Reject absolute path markers and directory separators entirely.
    if s == "." || s == ".." {
        return "_".to_owned();
    }
    s.chars()
        .map(|c| match c {
            '/' | '\\' | '\0' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c => c,
        })
        .collect()
}

fn parse_trackers(root: &Value) -> Vec<Vec<String>> {
    let mut tiers: Vec<Vec<String>> = Vec::new();

    // BEP-12 announce-list (preferred).
    if let Some(list) = root.get(b"announce-list").and_then(|v| v.as_list()) {
        for tier_val in list {
            if let Some(tier) = tier_val.as_list() {
                let urls: Vec<String> = tier
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect();
                if !urls.is_empty() {
                    tiers.push(urls);
                }
            }
        }
    }

    // BEP-3 single announce URL.
    if let Some(url) = root.get(b"announce").and_then(|v| v.as_str()) {
        let url = url.to_owned();
        if tiers.is_empty() {
            tiers.push(vec![url]);
        } else if !tiers[0].contains(&url) {
            tiers[0].insert(0, url);
        }
    }

    tiers
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal single-file torrent bencoded blob.
    ///
    /// Keys are sorted as required by bencode:
    ///   outer: `announce` < `info`
    ///   info:  `length` < `name` < `piece length` < `pieces`
    fn minimal_torrent_bytes(
        name: &str,
        length: u64,
        piece_length: u64,
        piece_hash: [u8; 20],
    ) -> Vec<u8> {
        let mut v: Vec<u8> = Vec::new();
        v.extend_from_slice(b"d");
        // announce
        let announce = "http://tracker.example.com/ann";
        v.extend_from_slice(format!("8:announce{}:{}", announce.len(), announce).as_bytes());
        // info dict
        v.extend_from_slice(b"4:infod");
        v.extend_from_slice(format!("6:lengthi{}e", length).as_bytes());
        v.extend_from_slice(format!("4:name{}:{}", name.len(), name).as_bytes());
        v.extend_from_slice(format!("12:piece lengthi{}e", piece_length).as_bytes());
        v.extend_from_slice(b"6:pieces20:");
        v.extend_from_slice(&piece_hash);
        v.extend_from_slice(b"e"); // end info dict
        v.extend_from_slice(b"e"); // end outer dict
        v
    }

    #[test]
    fn from_bytes_single_file_basic() {
        let data = minimal_torrent_bytes("test.txt", 65536, 65536, [0u8; 20]);
        let meta = Metainfo::from_bytes(&data).unwrap();
        assert_eq!(meta.name, "test.txt");
        assert_eq!(meta.total_length, 65536);
        assert_eq!(meta.piece_length, 65536);
        assert_eq!(meta.piece_hashes.len(), 1);
        assert_eq!(meta.piece_hashes[0], [0u8; 20]);
        assert_eq!(meta.files.len(), 1);
        assert_eq!(meta.files[0].length, 65536);
        assert_eq!(meta.files[0].offset, 0);
        assert!(!meta.is_private);
    }

    #[test]
    fn from_bytes_stores_announce_tracker() {
        let data = minimal_torrent_bytes("file.bin", 512, 512, [1u8; 20]);
        let meta = Metainfo::from_bytes(&data).unwrap();
        // At least one tier with the announce URL.
        assert!(!meta.trackers.is_empty());
        assert!(meta.trackers[0]
            .iter()
            .any(|u| u.contains("tracker.example.com")));
    }

    #[test]
    fn piece_count_matches_hashes_len() {
        let data = minimal_torrent_bytes("x", 1024, 1024, [0u8; 20]);
        let meta = Metainfo::from_bytes(&data).unwrap();
        assert_eq!(meta.piece_count(), 1);
    }

    #[test]
    fn piece_len_normal_and_last() {
        let _data = minimal_torrent_bytes("x", 1500, 1000, [0u8; 20]);
        // Note: pieces field has only 20 bytes (1 hash), but total=1500, piece_len=1000 →
        // 2 pieces. For a valid torrent we'd need 2 hashes; but from_bytes only checks
        // pieces.len() % 20 == 0, so we supply 2 hashes.
        let mut v: Vec<u8> = Vec::new();
        v.extend_from_slice(b"d");
        let announce = "http://tracker.example.com/ann";
        v.extend_from_slice(format!("8:announce{}:{}", announce.len(), announce).as_bytes());
        v.extend_from_slice(b"4:infod");
        v.extend_from_slice(b"6:lengthi1500e");
        v.extend_from_slice(b"4:name1:x");
        v.extend_from_slice(b"12:piece lengthi1000e");
        v.extend_from_slice(b"6:pieces40:"); // 40 bytes = 2 hashes
        v.extend_from_slice(&[0u8; 40]);
        v.extend_from_slice(b"e");
        v.extend_from_slice(b"e");
        let meta = Metainfo::from_bytes(&v).unwrap();
        assert_eq!(meta.piece_len(0), 1000);
        assert_eq!(meta.piece_len(1), 500); // 1500 % 1000 = 500
    }

    #[test]
    fn piece_len_last_is_full_when_exact_multiple() {
        // total = 2000, piece_length = 1000 → last piece = 1000.
        let mut v: Vec<u8> = Vec::new();
        v.extend_from_slice(b"d");
        let announce = "http://tracker.example.com/ann";
        v.extend_from_slice(format!("8:announce{}:{}", announce.len(), announce).as_bytes());
        v.extend_from_slice(b"4:infod");
        v.extend_from_slice(b"6:lengthi2000e");
        v.extend_from_slice(b"4:name1:x");
        v.extend_from_slice(b"12:piece lengthi1000e");
        v.extend_from_slice(b"6:pieces40:");
        v.extend_from_slice(&[0u8; 40]);
        v.extend_from_slice(b"e");
        v.extend_from_slice(b"e");
        let meta = Metainfo::from_bytes(&v).unwrap();
        assert_eq!(meta.piece_len(1), 1000);
    }

    #[test]
    fn from_info_bytes_applies_provided_info_hash() {
        let data = minimal_torrent_bytes("test.txt", 65536, 65536, [0u8; 20]);
        let meta_orig = Metainfo::from_bytes(&data).unwrap();
        // Extract the raw info bytes from the torrent.
        let (_, range) = crate::bencode::decode_torrent(&data).unwrap();
        let (start, end) = range.unwrap();
        let info_bytes = &data[start..end];
        // Provide a custom info_hash (overrides computed SHA-1).
        let custom_hash = InfoHash([42u8; 20]);
        let meta2 = Metainfo::from_info_bytes(info_bytes, &custom_hash).unwrap();
        assert_eq!(meta2.info_hash, custom_hash);
        assert_eq!(meta2.name, meta_orig.name);
    }

    #[test]
    fn placeholder_magnet_uses_magnet_fields() {
        use crate::magnet::MagnetLink;
        let uri = "magnet:?xt=urn:btih:aabbccddaabbccddaabbccddaabbccddaabbccdd&dn=MyFile&tr=http%3A%2F%2Ftracker.example.com%2Fann";
        let link = MagnetLink::parse(uri).unwrap();
        let meta = Metainfo::placeholder_magnet(&link);
        assert_eq!(meta.info_hash, link.info_hash);
        assert_eq!(meta.name, "MyFile");
        assert_eq!(meta.total_length, 0);
        assert!(meta.piece_hashes.is_empty());
        assert_eq!(meta.trackers.len(), 1);
    }

    #[test]
    fn sanitize_strips_dangerous_chars() {
        // Test via `name` field in a torrent: dangerous chars should become '_'.
        let raw_name = "foo/bar\\baz:file";
        let data = minimal_torrent_bytes(raw_name, 100, 100, [0u8; 20]);
        let meta = Metainfo::from_bytes(&data).unwrap();
        assert!(!meta.name.contains('/'));
        assert!(!meta.name.contains('\\'));
        assert!(!meta.name.contains(':'));
    }

    #[test]
    fn sanitize_dot_dot_becomes_underscore() {
        let data = minimal_torrent_bytes("..", 100, 100, [0u8; 20]);
        let meta = Metainfo::from_bytes(&data).unwrap();
        assert_eq!(meta.name, "_");
    }

    #[test]
    fn missing_info_key_errors() {
        let data = b"d4:name4:teste";
        assert!(Metainfo::from_bytes(data).is_err());
    }

    #[test]
    fn pieces_not_multiple_of_20_errors() {
        let mut v: Vec<u8> = Vec::new();
        v.extend_from_slice(b"d");
        let announce = "http://tracker.example.com/ann";
        v.extend_from_slice(format!("8:announce{}:{}", announce.len(), announce).as_bytes());
        v.extend_from_slice(b"4:infod");
        v.extend_from_slice(b"6:lengthi100e");
        v.extend_from_slice(b"4:name4:test");
        v.extend_from_slice(b"12:piece lengthi100e");
        v.extend_from_slice(b"6:pieces5:hello"); // 5 bytes, not multiple of 20
        v.extend_from_slice(b"e");
        v.extend_from_slice(b"e");
        assert!(Metainfo::from_bytes(&v).is_err());
    }
}
