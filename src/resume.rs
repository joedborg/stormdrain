//! Fast-resume: persist download state to disk and restore on restart.
//!
//! State file: `<download_dir>/.stormdrain/<info_hash_hex>.json`
//!
//! JSON schema:
//! ```json
//! {
//!   "info_hash": "aabb...",
//!   "pieces_done": [0, 1, 4, 5],
//!   "downloaded": 12345678,
//!   "uploaded": 0
//! }
//! ```

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha1::{Digest as _, Sha1};
use tokio::fs;

use crate::{
    error::{Error, Result},
    metainfo::Metainfo,
    piece_manager::PieceManager,
    types::InfoHash,
};

/// Persisted download state used for fast-resume across restarts.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ResumeState {
    pub info_hash: String,
    pub pieces_done: Vec<u32>,
    pub downloaded: u64,
    pub uploaded: u64,
}

impl ResumeState {
    fn path_for(download_dir: &Path, info_hash: &InfoHash) -> PathBuf {
        download_dir
            .join(".stormdrain")
            .join(format!("{}.json", info_hash.to_hex()))
    }

    /// Load the resume state file if it exists, otherwise return an empty state.
    pub async fn load(download_dir: &Path, info_hash: &InfoHash) -> Result<ResumeState> {
        let path = Self::path_for(download_dir, info_hash);
        match fs::read_to_string(&path).await {
            Ok(data) => {
                let state: ResumeState = serde_json::from_str(&data)?;
                Ok(state)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ResumeState {
                info_hash: info_hash.to_hex(),
                ..Default::default()
            }),
            Err(e) => Err(Error::Io(e)),
        }
    }

    /// Persist the current state to disk.
    pub async fn save(&self, download_dir: &Path) -> Result<()> {
        let path = download_dir
            .join(".stormdrain")
            .join(format!("{}.json", self.info_hash));
        fs::create_dir_all(path.parent().unwrap()).await?;
        let json = serde_json::to_string_pretty(self)?;
        // Write to a temp file then rename for atomicity.
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, json.as_bytes()).await?;
        fs::rename(&tmp, &path).await?;
        Ok(())
    }

    /// Remove the resume file (called when download completes).
    pub async fn remove(download_dir: &Path, info_hash: &InfoHash) -> Result<()> {
        let path = Self::path_for(download_dir, info_hash);
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Io(e)),
        }
    }
}

/// Verify the on-disk data for each allegedly-done piece, and mark verified
/// pieces as done in `PieceManager`.  Returns the number of verified pieces.
pub async fn verify_and_resume(
    state: &ResumeState,
    meta: &Metainfo,
    pm: &mut PieceManager,
    download_dir: &Path,
) -> Result<usize> {
    if state.pieces_done.is_empty() {
        return Ok(0);
    }

    let mut verified = 0usize;
    let base_dir = download_dir.to_path_buf();

    for &idx in &state.pieces_done {
        let expected_hash = match meta.piece_hashes.get(idx as usize) {
            Some(h) => *h,
            None => continue,
        };
        let piece_len = meta.piece_len(idx);
        let flat_offset = idx as u64 * meta.piece_length;

        // Read the piece bytes from disk.
        match read_piece_from_disk(meta, &base_dir, flat_offset, piece_len as u32).await {
            Ok(data) => {
                let hash: [u8; 20] = Sha1::digest(&data).into();
                if hash == expected_hash {
                    pm.mark_done(idx, piece_len);
                    verified += 1;
                } else {
                    tracing::debug!("Resume: piece {idx} hash mismatch, will re-download");
                }
            }
            Err(e) => {
                tracing::debug!("Resume: can't read piece {idx}: {e}");
            }
        }
    }

    tracing::info!(
        "Resume: verified {verified}/{} pieces from disk",
        state.pieces_done.len()
    );
    Ok(verified)
}

/// Read `len` bytes at `flat_offset` from the multi-file layout on disk.
async fn read_piece_from_disk(
    meta: &Metainfo,
    base_dir: &Path,
    flat_offset: u64,
    len: u32,
) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(len as usize);
    let mut remaining = len as u64;
    let mut offset = flat_offset;

    for file_info in &meta.files {
        if offset >= file_info.offset + file_info.length {
            continue;
        }
        if file_info.offset >= offset + remaining {
            break;
        }
        let file_start = offset.saturating_sub(file_info.offset);
        let file_end = ((offset + remaining) - file_info.offset).min(file_info.length);
        let read_len = (file_end - file_start) as usize;

        let path = base_dir.join(&file_info.path);
        let mut f = fs::File::open(&path).await?;
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        f.seek(std::io::SeekFrom::Start(file_start)).await?;
        let mut buf = vec![0u8; read_len];
        f.read_exact(&mut buf).await?;
        result.extend_from_slice(&buf);

        offset += read_len as u64;
        remaining -= read_len as u64;
        if remaining == 0 {
            break;
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_state_default_is_empty() {
        let s = ResumeState::default();
        assert!(s.pieces_done.is_empty());
        assert_eq!(s.downloaded, 0);
        assert_eq!(s.uploaded, 0);
    }

    #[tokio::test]
    async fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let info_hash = InfoHash([0xABu8; 20]);

        let state = ResumeState {
            info_hash: info_hash.to_hex(),
            pieces_done: vec![0, 1, 3],
            downloaded: 98304,
            uploaded: 0,
        };
        state.save(dir.path()).await.unwrap();

        let loaded = ResumeState::load(dir.path(), &info_hash).await.unwrap();
        assert_eq!(loaded.info_hash, state.info_hash);
        assert_eq!(loaded.pieces_done, vec![0, 1, 3]);
        assert_eq!(loaded.downloaded, 98304);
    }

    #[tokio::test]
    async fn load_returns_empty_when_file_absent() {
        let dir = tempfile::tempdir().unwrap();
        let info_hash = InfoHash([0x00u8; 20]);
        let loaded = ResumeState::load(dir.path(), &info_hash).await.unwrap();
        assert!(loaded.pieces_done.is_empty());
        assert_eq!(loaded.downloaded, 0);
    }

    #[tokio::test]
    async fn remove_deletes_file() {
        let dir = tempfile::tempdir().unwrap();
        let info_hash = InfoHash([0x01u8; 20]);

        let state = ResumeState {
            info_hash: info_hash.to_hex(),
            pieces_done: vec![0],
            downloaded: 512,
            uploaded: 0,
        };
        state.save(dir.path()).await.unwrap();

        let p = dir
            .path()
            .join(".stormdrain")
            .join(format!("{}.json", info_hash.to_hex()));
        assert!(p.exists());

        ResumeState::remove(dir.path(), &info_hash).await.unwrap();
        assert!(!p.exists());
    }

    #[tokio::test]
    async fn remove_is_no_op_when_file_absent() {
        let dir = tempfile::tempdir().unwrap();
        let info_hash = InfoHash([0x02u8; 20]);
        ResumeState::remove(dir.path(), &info_hash).await.unwrap();
    }

    #[tokio::test]
    async fn save_overwrites_previous_state() {
        let dir = tempfile::tempdir().unwrap();
        let info_hash = InfoHash([0x03u8; 20]);

        let s1 = ResumeState {
            info_hash: info_hash.to_hex(),
            pieces_done: vec![0],
            downloaded: 100,
            uploaded: 0,
        };
        s1.save(dir.path()).await.unwrap();

        let s2 = ResumeState {
            info_hash: info_hash.to_hex(),
            pieces_done: vec![0, 1, 2],
            downloaded: 300,
            uploaded: 0,
        };
        s2.save(dir.path()).await.unwrap();

        let loaded = ResumeState::load(dir.path(), &info_hash).await.unwrap();
        assert_eq!(loaded.pieces_done, vec![0, 1, 2]);
        assert_eq!(loaded.downloaded, 300);
    }
}
