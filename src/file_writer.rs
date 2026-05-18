//! Async file writer that maps a flat byte stream onto one or more output files.
//!
//! The BitTorrent protocol treats a multi-file torrent as one contiguous byte
//! stream.  When a piece is verified we compute which file(s) it overlaps and
//! write each slice at the correct offset.

use std::path::{Path, PathBuf};

use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{error::Result, metainfo::FileInfo};

/// Writes torrent data to disk, mapping the flat virtual byte stream to real files.
pub struct FileWriter {
    base_dir: PathBuf,
    files: Vec<FileInfo>,
}

impl FileWriter {
    /// Create all necessary directories and return a ready writer.
    pub async fn new(base_dir: PathBuf, files: &[FileInfo]) -> Result<Self> {
        // Create every directory that will be needed.
        let mut dirs_created = std::collections::HashSet::new();
        for f in files {
            let full_path = base_dir.join(&f.path);
            if let Some(parent) = full_path.parent() {
                if dirs_created.insert(parent.to_owned()) {
                    fs::create_dir_all(parent).await?;
                }
            }
        }
        Ok(FileWriter {
            base_dir,
            files: files.to_vec(),
        })
    }

    /// Write `data` for piece `piece_index` (flat byte offset = `piece_index *
    /// piece_length`).  The piece may span multiple files.
    pub async fn write_piece(&self, flat_offset: u64, data: &[u8]) -> Result<()> {
        let data_end = flat_offset + data.len() as u64;

        for file in &self.files {
            let file_start = file.offset;
            let file_end = file.offset + file.length;

            // Determine the overlap between [flat_offset, data_end) and [file_start, file_end).
            let write_start = flat_offset.max(file_start);
            let write_end = data_end.min(file_end);
            if write_start >= write_end {
                continue;
            }

            let data_slice_start = (write_start - flat_offset) as usize;
            let data_slice_end = (write_end - flat_offset) as usize;
            let file_seek_pos = write_start - file_start;

            let full_path = self.base_dir.join(&file.path);
            write_at(
                &full_path,
                file_seek_pos,
                &data[data_slice_start..data_slice_end],
            )
            .await?;
        }

        Ok(())
    }

    /// Read `len` bytes from the flat virtual stream starting at `flat_offset`.
    /// Used by the seeder to serve REQUEST messages.
    pub async fn read_data(&self, flat_offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity(len);
        let data_end = flat_offset + len as u64;

        for file in &self.files {
            let file_start = file.offset;
            let file_end = file.offset + file.length;

            let read_start = flat_offset.max(file_start);
            let read_end = data_end.min(file_end);
            if read_start >= read_end {
                continue;
            }

            let file_seek_pos = read_start - file_start;
            let read_len = (read_end - read_start) as usize;
            let full_path = self.base_dir.join(&file.path);

            let mut f = fs::File::open(&full_path).await?;
            f.seek(std::io::SeekFrom::Start(file_seek_pos)).await?;
            let mut buf = vec![0u8; read_len];
            f.read_exact(&mut buf).await?;
            result.extend_from_slice(&buf);
        }

        Ok(result)
    }
}

async fn write_at(path: &Path, offset: u64, data: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false) // keep existing content
        .open(path)
        .await?;

    file.seek(std::io::SeekFrom::Start(offset)).await?;
    file.write_all(data).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::FileInfo;

    fn make_file_info(path: &str, length: u64, offset: u64) -> FileInfo {
        FileInfo {
            path: PathBuf::from(path),
            length,
            offset,
        }
    }

    #[tokio::test]
    async fn write_and_read_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![make_file_info("data.bin", 1024, 0)];
        let fw = FileWriter::new(dir.path().to_owned(), &files).await.unwrap();

        let data = vec![0xABu8; 1024];
        fw.write_piece(0, &data).await.unwrap();

        let read_back = fw.read_data(0, 1024).await.unwrap();
        assert_eq!(read_back, data);
    }

    #[tokio::test]
    async fn write_partial_and_read_subset() {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![make_file_info("data.bin", 512, 0)];
        let fw = FileWriter::new(dir.path().to_owned(), &files).await.unwrap();

        let mut data = vec![0u8; 512];
        data[100] = 0xFF;
        data[101] = 0xEE;
        fw.write_piece(0, &data).await.unwrap();

        let slice = fw.read_data(100, 2).await.unwrap();
        assert_eq!(slice, vec![0xFF, 0xEE]);
    }

    #[tokio::test]
    async fn write_spanning_two_files() {
        let dir = tempfile::tempdir().unwrap();
        // Two files of 512 bytes each, contiguous in the virtual stream.
        let files = vec![
            make_file_info("part0.bin", 512, 0),
            make_file_info("part1.bin", 512, 512),
        ];
        let fw = FileWriter::new(dir.path().to_owned(), &files).await.unwrap();

        // Write 512 bytes that span the boundary (bytes 256..768).
        let data = vec![0xCCu8; 512];
        fw.write_piece(256, &data).await.unwrap();

        // Read back the last 256 bytes of part0 and first 256 bytes of part1.
        let read_back = fw.read_data(256, 512).await.unwrap();
        assert_eq!(read_back, data);
    }

    #[tokio::test]
    async fn new_creates_directories() {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![make_file_info("subdir/nested/data.bin", 256, 0)];
        // Should succeed even though subdir/nested does not exist yet.
        let fw = FileWriter::new(dir.path().to_owned(), &files).await.unwrap();
        let data = vec![42u8; 256];
        fw.write_piece(0, &data).await.unwrap();
        assert!(dir.path().join("subdir").join("nested").exists());
    }
}
