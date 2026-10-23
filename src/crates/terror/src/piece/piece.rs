use crate::torrent::torrent_info::FileInfo;
use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::cmp::PartialEq;
use std::io::SeekFrom;
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

pub(crate) struct Piece {
    pub(crate) index: usize,
    pub(crate) length: usize,
    pub(crate) piece_hash: Vec<u8>,
    pub(crate) piece_state: PieceState,
    pub(crate) blocks: Vec<Block>,
    pub(crate) files: Vec<FileInfo>,
}

pub(crate) struct Block {
    pub(crate) index: usize,
    pub(crate) begin: usize,
    pub(crate) block_size: usize,
    pub(crate) block_state: BlockState,
}

#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Priority {
    High,
    Normal,
    Low,
    Skip,
}

impl Default for Priority {
    fn default() -> Self {
        Priority::Normal
    }
}

#[derive(PartialEq, Eq)]
pub(crate) enum BlockState {
    Downloaded { data: Bytes, duration: Duration },
    Downloading,
    Missing,
    Saved,
}

#[derive(PartialEq, PartialOrd, Ord, Eq, Clone)]
pub(crate) enum PieceState {
    Missing,
    Downloading,
    Downloaded { piece_bytes: Vec<u8> },
    Saved,
}

impl Block {
    pub fn set_state(&mut self, state: BlockState) {
        self.block_state = state;
    }
}

impl Piece {
    pub const PIECE_SIZE: u32 = 1 << 15;
    pub const BLOCK_SIZE: usize = 1 << 14;
    pub fn set_state(&mut self, state: PieceState) {
        self.piece_state = state;
    }

    pub fn verify(&self) -> Option<bool> {
        match &self.piece_state {
            PieceState::Downloaded { piece_bytes } => {
                let mut hasher = Sha1::new();
                hasher.update(piece_bytes);
                let downloaded_bytes_hash = hasher.finalize().to_vec();
                let is_correct_hash = Some(downloaded_bytes_hash == *self.piece_hash);
                return is_correct_hash;
            }
            _ => None,
        }
    }

    pub async fn save_piece(&mut self) -> anyhow::Result<()> {
        for file_info in &self.files {
            // Get the bytes for the current file
            let (bytes, file_write_start_index): (&[u8], usize) = match &self.piece_state {
                PieceState::Downloaded { piece_bytes } => {
                    let start_piece = file_info.start_piece.unwrap_or(0);
                    let piece_offset = (self.index - start_piece) * Self::PIECE_SIZE as usize;
                    let file_write_start_index = piece_offset + file_info.offset.unwrap_or(0);
                    let bytes = if start_piece == self.index {
                        // Check where to start
                        let start_index = file_info.offset.unwrap_or(0);
                        &piece_bytes[start_index..]
                    } else {
                        piece_bytes.as_slice()
                    };
                    (bytes, file_write_start_index)
                }
                _ => return Err(anyhow!("Piece not downloaded")),
            };

            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&file_info.path[0])
                .await?;

            // We need to determine the position in the file to write the bytes to
            file.seek(SeekFrom::Start(file_write_start_index as u64))
                .await?;

            file.write_all(bytes).await?;
        }

        self.set_saved();
        Ok(())
    }

    pub fn set_saved(&mut self) {
        self.piece_state = PieceState::Saved;
        self.blocks.iter_mut().for_each(|block| {
            block.block_state = BlockState::Saved;
        });
    }
}
