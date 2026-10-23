use crate::piece::piece::{Block, BlockState, Piece, PieceState, Priority};
use crate::torrent::torrent_downloader::{CompletedTask, DownloadBlock};
use crate::torrent::torrent_info::{FileInfo, Torrent};
use anyhow::{bail, Context, Result};
use std::time::Duration;

/// Piece state is owned by the torrent actor, not shared with a worker task.
pub(crate) struct PiecePool {
    pieces: Vec<Piece>,
    next_piece: usize,
    saved_pieces: usize,
}

impl PiecePool {
    pub(crate) fn new(torrent: &Torrent) -> Result<Self> {
        let piece_length = torrent.info.piece_length.context("missing piece length")?;
        let total_length = torrent.info.length();
        let number_of_pieces = torrent.get_number_of_pieces();
        let mut pieces = Vec::with_capacity(number_of_pieces);

        for index in 0..number_of_pieces {
            let length = piece_length.min(total_length - index * piece_length);
            let number_of_blocks = length.div_ceil(Piece::BLOCK_SIZE);
            let blocks = (0..number_of_blocks)
                .map(|block_index| {
                    let begin = block_index * Piece::BLOCK_SIZE;
                    Block {
                        index: block_index,
                        begin,
                        block_size: Piece::BLOCK_SIZE.min(length - begin),
                        block_state: BlockState::Missing,
                    }
                })
                .collect();
            let files = match &torrent.info.files {
                Some(files) => files
                    .iter()
                    .filter(|file| {
                        file.start_piece.unwrap_or(0) <= index
                            && index <= file.end_piece.unwrap_or(0)
                    })
                    .cloned()
                    .collect(),
                None => vec![FileInfo {
                    start_piece: Some(0),
                    end_piece: Some(number_of_pieces - 1),
                    offset: Some(0),
                    priority: Some(Priority::default()),
                    length: total_length,
                    path: vec![torrent.info.name.clone()],
                    name: Some(torrent.info.name.clone()),
                    md5sum: torrent.info.md5hash.clone(),
                }],
            };
            pieces.push(Piece {
                index,
                length,
                piece_hash: torrent.info.pieces[index * 20..(index + 1) * 20].to_vec(),
                piece_state: PieceState::Missing,
                blocks,
                files,
            });
        }

        Ok(Self {
            pieces,
            next_piece: 0,
            saved_pieces: 0,
        })
    }

    /// Reserve one missing block only when the peer queue has capacity.
    pub(crate) fn next_task(&mut self) -> Option<DownloadBlock> {
        while let Some(piece) = self.pieces.get_mut(self.next_piece) {
            if let Some(block) = piece
                .blocks
                .iter_mut()
                .find(|block| block.block_state == BlockState::Missing)
            {
                piece.piece_state = PieceState::Downloading;
                block.block_state = BlockState::Downloading;
                return Some(DownloadBlock {
                    piece_index: piece.index,
                    block_index: block.index,
                    begin: block.begin,
                    length: block.block_size,
                });
            }
            self.next_piece += 1;
        }
        None
    }

    pub(crate) async fn handle_completed_task(&mut self, task: CompletedTask) -> Result<()> {
        match task {
            CompletedTask::DownloadedBlock {
                piece_index,
                block_index,
                bytes,
                ..
            } => {
                let piece = self.pieces.get_mut(piece_index).context("unknown piece")?;
                let block = piece.blocks.get_mut(block_index).context("unknown block")?;
                // A repeated response must not count the same saved piece twice.
                if matches!(
                    block.block_state,
                    BlockState::Saved | BlockState::Downloaded { .. }
                ) {
                    return Ok(());
                }
                if bytes.len() != block.block_size {
                    bail!("incorrect length for piece {piece_index}, block {block_index}");
                }
                block.set_state(BlockState::Downloaded {
                    data: bytes,
                    duration: Duration::ZERO,
                });
                if !piece
                    .blocks
                    .iter()
                    .all(|block| matches!(block.block_state, BlockState::Downloaded { .. }))
                {
                    return Ok(());
                }

                let mut piece_bytes = Vec::with_capacity(piece.length);
                for block in &piece.blocks {
                    if let BlockState::Downloaded { data, .. } = &block.block_state {
                        piece_bytes.extend_from_slice(data);
                    }
                }
                piece.set_state(PieceState::Downloaded { piece_bytes });
                if piece.verify() != Some(true) {
                    bail!("hash mismatch for piece {piece_index}");
                }
                piece.save_piece().await?;
                self.saved_pieces += 1;
            }
            CompletedTask::FailedBlock {
                piece_index,
                block_index,
                ..
            } => {
                let piece = self.pieces.get_mut(piece_index).context("unknown piece")?;
                let block = piece.blocks.get_mut(block_index).context("unknown block")?;
                if block.block_state == BlockState::Downloading {
                    block.block_state = BlockState::Missing;
                    self.next_piece = self.next_piece.min(piece_index);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.saved_pieces == self.pieces.len()
    }

    pub(crate) fn get_progress(&self) -> f64 {
        if self.pieces.is_empty() {
            return 100.0;
        }
        self.saved_pieces as f64 / self.pieces.len() as f64 * 100.0
    }
}
