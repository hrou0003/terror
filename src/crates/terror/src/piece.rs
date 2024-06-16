use std::cmp::PartialEq;
use tokio::sync::{mpsc};
use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use std::time::Duration;
use actix::prelude::*;
use anyhow::anyhow;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use crate::Torrent;
use crate::torrent::FileInfo;
use crate::torrent_manager::{CompletedTask, DownloadBlock};

pub(crate) struct Piece {
    pub(crate) index: usize,
    pub(crate) length: usize,
    pub(crate) piece_hash: Vec<u8>,
    pub(crate) piece_state: PieceState,
    pub(crate) blocks: Vec<Block>,
    pub(crate) number_of_blocks: usize,
    pub(crate) priority: Priority,
    pub(crate) files: Vec<FileInfo>,
}

#[derive(Message)]
#[rtype(result = "ResponseFuture<()>")]
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
    Downloaded {
        data: Vec<u8>,
        duration: Duration,
    },
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

pub(crate) struct PiecePool {
    pub(crate) pieces: HashMap<usize, Piece>,
    torrent: Torrent,
    task_tx: AsyncSender<DownloadBlock>,
    completed_task_rx: mpsc::Receiver<CompletedTask>,
    piece_peer_map: HashMap<usize, HashSet<String>>,
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
    pub fn set_priority(&mut self, priority: Priority) {
        self.priority = priority;
    }

    pub fn verify(&self) -> Option<bool> {
        match &self.piece_state {
            PieceState::Downloaded { piece_bytes } => {
                let mut hasher = Sha1::new();
                hasher.update(piece_bytes);
                let downloaded_bytes_hash = hasher.finalize().to_vec();
                return Some(*self.piece_hash == downloaded_bytes_hash);
            }
            _ => None
        }
    }

    pub async fn save_piece(&mut self) -> anyhow::Result<()> {
        // See the todo in the piece pool constructor
        for file_info in &self.files {
            // Get the bytes for the current file
            let (bytes, file_write_start_index): (Vec<u8>, usize) = match &self.piece_state {
                PieceState::Downloaded { piece_bytes } => {
                    let (start_piece, end_piece) = (file_info.start_piece.unwrap_or(0), file_info.end_piece.unwrap_or(0));
                    let piece_offset = (self.index - start_piece) * Self::PIECE_SIZE as usize;
                    let file_write_start_index = piece_offset + file_info.offset.unwrap_or(0);
                    let bytes = if start_piece == self.index {
                        // Check where to start
                        let start_index = file_info.offset.unwrap_or(0);
                        piece_bytes[start_index..].to_vec()
                    } else if end_piece == self.index {
                        // Check where to end 
                        let end_index = (file_info.length % Self::PIECE_SIZE as usize);
                        piece_bytes[..end_index].to_vec()
                    } else {
                        // Write all of the bytes
                        piece_bytes.clone()
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
            file.seek(SeekFrom::Start(file_write_start_index as u64)).await?;

            file.write_all(&bytes).await?;
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

impl PiecePool {
    pub(crate) fn new(torrent: &Torrent, task_tx: AsyncSender<DownloadBlock>, completed_task_rx: mpsc::Receiver<CompletedTask>) -> anyhow::Result<Self> {
        let number_of_pieces = (torrent.info.length as f64 / torrent.info.piece_length as f64).ceil() as usize;
        let block_size = 1 << 14;

        let pieces = (0..number_of_pieces).map(|index| {
            let length = if index == number_of_pieces - 1 {
                torrent.info.length % torrent.info.piece_length
            } else {
                torrent.info.piece_length
            };

            let number_of_blocks = (length + block_size - 1) / block_size;
            let blocks = (0..number_of_blocks).map(|block_index| Block {
                index: block_index,
                begin: block_index * block_size,
                block_size: if block_index == number_of_blocks - 1 && index == number_of_pieces - 1 {
                    length % block_size
                } else {
                    block_size
                },
                block_state: BlockState::Missing,
            }).collect();

            // TODO: Should we also create the files in advance?
            // TODO: Should we instead create a hashmap for each of the files ot maintain a list
            // of all of the pieces which each one has that way we don't need to clone the same file 
            // for each of its pieces? Might even be better to just do this same check in the receive
            // completed task handler

            // Ugh it's very different if it's a single file
            // This logic should probably be moved to the parsing
            let files = match torrent.info.files.as_ref() {
                Some(files) => {
                    files.iter().filter(|file_info| {
                        if let (start_piece, end_piece) = (file_info.start_piece.unwrap_or(0), file_info.end_piece.unwrap_or(0)) {
                            start_piece <= index && index <= end_piece
                        } else {
                            false
                        }
                    }
                    ).cloned().collect()
                }
                None => vec![FileInfo { start_piece: Some(0), end_piece: Some(torrent.get_number_of_pieces() - 1), offset: Some(0), priority: Priority::default(), length: torrent.info.length, path: vec![torrent.info.name.to_string()], name: Some(torrent.info.name.to_string()), md5sum: torrent.info.md5hash.clone() }]
            };

            (index, Piece {
                index,
                length,
                piece_hash: torrent.info.pieces[index * 20..(index + 1) * 20].to_vec(),
                piece_state: PieceState::Missing,
                blocks,
                number_of_blocks,
                priority: Priority::Normal,
                files,
            })
        }).collect();

        Ok(PiecePool {
            pieces,
            torrent: torrent.clone(),
            piece_peer_map: HashMap::new(),
            task_tx,
            completed_task_rx,
        })
    }

    pub async fn start(&mut self) {

        // Queue up the first 10 pieces
        self.queue_n_tasks(5).await;

        // Start listening on the response channel
        loop {
            let Some(completed_task) = self.completed_task_rx.recv().await else {
                break;
            };
            match completed_task {
                CompletedTask::DownloadedBlock { piece_index, block_index, bytes } => {
                    let mut piece = self.pieces.get_mut(&piece_index).unwrap();
                    let mut block = piece.blocks.get_mut(block_index).unwrap();

                    block.set_state(BlockState::Downloaded {
                        data: bytes,
                        duration: Duration::from_secs(0),
                    });

                    // Check if the piece is complete
                    if piece.blocks.iter().all(|block| matches!(block.block_state, BlockState::Downloaded { .. })) {
                        let piece_bytes: Vec<u8> = piece.blocks.iter().flat_map(|block| {
                            match &block.block_state {
                                BlockState::Downloaded { data, .. } => data.clone(),
                                _ => vec![],
                            }
                        }).collect();
                        piece.set_state(PieceState::Downloaded { piece_bytes: piece_bytes.clone() });
                        // TODO: Verify the piece hash and save the piece to its file
                        if piece.verify().unwrap() {

                            // Save to file
                            // We need to know the structure of the file system...
                            match piece.save_piece().await {
                                Ok(_) => {
                                    // Set piece and block states, assuming the save goes well
                                    // Queue more pieces to download
                                    let number_of_queued_pieces = self.queue_n_tasks(5).await;
                                    // Check if all pieces are downloaded
                                    if self.all_pieces_downloaded().unwrap() {
                                        self.task_tx.close();
                                        return;
                                    }
                                }
                                Err(_) => {
                                    return;
                                }
                            };
                        };
                    }
                }
                CompletedTask::FailedBlock { piece_index, block_index } => {
                    println!("Failed to download block {} of piece {}", block_index, piece_index);
                }
            }
        }
    }

    pub async fn queue_n_tasks(&mut self, n: usize) -> Option<usize> {
        // Pieces and blocks need to be flattened
        // Need to think about to handle this better in terms of flagging downloading state between
        // Blocks and Pieces
        let task_tx = self.task_tx.clone();
        for i in 0..n {
            if let Some(piece) = self.get_next_piece_mut() {
                for block in &piece.blocks {
                    task_tx.send(DownloadBlock {
                        piece_index: piece.index,
                        block_index: block.index,
                        begin: block.begin,
                        length: block.block_size,
                    }).await.unwrap();
                }
            } else {
                return None;
            }
        }
        return Some(n);
    }

    // Add peers that have specific pieces
    pub fn add_peer_piece(&mut self, peer_id: &str, piece_index: usize) {
        self.piece_peer_map
            .entry(piece_index)
            .or_insert_with(HashSet::new)
            .insert(peer_id.to_string());
    }

    // Get peers that have a specific piece
    pub fn get_peers_for_piece(&self, piece_index: usize) -> Option<&HashSet<String>> {
        self.piece_peer_map.get(&piece_index)
    }

    // Get the next piece to download based on prioritization logic
    pub fn get_next_piece_mut(&mut self) -> Option<&mut Piece> {
        // Example prioritization logic: by piece state and then by priority
        let mut sorted_pieces: Vec<_> = self.pieces.values_mut().collect();

        sorted_pieces.sort_by_key(|p| (p.piece_state.clone(), p.priority.clone()));

        for piece in sorted_pieces {
            if piece.piece_state == PieceState::Missing {
                piece.piece_state = PieceState::Downloading;
                return Some(piece);
            }
        }

        None
    }

    fn get_next_block(&mut self) -> Option<&Block> {
        let piece = self.get_next_piece_mut().unwrap();
        for block in &mut piece.blocks {
            if block.block_state == BlockState::Missing {
                block.block_state = BlockState::Downloading;
                return Some(block);
            }
        }

        None
    }

    fn get_next_piece(&self) -> Option<&Piece> {
        for piece in self.pieces.values() {
            if piece.piece_state == PieceState::Missing {
                return Some(piece);
            }
        }

        None
    }

    pub fn get_piece(&self, index: usize) -> Option<&Piece> {
        self.pieces.get(&index)
    }

    pub fn all_pieces_downloaded(&self) -> Option<bool> {
        Some(self.pieces.iter().all(|(_, piece)| piece.piece_state == PieceState::Saved))
    }
}
