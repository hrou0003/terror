use tokio::sync::Mutex;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use serde::{Deserialize, Serialize};
use crate::Torrent;

pub(crate) struct Piece {
    pub(crate) index: usize,
    pub(crate) length: usize,
    pub(crate) piece_hash: Vec<u8>,
    pub(crate) piece_state: PieceState,
    pub(crate) blocks: Vec<Block>,
    pub(crate) number_of_blocks: usize,
    pub(crate) priority: Priority,
}

pub(crate) struct Block {
    pub(crate) index: usize,
    pub(crate) begin: usize,
    pub(crate) block_size: usize,
    pub(crate) block_state: BlockState,
}

#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Debug)]
pub(crate) enum Priority {
    High,
    Normal,
    Low,
    Skip
}

impl Default for Priority {
    fn default() -> Self {
        Priority::Normal
    }
}

pub(crate) enum BlockState {
    Downloaded {
        data: Vec<u8>,
        duration: Duration,
    },
    Downloading,
    Missing
}

#[derive(PartialEq, PartialOrd, Ord, Eq, Clone)]
pub(crate) enum PieceState {
    Missing,
    Downloading,
    Downloaded { piece_bytes: Vec<u8> },
}

pub(crate) struct PiecePool {
    pub(crate) pieces: HashMap<usize, Arc<Mutex<Piece>>>,
    piece_peer_map: HashMap<usize, HashSet<String>>,
}

impl Block {
    pub fn set_state(&mut self, state: BlockState) {
        self.block_state = state;
    }
}

impl Piece {

    pub const PIECE_SIZE: u32 = 1 << 14;

    pub fn set_state(&mut self, state: PieceState) {
        self.piece_state = state;
    }

    pub fn get_bytes(&self) -> Vec<u8> {
        self.blocks.iter().fold(Vec::new(), |mut acc, block| {
            if let BlockState::Downloaded { data, .. } = &block.block_state {
                acc.extend_from_slice(data)
            }
            acc
        })
    }

    pub fn set_priority(&mut self, priority: Priority) {
        self.priority = priority;
    }

    pub fn get_blocks(&self) -> impl Iterator<Item = &Block> {
        self.blocks.iter()
    }

    pub fn get_blocks_mut(&mut self) -> impl Iterator<Item = &mut Block> {
        self.blocks.iter_mut()
    }
}


impl PiecePool {
    pub(crate) fn new(torrent: &Torrent) -> anyhow::Result<Self> {

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

            (index, Arc::new(Mutex::new(Piece {
                index,
                length,
                piece_hash: torrent.info.pieces[index * 20..(index + 1) * 20].to_vec(),
                piece_state: PieceState::Missing,
                blocks,
                number_of_blocks,
                priority: Priority::Normal,
            })))
        }).collect();

        Ok(PiecePool {
            pieces,
            piece_peer_map: HashMap::new(),
        })
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
    pub async fn get_next_piece_mut(&self) -> Option<Arc<Mutex<Piece>>> {
        // Example prioritization logic: by piece state and then by priority
        let mut sorted_pieces: Vec<_> = self.pieces.values().collect();
        sorted_pieces.sort_by_key(|p| {
            let piece = p.blocking_lock();
            (piece.piece_state.clone(), piece.priority.clone())
        });

        for piece in sorted_pieces {
            let piece = piece.clone();
            let piece_guard = piece.blocking_lock();
            if piece_guard.piece_state == PieceState::Missing {
                return Some(piece.clone());
            }
        }

        None
    }

    pub async fn mark_downloading(&self, index: usize) {
        if let Some(piece) = self.pieces.get(&index) {
            let mut piece = piece.lock().await;
            piece.piece_state = PieceState::Downloading;
        }
    }

    pub fn get_piece(&self, index: usize) -> Option<Arc<Mutex<Piece>>> {
        self.pieces.get(&index).cloned()
    }

    pub fn get_piece_mut(&self, index: usize) -> Option<Arc<Mutex<Piece>>> {
        self.pieces.get(&index).cloned()
    }

    pub fn get_peers_with_piece(&self, piece_index: usize) -> Option<&HashSet<String>> {
        self.piece_peer_map.get(&piece_index)
    }
}
