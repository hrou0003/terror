use std::cmp::PartialEq;
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use std::sync::{Arc};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use actix::prelude::*;
use crate::download::{CompletedTask, DownloadTask};
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

#[derive(Message)]
#[rtype(result = "ResponseFuture<()>")]
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
    pub(crate) pieces: HashMap<usize, Piece>,
    task_tx: broadcast::Sender<DownloadTask>,
    completed_task_rx: mpsc::Receiver<CompletedTask>,
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
    pub fn set_priority(&mut self, priority: Priority) {
        self.priority = priority;
    }

}

impl PiecePool {
    pub(crate) fn new(torrent: &Torrent, task_tx: broadcast::Sender<DownloadTask>, completed_task_rx: mpsc::Receiver<CompletedTask>) -> anyhow::Result<Self> {

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

            (index, Piece {
                index,
                length,
                piece_hash: torrent.info.pieces[index * 20..(index + 1) * 20].to_vec(),
                piece_state: PieceState::Missing,
                blocks,
                number_of_blocks,
                priority: Priority::Normal,
            })
        }).collect();

        Ok(PiecePool {
            pieces,
            piece_peer_map: HashMap::new(),
            task_tx,
            completed_task_rx,
        })
    }
    
    pub async fn start(&mut self) {
        
        // Queue up the first 10 pieces
        self.queue_n_pieces(10);
        
        // Start listening on the response channel
        while let Some(completed_task) = self.completed_task_rx.recv().await {
            let mut piece = self.pieces.get_mut(&completed_task.piece_index).unwrap();
            let mut block = piece.blocks.get_mut(completed_task.block_index).unwrap();
            match completed_task.status {
                Ok(_) => {
                    block.set_state(BlockState::Downloaded {
                        data: completed_task.bytes,
                        duration: Duration::from_secs(0),
                    });
                }
                Err(e) => {
                    block.set_state(BlockState::Missing);
                    println!("Error downloading block: {:?}", e);
                }
            }
            // Check if the piece is complete
            if piece.blocks.iter().all(|block| matches!(block.block_state, BlockState::Downloaded { .. })) {
                let piece_bytes: Vec<u8> = piece.blocks.iter().flat_map(|block| {
                    match &block.block_state {
                        BlockState::Downloaded { data, .. } => data.clone(),
                        _ => vec![],
                    }
                }).collect();
                piece.set_state(PieceState::Downloaded { piece_bytes });
                // TODO: Verify the piece hash and save the piece to its file
            }
        }
        
    }
    
    pub fn queue_n_pieces(&self, n: usize)  {
        for _ in 0..n {
            if let Some(piece) = self.get_next_piece() {
                for block in &piece.blocks {
                    self.task_tx.send(DownloadTask {
                        piece_index: piece.index,
                        block_index: block.index,
                        begin: block.begin,
                        length: block.block_size,
                    }).unwrap();
                }
            }
        }
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
    pub async fn get_next_piece_mut(&mut self) -> Option<&mut Piece> {
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

}
