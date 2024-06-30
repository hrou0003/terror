use std::collections::{HashMap, HashSet};
use std::time::Duration;
use bytes::Bytes;
use kanal::AsyncSender;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{debug, info};
use crate::piece::piece::{Block, BlockState, Piece, PieceState, Priority};
use crate::torrent::torrent_info::{FileInfo, Torrent};
use crate::torrent::torrent_manager::{CompletedTask, DownloadBlock};

pub(crate) struct PiecePool {
    pub(crate) pieces: HashMap<usize, Piece>,
    torrent: Torrent,
    task_tx: AsyncSender<DownloadBlock>,
    completed_task_rx: UnboundedReceiver<CompletedTask>,
    piece_peer_map: HashMap<usize, HashSet<String>>,
}

impl PiecePool {
    pub(crate) fn new(torrent: &Torrent, task_tx: AsyncSender<DownloadBlock>, completed_task_rx: UnboundedReceiver<CompletedTask>) -> anyhow::Result<Self> {
        let number_of_pieces = (torrent.info.length() as f64 / torrent.info.piece_length.unwrap() as f64).ceil() as usize;
        let block_size = 1 << 14;

        let pieces = (0..number_of_pieces).map(|index| {
            let length = if index == number_of_pieces - 1 {
                torrent.info.length() % torrent.info.piece_length.unwrap()
            } else {
                torrent.info.piece_length.unwrap()
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
                None => vec![FileInfo { start_piece: Some(0), end_piece: Some(torrent.get_number_of_pieces() - 1), offset: Some(0), priority: Some(Priority::default()), length: torrent.info.length(), path: vec![torrent.info.name.to_string()], name: Some(torrent.info.name.to_string()), md5sum: torrent.info.md5hash.clone() }]
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
        let mut downloaded_pieces: Vec<usize> = vec![];

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
                                _ => Bytes::new(),
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
                                    downloaded_pieces.insert(0, piece_index);
                                    info!("Downloaded: {:?}%", (downloaded_pieces.len() as f32 / self.pieces.len() as f32) * 100 as f32);
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
                    debug!("Failed to download block {} of piece {}", block_index, piece_index);
                    // Requeue the block
                    // Get the block
                    let piece = self.pieces.iter().find(|(&index, piece)| {
                        index == piece_index
                    });
                    let block = piece.unwrap().1.blocks.iter().find(|&block| {
                        block.index == block_index
                    }).unwrap();

                    self.task_tx.send(DownloadBlock {
                        piece_index,
                        block_index,
                        begin: block.begin,
                        length: block.block_size
                    }).await.unwrap();
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
        self.pieces.values().find(|&piece| piece.piece_state == PieceState::Missing)
    }

    pub fn get_piece(&self, index: usize) -> Option<&Piece> {
        self.pieces.get(&index)
    }

    pub fn all_pieces_downloaded(&self) -> Option<bool> {
        Some(self.pieces.iter().all(|(_, piece)| piece.piece_state == PieceState::Saved))
    }
}
