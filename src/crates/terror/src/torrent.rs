use std::collections::{BinaryHeap, HashMap};
use std::fs;
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::task::JoinSet;
use crate::message::Message;
use crate::peer::{Peer, PeerPool, PeerState};

use crate::piece::{BlockState, Piece, PiecePool, PieceState, Priority};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Torrent {
    // URL to a "tracker", which is a central server that keeps track of peers participating in the sharing of a torrent.
    pub announce: String,
    pub info: Info,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Info {
    // size of the file in bytes, for single-file torrents
    #[serde(default)]
    pub length: usize,
    // suggested name to save the file / directory as
    pub name: String,
    // number of bytes in each piece
    #[serde(rename = "piece length")]
    pub piece_length: usize,
    // concatenated SHA-1 hashes of each piece
    pub pieces: ByteBuf,
    #[serde(default)]
    pub md5hash: Option<String>,
    // list of files in a multi-file torrent
    #[serde(default)]
    pub files: Option<Vec<FileInfo>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct FileInfo {
    name: String,
    length: usize,
    path: String,
    md5sum: String,
    #[serde(skip)]
    offset: usize,
    #[serde(skip)]
    start_piece: usize,
    #[serde(skip)]
    end_piece: usize,
    #[serde(skip)]
    priority: Priority,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum FileTree {
    SingleFile {
        file_info: FileInfo
    },
    MultiFile {
        files: HashMap<String, FileInfo>
    }
}

impl Torrent {
    pub const MAX_CONCURRENT: usize = 4;
    pub const MAX_RETRIES: usize = 2;

    pub fn new(file_path: String) -> Torrent {
        let file = fs::read(file_path).expect("bad file");
        let torrent : Torrent = serde_bencode::de::from_bytes(&file).expect("Invalid torrent file");
        return torrent;
    }

    pub(crate) fn calculate_info_hash(&self) -> [u8; 20] {
        let info = &self.info;
        let info_raw = serde_bencode::to_bytes(&info).expect("Invalid info dictionary");
        let decoded = serde_bencode::from_bytes::<Info>(&info_raw).expect("Bad info");
        println!("{:?}", decoded);
        let mut hasher = Sha1::new();
        hasher.update(info_raw);
        return hasher.finalize().into();
    }
    
    pub(crate) fn get_number_of_pieces(&self) -> usize {
        let number_of_pieces = self.info.length as f64 / self.info.piece_length as f64;
        return number_of_pieces.ceil() as usize;
    }
    
    fn get_number_of_blocks(&self) -> usize {
        return  self.info.piece_length / (1<<14);
    }

    // pub async fn download(&mut self) -> Result<Vec<u8>, anyhow::Error> {
    // 
    //     let mut piece_pool = PiecePool::new(&self).expect("Failed to create piece pool");
    //     let peer_pool = Arc::new(Mutex::new(PeerPool::new(&self).await?));
    // 
    //     let semaphore = Arc::new(Semaphore::new(Self::MAX_CONCURRENT));
    //     let mut set = JoinSet::new();
    // 
    //     let torrent_info_hash = self.calculate_info_hash();
    // 
    //     for (piece_index, piece) in &mut piece_pool.pieces {
    //         let semaphore = semaphore.clone();
    //         let piece = piece.clone();
    //         let peer_pool = peer_pool.clone();
    //         let peer = peer_pool.lock().await.get_best_peer_mut().await.unwrap().clone();
    //         let peer = peer.clone();
    //         
    //         set.spawn(async move {
    //             let _permit = semaphore.acquire().await;
    //             let mut retries = 0;
    //             
    //             while retries < Self::MAX_RETRIES {
    //                 eprintln!("Retries: {} for piece {} with peer {}", retries, piece.lock().await.index, peer.clone().read().await.id);
    //                 match Peer::download_piece(peer.clone(), piece.clone(), torrent_info_hash.clone()).await {
    //                     Ok(()) => {
    //                         return Ok(());
    //                     }
    //                     Err(_) => {
    //                         let mut peer_write = peer.write().await;
    //                         peer_write.peer_metrics.failed_downloads += 1;
    //                         peer_write.state = PeerState::Disconnected;
    //                         retries += 1;
    //                     }
    //                 }
    //             }
    // 
    //             Err(anyhow::anyhow!("Failed to download piece after {} retries", Self::MAX_RETRIES))
    //         });
    //     }
    //     while let Some(res) = set.join_next().await {
    //         match res {
    //             Ok(Ok(piece)) => {},
    //             Ok(Err(e)) => eprintln!("Error: {e}"),
    //             Err(e) => eprintln!("Task failed: {e}"),
    //         }
    //     };
    // 
    //     let mut torrent_data = Vec::new();
    // 
    //     for (piece_index, piece) in &mut piece_pool.pieces {
    //         let piece = piece.lock().await;
    //         match &piece.piece_state {
    //             PieceState::Downloaded { piece_bytes } => torrent_data.extend_from_slice(piece_bytes),
    //             _ => return Err(anyhow::anyhow!("Piece not downloaded")),
    //         }
    //     }
    // 
    //     Ok(torrent_data)
    //     
    // }
    // 
    // async fn download_piece_from_peer(torrent_info_hash: [u8; 20], peer: Arc<RwLock<Peer>>, piece: Arc<Mutex<Piece>>) -> anyhow::Result<()> {
    //     let mut piece = piece.lock().await;
    // 
    //     let mut peer_write = peer.write().await;
    // 
    //     let mut stream = match peer_write.create_client(torrent_info_hash).await {
    //         Ok(()) => match &mut peer_write.state {
    //             PeerState::Connected { stream } => stream,
    //             _ => return Err(anyhow::anyhow!("Unexpected peer state")),
    //         },
    //         Err(e) => return Err(e),
    //     };
    // 
    //     let index = piece.index;
    // 
    //     for block_ in &mut piece.blocks {
    //         let start = Instant::now();
    //         let request = Message::Request {
    //             index: index as u32,
    //             begin: block_.begin as u32,
    //             length: block_.block_size as u32,
    //         };
    // 
    //         Message::send_message(request, &mut stream).await?;
    // 
    //         let response = Message::read_message(&mut stream).await?;
    //         match response {
    //             Message::Piece {
    //                 index: _,
    //                 begin: _,
    //                 block,
    //             } => {
    //                 let elapsed = start.elapsed();
    //                 block_.block_state = BlockState::Downloaded { duration: elapsed, data: block };
    //             }
    //             _ => {
    //                 block_.block_state = BlockState::Missing;
    //             },
    //         };
    //     }
    // 
    //     if piece.blocks.iter().all(|block| matches!(block.block_state, BlockState::Downloaded { .. })) {
    //         let piece_bytes = piece.blocks.iter().fold(Vec::new(), |mut acc, block| {
    //             if let BlockState::Downloaded { data, .. } = &block.block_state { acc.extend_from_slice(data) };
    //             acc
    //         });
    //         piece.piece_state = PieceState::Downloaded { piece_bytes };
    //         peer_write.state = PeerState::Disconnected;
    //         Ok(())
    //     } else {
    //         piece.piece_state = PieceState::Missing;
    //         Err(anyhow::anyhow!("Failed to download piece from peer"))
    //     }
    // }


}


mod tests {
    use super::*;

    #[test]
    fn test_calculate_info_hash() {
        let mut torrent = Torrent::new("sample.torrent".to_string());
        let info_hash = torrent.calculate_info_hash();
        assert_eq!(info_hash, [0x8e, 0x9e, 0x9f, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e]);
    }
    // #[tokio::test]
    // async fn test_download() {
    //     let mut torrent = Torrent::new("test/sample.torrent".to_string());
    //     let output = torrent.download().await.expect("Download failed");
    //     let correct_output = fs::read("test/sample_correct.txt").expect("Couldn't read correct output");
    //     fs::write("test/output", &output).expect("Couldn't write output");
    //     // assert_eq!(output, correct_output);
    // }
    
    #[test]
    fn test_get_number_of_pieces() {
        let torrent = Torrent::new("sample.torrent".to_string());
        assert_eq!(torrent.get_number_of_pieces(), 3);
    }
    
    #[test]
    fn test_get_number_of_blocks() {
        let torrent = Torrent::new("sample.torrent".to_string());
        assert_eq!(torrent.get_number_of_blocks(), 2);
    }
    
}
