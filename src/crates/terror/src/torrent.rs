use std::collections::BinaryHeap;
use std::fs;
use std::os::unix::raw::mode_t;
use std::sync::{Arc};
use std::time::Duration;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha1::{digest::generic_array::GenericArray, Digest, Sha1};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, Semaphore, RwLock};
use tokio::task::JoinSet;
use tokio::time::Instant;
use crate::download::{Block, BlockState, Message, Piece, PiecePool, PieceState};
use crate::handshake::Handshake;
use crate::metrics::PeerMetrics;
use crate::peer::{Peer, PeerState};

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

    pub async fn download(&mut self) -> Result<Vec<u8>, anyhow::Error> {

        let mut piece_pool = PiecePool::new(&self);
        let peer_pool = Peer::get_peers(&self).await?;

        let semaphore = Arc::new(Semaphore::new(Self::MAX_CONCURRENT));
        let mut set = JoinSet::new();

        let torrent_info_hash = self.calculate_info_hash();

        for piece in &mut piece_pool.pieces {
            let semaphore = semaphore.clone();
            let piece = piece.clone();
            let peer = peer_pool.get_free_peer().await?;

            set.spawn(async move {
                let _permit = semaphore.acquire().await;

                Self::download_piece_from_peer(torrent_info_hash.clone(), peer.clone(), piece.clone()).await

            });
        }
                
        while let Some(res) = set.join_next().await {
            match res {
                Ok(Ok(piece)) => {},
                Ok(Err(e)) => eprintln!("Error: {e}"),
                Err(e) => eprintln!("Task failed: {e}"),
            }
        };

        let mut torrent_data = Vec::new();

        for piece in &mut piece_pool.pieces {
            let piece = piece.lock().await;
            match &piece.piece_state {
                PieceState::Downloaded { piece_bytes } => torrent_data.extend_from_slice(piece_bytes),
                _ => return Err(anyhow::anyhow!("Piece not downloaded")),
            }
        }

        Ok(torrent_data)
        
    }

    async fn download_piece_from_peer(torrent_info_hash: [u8; 20], peer: Arc<RwLock<Peer>>, piece: Arc<Mutex<Piece>>) -> anyhow::Result<()> {
        let mut piece = piece.lock().await;

        let mut peer_write = peer.write().await;

        let mut stream = match peer_write.create_client(torrent_info_hash).await {
            Ok(()) => match &mut peer_write.state {
                PeerState::Connected { stream } => stream,
                _ => return Err(anyhow::anyhow!("Unexpected peer state")),
            },
            Err(e) => return Err(e),
        };

        let index = piece.index;

        for block_ in &mut piece.blocks {
            let start = Instant::now();
            let request = Message::Request {
                index: index as u32,
                begin: block_.begin as u32,
                length: block_.block_size as u32,
            };

            Message::send_message(request, &mut stream).await?;

            let response = Message::read_message(&mut stream).await?;
            match response {
                Message::Piece {
                    index: _,
                    begin: _,
                    block,
                } => {
                    let elapsed = start.elapsed();
                    block_.block_state = BlockState::Downloaded { duration: elapsed, data: block };
                }
                _ => {
                    block_.block_state = BlockState::Missing;
                },
            };
        }

        if piece.blocks.iter().all(|block| matches!(block.block_state, BlockState::Downloaded { .. })) {
            let piece_bytes = piece.blocks.iter().fold(Vec::new(), |mut acc, block| {
                if let BlockState::Downloaded { data, .. } = &block.block_state { acc.extend_from_slice(data) };
                acc
            });
            piece.piece_state = PieceState::Downloaded { piece_bytes };
            peer_write.state = PeerState::Disconnected;
            Ok(())
        } else {
            piece.piece_state = PieceState::Missing;
            Err(anyhow::anyhow!("Failed to download piece from peer"))
        }
    }

}


async fn print_statistics(peer_metrics: Arc<Mutex<BinaryHeap<PeerMetrics>>>) {
    let mut interval = tokio::time::interval(Duration::from_secs(5)); // Adjust the interval as needed

    loop {
        interval.tick().await;

        let peer_metrics = peer_metrics.lock().await;

        let total_peers = peer_metrics.len();
        let total_download_speed: f64 = peer_metrics.iter().map(|pm| pm.download_speed).sum();
        let avg_download_speed = total_download_speed / total_peers as f64;
        let total_successful_downloads: usize = peer_metrics.iter().map(|pm| pm.successful_downloads).sum();
        let total_failed_downloads: usize = peer_metrics.iter().map(|pm| pm.failed_downloads).sum();

        println!("=== Peer Statistics ===");
        println!("Total Peers: {}", total_peers);
        println!("Total Download Speed: {:.2} bytes/sec", total_download_speed);
        println!("Average Download Speed: {:.2} bytes/sec", avg_download_speed);
        println!("Total Successful Downloads: {}", total_successful_downloads);
        println!("Total Failed Downloads: {}", total_failed_downloads);
        println!();
    }
}

mod tests {
    use super::*;
    #[test]
    fn test_calculate_info_hash() {
        let mut torrent = Torrent::new("sample.torrent".to_string());
        let info_hash = torrent.calculate_info_hash();
        assert_eq!(info_hash, [0x8e, 0x9e, 0x9f, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e]);
    }
    #[tokio::test]
    async fn test_download() {
        let mut torrent = Torrent::new("test/sample.torrent".to_string());
        let output = torrent.download().await.expect("Download failed");
        let correct_output = fs::read("test/sample_correct.txt").expect("Couldn't read correct output");
        fs::write("test/output", &output).expect("Couldn't write output");
        assert_eq!(output, correct_output);
    }
    
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
