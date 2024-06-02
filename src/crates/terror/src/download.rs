use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc};
use crate::peer::PeerPool;
use crate::piece::PiecePool;
use crate::Torrent;

#[derive(Debug, Clone)]
pub(crate) struct DownloadTask {
    pub piece_index: usize,
    pub block_index: usize,
    pub begin: usize,
    pub length: usize,
}

#[derive(Debug)]
pub struct CompletedTask {
    pub piece_index: usize,
    pub block_index: usize,
    pub bytes: Vec<u8>,
    pub status: Result<(), String>,
}

#[derive(Debug)]
struct Peer {
    id: usize,
    port: u16,
    state: String,
}

struct TorrentDownloader {
    peer_pool: PeerPool,
    piece_pool: PiecePool,
}

impl TorrentDownloader {
    async fn new(t: &Torrent) -> Self {
        let (completed_task_tx, completed_task_rx) = mpsc::channel::<CompletedTask>(32);
        let (task_tx, task_rx) = broadcast::channel::<DownloadTask>(32);

        
        Self {
            peer_pool: PeerPool::new(t, task_rx, completed_task_tx).await.unwrap(),
            piece_pool: PiecePool::new(t, task_tx, completed_task_rx).expect("Failed to create piece pool"),
        }
    }

    async fn start(&mut self) {

        // start the piece pool
        
        self.piece_pool.start().await;

        // Start the peer pool
        self.peer_pool.start().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent::Torrent;

    #[tokio::test]
    async fn test_new() {
        let torrent = Torrent::new("test/sample.torrent".to_string());
        let mut downloader = TorrentDownloader::new(&torrent).await;

        downloader.start().await;
    }
}
