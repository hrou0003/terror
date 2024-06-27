use std::collections::HashMap;
use std::sync::Arc;
use bytes::Bytes;
use tokio::join;
use tokio::sync::Mutex;
use tracing::{debug, info};
use crate::peer::peer_actor_pool::PeerActorPool;
use crate::piece::piece::PiecePool;
use crate::torrent::torrent_info::Torrent;


#[derive(Debug, Clone)]
pub(crate) struct DownloadBlock {
    pub piece_index: usize,
    pub block_index: usize,
    pub begin: usize,
    pub length: usize,
}

#[derive(Debug)]
pub enum CompletedTask {
    DownloadedBlock{
        piece_index: usize,
        block_index: usize,
        bytes: Bytes,
    },
    FailedBlock {
        piece_index: usize,
        block_index: usize,
    },
}

pub struct TorrentManager {
    torrent_info: Torrent,
    pieces: HashMap<usize, String>,
    peer_actor_pool: PeerActorPool,
    piece_pool: PiecePool,
}

impl TorrentManager {
    pub async fn new(torrent_info: Torrent) -> Self {
        let pieces = (0..torrent_info.info.pieces.len())
            .map(|index| (index, "available".to_string()))
            .collect();
        
        let (task_queue_sender, task_queue) = kanal::bounded_async::<DownloadBlock>(32); 
        let (completed_task_tx, completed_task_rx) = tokio::sync::mpsc::unbounded_channel();
        let peer_actor_pool = PeerActorPool::new(&torrent_info, task_queue, completed_task_tx).await.unwrap();
        let piece_pool = PiecePool::new(&torrent_info, task_queue_sender, completed_task_rx).unwrap();
        
        Self {
            torrent_info,
            pieces,
            peer_actor_pool,
            piece_pool
        }
    }
    pub async fn run(self) {
        let peer_actor_pool = Arc::new(Mutex::new(self.peer_actor_pool));
        let piece_pool = Arc::new(Mutex::new(self.piece_pool));

        let peer_actor_pool_handle = {
            let peer_actor_pool = Arc::clone(&peer_actor_pool);
            tokio::spawn(async move {
                let mut pool = peer_actor_pool.lock().await;
                pool.run().await;
            })
        };

        let mut pool = piece_pool.lock().await;
        pool.start().await;

        let _ = join!(peer_actor_pool_handle);
        debug!("Torrent downloaded")
        
    }
}

#[cfg(test)]
mod tests
{

    use super::*;

    #[tokio::main(flavor = "multi_thread", worker_threads = 4)]
    #[test]
    async fn main() {
        
        let torrent = Torrent::new("test/mnist-ce990b28668abf16480b8b906640a6cd7e3b8b21.torrent_info".to_string());
        let mut torrent_manager = TorrentManager::new(torrent).await;

        torrent_manager.run().await;
    }
}