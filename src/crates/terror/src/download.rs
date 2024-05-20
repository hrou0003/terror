use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc::{self, Receiver, Sender}, Mutex, Semaphore};
use tokio::task::JoinSet;

use crate::peer::{Peer, PeerPool};
use crate::piece::{PiecePool, PieceState};
use crate::Torrent;

pub struct TorrentDownloader {
    piece_pool: PiecePool,
    peer_pool: PeerPool,
    torrent: Torrent
}

#[derive(Clone)]
pub struct DownloadTask {
    piece_index: usize,
    result_tx: Sender<Result<usize, anyhow::Error>>,
}

impl TorrentDownloader {
    pub const MAX_CONCURRENT: usize = 1;
    pub  const MAX_RETRIES: usize = 3;
    pub async fn new(torrent: &Torrent) -> anyhow::Result<Self> {
        let piece_pool = PiecePool::new(&torrent)?;
        let peer_pool = PeerPool::new(torrent).await?;
        let torrent = torrent.clone();

        Ok(TorrentDownloader { piece_pool, peer_pool, torrent })
    }

    // Start download workers that will monitor the download channel.
    pub async fn download(&mut self) -> Result<Vec<u8>, anyhow::Error> {

        let semaphore = Arc::new(Semaphore::new(Self::MAX_CONCURRENT));
        let mut set = JoinSet::new();

        let torrent_info_hash = self.torrent.calculate_info_hash();

        while let Some(piece) = self.piece_pool.get_next_piece_mut().await {
            let semaphore = semaphore.clone();
            let peer = self.peer_pool.get_best_peer_mut().await.unwrap();

            set.spawn(async move {
                let _permit = semaphore.acquire().await.unwrap(); // Proper unwrap for the semaphore acquire.
                let mut retries = 0;

                while retries < Self::MAX_RETRIES {
                        let mut piece = piece.clone();
                        let peer = peer.clone();
                        // eprintln!("Retries: {} for piece {} with peer {}", retries, piece.index, peer.id);
                        // Properly handle the result of download_piece and break if successful.
                        if Peer::download_piece(peer, piece, torrent_info_hash).await.is_ok() {
                            retries += 1;
                            break;
                        }
                }
            });
        }

        while let Some(res) = set.join_next().await {
            match res {
                Ok(()) => {},
                Err(e) => eprintln!("Task failed: {e}"),
            }
        };

        let mut torrent_data = Vec::new();

        // for piece in &mut self.piece_pool.pieces {
        //     let (piece_index, piece) = piece;
        //     let piece = piece.lock().await;
        //     match &piece.piece_state {
        //         PieceState::Downloaded { piece_bytes } => torrent_data.extend_from_slice(piece_bytes),
        //         _ => return Err(anyhow::anyhow!("Piece not downloaded")),
        //     }
        // }

        Ok(torrent_data)

    }

}

mod tests {
    use std::fs;

    use crate::download::TorrentDownloader;
    use crate::Torrent;

    #[tokio::test]
    async fn test_download() {
        let torrent = Torrent::new("test/sample.torrent".to_string());
        let output = TorrentDownloader::new(&torrent).await.unwrap().download().await.expect("Couldn't download torrent");
        let correct_output = fs::read("test/sample_correct.txt").expect("Couldn't read correct output");
        fs::write("test/output", &output).expect("Couldn't write output");
        assert_eq!(output, correct_output);
    }
}