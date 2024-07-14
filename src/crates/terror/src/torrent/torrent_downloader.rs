use crate::peer::peer::CycleMessage;
use crate::peer::peer_actor::{PeerActor, PeerMessage};
use crate::peer::peer_actor_pool::PeerActorPool;
use crate::piece::piece::Priority;
use crate::piece::piece_pool::PiecePool;
use crate::torrent::torrent_info::Torrent;
use anyhow::Result;
use bytes::Bytes;
use kanal::{AsyncReceiver, AsyncSender};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::join;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::sync::{oneshot, Mutex, RwLock};
use tracing::field::debug;
use tracing::{debug, info, span, Level};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloaderState {
    Downloading,
    Paused,
    Completed,
    Error,
}

#[derive(Debug, Clone)]
pub(crate) struct DownloadBlock {
    pub piece_index: usize,
    pub block_index: usize,
    pub begin: usize,
    pub length: usize,
}

#[derive(Debug)]
pub enum CompletedTask {
    DownloadedBlock {
        peer_id: String,
        download_time: Instant,
        piece_index: usize,
        block_index: usize,
        bytes: Bytes,
    },
    FailedBlock {
        peer_id: String,
        piece_index: usize,
        block_index: usize,
    },
}

pub struct TorrentDownloader {
    torrent_info: Torrent,
    pieces: HashMap<usize, String>,
    receiver: AsyncReceiver<TorrentDownloaderMessage>,
    peer_actor_pool: Arc<Mutex<PeerActorPool>>,
    piece_pool: Arc<Mutex<PiecePool>>,
    state: Arc<RwLock<DownloaderState>>,
}

impl TorrentDownloader {
    pub async fn new(
        torrent_info: Torrent,
        receiver: AsyncReceiver<TorrentDownloaderMessage>,
    ) -> Self {
        let pieces = (0..torrent_info.info.pieces.len())
            .map(|index| (index, "available".to_string()))
            .collect();

        let (task_queue_sender, task_queue) = kanal::bounded_async::<DownloadBlock>(3000);
        let (completed_task_tx, completed_task_rx) = tokio::sync::mpsc::unbounded_channel();
        let peer_actor_pool = PeerActorPool::new(&torrent_info, task_queue, completed_task_tx)
            .await
            .unwrap();
        let piece_pool =
            PiecePool::new(&torrent_info, task_queue_sender, completed_task_rx).unwrap();

        Self {
            torrent_info,
            pieces,
            receiver,
            peer_actor_pool: Arc::new(Mutex::new(peer_actor_pool)),
            piece_pool: Arc::new(Mutex::new(piece_pool)),
            state: Arc::new(RwLock::new(DownloaderState::Paused)),
        }
    }

    async fn handle_message(&mut self, msg: TorrentDownloaderMessage) -> anyhow::Result<()> {
        let span = span!(Level::DEBUG, "Torrent Downloader");
        let _guard = span.enter();
        match msg {
            TorrentDownloaderMessage::Start => {
                debug!("Starting torrent download");
                *self.state.write().await = DownloaderState::Downloading;
                self.run().await?;
            }
            TorrentDownloaderMessage::Pause => {
                debug!("Pausing torrent download");
                self.pause().await?;
            }
            TorrentDownloaderMessage::Resume => {
                debug!("Resuming torrent download");
                self.resume().await?;
            }
        }
        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        *self.state.write().await = DownloaderState::Downloading;

        let peer_actor_pool = Arc::clone(&self.peer_actor_pool);
        let piece_pool = Arc::clone(&self.piece_pool);
        let state = Arc::clone(&self.state);

        let peer_actor_pool_handle = tokio::spawn(async move {
            let mut pool = peer_actor_pool.lock().await;
            pool.run().await;
        });

        let piece_pool_handle = tokio::spawn(async move {
            let mut pool = piece_pool.lock().await;
            pool.start().await;
        });

        // let state_handle = tokio::spawn(async move {
        //     loop {
        //         tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        //         let current_state = *state.read().await;
        //         if current_state == DownloaderState::Completed || current_state == DownloaderState::Error {
        //             break;
        //         }
        //     }
        // });

        let _ = join!(peer_actor_pool_handle, piece_pool_handle);

        debug!("Torrent download finished");
        Ok(())
    }

    pub async fn pause(&self) -> Result<()> {
        let mut state = self.state.write().await;
        info!("Pausing torrent");
        if *state == DownloaderState::Downloading {
            *state = DownloaderState::Paused;
            let mut piece_pool = self.piece_pool.lock().await;
            piece_pool.pause().await?;
        }
        Ok(())
    }

    pub async fn resume(&self) -> Result<()> {
        let mut state = self.state.write().await;
        info!("Resuming torrent");
        if *state == DownloaderState::Paused {
            *state = DownloaderState::Downloading;
            let mut piece_pool = self.piece_pool.lock().await;
            piece_pool.resume().await?;
        }
        Ok(())
    }

    pub async fn get_progress(&self) -> Result<f64> {
        let piece_pool = self.piece_pool.lock().await;
        Ok(piece_pool.get_progress())
    }

    pub async fn get_state(&self) -> DownloaderState {
        *self.state.read().await
    }
}

async fn run_torrent_downloader_actor(mut actor: TorrentDownloader) {
    while let Ok(msg) = actor.receiver.recv().await {
        if let Err(e) = actor.handle_message(msg).await {
            debug!("Error handling message: {}", e);
            break;
        }
    }
}

pub enum TorrentDownloaderMessage {
    Start,
    Pause,
    Resume,
    // Quit
}

pub struct TorrentDownloaderHandle {
    pub(crate) sender: AsyncSender<TorrentDownloaderMessage>,
}

impl TorrentDownloaderHandle {
    pub async fn new(torrent: Torrent) -> Self {
        let (sender, receiver) = kanal::bounded_async(8);
        let actor = TorrentDownloader::new(torrent, receiver).await;
        tokio::spawn(run_torrent_downloader_actor(actor));

        Self { sender }
    }
}
