use crate::torrent::torrent_downloader::{
    TorrentDownloader, TorrentDownloaderHandle, TorrentDownloaderMessage,
};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

// Assuming these types are defined elsewhere in your project
use crate::torrent::torrent_info::Torrent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentState {
    Downloading,
    Paused,
    Completed,
    Error,
}

pub struct TorrentHandle {
    pub torrent: TorrentDownloaderHandle,
    state: TorrentState,
}

pub struct TorrentManager {
    pub torrents: HashMap<Uuid, Arc<Mutex<TorrentHandle>>>,
}

impl TorrentManager {
    pub fn new() -> Self {
        TorrentManager {
            torrents: HashMap::new(),
        }
    }

    pub async fn add_torrent(&mut self, torrent: TorrentDownloaderHandle) -> Result<Uuid> {
        let handle = TorrentHandle {
            torrent,
            state: TorrentState::Paused,
        };
        let id = Uuid::new_v4();
        self.torrents.insert(id, Arc::new(Mutex::new(handle)));
        Ok(id)
    }

    pub async fn start_torrent(&self, id: &Uuid) -> Result<()> {
        if let Some(handle) = self.torrents.get(id) {
            let mut handle = handle.lock().await;
            handle
                .torrent
                .sender
                .send(TorrentDownloaderMessage::Start)
                .await;
            if handle.state == TorrentState::Paused {
                handle.state = TorrentState::Downloading;
            }
        }
        Ok(())
    }

    pub async fn pause_torrent(&self, id: &Uuid) -> Result<()> {
        if let Some(handle) = self.torrents.get(id) {
            let mut handle = handle.lock().await;
            if handle.state == TorrentState::Downloading {
                handle.state = TorrentState::Paused;
                // Implement pausing logic for PiecePool
                handle
                    .torrent
                    .sender
                    .send(TorrentDownloaderMessage::Pause)
                    .await;
            }
        }
        Ok(())
    }

    pub async fn resume_torrent(&self, id: &Uuid) -> Result<()> {
        if let Some(handle) = self.torrents.get(id) {
            let mut handle = handle.lock().await;
            if handle.state == TorrentState::Paused {
                handle.state = TorrentState::Downloading;
                // Implement resuming logic for PiecePool
            }
        }
        Ok(())
    }

    pub async fn remove_torrent(&mut self, id: &Uuid) -> Result<()> {
        if let Some(handle) = self.torrents.remove(id) {
            let mut handle = handle.lock().await;
            // Implement cleanup logic
            handle.state = TorrentState::Error;
        }
        Ok(())
    }

    // pub async fn get_torrent_state(&self, id: &Uuid) -> Option<TorrentState> {
    //     self.torrents.get(id).map(async move |handle| {
    //         let handle = handle.lock().await;
    //         handle.state
    //     })
    // }

    // pub async fn get_torrent_progress(&self, id: &Uuid) -> Option<f64> {
    //     self.torrents.get(id).map(|handle| {
    //         let handle = handle.lock().await;
    //         let piece_pool = handle.piece_pool.lock().await;
    //         piece_pool.get_progress()
    //     })
    // }
}
