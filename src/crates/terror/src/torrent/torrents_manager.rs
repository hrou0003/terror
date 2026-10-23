use crate::torrent::torrent_downloader::{DownloaderState, TorrentDownloaderHandle};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Default)]
pub struct TorrentManager {
    torrents: HashMap<Uuid, TorrentDownloaderHandle>,
}

impl TorrentManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn add_torrent(&mut self, torrent: TorrentDownloaderHandle) -> Result<Uuid> {
        let id = Uuid::new_v4();
        self.torrents.insert(id, torrent);
        Ok(id)
    }

    fn torrent(&self, id: &Uuid) -> Result<&TorrentDownloaderHandle> {
        self.torrents
            .get(id)
            .ok_or_else(|| anyhow!("unknown torrent {id}"))
    }

    pub async fn start_torrent(&self, id: &Uuid) -> Result<()> {
        self.torrent(id)?.start().await
    }

    pub async fn pause_torrent(&self, id: &Uuid) -> Result<()> {
        self.torrent(id)?.pause().await
    }

    pub async fn resume_torrent(&self, id: &Uuid) -> Result<()> {
        self.torrent(id)?.resume().await
    }

    pub fn get_torrent_state(&self, id: &Uuid) -> Result<DownloaderState> {
        Ok(self.torrent(id)?.get_state())
    }

    pub fn get_torrent_progress(&self, id: &Uuid) -> Result<f64> {
        Ok(self.torrent(id)?.get_progress())
    }

    pub async fn remove_torrent(&mut self, id: &Uuid) -> Result<()> {
        // Keep ownership in the map while awaiting so cancelling removal cannot detach tasks.
        let torrent = self
            .torrents
            .get_mut(id)
            .ok_or_else(|| anyhow!("unknown torrent {id}"))?;
        let result = torrent.shutdown().await;
        self.torrents.remove(id);
        result
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        let mut first_error = None;
        for torrent in self.torrents.values_mut() {
            if let Err(error) = torrent.shutdown().await {
                first_error.get_or_insert(error);
            }
        }
        self.torrents.clear();
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
