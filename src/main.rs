use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, Level};
use tracing::level_filters::LevelFilter;
use terror::{Torrent, TorrentManager};

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
#[tracing::instrument(ret)]
async fn main() {
    // console_subscriber::init();

    tracing_subscriber::fmt()
        .with_target(false)
        .init();
    let start_time = Instant::now();

    let torrent = Torrent::new("test/downloads-d98540da6d34fb6a0150fd88b41580a377cb454d.torrent_info".to_string());
    let mut torrent_manager = TorrentManager::new(torrent).await;

    torrent_manager.run().await;

    let end_time = start_time.elapsed();

    info!("Torrent completed in {}", end_time.as_secs_f32())
}