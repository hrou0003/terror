use std::thread::sleep;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, Level};
use tracing::level_filters::LevelFilter;
use terror::torrent::torrent_info::Torrent;
use terror::torrent::torrent_downloader::{TorrentDownloader, TorrentDownloaderHandle};
use terror::torrent::torrents_manager::TorrentManager;

#[tokio::main(flavor = "multi_thread")]
#[tracing::instrument(ret)]
async fn main() -> anyhow::Result<()>{
    // console_subscriber::init();
    
    tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(LevelFilter::DEBUG)
        .init();
    
    let mut torrent_manager = TorrentManager::new();

    // Add multiple torrents
    let torrent_files = vec![
        "test/FEOW-TNC.zip-fb993412755d0bdc8aabd9c6959215293958b220.torrent",
        "test/downloads-d98540da6d34fb6a0150fd88b41580a377cb454d.torrent",
    ];

    let mut torrent_ids = Vec::new();

    for torrent_file in torrent_files {
        let torrent = Torrent::new(torrent_file.to_string());
        let torrent_downloader = TorrentDownloaderHandle::new(torrent).await;
        let id = torrent_manager.add_torrent(torrent_downloader).await?;
        torrent_ids.push(id);
        println!("Added torrent with ID: {}", id);
    }

    // Start all torrents
    for id in &torrent_ids {
        torrent_manager.start_torrent(id).await?;
        println!("Started torrent with ID: {}", id);
    }
    
    sleep(Duration::new(10000, 0));

    // // Simulate some time passing
    // tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    // 
    // // Pause the first torrent
    // torrent_manager.pause_torrent(&torrent_ids[0]).await?;
    // println!("Paused torrent with ID: {}", torrent_ids[0]);
    // 
    // // Resume the first torrent after a short delay
    // tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    // torrent_manager.resume_torrent(&torrent_ids[0]).await?;
    // println!("Resumed torrent with ID: {}", torrent_ids[0]);

    // Monitor progress of all torrents
    // loop {
    //     let mut all_completed = false;
    //     for id in &torrent_ids {
    //         if let Some(handle) = torrent_manager.torrents.get(id) {
    //             let handle = handle.lock().await;
    //             let progress = handle.torrent.get_progress().await?;
    //             println!("Torrent {} progress: {:.2}%", id, progress);
    //         }
    //     }
    // 
    //     if all_completed {
    //         break;
    //     }
    // 
    //     tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    // }

    // Remove all torrents
    for id in &torrent_ids {
        torrent_manager.remove_torrent(id).await?;
        println!("Removed torrent with ID: {}", id);
    }

    println!("All torrents completed and removed.");


    Ok(())
}