use std::time::Duration;
use tokio::time::Instant;
use terror::Torrent;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    console_subscriber::init();
    let start_time = Instant::now();

    let torrent = Torrent::new("test/sample.torrent".to_string());
    let mut torrent_manager = terror::TorrentManager::new(torrent).await;

    torrent_manager.run().await;
    
    let end_time = start_time.elapsed();
    
    println!("Torrent completed in {}", end_time.as_secs_f32())
}