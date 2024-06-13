use terror::Torrent;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    console_subscriber::init();

    let torrent = Torrent::new("test/BuzzFeedNewstranscriptionofAirbnbNYCdata.xlsx-968a3ff5e4182cdecd239980ecfd257a37451003.torrent".to_string());
    let mut torrent_manager = terror::TorrentManager::new(torrent).await;

    torrent_manager.run().await;
    
    println!("Torrent completed")
}