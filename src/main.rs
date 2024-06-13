use std::env;
use anyhow;



#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let torrent = Torrent::new("test/FEOW-TNC.zip-fb993412755d0bdc8aabd9c6959215293958b220.torrent".to_string());
    let mut torrent_manager = TorrentManager::new(torrent).await;

    torrent_manager.run().await;
}
