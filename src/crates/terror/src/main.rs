use anyhow::{bail, Result};
use clap::Parser;
use terror::torrent::torrent_downloader::{DownloaderState, TorrentDownloaderHandle};
use terror::torrent::torrent_info::Torrent;
use terror::torrent::torrents_manager::TorrentManager;
use tokio::time::{interval, Duration};

#[derive(Parser)]
#[command(about = "Download a torrent using the actor-based engine")]
struct Args {
    /// Path to a .torrent file. Output is written to the current directory.
    torrent: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt().with_target(false).init();
    let mut manager = TorrentManager::new();
    let downloader = TorrentDownloaderHandle::new(Torrent::new(args.torrent)).await?;
    let id = manager.add_torrent(downloader).await?;
    manager.start_torrent(&id).await?;
    let mut ticker = interval(Duration::from_millis(250));
    let mut failed = false;
    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                println!("Stopping torrent...");
                break;
            }
            _ = ticker.tick() => {
                match manager.get_torrent_state(&id)? {
                    DownloaderState::Completed => {
                        println!("Download completed (100%).");
                        break;
                    }
                    DownloaderState::Error => {
                        failed = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
    manager.shutdown().await?;
    if failed {
        bail!("download failed; see the engine log above");
    }
    Ok(())
}
