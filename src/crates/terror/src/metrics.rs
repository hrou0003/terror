use std::cmp::Ordering;
use tokio::sync::RwLock;
use crate::peer::Peer;

pub(crate) struct PeerMetrics {
    pub(crate) peer: std::sync::Arc<RwLock<Peer>>,
    pub(crate) download_speed: f64,
    pub(crate) successful_downloads: usize,
    pub(crate) failed_downloads: usize,
}

impl Ord for PeerMetrics {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare peers based on download speed and successful/failed downloads
        // Adjust the comparison logic based on your specific requirements
        self.download_speed.partial_cmp(&other.download_speed).unwrap().then_with(|| {
            self.successful_downloads.cmp(&other.successful_downloads).reverse().then_with(|| {
                self.failed_downloads.cmp(&other.failed_downloads)
            })
        })
    }
}

impl PartialOrd for PeerMetrics {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PeerMetrics {
    fn eq(&self, other: &Self) -> bool {
        self.download_speed == other.download_speed &&
            self.successful_downloads == other.successful_downloads &&
            self.failed_downloads == other.failed_downloads
    }
}

impl Eq for PeerMetrics {}

// ...