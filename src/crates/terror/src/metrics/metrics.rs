use std::cmp::Ordering as Ord;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

pub struct PeerMetrics {
    pub tasks_queued: AtomicUsize,
    pub successful_downloads: AtomicUsize,
    pub failed_downloads: AtomicUsize,
    pub time_active: Mutex<Duration>,
}

impl PartialEq<Self> for PeerMetrics {
    fn eq(&self, other: &Self) -> bool {
        self.get_download_speed() == other.get_download_speed()
            && self.get_success_rate() == other.get_success_rate()
    }
}

impl PartialOrd for PeerMetrics {
    fn partial_cmp(&self, other: &Self) -> Option<Ord> {
        let self_score = self.get_score();
        let other_score = other.get_score();
        self_score.partial_cmp(&other_score)
    }
}

impl PeerMetrics {
    pub(crate) fn new() -> Self {
        PeerMetrics {
            tasks_queued: AtomicUsize::new(0),
            successful_downloads: AtomicUsize::new(0),
            failed_downloads: AtomicUsize::new(0),
            time_active: Mutex::new(Duration::new(0, 0)),
        }
    }

    pub fn get_score(&self) -> f64 {
        self.get_download_speed() * self.get_success_rate()
    }
    pub fn get_download_speed(&self) -> f64 {
        if self.successful_downloads.load(Ordering::SeqCst) == 0
            && self.time_active.lock().unwrap().as_secs_f64() == 0f64
        {
            return 0f64;
        }

        ((self.successful_downloads.load(Ordering::SeqCst) * 1 << 14) as f64
            / self.time_active.lock().unwrap().as_secs_f64())
    }

    pub fn get_success_rate(&self) -> f64 {
        let successful_downloads = self.successful_downloads.load(Ordering::SeqCst);
        let failed_downloads = self.failed_downloads.load(Ordering::SeqCst);
        if successful_downloads == 0 && failed_downloads == 0 {
            return 0f64;
        }
        (successful_downloads / (successful_downloads + failed_downloads)) as f64
    }
}
