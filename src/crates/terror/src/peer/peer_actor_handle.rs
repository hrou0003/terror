use crate::metrics::metrics::PeerMetrics;
use crate::peer::peer::CycleMessage;
use crate::peer::peer_actor::{PeerActor, PeerMessage};
use crate::torrent::torrent_downloader::{CompletedTask, DownloadBlock};
use kanal::{AsyncReceiver, AsyncSender, Receiver};
use log::error;
use std::cmp::Ordering;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tracing::debug;
use tracing::field::debug;

pub struct PeerActorHandle {
    pub(crate) peer_id: String,
    pub(crate) sender: AsyncSender<PeerMessage>,
    pub(crate) metrics: Arc<PeerMetrics>,
}

impl PartialEq for PeerActorHandle {
    fn eq(&self, other: &Self) -> bool {
        self.metrics.eq(&other.metrics)
    }
}

impl PartialOrd for PeerActorHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.metrics.partial_cmp(&other.metrics)
    }
}

impl PeerActorHandle {
    pub fn new(
        peer_id: String,
        ip_addr: IpAddr,
        port: u16,
        info_hash: [u8; 20],
        completed_task_tx: AsyncSender<CompletedTask>,
    ) -> Self {
        let (sender, receiver) = kanal::bounded_async(8);
        let actor = PeerActor::new(
            peer_id.clone(),
            ip_addr,
            port,
            info_hash,
            receiver,
            completed_task_tx,
        );
        tokio::spawn(run_peer_actor(actor));

        let metrics = Arc::new(PeerMetrics::new());
        Self {
            peer_id,
            sender,
            metrics,
        }
    }
}

async fn run_peer_actor(mut actor: PeerActor) {
    loop {
        let msg = actor.receiver.recv().await;
        match msg {
            Ok(msg) => {
                actor.handle_message(msg).await;
                debug!("Passing message form peer actor handle");
            }
            Err(_) => {
                error!("couldn't pass message from peer actor handle");
            }
        }
    }
}
