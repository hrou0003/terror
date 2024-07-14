use crate::metrics::metrics::PeerMetrics;
use crate::peer::peer::{CycleMessage, Peer, PeerState};
use crate::peer::peer_actor::{ControlMessage, DataMessage, PeerMessage};
use crate::peer::peer_actor_handle::PeerActorHandle;
use crate::torrent::torrent_downloader::{CompletedTask, DownloadBlock};
use crate::torrent::torrent_info::Torrent;
use crate::utils::utils::{percent_encode_hash, TrackerRequest, TrackerResponse};
use futures_util::future::FutureExt;
use kanal::{bounded_async, unbounded, AsyncReceiver};
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::time::interval;
use tokio::{select, task};
use tracing::{debug, error, info};

pub struct PeerActorPool {
    pub(crate) actors: Vec<PeerActorHandle>,
    task_queue: AsyncReceiver<DownloadBlock>,
    completed_task_rx: AsyncReceiver<CompletedTask>,
    completed_task_tx: UnboundedSender<CompletedTask>,
}

impl PeerActorPool {
    pub async fn new(
        torrent: &Torrent,
        task_queue: AsyncReceiver<DownloadBlock>,
        completed_task_tx: UnboundedSender<CompletedTask>,
    ) -> anyhow::Result<Self> {
        let info_hash = torrent.calculate_info_hash();
        let encoded_info_hash = percent_encode_hash(hex::encode(info_hash.to_vec()).as_str());

        let request = TrackerRequest {
            peer_id: "codecraftersbittorre".to_string(),
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: torrent.info.length.unwrap_or(0),
            compact: 1,
        };

        let request_params =
            serde_urlencoded::to_string(&request).expect("Couldn't encode request");

        let request_url = format!(
            "{}?{}&info_hash={}",
            torrent.announce, request_params, encoded_info_hash
        );

        info!("Getting peers from tracker");
        debug!("Making request to {}", request_url);

        let response = reqwest::get(request_url).await?.bytes().await?;
        debug!("Raw response: {}", String::from_utf8_lossy(&response));
        debug!("Hex-encoded response: {}", hex::encode(&response));

        let tracker: TrackerResponse = serde_bencode::from_bytes(response.to_vec().as_slice())?;

        let peers: Vec<Peer> = tracker
            .peers
            .iter()
            .map(|peer| Peer {
                id: String::from_utf8_lossy(peer.peer_id.as_slice()).to_string(),
                ip_addr: IpAddr::V4(peer.ip.parse().unwrap()),
                port: peer.port as u16,
                state: PeerState::Disconnected,
                info_hash: info_hash,
            })
            .collect();

        let (completed_tx, completed_rx) = bounded_async::<CompletedTask>(100);

        let actors = peers
            .iter()
            .map(|peer| {
                return PeerActorHandle::new(
                    peer.id.clone(),
                    peer.ip_addr,
                    peer.port,
                    info_hash,
                    completed_tx.clone(),
                );
            })
            .collect();

        Ok(Self {
            actors,
            task_queue,
            completed_task_rx: completed_rx,
            completed_task_tx,
        })
    }

    pub async fn run(&mut self) {
        info!("Starting PeerActorPool");
        let mut interval = interval(Duration::from_secs(5));
        info!("Connecting to peers");
        for actor in self.actors.iter() {
            actor
                .sender
                .send(PeerMessage::Control(ControlMessage::Listen))
                .await
                .unwrap();
        }

        loop {
            select! {
                Ok(download_task) = self.task_queue.recv() => {
                    debug!("Received download task");
                    self.handle_download_block(download_task).await;
                }
                Ok(completed_task) = self.completed_task_rx.recv() => {
                    debug!("Received completed task");
                    self.handle_completed_task(completed_task).await;
                }
                _ = interval.tick() => {
                }
                else => {
                    error!("All channels have closed, stopping PeerActorPool");
                    break;
                }
            }
        }
        info!("PeerActorPool stopped");
    }

    async fn handle_download_block(&mut self, msg: DownloadBlock) {
        let peer_msg = PeerMessage::Data(DataMessage::Piece {
            index: msg.piece_index,
            begin: msg.begin,
            length: msg.length,
        });
        if let Some(best_peer) = self.get_best_peer_mut() {
            debug!(
                "sending download request to peer actor {}",
                best_peer.peer_id
            );
            if let Err(e) = best_peer.sender.send(peer_msg).await {
                debug!("Failed to send message to peer: {:?}", e);
            } else {
                best_peer.metrics.tasks_queued.fetch_add(1, Ordering::SeqCst);
                debug!("Queued successfully");
            }
        }
    }

    async fn handle_completed_task(&mut self, completed_task: CompletedTask) {
        match completed_task {
            CompletedTask::FailedBlock { peer_id, .. } => {
                // remove the bad peer
                debug!(
                    "removing bad peer {} and the task queue has {}",
                    peer_id,
                    self.task_queue.len()
                );
                self.actors.retain(|actor| actor.peer_id != peer_id);
            }
            CompletedTask::DownloadedBlock {
                peer_id,
                block_index,
                piece_index,
                bytes,
                download_time,
            } => {
                debug!("forwarding message");
                let peer = self.actors.iter().find(|&peer| peer.peer_id == peer_id);
                peer.unwrap().metrics.tasks_queued.fetch_sub(1, Ordering::SeqCst);
                let completed_task = CompletedTask::DownloadedBlock {
                    peer_id,
                    block_index,
                    piece_index,
                    bytes,
                    download_time,
                };
                if let Err(e) = self.completed_task_tx.send(completed_task) {
                    debug!("Failed to forward completed task: {:?}", e);
                }
            }
        }
    }

    fn get_best_peer_mut(&mut self) -> Option<&mut PeerActorHandle> {
        let best_index = self
            .actors
            .iter()
            .enumerate()
            .min_by_key(|(_, actor)| actor.metrics.tasks_queued.load(Ordering::SeqCst))
            .map(|(index, _)| index);

        match best_index {
            Some(index) => Some(&mut self.actors[index]),
            None => self.actors.first_mut(),
        }
    }
}
