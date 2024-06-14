use std::net::IpAddr;
use kanal::{AsyncReceiver};
use tokio::io;
use tokio::io::AsyncWriteExt;

use tokio::sync::mpsc::{Receiver, Sender};
use uuid::Uuid;

use crate::peer::{Peer, PeerState};
use crate::peer_actor::{PeerActorHandle, PeerMessage};
use crate::Torrent;
use crate::torrent_manager::{CompletedTask, DownloadBlock};
use crate::utils::{percent_encode_hash, TrackerRequest, TrackerResponse};

pub struct PeerActorPool {
    pub(crate) actors: Vec<PeerActorHandle>,
    task_queue: AsyncReceiver<DownloadBlock>,
    cycle_peer_tx: Receiver<crate::peer::CycleMessage>,
}

impl PeerActorPool {
    pub async fn new(torrent: &Torrent, task_queue: AsyncReceiver<DownloadBlock>, completed_task_tx: Sender<CompletedTask>) -> anyhow::Result<Self> {
        let info_hash = torrent.calculate_info_hash();
        let encoded_info_hash = percent_encode_hash(hex::encode(info_hash.to_vec()).as_str());

        let request = TrackerRequest {
            peer_id: "codecraftersbittorre".to_string(),
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: torrent.info.length,
            compact: 1,
        };

        let request_params = serde_urlencoded::to_string(&request).expect("Couldn't encode request");

        let request_url = format!(
            "{}?{}&info_hash={}",
            torrent.announce, request_params, encoded_info_hash
        );

        println!("Making request to {}", request_url);

        let response = reqwest::get(request_url).await?.bytes().await?;
        println!("Raw response: {}", String::from_utf8_lossy(&response));
        println!("Hex-encoded response: {}", hex::encode(&response));

        let tracker: TrackerResponse = serde_bencode::from_bytes(&response)?;

        let peers: Vec<Peer> = tracker.peers.iter().map(|peer| {
            Peer {
                id: "test".to_string(),
                ip_addr: IpAddr::V4(peer.ip.parse().unwrap()),
                port: peer.port as u16,
                state: PeerState::Disconnected,
                info_hash: info_hash
            }
        }).collect();

        let (cycle_tx, cycle_rx) = tokio::sync::mpsc::channel(10);
        
        let actors = peers.iter().map(|peer| {
            return PeerActorHandle::new(peer.ip_addr, peer.port, info_hash, task_queue.clone(), completed_task_tx.clone(), cycle_tx.clone());
        }).collect();
        
        Ok(Self {
            actors: actors,
            task_queue,
            cycle_peer_tx: cycle_rx,
        })
    }

    pub async fn run(&self) {
        for actor in self.actors.iter() {
            actor.sender.send(PeerMessage::Listen).await.unwrap();
        }
    }

}