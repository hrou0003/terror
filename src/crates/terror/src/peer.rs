use std::net::IpAddr;
use std::sync::{Arc};
use tokio::net::TcpStream;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex, oneshot};
use tokio::task;
use uuid::Uuid;
use crate::download::{CompletedTask, DownloadTask};
use crate::message::Message;
use crate::Torrent;
use crate::utils::*;

type CycleRx = mpsc::Receiver<CycleMessage>;

pub struct Peer {
    pub(crate) id: String,
    pub(crate) ip_addr: IpAddr,
    pub(crate) port: u16,
    state: PeerState,
    info_hash: [u8; 20],
    stream:
}


pub(crate) enum PeerState {
    Connected { stream: Arc<Mutex<TcpStream>> },
    Disconnected,
}



impl Peer {
    
    pub fn new(ip_addr: IpAddr, port: u16, info_hash: [u8; 20]) -> Self {
        Peer {
            id: Uuid::new_v4().to_string(),
            ip_addr,
            port,
            state: PeerState::Disconnected,
            info_hash,
        }
    }
    
    pub async fn download_block_from_stream(stream: Arc<Mutex<TcpStream>>, index: u32, begin: u32, length: u32) -> anyhow::Result<Vec<u8>> {
        let mut stream = stream.lock().await;

        let message = Message::Request {
            index, begin, length
        };

        Message::send_message(message, &mut stream).await?;

        let response = Message::read_message(&mut stream).await?;

        match response {
            Message::Piece { block, .. } => {
                Ok(block)
            }
            _ => {
                Err(anyhow::anyhow!("Unexpected message type"))
            }
        }
    }
}



pub struct PeerPool {
    active_peers: Vec<Peer>,
    inactive_peers: Vec<Peer>,
    task_rx: Receiver<DownloadTask>,
    completed_task_tx: Sender<CompletedTask>,
    cycle_tx: Sender<CycleMessage>,
    cycle_rx: CycleRx
}

pub(crate) struct CycleMessage {
    peer_id: String,
}

impl PeerPool {
    pub async fn new(torrent: &Torrent, task_rx: Receiver<DownloadTask>, completed_task_tx: Sender<CompletedTask>) -> anyhow::Result<Self> {
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

        let result = reqwest::get(request_url).await?.bytes().await?.to_vec();
        println!("Raw response: {}", String::from_utf8_lossy(&result));
        println!("Hex-encoded response: {}", hex::encode(result.to_vec()));

        let tracker: TrackerResponse = serde_bencode::from_bytes(&result)?;

        let peers = tracker.peers_raw.chunks_exact(6).map(|chunk| {
            let ip = format!("{}.{}.{}.{}", chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = ((chunk[4] as u16) << 8) | (chunk[5] as u16);
            let id = Uuid::new_v4().to_string();
            Peer {
                id,
                ip_addr: IpAddr::V4(ip.parse().unwrap()),
                port: port,
                state: PeerState::Disconnected,
                info_hash: info_hash
            }
        }).collect();

        let (cycle_tx, cycle_rx) = tokio::sync::mpsc::channel(10);

        Ok(PeerPool {
            inactive_peers: peers,
            active_peers: vec![],
            task_rx,
            completed_task_tx,
            cycle_tx,
            cycle_rx
        })
    }

    // Start the peer pool
    pub async fn start(&mut self) {
        // Get some peers to activate
        self.active_peers.push(self.inactive_peers.pop().unwrap());
        let Self { task_rx, cycle_tx, cycle_rx, completed_task_tx, .. } = self;


        // Start the peer connections
        for peer in &mut self.active_peers {
            let cycle_tx = cycle_tx.clone();
            let task_rx = task_rx.resubscribe();
            let completed_task_tx = completed_task_tx.clone();
            // Spawn workers for each of the active peers and start the connections
            peer.connect(task_rx, completed_task_tx, cycle_tx).await;
            // Cycle peers
        }
    }

    pub fn add_peer(&mut self, peer: Peer) {
        self.active_peers.push(peer);
    }

    fn get_next_peer(&mut self) -> Option<Peer> {
        self.active_peers.pop()
    }

    fn cycle_peer(&mut self, peer: Peer) {
        self.inactive_peers.push(peer);
    }
}