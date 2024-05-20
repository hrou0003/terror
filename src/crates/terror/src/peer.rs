use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use serde::{Deserialize, Serialize};
use anyhow::Result;
use serde_bytes::ByteBuf;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;
use crate::message::Message;
use crate::handshake::Handshake;
use crate::piece::{BlockState, Piece, PieceState};
use crate::torrent::Torrent;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackerRequest {
    peer_id: String,
    port: usize,
    uploaded: usize,
    downloaded: usize,
    left: usize,
    compact: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackerResponse {
    interval: usize,
    #[serde(rename = "peers")]
    peers_raw: ByteBuf,
}

#[derive(Debug)]
pub struct Peer {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub state: PeerState,
    pub peer_metrics: PeerMetrics,
    pieces: HashSet<usize>,
}

#[derive(Debug)]
pub enum PeerState {
    Connecting,
    Connected {
        stream: TcpStream,
    },
    Disconnected,
}

impl PartialEq for PeerState {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PeerState::Connecting, PeerState::Connecting) => true,
            (PeerState::Connected { .. }, PeerState::Connected { .. }) => true,
            (PeerState::Disconnected, PeerState::Disconnected) => true,
            _ => false,
        }
    }
}

impl Eq for PeerState {}

#[derive(Debug, Clone)]
pub(crate) struct PeerMetrics {
    pub(crate) download_speed: f64,
    pub(crate) successful_downloads: usize,
    pub(crate) failed_downloads: usize,
}

impl Ord for PeerMetrics {
    fn cmp(&self, other: &Self) -> Ordering {
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

impl PeerMetrics {
    pub fn new_default() -> Self {
        PeerMetrics {
            download_speed: 0.0,
            successful_downloads: 0,
            failed_downloads: 0,
        }
    }
}

impl Peer {
    pub fn to_string(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    fn percent_encode_hash(s: &str) -> String {
        let mut result = String::new();
        for (i, chr) in s.chars().enumerate() {
            if i % 2 == 0 {
                result.push('%');
            }
            result.push(chr);
        }
        result
    }

    pub async fn create_client(&mut self, info_hash: [u8; 20]) -> Result<()> {
        let mut stream = TcpStream::connect(format!("{}:{}", self.ip, self.port)).await?;
        Handshake::handshake(info_hash, &mut stream).await?;
        match Message::read_message(&mut stream).await? {
            Message::Bitfield => {
                let request = Message::Interested;
                Message::send_message(request, &mut stream).await?;
            }
            _ => return Err(anyhow::anyhow!("Didn't receive bitfield")),
        }
        match Message::read_message(&mut stream).await? {
            Message::Unchoke => {
                self.state = PeerState::Connected { stream };
                Ok(())
            }
            _ => Err(anyhow::anyhow!("Didn't unchoke")),
        }
    }

    pub async fn download_piece(
        peer: Arc<RwLock<Peer>>,
        piece: Arc<Mutex<Piece>>,
        torrent_info_hash: [u8; 20],
    ) -> anyhow::Result<()> {
        let mut peer = peer.write().await;
        let mut piece = piece.lock().await;
        let stream = match peer.create_client(torrent_info_hash).await {
            Ok(()) => match &mut peer.state {
                PeerState::Connected { stream } => stream,
                _ => return Err(anyhow::anyhow!("Unexpected peer state")),
            },
            Err(e) => return Err(e),
        };

        let index = piece.index;

        for undownloaded_block in &mut piece.get_blocks_mut() {
            let start = Instant::now();
            let request = Message::Request {
                index: index as u32,
                begin: undownloaded_block.begin as u32,
                length: undownloaded_block.block_size as u32,
            };

            Message::send_message(request, stream).await?;
            match Message::read_message(stream).await? {
                Message::Piece { block: block_bytes, .. } => {
                    let elapsed = start.elapsed();
                    undownloaded_block.block_state = BlockState::Downloaded {
                        data: block_bytes,
                        duration: elapsed,
                    };
                }
                _ => {
                    undownloaded_block.block_state = BlockState::Missing;
                }
            }
        }

        if piece
            .blocks
            .iter()
            .all(|block| matches!(block.block_state, BlockState::Downloaded { .. }))
        {
            let piece_bytes = piece.blocks.iter().fold(Vec::new(), |mut acc, block| {
                if let BlockState::Downloaded { data, .. } = &block.block_state {
                    acc.extend_from_slice(data);
                }
                acc
            });
            assert_eq!(piece_bytes.len(), piece.length, "Piece length mismatch");
            piece.set_state(PieceState::Downloaded { piece_bytes });
            peer.state = PeerState::Disconnected;
            Ok(())
        } else {
            piece.piece_state = PieceState::Missing;
            Err(anyhow::anyhow!("Failed to download piece from peer"))
        }
    }
}

pub struct PeerPool {
    peers: HashMap<String, Arc<RwLock<Peer>>>,
    peer_piece_map: HashMap<String, HashSet<usize>>,
}

impl PeerPool {
    pub async fn new(torrent: &Torrent) -> Result<Self> {
        let info_hash = torrent.calculate_info_hash();
        let encoded_info_hash = Peer::percent_encode_hash(hex::encode(info_hash.to_vec()).as_str());

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
            (
                id.clone(),
                Arc::new(RwLock::new(Peer {
                    id,
                    ip,
                    port,
                    state: PeerState::Disconnected,
                    peer_metrics: PeerMetrics::new_default(),
                    pieces: HashSet::new(),
                })),
            )
        }).collect();

        Ok(PeerPool {
            peers,
            peer_piece_map: HashMap::new(),
        })
    }

    pub async fn get_mut(&self, peer_id: &str) -> Option<Arc<RwLock<Peer>>> {
        self.peers.get(peer_id).cloned()
    }

    pub async fn get_best_peer_mut(&self) -> Option<Arc<RwLock<Peer>>> {
        let mut best_peer: Option<Arc<RwLock<Peer>>> = None;

        for peer in self.peers.values() {
            let peer = peer.clone();
            
            {
                let peer_guard = peer.read().await;
                if peer_guard.state != PeerState::Disconnected {
                    continue;  // Skip this peer if it's not disconnected
                }
            }
            
            let peer_metrics = {
                let peer_guard = peer.read().await;
                peer_guard.peer_metrics.clone()
            };

            best_peer = match best_peer {
                Some(current_best) => {
                    let current_best_metrics = {
                        let current_best_guard = current_best.read().await;
                        current_best_guard.peer_metrics.clone()
                    };

                    if peer_metrics > current_best_metrics {
                        peer.write().await.state = PeerState::Connecting;
                        Some(peer)
                    } else {
                        current_best.write().await.state = PeerState::Connecting;
                        Some(current_best)
                    }
                },
                None => Some(peer),
            };
        }
        
        best_peer
    }

    pub fn add_peer_piece(&mut self, peer_id: &str, piece_index: usize) {
        self.peer_piece_map
            .entry(peer_id.to_string())
            .or_insert_with(HashSet::new)
            .insert(piece_index);
    }

    pub fn get_peers_with_piece(&self, piece_index: usize) -> Vec<Arc<RwLock<Peer>>> {
        self.peer_piece_map.iter()
            .filter_map(|(peer_id, pieces)| {
                if pieces.contains(&piece_index) {
                    self.peers.get(peer_id).cloned()
                } else {
                    None
                }
            })
            .collect()
    }
}
