use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc};
use std::time::Instant;
use serde::{Deserialize, Serialize};
use anyhow::Result;
use regex::Match;
use serde_bytes::ByteBuf;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;
use crate::message::Message;
use crate::handshake::Handshake;
use crate::piece::{Block, BlockState, Piece, PieceState};
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
    pub pieces: HashSet<usize>,
    pub  info_hash: [u8; 20],
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

    pub async fn create_client(&mut self) -> Result<()> {
        let mut stream = TcpStream::connect(format!("{}:{}", self.ip, self.port)).await?;
        eprintln!("Sending handshake to {}:{}", self.ip, self.port);
        match Handshake::handshake(self.info_hash, &mut stream).await {
            Ok(_) => (),
            Err(e) => return Err(e),
        };
        match Message::read_message(&mut stream).await {
            Ok(Message::Bitfield { bitfield }) => {
                self.process_bitfield(bitfield);
                let request = Message::Interested;
                Message::send_message(request, &mut stream).await?;
            }
            _ => return Err(anyhow::anyhow!("Didn't receive bitfield")),
        }
        match Message::read_message(&mut stream).await {
            Ok(Message::Unchoke) => {
                self.state = PeerState::Connected { stream };
                Ok(())
            }
            _ => Err(anyhow::anyhow!("Didn't unchoke")),
        }
    }
    
    fn process_bitfield(&mut self, bitfield: Vec<u8>) {
        for (i, byte) in bitfield.iter().enumerate() {
            for j in 0..8 {
                if byte & (1 << (7 - j)) != 0 {
                    let index = i * 8 + j;
                    self.pieces.insert(index);
                }
            }
        }
    }
    
    pub fn piece_in_bitfield(&self, piece_index: usize) -> bool {
        self.pieces.contains(&piece_index)
    }

    // pub async fn download_piece(
    //     peer: Arc<RwLock<Peer>>,
    //     piece: Arc<Mutex<Piece>>,
    //     torrent_info_hash: [u8; 20],
    // ) -> Result<()> {
    //     // Acquire write lock on peer
    //     let mut peer = peer.write().await;
    // 
    //     // Attempt to connect the peer using torrent info hash
    //     let stream = match peer.create_client().await {
    //         Ok(()) => match &mut peer.state {
    //             PeerState::Connected { stream } => stream,
    //             _ => return Err(anyhow::anyhow!("Unexpected peer state")),
    //         },
    //         Err(e) => return Err(e),
    //     };
    // 
    //     // Acquire lock on piece
    //     let mut piece = piece.lock().await;
    // 
    //     let index = piece.index;
    // 
    //     for undownloaded_block in &mut piece.blocks {
    //         let start = Instant::now();
    //         let request = Message::Request {
    //             index: index as u32,
    //             begin: undownloaded_block.begin as u32,
    //             length: undownloaded_block.block_size as u32,
    //         };
    // 
    //         Message::send_message(request, stream).await?;
    // 
    //         match Message::read_message(stream).await? {
    //             Message::Piece {
    //                 index: _,
    //                 begin: _,
    //                 block,
    //             } => {
    //                 let elapsed = start.elapsed();
    //                 undownloaded_block.block_state = BlockState::Downloaded { duration: elapsed, data: block };
    //             }
    //             _ => {
    //                 undownloaded_block.block_state = BlockState::Missing;
    //             },
    //         }
    //     }
    // 
    //     if piece
    //         .blocks
    //         .iter()
    //         .all(|block| matches!(block.block_state, BlockState::Downloaded { .. }))
    //     {
    //         let piece_bytes = piece.blocks.iter().fold(Vec::new(), |mut acc, block| {
    //             if let BlockState::Downloaded { data, .. } = &block.block_state {
    //                 acc.extend_from_slice(data);
    //             }
    //             acc
    //         });
    //         assert_eq!(piece_bytes.len(), piece.length, "Piece length mismatch");
    //         piece.set_state(PieceState::Downloaded { piece_bytes });
    //         peer.state = PeerState::Disconnected;
    //         Ok(())
    //     } else {
    //         piece.piece_state = PieceState::Missing;
    //         Err(anyhow::anyhow!("Failed to download piece from peer"))
    //     }
    // }
    
    pub async fn download_block(peer: Arc<RwLock<Self>>, block: Arc<RwLock<Block>>) -> Result<()>{
        
        let mut peer = peer.write().await;
        let mut block = block.write().await;
        let start = Instant::now();
        let request = Message::Request {
            index: block.index as u32,
            begin: block.begin as u32,
            length: block.block_size as u32,
        };

        // Check if there isn't already a connected stream
        let stream = match &mut peer.state {
            PeerState::Connected { stream } => stream,
            _ => {
                // Attempt to connect the peer using torrent info hash
                match peer.create_client().await {
                    Ok(()) => match &mut peer.state {
                        PeerState::Connected { stream } => stream,
                        _ => return Err(anyhow::anyhow!("Unexpected peer state")),
                    },
                    Err(e) => return Err(e),
                }
            },
        };

        Message::send_message(request, stream).await.unwrap();

        match Message::read_message(stream).await.unwrap() {
            Message::Piece {
                index: _,
                begin: _,
                block: block_bytes,
            } => {
                let elapsed = start.elapsed();
                block.block_state = BlockState::Downloaded { duration: elapsed, data: block_bytes };
                return Ok(());
            }
            _ => {
                block.block_state = BlockState::Missing;
                return Err(anyhow::anyhow!("Failed to download block from peer"));
            },
        }
    }
}

pub struct PeerPool {
    pub peers: HashMap<String, Arc<RwLock<Peer>>>,
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
                    info_hash: info_hash,
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

    // pub fn get_best_peer_mut(&self) -> Option<Arc<RwLock<Peer>>> {
    //     let mut best_peer: Option<Arc<RwLock<Peer>>> = None;
    // 
    //     for peer in self.peers.values() {
    //         let peer = peer.clone();
    //         
    //         {
    //             let peer_guard = peer.read().unwrap();
    //             if peer_guard.state != PeerState::Disconnected {
    //                 continue;  // Skip this peer if it's not disconnected
    //             }
    //         }
    //         
    //         let peer_metrics = {
    //             let peer_guard = peer.read().unwrap();
    //             peer_guard.peer_metrics.clone()
    //         };
    // 
    //         best_peer = match best_peer {
    //             Some(current_best) => {
    //                 let current_best_metrics = {
    //                     let current_best_guard = current_best.read().unwrap();
    //                     current_best_guard.peer_metrics.clone()
    //                 };
    // 
    //                 if peer_metrics > current_best_metrics {
    //                     peer.write().unwrap().state = PeerState::Connecting;
    //                     Some(peer)
    //                 } else {
    //                     current_best.write().unwrap().state = PeerState::Connecting;
    //                     Some(current_best)
    //                 }
    //             },
    //             None => Some(peer),
    //         };
    //     }
    //     
    //     best_peer
    // }

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
    
    pub fn with_bad_peer(&mut self) {
        let bad_peer = Peer { id: "bad_peer".to_string(), ip: "".to_string(), port: 0, state: PeerState::Disconnected, peer_metrics: PeerMetrics::new_default(), pieces: HashSet::new(), info_hash: [0; 20] };
        self.peers.insert("bad_peer".to_string(), Arc::new(RwLock::new(bad_peer)));
    }
}

mod test {
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use crate::peer::{Peer, PeerMetrics, PeerPool, PeerState};
    use crate::Torrent;

    #[tokio::test]
    async fn test_get_next_peer_mut() {
        
        
        // let peer1 = Arc::new(RwLock::new(Peer {
        //     id: "1".to_string(),
        //     ip: "".to_string(),
        //     port: 0,
        //     state: PeerState::Disconnected,
        //     peer_metrics: PeerMetrics::new_default(),
        //     pieces: HashSet::new(),
        // }));
        // 
        // let peer2 = Arc::new(RwLock::new(Peer {
        //     id: "2".to_string(),
        //     ip: "".to_string(),
        //     port: 0,
        //     state: PeerState::Disconnected,
        //     peer_metrics: PeerMetrics::new_default(),
        //     pieces: HashSet::new(),
        // }));
        // 
        // let peer3 = Arc::new(RwLock::new(Peer {
        //     id: "3".to_string(),
        //     ip: "".to_string(),
        //     port: 0,
        //     state: PeerState::Disconnected,
        //     peer_metrics: PeerMetrics::new_default(),
        //     pieces: HashSet::new(),
        // }));
        // 
        // let mut peers = PeerPool {
        //     peers: vec![
        //         ("1".to_string(), peer1.clone()),
        //         ("2".to_string(), peer2.clone()),
        //         ("3".to_string(), peer3.clone()),
        //     ].into_iter().collect(),
        //     peer_piece_map: HashMap::new(),
        // };

        // let best_peer = peers.get_best_peer_mut();

        // assert!(best_peer.is_some());
        // let peer_guard = best_peer.unwrap().read().await;
        // assert_eq!(peer_guard.peer_metrics.quality, 20);
        // assert_eq!(peer_guard.state, PeerState::Connecting);
    }
}
