use std::sync::{Arc, Mutex};
use tokio::sync::{RwLock};
use serde::{Deserialize, Serialize};
use anyhow::Result;
use serde_bytes::ByteBuf;
use tokio::net::TcpStream;
use uuid::Uuid;
use crate::download::Message;
use crate::handshake::Handshake;
use crate::torrent::{Torrent};

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

impl Peer {
    pub fn to_string(&mut self) -> String {
        return format!("{}:{}", self.ip, self.port);
    }

    pub async fn get_peers(torrent: &Torrent) -> Result<PeerPool> {
        let info_hash = torrent.calculate_info_hash();
        let encoded_info_hash = Self::percent_encode_hash(hex::encode(info_hash.to_vec()).as_str());

        let request = TrackerRequest {
            peer_id: "codecraftersbittorre".to_string(),
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: torrent.info.length,
            compact: 1,
        };

        let request_params = serde_urlencoded::to_string(&request).expect("Couldn't decode request");

        let request_url = format!(
            "{}?{}&info_hash={}",
            torrent.announce,
            request_params,
            encoded_info_hash
        );

        println!("Making request to {}", request_url);

        let result = reqwest::get(request_url).await?.bytes().await?.to_vec();
        println!("Raw response: {}", String::from_utf8_lossy(&result));
        println!("Hex-encoded response: {}", hex::encode(result.to_vec()));

        let tracker: TrackerResponse = serde_bencode::from_bytes(&result)?;

        let mut peers = Vec::new();
        for chunk in tracker.peers_raw.chunks_exact(6) {
            let ip = format!("{}.{}.{}.{}", chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = ((chunk[4] as u16) << 8) | (chunk[5] as u16);
            let id = Uuid::new_v4().to_string();
            peers.push(Arc::new(RwLock::new(Peer { id, ip, port, state: PeerState::Disconnected })));
        }

        return Ok(PeerPool { peers });
    }

    fn percent_encode_hash(s: &str) -> String {
        let mut result = String::new();
        for (i, chr) in s.chars().enumerate() {
            if i % 2 == 0 {
                result.push('%');
            }
            result.push(chr);
        }
        return result;
    }

    pub async fn create_client(&mut self, info_hash: [u8; 20]) -> Result<()> {
        let mut stream = TcpStream::connect(format!("{}:{}", self.ip, self.port)).await?;
        Handshake::handshake(info_hash, &mut stream).await?;
        let message = Message::read_message(&mut stream).await?;
        if message == Message::Bitfield {
            let request = Message::Interested;
            Message::send_message(request, &mut stream).await?;
        }
        match Message::read_message(&mut stream).await? {
            Message::Unchoke => {
                self.state = PeerState::Connected { stream };
                Ok(())
            },
            _ => Err(anyhow::anyhow!("Didn't unchoke")),
        }
    }
}

pub struct PeerPool {
    pub peers: Vec<Arc<RwLock<Peer>>>,
}

impl PeerPool {
    pub async fn get_free_peer(&self) -> Result<Arc<RwLock<Peer>>, anyhow::Error> {
        for peer in &self.peers {
            let mut peer_guard = peer.write().await;
            if peer_guard.state == PeerState::Disconnected {
                peer_guard.state = PeerState::Connecting;
                return Ok(peer.clone());
            }
        }
        Err(anyhow::anyhow!("No free peers available"))
    }

}


