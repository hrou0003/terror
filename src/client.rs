use std::{fs, io::{self, Read}, net::SocketAddrV4};
use serde::{Deserialize, Serialize};
use anyhow::{Context, Result};
use serde_bytes::ByteBuf;
use tokio::{io::{AsyncReadExt, AsyncWriteExt, BufWriter}, net::TcpStream, stream};

use crate::torrent::{calculate_info_hash, parse_file, Torrent};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackerRequest {
    peer_id: String,
    port: usize,
    uploaded: usize,
    downloaded: usize,
    left: usize,
    compact: usize
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackerResponse {
    interval: usize,
    #[serde(rename = "peers")]
    peers_raw: ByteBuf
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Peer {
    pub ip: String,
    pub port: u16,
}
#[derive(Serialize, Deserialize)]
pub struct Handshake {
    pub length: u8,
    pub bittorrent: [u8; 19],
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20]
}

impl Handshake {
    fn new(info_hash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Self {
            length: 19,
            bittorrent: *b"BitTorrent protocol",
            reserved: [0; 8],
            info_hash,
            peer_id
        }
    }

    fn to_bytes(&mut self) -> [u8; 68] {
        let mut bytes = [0; 68];
        let mut pos = 0;

        // Write length
        bytes[pos] = self.length;
        pos += 1;

        // Write bittorrent protocol
        bytes[pos..pos + 19].copy_from_slice(&self.bittorrent);
        pos += 19;

        // Write reserved bytes
        bytes[pos..pos + 8].copy_from_slice(&self.reserved);
        pos += 8;

        // Write info_hash
        bytes[pos..pos + 20].copy_from_slice(&self.info_hash);
        pos += 20;

        // Write peer_id
        bytes[pos..pos + 20].copy_from_slice(&self.peer_id);

        bytes
    }

    async fn from_stream(mut stream: tokio::net::TcpStream) -> io::Result<Self> {
        let mut bytes = vec![0; 68]; // length of handshake message


        if stream.ready(tokio::io::Interest::READABLE).await?.is_readable() {
            // The stream is readable, proceed with reading
            eprintln!("Reading bytes");
            stream.read_exact(&mut bytes).await?;
            // Process the read bytes
        } else {
            // The stream is not readable, handle accordingly
            println!("Stream is not readable");
        }

        let length = bytes[0];
        let bittorrent = {
            let mut arr = [0; 19];
            arr.copy_from_slice(&bytes[1..20]);
            arr
        };
        let reserved = {
            let mut arr = [0; 8];
            arr.copy_from_slice(&bytes[20..28]);
            arr
        };
        let info_hash = {
            let mut arr = [0; 20];
            arr.copy_from_slice(&bytes[28..48]);
            arr
        };
        let peer_id = {
            let mut arr = [0; 20];
            arr.copy_from_slice(&bytes[48..68]);
            arr
        };

        Ok(Self {
            length,
            bittorrent,
            reserved,
            info_hash,
            peer_id,
        })
    }
}

fn deserialize_peers<'de, D>(deserializer: D) -> Result<Vec<Peer>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let bytes = serde_bytes::ByteBuf::deserialize(deserializer)?;
    let mut peers = Vec::new();
    for chunk in bytes.chunks_exact(6) {
        let ip = format!("{}.{}.{}.{}", chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = ((chunk[4] as u16) << 8) | (chunk[5] as u16);
        peers.push(Peer { ip, port });
    }
    Ok(peers)
}

pub async fn get_peers(file_path: &str) -> Result<Vec<Peer>> {
    let raw_torrent_file = fs::read(file_path).expect("Invalid torrent file");
    let torrent = serde_bencode::from_bytes::<Torrent>(&raw_torrent_file)?;
    let info_hash = calculate_info_hash(&torrent.info);
    let encoded_info_hash = percent_encode_hash(hex::encode(info_hash.to_vec()).as_str());

    let request = TrackerRequest {
        peer_id: "codecraftersbittorre".to_string(),
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: torrent.info.length,
        compact: 1
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
        peers.push(Peer { ip, port });
    }

    Ok(peers)
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

pub async fn handshake(file_path: String, peer_address: &str) -> anyhow::Result<()> {
    
    let torrent = parse_file(file_path);

    let info_hash = calculate_info_hash(&torrent.info);

    let peer = peer_address.parse::<SocketAddrV4>().context("parse peer address")?;

    let mut peer = tokio::net::TcpStream::connect(peer)
        .await
        .context("connect to peer")?;

    let peer_id: [u8; 20] = *b"00112233445566778899";
    let mut handshake = Handshake::new(info_hash, peer_id);
    let handshake_bytes = handshake.to_bytes();
    
    eprintln!("Send handshake request {}", hex::encode(handshake_bytes.to_vec()));
    peer.write_all(&handshake_bytes).await?;

    let received_handshake = Handshake::from_stream(peer).await?;

    println!("Peer ID: {}", hex::encode(received_handshake.peer_id));

    Ok(())
}