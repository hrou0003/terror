use std::fs;
use serde::{Deserialize, Serialize};
use anyhow::Result;

use crate::torrent::{calculate_info_hash, Torrent};

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
    #[serde(deserialize_with = "deserialize_peers")]
    peers: Vec<Peer>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Peer {
    pub ip: String,
    pub port: u16,
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

    let tracker = serde_bencode::from_bytes::<TrackerResponse>(&result)?;

    Ok(tracker.peers)
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