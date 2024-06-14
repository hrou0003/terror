use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

pub fn percent_encode_hash(s: &str) -> String {
    let mut result = String::new();
    for (i, chr) in s.chars().enumerate() {
        if i % 2 == 0 {
            result.push('%');
        }
        result.push(chr);
    }
    result
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TrackerResponse {
    interval: usize,
    #[serde(deserialize_with = "deserialize_peers")]
    pub peers: Vec<Peer>,
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
        peers.push(Peer { peer_id: "test".to_string(), ip, port: port as i64 });
    }
    Ok(peers)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Peer {
    pub(crate) ip: String,
    #[serde(rename = "peer id", skip)]
    pub peer_id: String,
    pub port: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackerRequest {
    pub peer_id: String,
    pub port: usize,
    pub uploaded: usize,
    pub downloaded: usize,
    pub left: usize,
    pub compact: usize,
}
