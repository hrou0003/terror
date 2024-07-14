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
    interval: Option<usize>,
    pub peers: Vec<Peer>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Peer {
    pub(crate) ip: String,
    #[serde(rename = "peer id", with = "serde_bytes")]
    pub(crate) peer_id: ByteBuf,
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
