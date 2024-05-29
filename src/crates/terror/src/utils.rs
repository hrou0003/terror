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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackerResponse {
    interval: usize,
    #[serde(rename = "peers")]
    pub(crate) peers_raw: ByteBuf,
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
