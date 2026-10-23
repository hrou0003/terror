use std::fs;

use crate::piece::piece::Priority;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};
use tracing::debug;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Torrent {
    // URL to a "tracker", which is a central server that keeps track of peers participating in the sharing of a torrent_info.
    pub announce: String,
    #[serde(rename = "announce-list")]
    announce_list: Option<Vec<Vec<String>>>,
    comment: Option<String>,
    #[serde(rename = "created by")]
    created_by: Option<String>,
    #[serde(rename = "creation date")]
    creation_date: Option<i64>,
    pub info: Info,
    #[serde(rename = "url-list")]
    pub url_list: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Info {
    // size of the file in bytes, for single-file torrents
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<usize>,
    // suggested name to save the file / directory as
    pub name: String,
    // number of bytes in each piece
    #[serde(rename = "piece length")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub piece_length: Option<usize>,
    // concatenated SHA-1 hashes of each piece
    pub pieces: ByteBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub md5hash: Option<String>,
    // list of files in a multi-file torrent_info
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<FileInfo>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FileInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    pub(crate) length: usize,
    pub(crate) path: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) md5sum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) offset: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) start_piece: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) end_piece: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) priority: Option<Priority>,
}

impl Torrent {
    pub const MAX_CONCURRENT: usize = 4;
    pub const MAX_RETRIES: usize = 2;

    pub fn new(file_path: String) -> Torrent {
        let file = fs::read(file_path).expect("bad file");
        let torrent: Torrent = serde_bencode::de::from_bytes(&file).unwrap();
        return torrent;
    }

    pub(crate) fn calculate_info_hash(&self) -> [u8; 20] {
        let info = &self.info;
        let info_raw = serde_bencode::to_bytes(&info).expect("Invalid info dictionary");
        let decoded = serde_bencode::from_bytes::<Info>(&info_raw).expect("Bad info");
        debug!("Decoded info {:?}", decoded);
        let mut hasher = Sha1::new();
        hasher.update(info_raw);
        return hasher.finalize().into();
    }

    pub(crate) fn get_number_of_pieces(&self) -> usize {
        self.info.length().div_ceil(self.info.piece_length.unwrap())
    }
}

impl Info {
    pub fn length(&self) -> usize {
        match self.length {
            Some(length) => length,
            None => self
                .files
                .as_ref()
                .unwrap()
                .iter()
                .fold(0, |acc, file| acc + file.length),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Torrent {
        serde_bencode::from_bytes(include_bytes!("../../test/sample.torrent")).unwrap()
    }

    #[test]
    fn sample_info_hash_matches_the_wire_dictionary() {
        assert_eq!(
            hex::encode(sample().calculate_info_hash()),
            "d69f91e6b2ae4c542468d1073a71d4ea13879a7f"
        );
    }

    #[test]
    fn piece_count_includes_a_partial_final_piece() {
        let torrent = sample();
        assert_eq!(torrent.get_number_of_pieces(), 3);
        let mut exact = torrent.clone();
        exact.info.length = Some(2 * exact.info.piece_length.unwrap());
        assert_eq!(exact.get_number_of_pieces(), 2);
    }
}
