use std::collections::HashMap;
use std::fs;

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};

use crate::piece::Priority;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Torrent {
    // URL to a "tracker", which is a central server that keeps track of peers participating in the sharing of a torrent.
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
    pub url_list: Option<Vec<String>>
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Info {
    // size of the file in bytes, for single-file torrents
    #[serde(default)]
    pub length: usize,
    // suggested name to save the file / directory as
    pub name: String,
    // number of bytes in each piece
    #[serde(rename = "piece length")]
    pub piece_length: usize,
    // concatenated SHA-1 hashes of each piece
    pub pieces: ByteBuf,
    #[serde(default)]
    pub md5hash: Option<String>,
    // list of files in a multi-file torrent
    #[serde(default)]
    pub files: Option<Vec<FileInfo>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct FileInfo {
    pub(crate) name: Option<String>,
    pub(crate) length: usize,
    pub(crate) path: Vec<String>,
    pub(crate) md5sum: Option<String>,
    #[serde(default)]
    pub(crate) offset: Option<usize>,
    #[serde(default)]
    pub(crate) start_piece: Option<usize>,
    #[serde(default)]
    pub(crate) end_piece: Option<usize>,
    #[serde(default)]
    pub(crate) priority: Priority
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum FileTree {
    SingleFile {
        file_info: FileInfo
    },
    MultiFile {
        files: HashMap<String, FileInfo>
    }
}

impl Torrent {
    pub const MAX_CONCURRENT: usize = 4;
    pub const MAX_RETRIES: usize = 2;

    pub fn new(file_path: String) -> Torrent {
        let file = fs::read(file_path).expect("bad file");
        let torrent : Torrent = serde_bencode::de::from_bytes(&file).unwrap();
        return torrent;
    }

    pub(crate) fn calculate_info_hash(&self) -> [u8; 20] {
        let info = &self.info;
        let info_raw = serde_bencode::to_bytes(&info).expect("Invalid info dictionary");
        let decoded = serde_bencode::from_bytes::<Info>(&info_raw).expect("Bad info");
        println!("{:?}", decoded);
        let mut hasher = Sha1::new();
        hasher.update(info_raw);
        return hasher.finalize().into();
    }
    
    pub(crate) fn get_number_of_pieces(&self) -> usize {
        let number_of_pieces = self.info.length as f64 / self.info.piece_length as f64;
        return number_of_pieces.ceil() as usize;
    }
    
    fn get_number_of_blocks(&self) -> usize {
        return  self.info.piece_length / (1<<14);
    }

}


mod tests {
    use super::*;

    #[test]
    fn test_calculate_info_hash() {
        let mut torrent = Torrent::new("sample.torrent".to_string());
        let info_hash = torrent.calculate_info_hash();
        assert_eq!(info_hash, [0x8e, 0x9e, 0x9f, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e]);
    }
    
    #[test]
    fn test_get_number_of_pieces() {
        let torrent = Torrent::new("sample.torrent".to_string());
        assert_eq!(torrent.get_number_of_pieces(), 3);
    }
    
    #[test]
    fn test_get_number_of_blocks() {
        let torrent = Torrent::new("sample.torrent".to_string());
        assert_eq!(torrent.get_number_of_blocks(), 2);
    }
    
}
