use std::fs;
use serde::{Deserialize, Serialize};
use sha1::{digest::generic_array::GenericArray, Digest, Sha1};
use crate::decoder::decode_bencoded_value;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Torrent {
    // URL to a "tracker", which is a central server that keeps track of peers participating in the sharing of a torrent.
    pub announce: String,
    pub info: Info,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Info {
    // size of the file in bytes, for single-file torrents
    pub length: i64,
    // suggested name to save the file / directory as
    pub name: String,
    // number of bytes in each piece
    #[serde(rename = "piece length")]
    pub piece_length: i64,
    // concatenated SHA-1 hashes of each piece
    pub pieces: Vec<u8>,
}

pub fn parse_file(file_path: String) -> Torrent {
    let file = fs::read(file_path).expect("bad file");
    // println!("{:?}", file);

    let (parsed_value, _) = decode_bencoded_value(file);

    let announce = parsed_value["announce"].as_str().expect("Invalid URL").to_string();
    let info = &parsed_value["info"];
    let length = &info["length"].as_i64();

    let info = Info {
        length: length.expect("Invalid length"),
        name: info["name"].as_str().expect("Invalid name").to_string(),
        piece_length: info["piece length"].as_i64().expect("Invalid piece length"),
        pieces: info["pieces"].as_str().expect("Invalid pieces").as_bytes().to_vec()
    };

    let torrent = Torrent {
        announce: announce,
        info: info
    };

    return torrent;
}

pub fn calculate_info_hash(info: &Info) -> [u8; 20] {
    let info_raw = serde_bencode::to_bytes(&info).expect("Invalid info dictionary");

    let mut hasher = Sha1::new();

    hasher.update(info_raw);
    
    return hasher.finalize().into();
}