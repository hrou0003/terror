use std::fs;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
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
    #[serde(default)]
    pub length: usize,
    // suggested name to save the file / directory as
    pub name: String,
    // number of bytes in each piece
    #[serde(rename = "piece length")]
    pub piece_length: usize,
    // concatenated SHA-1 hashes of each piece
    pub pieces: ByteBuf,
}

pub fn parse_file(file_path: String) -> Torrent {
    let file = fs::read(file_path).expect("bad file");
    // println!("{:?}", file);

    let torrent : Torrent = serde_bencode::de::from_bytes(&file).expect("Invalid torrent file");

    return torrent;
}

pub fn calculate_info_hash(info: &Info) -> [u8; 20] {
    let info_raw = serde_bencode::to_bytes(&info).expect("Invalid info dictionary");


    let decoded = serde_bencode::from_bytes::<Info>(&info_raw).expect("Bad info");

    println!("{:?}", decoded);

    let mut hasher = Sha1::new();

    hasher.update(info_raw);
    
    return hasher.finalize().into();
}