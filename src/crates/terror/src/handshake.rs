use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::torrent::{Torrent};

#[derive(Serialize, Deserialize)]
pub(crate) struct Handshake {
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

    async fn from_stream(stream: &mut tokio::net::TcpStream) -> tokio::io::Result<Self> {
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

    pub(crate) async fn handshake(info_hash: [u8; 20], stream: &mut tokio::net::TcpStream) -> anyhow::Result<Handshake> {

        let peer_id: [u8; 20] = *b"00112233445566778899";
        let mut handshake = Handshake::new(info_hash, peer_id);
        let handshake_bytes = handshake.to_bytes();
        
        eprintln!("Send handshake request {}", hex::encode(handshake_bytes.to_vec()));
        stream.write_all(&handshake_bytes).await?;

        let received_handshake = Handshake::from_stream(stream).await?;

        println!("Peer ID: {}", hex::encode(received_handshake.peer_id));

        Ok(received_handshake)
    }
}