use std::env;
use anyhow;

use bittorrent_starter_rust::client::download;
use bittorrent_starter_rust::client::tracking::get_peers;
use bittorrent_starter_rust::client::handshake::Handshake;
use bittorrent_starter_rust::torrent::{calculate_info_hash, parse_file};
use bittorrent_starter_rust::decoder::decode_bencoded_value;


// Usage: your_bittorrent.sh decode "<encoded_value>"
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    let command = args[1].as_str();

    match command {
    "decode" => {
        // Uncomment this block to pass the first stage
        let encoded_value = args[2].as_bytes().to_vec();
        let (value, _) = decode_bencoded_value(encoded_value);
        println!("{}", value.to_string());
    }
    "info" => {
        let file_path = &args[2];
        let torrent = parse_file(file_path.to_string());
        let info_hash = calculate_info_hash(&torrent.info);
        println!(
            "Tracker URL: {}\nLength: {}\nInfo Hash: {}\nPiece Length: {}\nPiece Hashes: ",
            torrent.announce,
            torrent.info.length,
            hex::encode(info_hash),
            torrent.info.piece_length
        );

        let mut chunks = torrent.info.pieces.chunks_exact(20);
        for chunk in &mut chunks {
            println!("{}", hex::encode(chunk))
        }

        let remainder = chunks.remainder();
        println!("{}", hex::encode(remainder))
    }
    "peers" => {
        let file_path = &args[2];
        let torrent = parse_file(file_path.to_string());

        let peers = get_peers(&torrent).await.expect("Peers couldn't be found");

        for peer in peers {
            println!("{}:{}", peer.ip, peer.port.to_string())
        }
    }
    "handshake" => {
        let file_path = &args[2];
        let peer_address = &args[3];

        let torrent = parse_file(file_path.to_string());

        let mut stream = tokio::net::TcpStream::connect(peer_address).await?;

        Handshake::do_handshake(&torrent, &mut stream).await.expect("Couldn't perform handshake");
    }
    "download_piece" => {
        let output_path = &args[2];
        let file_path = &args[3];
        let piece_index = &args[4];

        let torrent = parse_file(file_path.to_string());

        let piece = download::Message::download_piece(&torrent, piece_index.parse::<usize>()?).await?;
    }
    _ => eprintln!("unknown command: {}", args[1])
    }

    Ok(())
}
