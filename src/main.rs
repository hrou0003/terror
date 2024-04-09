use std::env;

use bittorrent_starter_rust::torrent::parse_file;
use bittorrent_starter_rust::decoder::decode_bencoded_value;


// Usage: your_bittorrent.sh decode "<encoded_value>"
fn main() {
    let args: Vec<String> = env::args().collect();
    let command = &args[1];

    if command == "decode" {
        // Uncomment this block to pass the first stage
        let encoded_value = args[2].as_bytes().to_vec();
        let (value, _) = decode_bencoded_value(encoded_value);
        println!("{}", value.to_string());
    } else if command == "info" {
        let file_path = &args[2];
        let torrent = parse_file(file_path.to_string());
        println!(
            "Tracker URL: {}, Length: {}",
            torrent.announce, torrent.info.length
        )
    } else {
        eprintln!("unknown command: {}", args[1])
    }
}
