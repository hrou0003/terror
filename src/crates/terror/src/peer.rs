use std::net::IpAddr;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

use crate::message::Message;

type CycleRx = mpsc::Receiver<CycleMessage>;

pub(crate) struct CycleMessage {}

pub struct Peer {
    pub(crate) id: String,
    pub(crate) ip_addr: IpAddr,
    pub(crate) port: u16,
    pub(crate) state: PeerState,
    pub(crate) info_hash: [u8; 20],
}


pub(crate) enum PeerState {
    Connected { stream: Arc<Mutex<TcpStream>> },
    Disconnected,
}


impl Peer {
    
    pub fn new(ip_addr: IpAddr, port: u16, info_hash: [u8; 20]) -> Self {
        Peer {
            id: Uuid::new_v4().to_string(),
            ip_addr,
            port,
            state: PeerState::Disconnected,
            info_hash,
        }
    }
    
    pub async fn download_block_from_stream(stream: Arc<Mutex<TcpStream>>, index: u32, begin: u32, length: u32) -> anyhow::Result<Vec<u8>> {
        let mut stream = stream.lock().await;

        let message = Message::Request {
            index, begin, length
        };

        Message::send_message(message, &mut stream).await.expect("Couldn't send message");

        let response = Message::read_message(&mut stream).await;

        match response {
            Ok(Message::Piece { block, .. }) => {
                Ok(block)
            }
            _ => {
                Err(anyhow::anyhow!("Unexpected message type"))
            }
        }
    }
}

