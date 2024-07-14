use std::net::IpAddr;
use std::sync::Arc;

use crate::tcp::message::Message;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

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
    pub fn new(peer_id: String, ip_addr: IpAddr, port: u16, info_hash: [u8; 20]) -> Self {
        Peer {
            id: peer_id,
            ip_addr,
            port,
            state: PeerState::Disconnected,
            info_hash,
        }
    }
}
