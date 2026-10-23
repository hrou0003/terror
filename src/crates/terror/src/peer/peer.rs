use std::net::IpAddr;

pub struct Peer {
    pub(crate) id: String,
    pub(crate) ip_addr: IpAddr,
    pub(crate) port: u16,
    pub(crate) info_hash: [u8; 20],
}

impl Peer {
    pub fn new(peer_id: String, ip_addr: IpAddr, port: u16, info_hash: [u8; 20]) -> Self {
        Self {
            id: peer_id,
            ip_addr,
            port,
            info_hash,
        }
    }
}
