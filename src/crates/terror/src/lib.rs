extern crate core;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

pub mod torrent;

pub(crate) mod handshake;
pub(crate) mod peer;
pub(crate) mod message;
pub(crate) mod metrics;
mod piece;
mod actor;
mod utils;
mod peer_actor;
mod peer_actor_pool;
mod torrent_manager;


pub use {
    torrent::Torrent,
    torrent_manager::TorrentManager,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
