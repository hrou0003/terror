pub fn add(left: usize, right: usize) -> usize {
    left + right
}

pub mod torrent;
pub use {
    torrent::Torrent,
};

pub(crate) mod handshake;
pub(crate) mod peer;
pub(crate) mod download;
pub(crate) mod metrics;


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
