extern crate core;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

pub mod torrent;

pub(crate) mod peer;

mod metrics;
mod piece;
pub(crate) mod tcp;
mod utils;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
