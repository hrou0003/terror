extern crate core;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

pub mod torrent;

pub(crate) mod peer;

mod piece;
mod utils;


pub use {
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
