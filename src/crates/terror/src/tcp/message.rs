use bytes::Bytes;
use std::fmt::Debug;

#[derive(PartialEq, Debug)]
pub enum Message {
    Bitfield(Bytes),
    Interested,
    Unchoke,
    Request {
        index: usize,
        begin: usize,
        length: usize,
    },
    Piece {
        index: usize,
        begin: usize,
        block: Bytes,
    },
}
