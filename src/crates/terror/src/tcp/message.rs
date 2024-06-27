use std::fmt::Debug;
use bytes::Bytes;

#[derive(PartialEq, Debug)]
pub enum Message {
    Bitfield(Bytes), 
    Interested,
    Unchoke,
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Bytes
    }
}