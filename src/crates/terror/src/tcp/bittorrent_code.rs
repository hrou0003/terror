use tokio_util::codec::{Decoder, Encoder};
use bytes::{BytesMut, Buf, BufMut};
use std::io;
use crate::tcp::message::Message;

pub struct BitTorrentCodec;

impl Decoder for BitTorrentCodec {
    type Item = Message;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        
        if src.len() < 4 {
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(length_bytes) as usize;

        if src.len() < length + 4 {
            return Ok(None);
        }

        src.advance(4);
        let message_type = src.get_u8();

        match message_type {
            1 => Ok(Some(Message::Unchoke)),
            2 => Ok(Some(Message::Interested)),
            5 => {
                let bitfield = src.split_to(length - 1).freeze();
                Ok(Some(Message::Bitfield(bitfield)))
            },
            6 => {
                let index = src.get_u32();
                let begin = src.get_u32();
                let length = src.get_u32();
                Ok(Some(Message::Request { index, begin, length }))
            },
            7 => {
                let index = src.get_u32();
                let begin = src.get_u32();
                let block = src.split_to(length - 9).freeze();
                Ok(Some(Message::Piece { index, begin, block }))
            },
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown message type")),
        }
    }
}

impl Encoder<Message> for BitTorrentCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Message::Unchoke => {
                dst.put_u32(1);
                dst.put_u8(1);
            },
            Message::Interested => {
                dst.put_u32(1);
                dst.put_u8(2);
            },
            Message::Bitfield(bitfield) => {
                dst.put_u32((1 + bitfield.len()) as u32);
                dst.put_u8(5);
                dst.extend_from_slice(&bitfield);
            },
            Message::Request { index, begin, length } => {
                dst.put_u32(13);
                dst.put_u8(6);
                dst.put_u32(index);
                dst.put_u32(begin);
                dst.put_u32(length);
            },
            Message::Piece { index, begin, block } => {
                dst.put_u32((9 + block.len()) as u32);
                dst.put_u8(7);
                dst.put_u32(index);
                dst.put_u32(begin);
                dst.extend_from_slice(&block);
            },
        }
        Ok(())
    }
}