use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc};
use std::time::Duration;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, sync::{RwLock, Mutex}};
use crate::peer::Peer;
use crate::torrent::{Info, Torrent};

const BLOCK_LENGTH : usize = 1 << 14;

#[derive(PartialEq, Debug)]
pub(crate) enum Message {
    Bitfield {
        bitfield: Vec<u8>
    },
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
        block: Vec<u8>
    }
}

impl Message {
    fn type_byte(&self) -> u8 {
        match self {
            Message::Unchoke => 1,
            Message::Interested => 2,
            Message::Bitfield  { .. } => 5,
            Message::Request { .. } => 6,
            Message::Piece { .. } => 7,
        }
    }

    fn encode(&self) -> anyhow::Result<Vec<u8>> {
        let payload = match self {
            Message::Unchoke | Message::Interested => vec![],
            Message::Bitfield { bitfield } => {
                let mut buf = Vec::new();
                buf.extend(bitfield);
                buf
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                let mut buf = Vec::new();
                buf.extend(index.to_be_bytes());
                buf.extend(begin.to_be_bytes());
                buf.extend(length.to_be_bytes());
                buf
            }
            Message::Piece {
                index,
                begin,
                block,
            } => {
                let mut buf = Vec::new();
                buf.extend(index.to_be_bytes());
                buf.extend(begin.to_be_bytes());
                buf.extend(block);
                buf
            }
        };

        let mut buf = Vec::new();
        buf.extend(((1 + payload.len()) as u32).to_be_bytes());
        buf.push(self.type_byte());
        buf.extend(payload);
        Ok(buf)
    }
    
    pub(crate) async fn send_message(message: Message, stream: &mut tokio::net::TcpStream) -> anyhow::Result<()> {
        let payload = Self::encode(&message)?;
        eprintln!("Sending message {}", message.type_byte());
        stream.write(&payload).await?;
        return Ok(());
    }


    pub(crate) async fn read_message(stream: &mut tokio::net::TcpStream) -> anyhow::Result<Message> {
        let mut length_bytes = [0; 4];

        stream.read_exact(&mut length_bytes).await?;

        while u32::from_be_bytes(length_bytes) == 0 {
            stream.read_exact(&mut length_bytes).await?;
        }

        let message_type = stream.read_u8().await?;
        
        eprintln!("Reading message {}", message_type);

        let len = u32::from_be_bytes(length_bytes) as usize;

        let mut payload = match len {
            0 => Vec::new(),
            1 => Vec::new(),
            _ => vec![0; len - 1],
        };

        stream.read_exact(&mut payload).await?;

        match message_type {
            2 => Ok(Message::Interested),
            1 => Ok(Message::Unchoke),
            5 => {
                let bitfield = payload;
                Ok(Message::Bitfield { bitfield })
            },
            6 => {
                let index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let begin = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                let length = u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]);
                Ok(Message::Request { index, begin, length })
            },
            7 => {
                let index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let begin = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                let block = payload[8..].to_vec();
                Ok(Message::Piece { index, begin, block })
            },
            t => Err(anyhow::anyhow!("Unknown message type {t}")),
        }

    }
}




mod tests {
    use std::fs;
    use crate::message::{Message};
    use crate::Torrent;
    
    #[test]
    fn test_encode() {
        let message = Message::Request { index: 0, begin: 0, length: 1 };
        let encoded = message.encode().expect("Couldn't encode message");
        assert_eq!(encoded, vec![0, 0, 0, 13, 6, 0, 0, 0, 0, 0, 0, 1]);
    }
    

}