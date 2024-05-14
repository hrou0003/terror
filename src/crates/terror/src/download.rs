use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc};
use std::time::Duration;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, sync::{RwLock, Mutex}};
use crate::torrent::Torrent;

const BLOCK_LENGTH : usize = 1 << 14;

#[derive(PartialEq, Debug)]
pub(crate) enum Message {
    Bitfield,
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
            Message::Bitfield => 5,
            Message::Request { .. } => 6,
            Message::Piece { .. } => 7,
        }
    }

    fn encode(&self) -> anyhow::Result<Vec<u8>> {
        let payload = match self {
            Message::Unchoke | Message::Interested | Message::Bitfield => vec![],
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

        return match message_type {
            5 => Ok(Message::Bitfield),
            2 => Ok(Message::Interested),
            1 => Ok(Message::Unchoke),
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
        };

    }
}

pub(crate) struct Piece {
    pub(crate) index: usize,
    pub(crate) length: usize,
    pub(crate) piece_hash: Vec<u8>,
    pub(crate) piece_state: PieceState,
    pub(crate) blocks: Vec<Block>,
    pub(crate) number_of_blocks: usize,
}

pub(crate) struct Block {
    pub(crate) index: usize,
    pub(crate) begin: usize,
    pub(crate) block_size: usize,
    pub(crate) block_state: BlockState,
}

pub(crate) enum BlockState {
    Downloaded {
        data: Vec<u8>,
        duration: Duration,
    },
    Downloading,
    Missing
}

pub(crate) enum PieceState {
    Downloaded { piece_bytes: Vec<u8> },
    Downloading,
    Missing,
}

pub(crate) struct PiecePool {
    pub(crate) pieces: Vec<Arc<Mutex<Piece>>>,
    pub(crate) number_of_pieces: usize,
}

impl PiecePool {
    pub(crate) fn new(torrent: &Torrent) -> PiecePool {
        let number_of_pieces = torrent.get_number_of_pieces();
        let block_size = 1 << 14;

        let pieces = (0..number_of_pieces)
            .map(|index| {
                let length = if index == number_of_pieces - 1 {
                    torrent.info.length % torrent.info.piece_length
                } else {
                    torrent.info.piece_length
                };

                let number_of_blocks = (length + block_size - 1) / block_size;

                let blocks = (0..number_of_blocks)
                    .map(|block_index| Block {
                        index: block_index,
                        begin: block_index * block_size,
                        block_size: if block_index == number_of_blocks - 1 && index == number_of_pieces - 1 {
                            length % block_size
                        } else {
                            block_size
                        },
                        block_state: BlockState::Missing
                    })
                    .collect();

                Arc::new(Mutex::new(Piece {
                    index,
                    length,
                    piece_hash: torrent.info.pieces[index * 20..(index + 1) * 20].to_vec(),
                    piece_state: PieceState::Missing,
                    blocks,
                    number_of_blocks
                }))
            })
            .collect();

        PiecePool {
            pieces,
            number_of_pieces,
        }
    }
    pub(crate) async fn get_free_piece(&self) -> Option<Arc<Mutex<Piece>>> {
        for piece in &self.pieces {
            let piece_guard = piece.lock().await;
            match piece_guard.piece_state {
                PieceState::Missing => return Some(piece.clone()),
                _ => continue,
            }
        }
        None
    }
}

mod tests {
    use std::fs;
    use crate::download::{Message};
    use crate::Torrent;
    
    #[test]
    fn test_encode() {
        let message = Message::Request { index: 0, begin: 0, length: 1 };
        let encoded = message.encode().expect("Couldn't encode message");
        assert_eq!(encoded, vec![0, 0, 0, 13, 6, 0, 0, 0, 0, 0, 0, 1]);
    }
    

}