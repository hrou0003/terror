use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};

use crate::torrent::Torrent;

use crate::{client::handshake::Handshake, client::tracking::get_peers};

#[derive(PartialEq, Debug)]
pub enum Message {
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
    pub async fn download_piece(torrent: &Torrent, piece_index: usize) -> anyhow::Result<()> {

        let peers = get_peers(torrent).await?;

        let mut peer = peers.get(0).expect("Invalid peers").to_owned();

        let peer_address = peer.to_string();

        let mut stream = tokio::net::TcpStream::connect(peer_address).await?;

        let handshake = Handshake::do_handshake(torrent, &mut stream).await?;

        let message = Self::read_message(&mut stream).await?;

        if message == Message::Bitfield {
            let request = Message::Interested;
            Self::send_message(request, &mut stream).await?;
        }

        let unchoke = match Self::read_message(&mut stream).await? {
            Message::Unchoke => Ok(Message::Unchoke),
            _ => Err(anyhow::anyhow!("Didn't unchoke"))
        };

        Ok(())
    }

    async fn send_message(message: Message, stream: &mut tokio::net::TcpStream) -> anyhow::Result<()> {
        let payload = Self::encode(&message)?;

        stream.write(&payload).await?;

        return Ok(());
    }


    async fn read_message(stream: &mut tokio::net::TcpStream) -> anyhow::Result<Message> {
        let mut length_bytes = [0; 4];

        stream.read_exact(&mut length_bytes).await?;

        while u32::from_be_bytes(length_bytes) == 0 {
            stream.read_exact(&mut length_bytes).await?;
        }
        
        let message_type = stream.read_u8().await?;
        
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
                Ok(Message::Request { index: index, begin: begin, length: length })
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
