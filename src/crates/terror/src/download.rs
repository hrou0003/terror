use std::sync::Arc;
use std::time::Instant;
use actix::prelude::*;
use tokio::sync::RwLock;
use crate::peer::{Peer, PeerPool};
use crate::piece::{Block, Piece, PiecePool};
use anyhow::Result;


struct PeerActor {
    peer: Arc<RwLock<Peer>>,
}
impl Actor for PeerActor {
    type Context = Context<Self>;
}

impl Supervised for PeerActor {
}

impl Default for PeerActor {
    fn default() -> Self {
        todo!()
    }
}// Manually implement SystemService but leave Default out
impl SystemService for PeerActor {
    fn service_started(&mut self, _ctx: &mut Context<Self>) {
        println!("PeerActor is started");
    }
}

struct BlockMessage {
    block: Arc<RwLock<Block>>,
}

impl Message for BlockMessage {
    type Result = Result<()>;
}
impl Handler<BlockMessage> for PeerPool {
    type Result = ResponseActFuture<Self, Result<()>>;

    fn handle(&mut self, msg: BlockMessage, _: &mut Self::Context) -> Self::Result {
        let block = msg.block.clone();
        let peer = self.peers.values().next().unwrap().clone();

       let fut = async move {
            // Extract start time before async operation
            let start_time = Instant::now();

            let success = Peer::download_block(peer, block).await;

            // Calculate duration
            let duration = start_time.elapsed();
            
            Ok(())
        }.into_actor(self);
        
        Box::pin(fut)
    }
}

impl Handler<BlockMessage> for PeerActor {
    type Result = ResponseActFuture<Self, Result<()>>;

    fn handle(&mut self, msg: BlockMessage, _: &mut Self::Context) -> Self::Result {
        let block = msg.block.clone();
        let peer = self.peer.clone();

        let fut = async move {
            // Extract start time before async operation
            let start_time = Instant::now();

            let success = Peer::download_block(peer, block).await;

            // Calculate duration
            let duration = start_time.elapsed();

            Ok(())
        }.into_actor(self);

        Box::pin(fut)
    }
}

struct PieceMessage {
    piece: Arc<RwLock<Piece>>,
}

impl Message for PieceMessage {
    type Result = Result<()>;
}

impl Actor for PiecePool {
    type Context = Context<Self>;
}

impl Handler<PieceMessage> for PiecePool {
    type Result = ResponseActFuture<Self, Result<()>>;
    
    fn handle(&mut self, msg: PieceMessage, _: &mut Self::Context) -> Self::Result {
        let piece = msg.piece.clone();
        let fut = async move {
            let mut piece = piece.write().await;
            for block in &mut piece.blocks {
                let block_message = BlockMessage {
                    block: block.clone(),
                };
                let _ = PeerPool::from_registry().send(block_message).await?;
            }
            Ok(())
        }.into_actor(self);
        
        Box::pin(fut)
    }
}

impl Actor for PeerPool {
    type Context = Context<Self>;
}

impl Supervised for PeerPool {
}

impl Default for PeerPool {
    fn default() -> Self {
        todo!()
    }
}

impl SystemService for PeerPool {
    fn service_started(&mut self, _ctx: &mut Context<Self>) {
        println!("PeerPool is started");
    }
}

#[cfg(test)]
mod tests {
    use crate::peer::{PeerPool};
    use crate::piece::BlockState;
    use super::*;
    use crate::Torrent;

    #[actix::test]
    async fn test_peer_actor() {
        let torrent = Torrent::new("test/sample.torrent".to_string());
        let peer_pool = PeerPool::new(&torrent).await.unwrap();
        
        let first_peer_id = peer_pool.peers.keys().nth(1).unwrap().clone();
        let (_, peer) = peer_pool.peers.get_key_value::<String>(&first_peer_id).unwrap();
        
        let peer_actor = PeerActor {
            peer: peer.clone(),
        }.start();
        
        let block = Block {
            index: 0,
            begin: 0,
            block_size: 1 << 14,
            block_state: BlockState::Missing,
        };
        
        let block_message = BlockMessage {
            block: Arc::new(RwLock::new(block)),
        };
        
        let result = peer_actor.send(block_message).await;
        
        assert!(result.is_ok());
        
    }
    
    #[actix::test]
    async fn test_piece_actor() {
        let torrent = Torrent::new("test/sample.torrent".to_string());
        let mut piece_pool = PiecePool::new(&torrent).unwrap();
        
        let first_piece = piece_pool.pieces.keys().nth(0).unwrap().clone();
        let (_, piece) = piece_pool.pieces.get_key_value::<usize>(&first_piece).unwrap();
        
        let piece_message = PieceMessage {
            piece: piece.clone(),
        };
        
        // let result = piece_pool.send(piece_message).await;
        
        // assert!(result.is_ok());
    }
}
    