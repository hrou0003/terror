use std::net::IpAddr;
use std::sync::Arc;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, oneshot};
use tokio::sync::mpsc::Sender;
use crate::download::{CompletedTask, DownloadTask};
use crate::peer::{Peer};

pub struct PeerActor {
    peer: Peer,
    receiver: AsyncReceiver<PeerMessage>,
    completed_task_tx: Sender<CompletedTask>,
    cycle_tx: Sender<crate::peer::CycleMessage>,
    peer_actor_state: PeerActorState,
}

pub enum PeerActorState {
    Connected {    
        stream: Arc<Mutex<TcpStream>>,
    },
    Disconnected,
}

enum PeerMessage {
    Connect {
        respond_to: oneshot::Sender<()>,
    },
    DownloadTask(DownloadTask),
}

impl PeerActor {
    pub fn new(ip_addr: IpAddr, port: u16, info_hash: [u8; 20], receiver: AsyncReceiver<PeerMessage>, completed_task_tx: Sender<CompletedTask>, cycle_tx: Sender<crate::peer::CycleMessage>) -> Self {
        
        let peer = Peer::new(ip_addr, port, info_hash);
        
        PeerActor {
            peer,
            receiver,
            completed_task_tx: completed_task_tx.clone(),
            cycle_tx: cycle_tx.clone(),
            peer_actor_state: PeerActorState::Disconnected,
        }
    }
    async fn handle_message(&mut self, msg: PeerMessage) {
        match msg {
            PeerMessage::Connect { respond_to } => {
                let _ = self.connect().await;
                let _ = respond_to.send(());
            },
            PeerMessage::DownloadTask(task) => {
                
                // Check that the peer is connected
                let stream = match &self.peer_actor_state {
                    PeerActorState::Connected { stream } => stream.clone(),
                    PeerActorState::Disconnected => {
                        println!("Peer is not connected");
                        return;
                    }
                };
                    
                // download the block
                Peer::download_block_from_stream(stream, task.piece_index as u32, task.begin as u32, task.length as u32).await.unwrap();

                self.completed_task_tx.send(CompletedTask {
                    piece_index: task.piece_index,
                    block_index: task.block_index,
                    bytes: vec![],
                    status: Ok(()),
                }).await.unwrap();
            }
        }
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        match self.peer_actor_state {
            PeerActorState::Connected { .. } => {
                println!("Peer {} is already connected", self.peer.id);
                return Ok(());
            }
            PeerActorState::Disconnected => {
                self.peer_actor_state = PeerActorState::Connected {
                    stream: Arc::new(Mutex::new(TcpStream::connect((self.peer.ip_addr, self.peer.port)).await?)),
                };
                println!("Connecting to peer {}", self.peer.id);
            }
        }
        
        Ok(())
    }

}

async fn run_peer_actor(mut actor: PeerActor) {
    while let Ok(msg) = actor.receiver.recv().await {
        actor.handle_message(msg);
    }
}


pub struct PeerActorHandle {
    sender: AsyncSender<PeerMessage>,
}

impl PeerActorHandle {
    pub fn new(ip_addr: IpAddr, port: u16, info_hash: [u8; 20], completed_task_tx: Sender<CompletedTask>, cycle_tx: Sender<crate::peer::CycleMessage>) -> Self {
        let (sender, receiver) = kanal::bounded_async(8);
        let actor = PeerActor::new(ip_addr, port, info_hash, receiver, completed_task_tx, cycle_tx);
        tokio::spawn(run_peer_actor(actor));
        
        Self { sender }
    }
}


