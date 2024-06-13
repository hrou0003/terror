use std::net::IpAddr;
use std::sync::Arc;
use anyhow::anyhow;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, oneshot};
use tokio::sync::mpsc::Sender;
use crate::peer::{Peer};
use crate::torrent_manager::{CompletedTask, DownloadBlock};
use crate::handshake::Handshake;
use crate::message::Message;

pub struct PeerActor {
    peer: Peer,
    receiver: AsyncReceiver<PeerMessage>,
    task_queue: AsyncReceiver<DownloadBlock>,
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

pub enum PeerMessage {
    Connect {
        respond_to: oneshot::Sender<()>,
    },
    Listen,
}

impl PeerActor {
    pub fn new(ip_addr: IpAddr, port: u16, info_hash: [u8; 20], receiver: AsyncReceiver<PeerMessage>, task_queue: AsyncReceiver<DownloadBlock>, completed_task_tx: Sender<CompletedTask>, cycle_tx: Sender<crate::peer::CycleMessage>) -> Self {
        
        let peer = Peer::new(ip_addr, port, info_hash);
        
        PeerActor {
            peer,
            receiver,
            task_queue,
            completed_task_tx: completed_task_tx.clone(),
            cycle_tx: cycle_tx.clone(),
            peer_actor_state: PeerActorState::Disconnected,
        }
    }
    async fn handle_message(&mut self, msg: PeerMessage) {
        match msg {
            PeerMessage::Connect { respond_to } => {
                let _ = respond_to.send(());
            },
            PeerMessage::Listen => {
                let _ = self.connect().await;
                while let Ok(task) = self.task_queue.recv().await {

                    // Check that the peer is connected
                    let stream = match &self.peer_actor_state {
                        PeerActorState::Connected { stream } => stream.clone(),
                        PeerActorState::Disconnected => {
                            println!("Peer is not connected");
                            println!("Reconnecting");
                            if let Ok(stream) = self.connect().await {
                               stream 
                            } else {
                                return;
                            }
                        }
                    };

                    // download the block
                    eprintln!("Downloading Block {} of Piece {} on Peer {}", task.piece_index, task.block_index, self.peer.ip_addr);
                    let block= Peer::download_block_from_stream(stream, task.piece_index as u32, task.begin as u32, task.length as u32).await;
                    
                    match block {
                        Ok(block_byets) => {
                            self.completed_task_tx.send(CompletedTask::DownloadedBlock {
                                piece_index: task.piece_index,
                                block_index: task.block_index,
                                bytes: block_byets,
                            }).await.unwrap();
                        },
                        Err(_) => {
                            self.completed_task_tx.send(CompletedTask::FailedBlock {
                                piece_index: 0,
                                block_index: 0,
                            }).await.unwrap();                        
                        }
                    }

                }
            }
        }
    }

    async fn connect(&mut self) -> anyhow::Result<Arc<Mutex<TcpStream>>> {
        match &self.peer_actor_state {
            PeerActorState::Connected { stream } => {
                println!("Peer {} is already connected", self.peer.id);
                return Ok(stream.clone());
            }
            PeerActorState::Disconnected => {
                let mut stream = TcpStream::connect((self.peer.ip_addr, self.peer.port)).await?;
                let _ = Handshake::handshake(self.peer.info_hash, &mut stream).await;
                match Message::read_message(&mut stream).await? {
                    Message::Bitfield { .. } => {
                        let request = Message::Interested;
                        Message::send_message(request, &mut stream).await?;
                    },
                    _ => return Err(anyhow::anyhow!("Didn't receive bitfield")),
                }
                if Message::read_message(&mut stream).await? == Message::Unchoke {
                    let stream = Arc::new(Mutex::new(stream));
                    self.peer_actor_state = PeerActorState::Connected {
                        // Perform handshake
                        stream: stream.clone(),
                    };
                    return Ok(stream.clone());
                } else {
                    return Err(anyhow::anyhow!("Couldn't connect"));
                }
                
            }
            _ => return Err(anyhow!("Peer has been closed intentionally"))
        }
        
    }

}

async fn run_peer_actor(mut actor: PeerActor) {
    while let Ok(msg) = actor.receiver.recv().await {
        actor.handle_message(msg).await;

    }
}


pub struct PeerActorHandle {
    pub(crate) sender: AsyncSender<PeerMessage>,
}

impl PeerActorHandle {
    pub fn new(ip_addr: IpAddr, port: u16, info_hash: [u8; 20], task_queue: AsyncReceiver<DownloadBlock>, completed_task_tx: Sender<CompletedTask>, cycle_tx: Sender<crate::peer::CycleMessage>) -> Self {
        let (sender, receiver) = kanal::bounded_async(8);
        let actor = PeerActor::new(ip_addr, port, info_hash, receiver, task_queue, completed_task_tx, cycle_tx);
        tokio::spawn(run_peer_actor(actor));
        
        Self { sender }
    }
}
