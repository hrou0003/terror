use std::net::IpAddr;
use std::sync::Arc;
use anyhow::anyhow;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, oneshot};
use tokio::sync::mpsc::{Sender, UnboundedSender};
use crate::peer::{Peer};
use crate::torrent_manager::{CompletedTask, DownloadBlock};
use crate::handshake::Handshake;
use crate::message::Message;

pub struct PeerActor {
    peer: Peer,
    receiver: AsyncReceiver<PeerMessage>,
    task_queue: AsyncReceiver<DownloadBlock>,
    completed_task_tx: UnboundedSender<CompletedTask>,
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
    pub fn new(ip_addr: IpAddr, port: u16, info_hash: [u8; 20], receiver: AsyncReceiver<PeerMessage>, task_queue: AsyncReceiver<DownloadBlock>, completed_task_tx: UnboundedSender<CompletedTask>, cycle_tx: Sender<crate::peer::CycleMessage>) -> Self {
        
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
    async fn handle_message(&mut self, msg: PeerMessage) -> anyhow::Result<()> {
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
                                println!("Reconnected");
                               stream 
                            } else {
                                println!("Failed to reconnect");
                                // Definitely need to do this otherwise the task queue won't know to put in more work
                                self.completed_task_tx.send(CompletedTask::FailedBlock {
                                    piece_index: task.piece_index,
                                    block_index: task.block_index,
                                }).unwrap();
                                println!("Broken peer");
                                return Err(anyhow!("Broken peer"));
                            }
                        }
                    };

                    // download the block
                    eprintln!("Downloading Block {} of Piece {} on Peer {}", task.block_index, task., self.peer.ip_addr);
                    let block= Peer::download_block_from_stream(stream, task.piece_index as u32, task.begin as u32, task.length as u32).await;
                    
                    match block {
                        Ok(block_byets) => {
                            self.completed_task_tx.send(CompletedTask::DownloadedBlock {
                                piece_index: task.piece_index,
                                block_index: task.block_index,
                                bytes: block_byets,
                            }).unwrap();
                        },
                        Err(_) => {
                            self.completed_task_tx.send(CompletedTask::FailedBlock {
                                piece_index: 0,
                                block_index: 0,
                            }).unwrap();                        
                        }
                    }
                    

                }
            }
        }
        return Ok(());
    }

    async fn connect(&mut self) -> anyhow::Result<Arc<Mutex<TcpStream>>> {
        match &self.peer_actor_state {
            PeerActorState::Connected { stream } => {
                println!("Peer {} is already connected", self.peer.id);
                Ok(stream.clone())
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
                
                // Need to time this out and give up on connecting to this peer DON'T UNWRAP
                match Message::read_message(&mut stream).await {
                    Ok(Message::Unchoke) => {
                        let stream = Arc::new(Mutex::new(stream));
                        self.peer_actor_state = PeerActorState::Connected {
                            stream: stream.clone(),
                        };
                        Ok(stream.clone())
                    },
                    Ok(_) => Err(anyhow::anyhow!("Unexpected message")),
                    Err(e) => {
                        eprintln!("Error reading message: {}", e);
                        Err(anyhow::anyhow!("Couldn't connect"))
                    }
                }
            }
        }
        
    }

}

async fn run_peer_actor(mut actor: PeerActor) {
    while let Ok(msg) = actor.receiver.recv().await {
        if let Err(e) = actor.handle_message(msg).await {
            eprintln!("Error handling message: {}", e);
            // Clean up resources before exiting
            match &actor.peer_actor_state {
                PeerActorState::Connected { stream } => {
                    let mut locked_stream = stream.lock().await;
                    let _ = locked_stream.shutdown().await;
                }
                PeerActorState::Disconnected => {}
            }
            break;
        }
    }
}


pub struct PeerActorHandle {
    pub(crate) sender: AsyncSender<PeerMessage>,
}

impl PeerActorHandle {
    pub fn new(ip_addr: IpAddr, port: u16, info_hash: [u8; 20], task_queue: AsyncReceiver<DownloadBlock>, completed_task_tx: UnboundedSender<CompletedTask>, cycle_tx: Sender<crate::peer::CycleMessage>) -> Self {
        let (sender, receiver) = kanal::bounded_async(8);
        let actor = PeerActor::new(ip_addr, port, info_hash, receiver, task_queue, completed_task_tx, cycle_tx);
        tokio::spawn(run_peer_actor(actor));
        
        Self { sender }
    }
}
