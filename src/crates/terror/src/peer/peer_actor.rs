use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use anyhow::anyhow;
use futures_util::{SinkExt, StreamExt};
use kanal::{AsyncReceiver, AsyncSender};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::task;
use tokio_util::codec::Framed;
use tracing::debug;
use crate::peer::handshake::Handshake;
use crate::peer::peer::{CycleMessage, Peer};
use crate::tcp::message::Message;
use crate::tcp::bittorrent_code::BitTorrentCodec;
use crate::torrent::torrent_downloader::{CompletedTask, DownloadBlock};

pub struct PeerActor {
    peer: Peer,
    receiver: AsyncReceiver<PeerMessage>,
    task_queue: AsyncReceiver<DownloadBlock>,
    completed_task_tx: UnboundedSender<CompletedTask>,
    cycle_tx: Sender<CycleMessage>,
    peer_actor_state: PeerActorState,
    tasks_notify: Arc<Notify>,
    tasks_count: Arc<AtomicUsize>,
}

pub enum PeerActorState {
    Connected {
        framed: Arc<Mutex<Framed<TcpStream, BitTorrentCodec>>>,
    },
    Disconnected,
}

impl PeerActorState {
    fn new_connected(stream: TcpStream) -> Self {
        let framed = Framed::new(stream, BitTorrentCodec);
        PeerActorState::Connected {
            framed: Arc::new(Mutex::new(framed)),
        }
    }

    fn get_framed(&self) -> Option<Arc<Mutex<Framed<TcpStream, BitTorrentCodec>>>> {
        match self {
            PeerActorState::Connected { framed } => Some(Arc::clone(framed)),
            PeerActorState::Disconnected => None,
        }
    }
}

pub enum PeerMessage {
    Connect {
        respond_to: oneshot::Sender<()>,
    },
    Listen,
}

impl PeerActor {
    pub fn new(ip_addr: IpAddr, port: u16, info_hash: [u8; 20], receiver: AsyncReceiver<PeerMessage>, task_queue: AsyncReceiver<DownloadBlock>, completed_task_tx: UnboundedSender<CompletedTask>, cycle_tx: Sender<CycleMessage>) -> Self {
        let peer = Peer::new(ip_addr, port, info_hash);

        PeerActor {
            peer,
            receiver,
            task_queue,
            completed_task_tx: completed_task_tx.clone(),
            cycle_tx: cycle_tx.clone(),
            peer_actor_state: PeerActorState::Disconnected,
            tasks_notify: Arc::new(Notify::new()),
            tasks_count: Arc::new(AtomicUsize::new(0)),        }
    }

    pub async fn send_messages(&self) {
        let task_queue = self.task_queue.clone();
        let framed = match self.peer_actor_state.get_framed() {
            Some(f) => f,
            None => return,
        };
        let tasks_count = Arc::clone(&self.tasks_count);
        let tasks_notify = Arc::clone(&self.tasks_notify);

        task::spawn(async move {
            while let Ok(task) = task_queue.recv().await {
                let mut framed = framed.lock().await;
                let message = Message::Request {
                    index: task.piece_index as u32,
                    begin: task.begin as u32,
                    length: task.length as u32
                };
                if framed.send(message).await.is_ok() {
                    tasks_count.fetch_add(1, Ordering::SeqCst);
                }
                if tasks_count.load(Ordering::SeqCst) > 5 {
                    tasks_notify.notify_one();
                }
            }
        });
    }


    pub async fn read_messages(&self) {
        let completed_task_tx = self.completed_task_tx.clone();
        let framed = match self.peer_actor_state.get_framed() {
            Some(f) => f,
            None => return,
        };
        let tasks_count = Arc::clone(&self.tasks_count);
        let tasks_notify = Arc::clone(&self.tasks_notify);

        loop {
            if tasks_count.load(Ordering::SeqCst) == 0 {
                tasks_notify.notified().await;
            }

            let mut framed = framed.lock().await;
            let message = framed.next().await;

            match message {
                Some(Ok(Message::Piece { index, begin, block })) => {
                    completed_task_tx.send(CompletedTask::DownloadedBlock {
                        piece_index: index as usize,
                        block_index: (begin / (1 << 14)) as usize,
                        bytes: block,
                    }).unwrap();
                    tasks_count.fetch_sub(1, Ordering::SeqCst);
                },
                Some(Ok(_)) => {},
                Some(Err(_)) | None => {
                    completed_task_tx.send(CompletedTask::FailedBlock {
                        piece_index: 0,
                        block_index: 0,
                    }).unwrap();
                    tasks_count.fetch_sub(1, Ordering::SeqCst);
                    break;  // Exit the loop on error or end of stream
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: PeerMessage) -> anyhow::Result<()> {
        match msg {
            PeerMessage::Connect { respond_to } => {
                let _ = respond_to.send(());
            },
            PeerMessage::Listen => {
                let _ = self.connect().await;
                self.send_messages().await;
                self.read_messages().await;
                while let Ok(_) = self.task_queue.recv().await {
                    self.get_stream().await?;
                }
            }
        }
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        match &self.peer_actor_state {
            PeerActorState::Connected { .. } => {
                debug!("Peer {} is already connected", self.peer.id);
                Ok(())
            }
            PeerActorState::Disconnected => {
                let mut stream = TcpStream::connect((self.peer.ip_addr, self.peer.port)).await?;

                // Perform handshake manually
                let mut handshake = Handshake::new(self.peer.info_hash, *b"00112233445566778899");
                let handshake_bytes = handshake.to_bytes();
                stream.write_all(&handshake_bytes).await?;

                let received_handshake = Handshake::from_stream(&mut stream).await?;
                debug!("Handshake completed on: {}", hex::encode(received_handshake.peer_id));

                // Switch to BitTorrent codec
                let mut framed = Framed::new(stream, BitTorrentCodec);

                // Expect bitfield
                match framed.next().await {
                    Some(Ok(Message::Bitfield { .. })) => {
                        framed.send(Message::Interested).await?;
                    },
                    _ => return Err(anyhow!("Didn't receive bitfield")),
                }

                // Expect unchoke
                match framed.next().await {
                    Some(Ok(Message::Unchoke)) => {
                        self.peer_actor_state = PeerActorState::Connected { framed: Arc::new(Mutex::new(framed)) };
                        Ok(())
                    },
                    Some(Ok(_)) => Err(anyhow!("Unexpected message")),
                    Some(Err(e)) => {
                        debug!("Error reading message: {}", e);
                        Err(anyhow!("Couldn't connect"))
                    },
                    None => Err(anyhow!("Connection closed unexpectedly")),
                }
            }
        }
    }

    pub async fn get_stream(&mut self) -> anyhow::Result<()> {
        match &self.peer_actor_state {
            PeerActorState::Connected { .. } => Ok(()),
            PeerActorState::Disconnected => {
                debug!("Peer is not connected");
                debug!("Reconnecting");
                if let Ok(_) = self.connect().await {
                    debug!("Reconnected");
                    Ok(())
                } else {
                    debug!("Failed to reconnect");
                    debug!("Broken peer");
                    Err(anyhow!("Broken peer"))
                }
            }
        }
    }
}

async fn run_peer_actor(mut actor: PeerActor) {
    while let Ok(msg) = actor.receiver.recv().await {
        if let Err(e) = actor.handle_message(msg).await {
            debug!("Error handling message: {}", e);
            break;
        }
    }
}

pub struct PeerActorHandle {
    pub(crate) sender: AsyncSender<PeerMessage>,
}

impl PeerActorHandle {
    pub fn new(ip_addr: IpAddr, port: u16, info_hash: [u8; 20], task_queue: AsyncReceiver<DownloadBlock>, completed_task_tx: UnboundedSender<CompletedTask>, cycle_tx: Sender<CycleMessage>) -> Self {
        let (sender, receiver) = kanal::bounded_async(8);
        let actor = PeerActor::new(ip_addr, port, info_hash, receiver, task_queue, completed_task_tx, cycle_tx);
        tokio::spawn(run_peer_actor(actor));

        Self { sender }
    }
}