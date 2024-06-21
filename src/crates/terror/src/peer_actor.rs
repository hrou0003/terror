use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use anyhow::anyhow;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::task;
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
    tasks_notify: Arc<Notify>,
    tasks_count: Arc<AtomicUsize>,
}

pub enum PeerActorState {
    Connected {
        reader: Arc<Mutex<OwnedReadHalf>>,
        writer: Arc<Mutex<OwnedWriteHalf>>,
    },
    Disconnected,
}

impl PeerActorState {
    fn new_connected(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();
        PeerActorState::Connected {
            reader: Arc::new(Mutex::new(reader)),
            writer: Arc::new(Mutex::new(writer)),
        }
    }

    fn get_reader(&self) -> Option<Arc<Mutex<OwnedReadHalf>>> {
        match self {
            PeerActorState::Connected { reader, .. } => Some(Arc::clone(reader)),
            PeerActorState::Disconnected => None,
        }
    }

    fn get_writer(&self) -> Option<Arc<Mutex<OwnedWriteHalf>>> {
        match self {
            PeerActorState::Connected { writer, .. } => Some(Arc::clone(writer)),
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
    pub fn new(ip_addr: IpAddr, port: u16, info_hash: [u8; 20], receiver: AsyncReceiver<PeerMessage>, task_queue: AsyncReceiver<DownloadBlock>, completed_task_tx: UnboundedSender<CompletedTask>, cycle_tx: Sender<crate::peer::CycleMessage>) -> Self {
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
        let writer = match self.peer_actor_state.get_writer() {
            Some(w) => w,
            None => return,
        };
        let tasks_count = Arc::clone(&self.tasks_count);
        let tasks_notify = Arc::clone(&self.tasks_notify);

        task::spawn(async move {
            while let Ok(task) = task_queue.recv().await {
                let writer = writer.clone();
                let message = Message::Request { index: task.piece_index as u32, begin: task.begin as u32, length: task.length as u32};
                if let Ok(_) = Message::send_message_write_half(message, writer).await {
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
        let reader = match self.peer_actor_state.get_reader() {
            Some(r) => r,
            None => return,
        };
        let tasks_count = Arc::clone(&self.tasks_count);
        let tasks_notify = Arc::clone(&self.tasks_notify);

        loop {
            if tasks_count.load(Ordering::SeqCst) == 0 {
                tasks_notify.notified().await;
            }

            let reader = reader.clone();
            let message = Message::read_message_write_half(reader).await;

            match message {
                Ok(Message::Piece { index, begin, block }) => {
                    completed_task_tx.send(CompletedTask::DownloadedBlock {
                        piece_index: index as usize,
                        block_index: (begin / (1 << 14)) as usize,
                        bytes: block,
                    }).unwrap();
                    tasks_count.fetch_sub(1, Ordering::SeqCst);
                },
                Ok(_) => {},
                Err(_) => {
                    completed_task_tx.send(CompletedTask::FailedBlock {
                        piece_index: 0,
                        block_index: 0,
                    }).unwrap();
                    tasks_count.fetch_sub(1, Ordering::SeqCst);
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
                println!("Peer {} is already connected", self.peer.id);
                Ok(())
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

                match Message::read_message(&mut stream).await {
                    Ok(Message::Unchoke) => {
                        self.peer_actor_state = PeerActorState::new_connected(stream);
                        Ok(())
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

    pub async fn get_stream(&mut self) -> anyhow::Result<()> {
        match &self.peer_actor_state {
            PeerActorState::Connected { .. } => Ok(()),
            PeerActorState::Disconnected => {
                println!("Peer is not connected");
                println!("Reconnecting");
                if let Ok(_) = self.connect().await {
                    println!("Reconnected");
                    Ok(())
                } else {
                    println!("Failed to reconnect");
                    println!("Broken peer");
                    Err(anyhow!("Broken peer"))
                }
            }
        }
    }
}

async fn run_peer_actor(mut actor: PeerActor) {
    while let Ok(msg) = actor.receiver.recv().await {
        if let Err(e) = actor.handle_message(msg).await {
            eprintln!("Error handling message: {}", e);
            if let PeerActorState::Connected { writer, .. } = &actor.peer_actor_state {
                let mut locked_writer = writer.lock().await;
                let _ = locked_writer.shutdown().await;
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