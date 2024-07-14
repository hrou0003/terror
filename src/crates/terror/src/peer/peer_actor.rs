use crate::metrics::metrics::PeerMetrics;
use crate::peer::handshake::Handshake;
use crate::peer::peer::{CycleMessage, Peer};
use crate::tcp::bittorrent_code::BitTorrentCodec;
use crate::tcp::message::Message;
use crate::torrent::torrent_downloader::{CompletedTask, DownloadBlock};
use anyhow::anyhow;
use futures_util::{SinkExt, StreamExt};
use kanal::{AsyncReceiver, AsyncSender, Receiver};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::sync::{oneshot, Mutex, Notify};
use tokio::task;
use tokio_util::codec::Framed;
use tracing::debug;

pub struct PeerActor {
    peer: Peer,
    pub(crate) receiver: AsyncReceiver<PeerMessage>,
    completed_task_tx: AsyncSender<CompletedTask>,
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
    Control(ControlMessage),
    Data(DataMessage),
}

pub enum ControlMessage {
    Connect { respond_to: oneshot::Sender<()> },
    Listen,
    Pause,
}

pub enum DataMessage {
    Piece {
        index: usize,
        begin: usize,
        length: usize,
    },
}

impl PeerActor {
    pub fn new(
        peer_id: String,
        ip_addr: IpAddr,
        port: u16,
        info_hash: [u8; 20],
        receiver: AsyncReceiver<PeerMessage>,
        completed_task_tx: AsyncSender<CompletedTask>,
    ) -> Self {
        let peer = Peer::new(peer_id, ip_addr, port, info_hash);

        PeerActor {
            peer,
            receiver,
            completed_task_tx: completed_task_tx.clone(),
            peer_actor_state: PeerActorState::Disconnected,
            tasks_notify: Arc::new(Notify::new()),
            tasks_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn send_message(&self, index: usize, begin: usize, length: usize) {
        debug!("attempting to send message");
        let framed = match self.peer_actor_state.get_framed() {
            Some(f) => f,
            None => return,
        };
        debug!("stuck here?");
        let tasks_count = Arc::clone(&self.tasks_count);
        let tasks_notify = Arc::clone(&self.tasks_notify);

        let mut framed = framed.lock().await;

        let message = Message::Request {
            index,
            begin,
            length,
        };
        if framed.send(message).await.is_ok() {
            tasks_count.fetch_add(1, Ordering::SeqCst);
            debug!("send message to peer");
        }
        if tasks_count.load(Ordering::SeqCst) > 5 {
            tasks_notify.notify_one();
        }
    }

    pub async fn read_messages(&self) {
        debug!("read messages");
        let completed_task_tx = self.completed_task_tx.clone();
        let framed = match self.peer_actor_state.get_framed() {
            Some(f) => f,
            None => return,
        };
        let tasks_count = Arc::clone(&self.tasks_count);
        let tasks_notify = Arc::clone(&self.tasks_notify);
        let peer_id = self.peer.id.clone();

        task::spawn(async move {
            loop {
                if tasks_count.load(Ordering::SeqCst) == 0 {
                    tasks_notify.notified().await;
                }

                debug!("am I stuck in the read messages?");
                let mut framed = framed.lock().await;
                let message = framed.next().await;

                let peer_id = peer_id.clone();

                match message {
                    Some(Ok(Message::Piece {
                        index,
                        begin,
                        block,
                    })) => {
                        debug!("received piece message from tcp client and sending for forwarding by peer pool");
                        completed_task_tx
                            .send(CompletedTask::DownloadedBlock {
                                peer_id: peer_id.clone(),
                                download_time: Instant::now(),
                                piece_index: index,
                                block_index: (begin / (1 << 14)),
                                bytes: block,
                            })
                            .await
                            .unwrap();
                        tasks_count.fetch_sub(1, Ordering::SeqCst);
                        debug!("am I getting stuck here??");
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => {
                        completed_task_tx
                            .send(CompletedTask::FailedBlock {
                                peer_id: peer_id,
                                piece_index: 0,
                                block_index: 0,
                            })
                            .await
                            .unwrap();
                        tasks_count.fetch_sub(1, Ordering::SeqCst);
                        debug!("failed message from tcp client");
                        break;
                    }
                }
            }
        });
    }

    pub(crate) async fn handle_message(&mut self, msg: PeerMessage) -> anyhow::Result<()> {
        match msg {
            PeerMessage::Control(control) => match control {
                ControlMessage::Connect { respond_to } => {
                    let _ = respond_to.send(());
                }
                ControlMessage::Listen => {
                    let _ = self.connect().await;
                    self.read_messages().await;
                }
                ControlMessage::Pause => {}
            },
            PeerMessage::Data(data) => match data {
                DataMessage::Piece {
                    index,
                    begin,
                    length,
                } => {
                    debug!("Received data piece message");
                    self.send_message(index, begin, length).await
                }
            },
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
                debug!(
                    "Handshake completed on: {}",
                    hex::encode(received_handshake.peer_id)
                );

                // Switch to BitTorrent codec
                let mut framed = Framed::new(stream, BitTorrentCodec);

                // Expect bitfield
                match framed.next().await {
                    Some(Ok(Message::Bitfield { .. })) => {
                        framed.send(Message::Interested).await?;
                    }
                    _ => return Err(anyhow!("Didn't receive bitfield")),
                }

                // Expect unchoke
                match framed.next().await {
                    Some(Ok(Message::Unchoke)) => {
                        self.peer_actor_state = PeerActorState::Connected {
                            framed: Arc::new(Mutex::new(framed)),
                        };
                        Ok(())
                    }
                    Some(Ok(_)) => Err(anyhow!("Unexpected message")),
                    Some(Err(e)) => {
                        debug!("Error reading message: {}", e);
                        Err(anyhow!("Couldn't connect"))
                    }
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
