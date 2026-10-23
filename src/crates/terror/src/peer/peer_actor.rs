use crate::peer::handshake::Handshake;
use crate::peer::peer::Peer;
use crate::tcp::bittorrent_code::BitTorrentCodec;
use crate::tcp::message::Message;
use crate::torrent::torrent_downloader::{CompletedTask, DownloadBlock};
use anyhow::{anyhow, bail, Context};
use futures_util::{future::poll_fn, Sink, SinkExt, Stream, StreamExt};
use std::pin::Pin as Pinned;
use std::task::Poll;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

pub(crate) enum PeerEvent {
    Ready(usize),
    Completed {
        actor_id: usize,
        task: CompletedTask,
    },
}

struct PendingRequest {
    block: DownloadBlock,
    sent: bool,
}

pub(crate) struct PeerActor {
    peer: Peer,
    actor_id: usize,
    receiver: mpsc::Receiver<DownloadBlock>,
    events: mpsc::UnboundedSender<PeerEvent>,
    pending: Vec<PendingRequest>,
}

impl PeerActor {
    pub(crate) fn new(
        peer: Peer,
        actor_id: usize,
        receiver: mpsc::Receiver<DownloadBlock>,
        events: mpsc::UnboundedSender<PeerEvent>,
    ) -> Self {
        Self {
            peer,
            actor_id,
            receiver,
            events,
            pending: Vec::new(),
        }
    }

    pub(crate) async fn run(mut self, mut shutdown: oneshot::Receiver<()>) -> anyhow::Result<()> {
        // Cancellation also covers TCP connect, handshake, and initial protocol reads.
        // Closing an idle mailbox must not wait for a remote peer to answer.
        let connection = Self::connect(&self.peer);
        tokio::pin!(connection);
        let mut framed = loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(()),
                block = self.receiver.recv() => match block {
                    Some(block) => self.pending.push(PendingRequest { block, sent: false }),
                    None => return Ok(()),
                },
                connected = &mut connection => break connected?,
            }
        };
        self.events
            .send(PeerEvent::Ready(self.actor_id))
            .map_err(|_| anyhow!("Peer pool event channel closed"))?;

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(()),
                block = self.receiver.recv() => match block {
                    Some(block) => self.pending.push(PendingRequest { block, sent: false }),
                    None => return Ok(()),
                },
                message = poll_fn(|cx| {
                    // Poll both directions on the same Framed. A pending read never holds
                    // a socket lock, and write backpressure never prevents reading replies.
                    for pending in self.pending.iter_mut().filter(|pending| !pending.sent) {
                        match Pinned::new(&mut framed).poll_ready(cx) {
                            Poll::Ready(Ok(())) => {
                                let block = pending.block;
                                if let Err(error) = Pinned::new(&mut framed).start_send(Message::Request {
                                    index: block.piece_index, begin: block.begin, length: block.length,
                                }) {
                                    return Poll::Ready(Some(Err(error)));
                                }
                                pending.sent = true;
                            }
                            Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                            Poll::Pending => break,
                        }
                    }
                    if let Poll::Ready(Err(error)) = Pinned::new(&mut framed).poll_flush(cx) {
                        return Poll::Ready(Some(Err(error)));
                    }
                    Pinned::new(&mut framed).poll_next(cx)
                }) => match message {
                    Some(Ok(Message::Piece { index, begin, block })) => {
                        let position = self.pending.iter().position(|pending| {
                            pending.sent && pending.block.piece_index == index && pending.block.begin == begin
                        }).ok_or_else(|| anyhow!("Peer {} returned an unrequested block {index}:{begin}", self.peer.id))?;
                        if block.len() != self.pending[position].block.length {
                            bail!("Peer {} returned an incorrect block length", self.peer.id);
                        }
                        let request = self.pending.swap_remove(position).block;
                        self.events.send(PeerEvent::Completed {
                            actor_id: self.actor_id,
                            task: CompletedTask::DownloadedBlock {
                                piece_index: request.piece_index,
                                block_index: request.block_index,
                                bytes: block,
                            },
                        }).map_err(|_| anyhow!("Peer pool event channel closed"))?;
                    }
                    Some(Ok(_)) => {},
                    Some(Err(error)) => return Err(error).context("Reading peer message"),
                    None => bail!("Peer {} disconnected", self.peer.id),
                }
            }
        }
    }

    async fn connect(peer: &Peer) -> anyhow::Result<Framed<TcpStream, BitTorrentCodec>> {
        let mut stream = TcpStream::connect((peer.ip_addr, peer.port))
            .await
            .with_context(|| format!("Connecting to peer {}", peer.id))?;
        let mut handshake = Handshake::new(peer.info_hash, *b"00112233445566778899");
        stream.write_all(&handshake.to_bytes()).await?;
        Handshake::from_stream(&mut stream).await?;
        let mut framed = Framed::new(stream, BitTorrentCodec);
        match framed.next().await.transpose()? {
            Some(Message::Bitfield(_)) => framed.send(Message::Interested).await?,
            _ => bail!("Peer {} did not send a bitfield", peer.id),
        }
        match framed.next().await.transpose()? {
            Some(Message::Unchoke) => Ok(framed),
            _ => bail!("Peer {} did not unchoke the connection", peer.id),
        }
    }

    fn fail(&self, block: DownloadBlock) {
        let _ = self.events.send(PeerEvent::Completed {
            actor_id: self.actor_id,
            task: CompletedTask::FailedBlock {
                piece_index: block.piece_index,
                block_index: block.block_index,
            },
        });
    }
}

impl Drop for PeerActor {
    fn drop(&mut self) {
        // Close before draining so no sender can enqueue a request after this drain.
        self.receiver.close();
        for request in &self.pending {
            self.fail(request.block);
        }
        while let Ok(block) = self.receiver.try_recv() {
            self.fail(block);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::peer_actor_handle::PeerActorHandle;
    use bytes::Bytes;
    use std::future::Future;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    async fn bounded<T>(future: impl Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .expect("local peer timed out")
    }

    fn peer(listener: &TcpListener) -> Peer {
        let address = listener.local_addr().unwrap();
        Peer::new("local-peer".into(), address.ip(), address.port(), [3; 20])
    }

    async fn accept_ready(listener: &TcpListener) -> Framed<TcpStream, BitTorrentCodec> {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut handshake = [0; 68];
        stream.read_exact(&mut handshake).await.unwrap();
        stream.write_all(&handshake).await.unwrap();
        let mut framed = Framed::new(stream, BitTorrentCodec);
        framed
            .send(Message::Bitfield(Bytes::from_static(&[0xff])))
            .await
            .unwrap();
        assert_eq!(framed.next().await.unwrap().unwrap(), Message::Interested);
        framed.send(Message::Unchoke).await.unwrap();
        framed
    }

    async fn connected() -> (
        PeerActorHandle,
        Framed<TcpStream, BitTorrentCodec>,
        mpsc::UnboundedReceiver<PeerEvent>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (events_tx, mut events) = mpsc::unbounded_channel();
        let handle = PeerActorHandle::new(peer(&listener), 4, events_tx);
        let framed = accept_ready(&listener).await;
        assert!(matches!(events.recv().await, Some(PeerEvent::Ready(4))));
        (handle, framed, events)
    }

    fn block(piece_index: usize, block_index: usize, begin: usize) -> DownloadBlock {
        DownloadBlock {
            piece_index,
            block_index,
            begin,
            length: 4,
        }
    }

    #[tokio::test]
    async fn reader_does_not_block_the_next_request() {
        bounded(async {
            let (mut handle, mut server, mut events) = connected().await;
            handle.sender.send(block(7, 2, 8)).await.unwrap();
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                Message::Request {
                    index: 7,
                    begin: 8,
                    length: 4
                }
            );
            // Do not answer the first request until the second arrives.
            handle.sender.send(block(7, 3, 12)).await.unwrap();
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                Message::Request {
                    index: 7,
                    begin: 12,
                    length: 4
                }
            );
            server
                .send(Message::Piece {
                    index: 7,
                    begin: 12,
                    block: Bytes::from_static(b"data"),
                })
                .await
                .unwrap();
            assert!(matches!(
                events.recv().await,
                Some(PeerEvent::Completed {
                    task: CompletedTask::DownloadedBlock {
                        piece_index: 7,
                        block_index: 3,
                        ..
                    },
                    ..
                })
            ));
            handle.cancel();
            handle.join().await.unwrap();
            assert!(server.next().await.is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn closed_mailbox_exits_while_waiting_for_a_handshake() {
        bounded(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (sender, receiver) = mpsc::channel(1);
            let (events, _events_rx) = mpsc::unbounded_channel();
            let (_shutdown, cancellation) = oneshot::channel();
            let actor = PeerActor::new(peer(&listener), 0, receiver, events);
            let task = tokio::spawn(actor.run(cancellation));
            let (mut server, _) = listener.accept().await.unwrap();
            let mut handshake = [0; 68];
            server.read_exact(&mut handshake).await.unwrap();
            drop(sender);
            task.await.unwrap().unwrap();
            assert_eq!(server.read(&mut [0]).await.unwrap(), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn shutdown_interrupts_connection_handshake() {
        bounded(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (events, _events_rx) = mpsc::unbounded_channel();
            let mut handle = PeerActorHandle::new(peer(&listener), 0, events);
            let (mut server, _) = listener.accept().await.unwrap();
            let mut handshake = [0; 68];
            server.read_exact(&mut handshake).await.unwrap();
            handle.cancel();
            handle.join().await.unwrap();
            assert_eq!(server.read(&mut [0]).await.unwrap(), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn shutdown_interrupts_an_idle_read() {
        bounded(async {
            let (mut handle, mut server, _events) = connected().await;
            handle.cancel();
            handle.join().await.unwrap();
            assert!(server.next().await.is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn disconnect_reports_each_actual_request_identity() {
        bounded(async {
            let (mut handle, mut server, mut events) = connected().await;
            handle.sender.send(block(11, 9, 36)).await.unwrap();
            handle.sender.send(block(12, 5, 20)).await.unwrap();
            for _ in 0..2 {
                assert!(matches!(
                    server.next().await.unwrap().unwrap(),
                    Message::Request { .. }
                ));
            }
            drop(server);
            assert!(handle.join().await.is_err());
            let mut failures = Vec::new();
            for _ in 0..2 {
                match events.recv().await.unwrap() {
                    PeerEvent::Completed {
                        task:
                            CompletedTask::FailedBlock {
                                piece_index,
                                block_index,
                            },
                        actor_id,
                    } => {
                        assert_eq!(actor_id, 4);
                        failures.push((piece_index, block_index));
                    }
                    _ => panic!("expected a failure for an actual pending request"),
                }
            }
            failures.sort_unstable();
            assert_eq!(failures, [(11, 9), (12, 5)]);
            assert!(events.try_recv().is_err());
        })
        .await;
    }

    #[tokio::test]
    async fn dropping_a_handle_aborts_its_socket_task() {
        bounded(async {
            let (handle, mut server, _events) = connected().await;
            drop(handle);
            assert!(server.next().await.is_none());
        })
        .await;
    }
}
