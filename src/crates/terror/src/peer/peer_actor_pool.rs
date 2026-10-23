use crate::peer::peer::Peer;
use crate::peer::peer_actor::PeerEvent;
use crate::peer::peer_actor_handle::{PeerActorHandle, MAX_IN_FLIGHT};
use crate::torrent::torrent_downloader::{CompletedTask, DownloadBlock};
use crate::torrent::torrent_info::Torrent;
use crate::utils::utils::{percent_encode_hash, TrackerRequest, TrackerResponse};
use anyhow::{anyhow, bail, Context};
use futures_util::future::poll_fn;
use kanal::AsyncReceiver;
use std::net::{IpAddr, Ipv4Addr};
use std::task::Poll;
use tokio::sync::{mpsc, oneshot};

pub(crate) enum PeerPoolCommand {
    Pause(oneshot::Sender<()>),
    Resume(oneshot::Sender<()>),
    Shutdown(oneshot::Sender<()>),
}

pub struct PeerActorPool {
    actors: Vec<PeerActorHandle>,
    task_queue: AsyncReceiver<DownloadBlock>,
    events: mpsc::UnboundedReceiver<PeerEvent>,
    completed_task_tx: mpsc::UnboundedSender<CompletedTask>,
}

impl PeerActorPool {
    pub async fn new(
        torrent: &Torrent,
        task_queue: AsyncReceiver<DownloadBlock>,
        completed_task_tx: mpsc::UnboundedSender<CompletedTask>,
    ) -> anyhow::Result<Self> {
        let info_hash = torrent.calculate_info_hash();
        let encoded_info_hash = percent_encode_hash(&hex::encode(info_hash));
        let request = TrackerRequest {
            peer_id: "codecraftersbittorre".to_string(),
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: torrent.info.length.unwrap_or(0),
            compact: 1,
        };
        let request_params = serde_urlencoded::to_string(&request)?;
        let request_url = format!(
            "{}?{}&info_hash={}",
            torrent.announce, request_params, encoded_info_hash
        );
        let response = reqwest::get(request_url).await?.bytes().await?;
        // Preserve the tracker's existing dictionary-peer response format.
        let tracker: TrackerResponse = serde_bencode::from_bytes(&response)?;
        let peers = tracker
            .peers
            .into_iter()
            .map(|peer| {
                let ip = peer
                    .ip
                    .parse::<Ipv4Addr>()
                    .context("Parsing tracker peer IP")?;
                let port = u16::try_from(peer.port).context("Parsing tracker peer port")?;
                Ok(Peer::new(
                    String::from_utf8_lossy(peer.peer_id.as_slice()).into_owned(),
                    IpAddr::V4(ip),
                    port,
                    info_hash,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        if peers.is_empty() {
            bail!("Tracker returned no usable peers");
        }
        Ok(Self::from_peers(peers, task_queue, completed_task_tx))
    }

    fn from_peers(
        peers: Vec<Peer>,
        task_queue: AsyncReceiver<DownloadBlock>,
        completed_task_tx: mpsc::UnboundedSender<CompletedTask>,
    ) -> Self {
        let (events_tx, events) = mpsc::unbounded_channel();
        let actors = peers
            .into_iter()
            .enumerate()
            .map(|(id, peer)| PeerActorHandle::new(peer, id, events_tx.clone()))
            .collect();
        Self {
            actors,
            task_queue,
            events,
            completed_task_tx,
        }
    }

    pub(crate) async fn run(
        mut self,
        mut commands: mpsc::Receiver<PeerPoolCommand>,
    ) -> anyhow::Result<()> {
        let mut shutdown_ack = None;
        let outcome = self
            .run_until_stopped(&mut commands, &mut shutdown_ack)
            .await;
        // Every return path, including errors, cancels and joins all owned tasks.
        let cleanup = self.shutdown_all().await;
        if let Some(ack) = shutdown_ack {
            let _ = ack.send(());
        }
        outcome.and(cleanup)
    }

    async fn run_until_stopped(
        &mut self,
        commands: &mut mpsc::Receiver<PeerPoolCommand>,
        shutdown_ack: &mut Option<oneshot::Sender<()>>,
    ) -> anyhow::Result<()> {
        let mut paused = true;
        let mut last_peer_error = None;
        loop {
            if self.actors.is_empty() {
                return Err(last_peer_error.unwrap_or_else(|| anyhow!("No usable peers remain")));
            }
            let available = self
                .actors
                .iter()
                .any(|actor| actor.ready && actor.in_flight < MAX_IN_FLIGHT);
            tokio::select! {
                biased;
                command = commands.recv() => match command {
                    Some(PeerPoolCommand::Pause(ack)) => {
                        // Already-dispatched/in-flight requests may still finish.
                        paused = true;
                        let _ = ack.send(());
                    }
                    Some(PeerPoolCommand::Resume(ack)) => {
                        paused = false;
                        let _ = ack.send(());
                    }
                    Some(PeerPoolCommand::Shutdown(ack)) => {
                        *shutdown_ack = Some(ack);
                        return Ok(());
                    }
                    None => bail!("Peer pool command channel closed"),
                },
                _ = self.completed_task_tx.closed() => bail!("Torrent completion channel closed"),
                event = self.events.recv() => {
                    self.handle_event(event.ok_or_else(|| anyhow!("All peer event channels closed"))?)?;
                }
                (index, result) = poll_fn(|cx| {
                    for (index, actor) in self.actors.iter_mut().enumerate() {
                        if let Poll::Ready(result) = actor.poll_join(cx) {
                            return Poll::Ready((index, result));
                        }
                    }
                    Poll::Pending
                }) => {
                    let actor = self.actors.swap_remove(index);
                    last_peer_error = Some(match result {
                        Ok(()) => anyhow!("Peer {} stopped unexpectedly", actor.peer_id),
                        Err(error) => error.context(format!("Peer {} failed", actor.peer_id)),
                    });
                }
                task = self.task_queue.recv(), if !paused && available => {
                    let block = task.map_err(|_| anyhow!("Download task channel closed"))?;
                    self.dispatch(block)?;
                }
            }
        }
    }

    fn dispatch(&mut self, block: DownloadBlock) -> anyhow::Result<()> {
        let actor = self
            .actors
            .iter_mut()
            .filter(|actor| actor.ready && actor.in_flight < MAX_IN_FLIGHT)
            .min_by_key(|actor| actor.in_flight)
            .ok_or_else(|| anyhow!("No peer available for scheduled block"))?;
        match actor.sender.try_send(block) {
            Ok(()) => actor.in_flight += 1,
            Err(_) => {
                actor.ready = false;
                // The mailbox never accepted this request, so the actor cannot report it.
                self.completed_task_tx
                    .send(CompletedTask::FailedBlock {
                        piece_index: block.piece_index,
                        block_index: block.block_index,
                    })
                    .map_err(|_| anyhow!("Torrent completion channel closed"))?;
                actor.cancel();
            }
        }
        Ok(())
    }

    fn handle_event(&mut self, event: PeerEvent) -> anyhow::Result<()> {
        match event {
            PeerEvent::Ready(id) => {
                if let Some(actor) = self.actors.iter_mut().find(|actor| actor.actor_id == id) {
                    actor.ready = true;
                }
            }
            PeerEvent::Completed { actor_id, task } => {
                if let Some(actor) = self
                    .actors
                    .iter_mut()
                    .find(|actor| actor.actor_id == actor_id)
                {
                    actor.in_flight = actor.in_flight.saturating_sub(1);
                    if matches!(&task, CompletedTask::FailedBlock { .. }) {
                        actor.ready = false;
                    }
                }
                self.completed_task_tx
                    .send(task)
                    .map_err(|_| anyhow!("Torrent completion channel closed"))?;
            }
        }
        Ok(())
    }

    async fn shutdown_all(&mut self) -> anyhow::Result<()> {
        for actor in &mut self.actors {
            actor.cancel();
        }
        let mut result = Ok(());
        for actor in &mut self.actors {
            if let Err(error) = actor.join().await {
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }
        // Drop cleanup emits exact failures before each task's join completes.
        while let Ok(event) = self.events.try_recv() {
            if let Err(error) = self.handle_event(event) {
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }
        result
    }
}

impl Drop for PeerActorPool {
    fn drop(&mut self) {
        // PeerActorHandle's Drop aborts any task not already joined.
        for actor in &mut self.actors {
            actor.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tcp::bittorrent_code::BitTorrentCodec;
    use crate::tcp::message::Message;
    use bytes::Bytes;
    use futures_util::{SinkExt, StreamExt};
    use std::future::Future;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::Framed;

    async fn bounded<T>(future: impl Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .expect("local pool timed out")
    }

    fn pool(
        listener: &TcpListener,
    ) -> (
        PeerActorPool,
        kanal::AsyncSender<DownloadBlock>,
        mpsc::UnboundedReceiver<CompletedTask>,
    ) {
        let address = listener.local_addr().unwrap();
        let peer = Peer::new("local-peer".into(), address.ip(), address.port(), [0; 20]);
        let (tasks, queue) = kanal::bounded_async(8);
        let (completed, completions) = mpsc::unbounded_channel();
        (
            PeerActorPool::from_peers(vec![peer], queue, completed),
            tasks,
            completions,
        )
    }

    async fn ready(listener: &TcpListener) -> Framed<TcpStream, BitTorrentCodec> {
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

    async fn command(
        commands: &mpsc::Sender<PeerPoolCommand>,
        make: impl FnOnce(oneshot::Sender<()>) -> PeerPoolCommand,
    ) {
        let (ack, acknowledged) = oneshot::channel();
        commands.send(make(ack)).await.unwrap();
        acknowledged.await.unwrap();
    }

    #[tokio::test]
    async fn pause_ack_prevents_dispatch_and_shutdown_ack_joins_socket_io() {
        bounded(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (pool, tasks, _completions) = pool(&listener);
            let (commands, receiver) = mpsc::channel(4);
            let task = tokio::spawn(pool.run(receiver));
            let mut server = ready(&listener).await;
            tasks
                .send(DownloadBlock {
                    piece_index: 6,
                    block_index: 2,
                    begin: 8,
                    length: 4,
                })
                .await
                .unwrap();
            command(&commands, PeerPoolCommand::Pause).await;
            assert_eq!(tasks.len(), 1, "pool starts paused");
            command(&commands, PeerPoolCommand::Resume).await;
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                Message::Request {
                    index: 6,
                    begin: 8,
                    length: 4
                }
            );
            command(&commands, PeerPoolCommand::Pause).await;
            tasks
                .send(DownloadBlock {
                    piece_index: 7,
                    block_index: 3,
                    begin: 12,
                    length: 4,
                })
                .await
                .unwrap();
            command(&commands, PeerPoolCommand::Pause).await;
            assert_eq!(
                tasks.len(),
                1,
                "pause ack must precede any further dispatch"
            );
            command(&commands, PeerPoolCommand::Shutdown).await;
            task.await.unwrap().unwrap();
            assert!(
                server.next().await.is_none(),
                "shutdown left no socket task behind"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn shutdown_joins_a_peer_stuck_in_connection_setup() {
        bounded(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (pool, _tasks, _completions) = pool(&listener);
            let (commands, receiver) = mpsc::channel(4);
            let task = tokio::spawn(pool.run(receiver));
            let (mut server, _) = listener.accept().await.unwrap();
            let mut handshake = [0; 68];
            server.read_exact(&mut handshake).await.unwrap();
            command(&commands, PeerPoolCommand::Shutdown).await;
            task.await.unwrap().unwrap();
            assert_eq!(server.read(&mut [0]).await.unwrap(), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn exhausted_peers_report_real_failure_and_return_an_error() {
        bounded(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (pool, tasks, mut completions) = pool(&listener);
            let (commands, receiver) = mpsc::channel(4);
            let task = tokio::spawn(pool.run(receiver));
            let mut server = ready(&listener).await;
            command(&commands, PeerPoolCommand::Resume).await;
            tasks
                .send(DownloadBlock {
                    piece_index: 8,
                    block_index: 13,
                    begin: 52,
                    length: 4,
                })
                .await
                .unwrap();
            assert!(matches!(
                server.next().await.unwrap().unwrap(),
                Message::Request { .. }
            ));
            drop(server);
            assert!(matches!(
                completions.recv().await,
                Some(CompletedTask::FailedBlock {
                    piece_index: 8,
                    block_index: 13,
                    ..
                })
            ));
            assert!(task.await.unwrap().is_err());
        })
        .await;
    }
}
