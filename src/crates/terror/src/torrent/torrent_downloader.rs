use crate::peer::peer_actor_pool::{PeerActorPool, PeerPoolCommand};
use crate::piece::piece_pool::PiecePool;
use crate::torrent::torrent_info::Torrent;
use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use kanal::AsyncSender;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloaderState {
    Downloading,
    Paused,
    Completed,
    Error,
    Stopped,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DownloadBlock {
    pub piece_index: usize,
    pub block_index: usize,
    pub begin: usize,
    pub length: usize,
}

#[derive(Debug)]
pub(crate) enum CompletedTask {
    DownloadedBlock {
        piece_index: usize,
        block_index: usize,
        bytes: Bytes,
    },
    FailedBlock {
        piece_index: usize,
        block_index: usize,
    },
}

#[derive(Clone, Copy)]
struct DownloadStatus {
    state: DownloaderState,
    progress: f64,
}

enum TorrentCommand {
    Start,
    Pause,
    Resume,
    Shutdown,
}

struct TorrentRequest {
    command: TorrentCommand,
    reply: oneshot::Sender<Result<()>>,
}

/// Owns the worker task even while a shutdown future is being cancelled.
struct PeerPoolWorker {
    sender: mpsc::Sender<PeerPoolCommand>,
    task: Option<JoinHandle<Result<()>>>,
}

impl PeerPoolWorker {
    async fn set_paused(&self, paused: bool) -> Result<()> {
        let (reply, received) = oneshot::channel();
        let command = if paused {
            PeerPoolCommand::Pause(reply)
        } else {
            PeerPoolCommand::Resume(reply)
        };
        self.sender
            .send(command)
            .await
            .map_err(|_| anyhow!("peer pool has stopped"))?;
        received
            .await
            .context("peer pool did not acknowledge control")
    }

    async fn shutdown(&mut self) -> Result<()> {
        let Some(task) = self.task.as_mut() else {
            return Ok(());
        };
        let mut acknowledged = Ok(());
        if !task.is_finished() {
            let (reply, received) = oneshot::channel();
            if self
                .sender
                .send(PeerPoolCommand::Shutdown(reply))
                .await
                .is_ok()
            {
                acknowledged = received
                    .await
                    .context("peer pool did not acknowledge shutdown");
            }
        }
        let result = task.await.context("peer pool task failed");
        self.task.take();
        result??;
        acknowledged
    }
}

impl Drop for PeerPoolWorker {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct TorrentDownloader {
    receiver: mpsc::Receiver<TorrentRequest>,
    status: watch::Sender<DownloadStatus>,
    pieces: PiecePool,
    task_tx: AsyncSender<DownloadBlock>,
    completed_rx: mpsc::UnboundedReceiver<CompletedTask>,
    worker: Option<PeerPoolWorker>,
}

impl TorrentDownloader {
    fn state(&self) -> DownloaderState {
        self.status.borrow().state
    }

    fn publish(&self, state: DownloaderState) {
        self.status.send_replace(DownloadStatus {
            state,
            progress: self.pieces.get_progress(),
        });
    }

    async fn stop_worker(&mut self) -> Result<()> {
        if let Some(worker) = &mut self.worker {
            worker.shutdown().await?;
        }
        self.worker.take();
        Ok(())
    }

    async fn fail(&mut self, error: anyhow::Error) {
        warn!(%error, "torrent stopped with an error");
        if let Err(shutdown_error) = self.stop_worker().await {
            warn!(%shutdown_error, "peer pool shutdown failed");
        }
        self.worker.take();
        self.publish(DownloaderState::Error);
    }

    async fn handle_command(&mut self, command: TorrentCommand) -> Result<bool> {
        match command {
            TorrentCommand::Start | TorrentCommand::Resume => {
                match self.state() {
                    DownloaderState::Downloading => return Ok(false),
                    DownloaderState::Paused => {}
                    state => bail!("cannot start a torrent in state {state:?}"),
                }
                self.worker
                    .as_ref()
                    .context("peer pool has stopped")?
                    .set_paused(false)
                    .await?;
                self.publish(DownloaderState::Downloading);
            }
            TorrentCommand::Pause => {
                if self.state() == DownloaderState::Downloading {
                    self.worker
                        .as_ref()
                        .context("peer pool has stopped")?
                        .set_paused(true)
                        .await?;
                    self.publish(DownloaderState::Paused);
                }
            }
            TorrentCommand::Shutdown => {
                let result = self.stop_worker().await;
                self.publish(if result.is_ok() {
                    DownloaderState::Stopped
                } else {
                    DownloaderState::Error
                });
                result?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn run(mut self) {
        let mut pending = None;
        loop {
            let downloading = self.state() == DownloaderState::Downloading;
            if downloading && pending.is_none() {
                pending = self.pieces.next_task();
            }
            let has_worker = self.worker.is_some();
            tokio::select! {
                biased;
                request = self.receiver.recv() => {
                    let Some(request) = request else {
                        let _ = self.stop_worker().await;
                        self.publish(DownloaderState::Stopped);
                        break;
                    };
                    let stopping = matches!(request.command, TorrentCommand::Shutdown);
                    let result = self.handle_command(request.command).await;
                    let exit = matches!(result, Ok(true));
                    let _ = request.reply.send(result.map(|_| ()));
                    if exit || stopping {
                        break;
                    }
                }
                completed = self.completed_rx.recv(), if has_worker => {
                    match completed {
                        Some(task) => {
                            if let Err(error) = self.pieces.handle_completed_task(task).await {
                                self.fail(error).await;
                            } else if self.pieces.is_complete() {
                                // A peer can disconnect immediately after its final reply.
                                // Verified, saved output remains a completed download.
                                if let Err(error) = self.stop_worker().await {
                                    warn!(%error, "peer pool stopped after the final piece");
                                    self.worker.take();
                                }
                                self.publish(DownloaderState::Completed);
                            } else {
                                self.publish(self.state());
                            }
                        }
                        None => self.fail(anyhow!("peer completion channel closed")).await,
                    }
                }
                result = async { self.worker.as_mut().unwrap().task.as_mut().unwrap().await }, if has_worker => {
                    // The join result has been consumed; dropping the guard must not detach anything.
                    self.worker.as_mut().unwrap().task.take();
                    self.worker.take();
                    let error = match result {
                        Ok(Err(error)) => error,
                        Err(error) => anyhow!("peer pool task failed: {error}"),
                        Ok(Ok(())) => anyhow!("peer pool stopped before the torrent completed"),
                    };
                    self.fail(error).await;
                }
                sent = async { self.task_tx.send(pending.unwrap()).await }, if downloading && pending.is_some() && has_worker => {
                    if sent.is_err() {
                        self.fail(anyhow!("peer task queue closed")).await;
                    } else {
                        pending = None;
                    }
                }
            }
        }
    }
}

/// The only public route to torrent mutation. Commands acknowledge actor handling,
/// not just mailbox insertion. Dropping the last owner cancels its task tree.
pub struct TorrentDownloaderHandle {
    sender: mpsc::Sender<TorrentRequest>,
    status: watch::Receiver<DownloadStatus>,
    task: Option<JoinHandle<()>>,
}

impl TorrentDownloaderHandle {
    pub async fn new(torrent: Torrent) -> Result<Self> {
        let pieces = PiecePool::new(&torrent)?;
        let (task_tx, task_rx) = kanal::bounded_async(64);
        let (completed_tx, completed_rx) = mpsc::unbounded_channel();
        let pool = PeerActorPool::new(&torrent, task_rx, completed_tx).await?;
        let (control_tx, control_rx) = mpsc::channel(8);
        let (sender, receiver) = mpsc::channel(8);
        let (status_tx, status) = watch::channel(DownloadStatus {
            state: DownloaderState::Paused,
            progress: pieces.get_progress(),
        });
        let worker = PeerPoolWorker {
            sender: control_tx,
            task: Some(tokio::spawn(pool.run(control_rx))),
        };
        let actor = TorrentDownloader {
            receiver,
            status: status_tx,
            pieces,
            task_tx,
            completed_rx,
            worker: Some(worker),
        };
        Ok(Self {
            sender,
            status,
            task: Some(tokio::spawn(actor.run())),
        })
    }

    async fn request(&self, command: TorrentCommand) -> Result<()> {
        let (reply, received) = oneshot::channel();
        self.sender
            .send(TorrentRequest { command, reply })
            .await
            .map_err(|_| anyhow!("torrent actor has stopped"))?;
        received
            .await
            .context("torrent actor did not acknowledge command")?
    }

    pub async fn start(&self) -> Result<()> {
        self.request(TorrentCommand::Start).await
    }

    /// Stop new peer dispatch. Already-dispatched requests may still finish.
    pub async fn pause(&self) -> Result<()> {
        self.request(TorrentCommand::Pause).await
    }

    pub async fn resume(&self) -> Result<()> {
        self.request(TorrentCommand::Resume).await
    }

    pub fn get_state(&self) -> DownloaderState {
        self.status.borrow().state
    }

    pub fn get_progress(&self) -> f64 {
        self.status.borrow().progress
    }

    /// Wait for the torrent actor and all of its peer workers to terminate.
    pub async fn shutdown(&mut self) -> Result<()> {
        if self.task.is_none() {
            return Ok(());
        }
        let result = self.request(TorrentCommand::Shutdown).await;
        let joined = self
            .task
            .as_mut()
            .unwrap()
            .await
            .context("torrent actor task failed");
        self.task.take();
        joined?;
        result
    }
}

impl Drop for TorrentDownloaderHandle {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
