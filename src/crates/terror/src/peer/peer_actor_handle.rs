use crate::peer::peer::Peer;
use crate::peer::peer_actor::{PeerActor, PeerEvent};
use crate::torrent::torrent_downloader::DownloadBlock;
use futures_util::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub(crate) const MAX_IN_FLIGHT: usize = 8;

pub(crate) struct PeerActorHandle {
    pub(crate) actor_id: usize,
    pub(crate) peer_id: String,
    pub(crate) sender: mpsc::Sender<DownloadBlock>,
    pub(crate) ready: bool,
    pub(crate) in_flight: usize,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<anyhow::Result<()>>>,
}

impl PeerActorHandle {
    pub(crate) fn new(
        peer: Peer,
        actor_id: usize,
        events: mpsc::UnboundedSender<PeerEvent>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(MAX_IN_FLIGHT);
        let (shutdown, cancellation) = oneshot::channel();
        let peer_id = peer.id.clone();
        let actor = PeerActor::new(peer, actor_id, receiver, events);
        let task = tokio::spawn(actor.run(cancellation));
        Self {
            actor_id,
            peer_id,
            sender,
            ready: false,
            in_flight: 0,
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    pub(crate) fn cancel(&mut self) {
        self.ready = false;
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    pub(crate) fn poll_join(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        let Some(task) = self.task.as_mut() else {
            return Poll::Pending;
        };
        match Pin::new(task).poll(cx) {
            Poll::Ready(result) => {
                self.task.take();
                Poll::Ready(
                    result
                        .map_err(anyhow::Error::from)
                        .and_then(|result| result),
                )
            }
            Poll::Pending => Poll::Pending,
        }
    }

    pub(crate) async fn join(&mut self) -> anyhow::Result<()> {
        let Some(task) = self.task.as_mut() else {
            return Ok(());
        };
        let result = task.await;
        self.task.take();
        result?
    }
}

impl Drop for PeerActorHandle {
    fn drop(&mut self) {
        self.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
