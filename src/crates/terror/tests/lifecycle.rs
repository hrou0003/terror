use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::json;
use sha1::{Digest, Sha1};
use std::future::Future;
use std::time::Duration;
use tempfile::TempDir;
use terror::torrent::torrent_downloader::{DownloaderState, TorrentDownloaderHandle};
use terror::torrent::torrent_info::Torrent;
use terror::torrent::torrents_manager::TorrentManager;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const BLOCK: usize = 1 << 14;
const PIECE: usize = 2 * BLOCK;

#[derive(Debug, Clone, Copy)]
struct Request {
    piece: usize,
    begin: usize,
    length: usize,
}

#[derive(Serialize)]
struct TrackerPeer {
    ip: &'static str,
    port: u16,
    #[serde(rename = "peer id", with = "serde_bytes")]
    peer_id: &'static [u8],
}

#[derive(Serialize)]
struct TrackerReply {
    peers: Vec<TrackerPeer>,
}

/// A real HTTP tracker and BitTorrent peer, with responses controlled by the test.
struct Swarm {
    torrent: Torrent,
    output: std::path::PathBuf,
    requests: mpsc::UnboundedReceiver<Request>,
    responses: mpsc::UnboundedSender<Bytes>,
    connected: Option<oneshot::Receiver<()>>,
    peer: Option<JoinHandle<()>>,
    tracker: JoinHandle<()>,
    _directory: TempDir,
}

impl Swarm {
    async fn new(data: &[u8], hold_handshake: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("payload.bin");
        let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_address = peer_listener.local_addr().unwrap();
        let tracker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tracker_address = tracker_listener.local_addr().unwrap();
        let response = serde_bencode::to_bytes(&TrackerReply {
            peers: vec![TrackerPeer {
                ip: "127.0.0.1",
                port: peer_address.port(),
                peer_id: b"01234567890123456789",
            }],
        })
        .unwrap();
        let tracker = tokio::spawn(async move {
            let (mut stream, _) = tracker_listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 512];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer).await.unwrap();
                assert_ne!(read, 0, "tracker request closed before headers");
                request.extend_from_slice(&buffer[..read]);
            }
            assert!(request.starts_with(b"GET /announce?"));
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(&response).await.unwrap();
        });
        let (observed, requests) = mpsc::unbounded_channel();
        let (responses, mut replies) = mpsc::unbounded_channel::<Bytes>();
        let (connected_tx, connected) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = peer_listener.accept().await.unwrap();
            let mut handshake = [0; 68];
            stream.read_exact(&mut handshake).await.unwrap();
            assert_eq!(&handshake[1..20], b"BitTorrent protocol");
            let _ = connected_tx.send(());
            if hold_handshake {
                assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
                return;
            }
            stream.write_all(&handshake).await.unwrap();
            let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
            framed.send(Bytes::from_static(&[5, 0xff])).await.unwrap();
            assert_eq!(framed.next().await.unwrap().unwrap().as_ref(), &[2]);
            framed.send(Bytes::from_static(&[1])).await.unwrap();
            loop {
                tokio::select! {
                    frame = framed.next() => match frame {
                        Some(Ok(frame)) => {
                            assert_eq!(frame.len(), 13);
                            assert_eq!(frame[0], 6);
                            observed.send(Request {
                                piece: u32::from_be_bytes(frame[1..5].try_into().unwrap()) as usize,
                                begin: u32::from_be_bytes(frame[5..9].try_into().unwrap()) as usize,
                                length: u32::from_be_bytes(frame[9..13].try_into().unwrap()) as usize,
                            }).unwrap();
                        }
                        None => break,
                        Some(Err(error)) => panic!("peer read failed: {error}"),
                    },
                    Some(reply) = replies.recv() => framed.send(reply).await.unwrap(),
                }
            }
        });
        let hashes: Vec<u8> = data
            .chunks(PIECE)
            .flat_map(|piece| Sha1::digest(piece).to_vec())
            .collect();
        let torrent = serde_json::from_value(json!({
            "announce": format!("http://{tracker_address}/announce"),
            "info": {
                "name": output.to_str().unwrap(),
                "length": data.len(),
                "piece length": PIECE,
                "pieces": hashes,
            },
        }))
        .unwrap();
        Self {
            torrent,
            output,
            requests,
            responses,
            connected: Some(connected),
            peer: Some(peer),
            tracker,
            _directory: directory,
        }
    }

    fn respond(&self, request: Request, data: &[u8]) {
        let offset = request.piece * PIECE + request.begin;
        let mut frame = BytesMut::with_capacity(9 + request.length);
        frame.put_u8(7);
        frame.put_u32(request.piece as u32);
        frame.put_u32(request.begin as u32);
        frame.extend_from_slice(&data[offset..offset + request.length]);
        self.responses.send(frame.freeze()).unwrap();
    }

    async fn wait_closed(&mut self) {
        self.peer.as_mut().unwrap().await.unwrap();
        self.peer.take();
    }
}

impl Drop for Swarm {
    fn drop(&mut self) {
        if let Some(peer) = &self.peer {
            peer.abort();
        }
        self.tracker.abort();
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("local swarm timed out")
}

async fn until(mut condition: impl FnMut() -> bool) {
    while !condition() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test]
async fn pause_resume_preserves_pending_work_and_completes_verified_output() {
    bounded(async {
        // More than one peer's in-flight window, including exact and partial blocks/pieces.
        let data: Vec<u8> = (0..10 * BLOCK + 7)
            .map(|index| (index % 251) as u8)
            .collect();
        let mut swarm = Swarm::new(&data, false).await;
        let handle = TorrentDownloaderHandle::new(swarm.torrent.clone())
            .await
            .unwrap();
        let mut manager = TorrentManager::new();
        let id = manager.add_torrent(handle).await.unwrap();
        manager.start_torrent(&id).await.unwrap();
        manager.start_torrent(&id).await.unwrap();
        let mut in_flight = Vec::new();
        for _ in 0..8 {
            in_flight.push(swarm.requests.recv().await.unwrap());
        }
        manager.pause_torrent(&id).await.unwrap();
        assert_eq!(
            manager.get_torrent_state(&id).unwrap(),
            DownloaderState::Paused
        );
        for request in in_flight {
            swarm.respond(request, &data);
        }
        until(|| manager.get_torrent_progress(&id).unwrap() == 4.0 / 6.0 * 100.0).await;
        // All eight completions were processed, freeing capacity, but pause forbids dispatch.
        assert!(matches!(
            swarm.requests.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(
            manager.get_torrent_state(&id).unwrap(),
            DownloaderState::Paused
        );
        manager.resume_torrent(&id).await.unwrap();
        assert_eq!(
            manager.get_torrent_state(&id).unwrap(),
            DownloaderState::Downloading
        );
        for _ in 0..3 {
            let request = swarm.requests.recv().await.unwrap();
            swarm.respond(request, &data);
        }
        until(|| manager.get_torrent_state(&id).unwrap() == DownloaderState::Completed).await;
        assert_eq!(manager.get_torrent_progress(&id).unwrap(), 100.0);
        assert_eq!(tokio::fs::read(&swarm.output).await.unwrap(), data);
        swarm.wait_closed().await;
        manager.remove_torrent(&id).await.unwrap();
        assert!(manager.get_torrent_state(&id).is_err());
    })
    .await;
}

#[tokio::test]
async fn removal_interrupts_a_pending_handshake_and_joins_the_task_tree() {
    bounded(async {
        let mut swarm = Swarm::new(b"handshake cancellation", true).await;
        let handle = TorrentDownloaderHandle::new(swarm.torrent.clone())
            .await
            .unwrap();
        let mut manager = TorrentManager::new();
        let id = manager.add_torrent(handle).await.unwrap();
        manager.start_torrent(&id).await.unwrap();
        swarm.connected.take().unwrap().await.unwrap();
        manager.pause_torrent(&id).await.unwrap();
        assert_eq!(
            manager.get_torrent_state(&id).unwrap(),
            DownloaderState::Paused
        );
        manager.remove_torrent(&id).await.unwrap();
        swarm.wait_closed().await;
        assert!(manager.resume_torrent(&id).await.is_err());
    })
    .await;
}

#[tokio::test]
async fn dropping_the_torrent_owner_cancels_in_flight_socket_io() {
    bounded(async {
        let mut swarm = Swarm::new(b"pending block", false).await;
        let handle = TorrentDownloaderHandle::new(swarm.torrent.clone())
            .await
            .unwrap();
        handle.start().await.unwrap();
        let request = swarm.requests.recv().await.unwrap();
        assert_eq!(request.length, b"pending block".len());
        drop(handle);
        swarm.wait_closed().await;
        assert!(!swarm.output.exists());
    })
    .await;
}

#[tokio::test]
async fn exhausted_peers_publish_error_instead_of_false_progress() {
    bounded(async {
        let mut swarm = Swarm::new(b"unanswered request", false).await;
        let mut handle = TorrentDownloaderHandle::new(swarm.torrent.clone())
            .await
            .unwrap();
        handle.start().await.unwrap();
        swarm.requests.recv().await.unwrap();
        let peer = swarm.peer.take().unwrap();
        peer.abort();
        let _ = peer.await;
        until(|| handle.get_state() == DownloaderState::Error).await;
        assert_eq!(handle.get_progress(), 0.0);
        assert!(handle.resume().await.is_err());
        handle.shutdown().await.unwrap();
        assert_eq!(handle.get_state(), DownloaderState::Stopped);
    })
    .await;
}

#[tokio::test]
async fn an_exact_final_piece_accepts_out_of_order_blocks_without_zero_length_requests() {
    bounded(async {
        let data: Vec<u8> = (0..PIECE).map(|index| (index % 239) as u8).collect();
        let mut swarm = Swarm::new(&data, false).await;
        let mut handle = TorrentDownloaderHandle::new(swarm.torrent.clone())
            .await
            .unwrap();
        handle.start().await.unwrap();
        let first = swarm.requests.recv().await.unwrap();
        let second = swarm.requests.recv().await.unwrap();
        assert_eq!((first.begin, first.length), (0, BLOCK));
        assert_eq!((second.begin, second.length), (BLOCK, BLOCK));
        swarm.respond(second, &data);
        swarm.respond(first, &data);
        until(|| handle.get_state() == DownloaderState::Completed).await;
        assert_eq!(tokio::fs::read(&swarm.output).await.unwrap(), data);
        swarm.wait_closed().await;
        handle.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn a_hash_mismatch_reports_error_and_does_not_save_corrupt_output() {
    bounded(async {
        let mut swarm = Swarm::new(b"wanted", false).await;
        let mut handle = TorrentDownloaderHandle::new(swarm.torrent.clone())
            .await
            .unwrap();
        handle.start().await.unwrap();
        let request = swarm.requests.recv().await.unwrap();
        swarm.respond(request, b"wrong!");
        until(|| handle.get_state() == DownloaderState::Error).await;
        assert_eq!(handle.get_progress(), 0.0);
        assert!(!swarm.output.exists());
        swarm.wait_closed().await;
        handle.shutdown().await.unwrap();
    })
    .await;
}
