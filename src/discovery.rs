use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time::{Instant, sleep},
};

use crate::{
    dht::Dht,
    download::Config,
    metainfo::Metainfo,
    tracker::{self, AnnounceRequest, Event},
    types::{PeerAddr, PeerId},
};

const INITIAL_BACKOFF: Duration = Duration::from_secs(30);
const MAX_BACKOFF: Duration = Duration::from_secs(600);
const DHT_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
struct TransferStats {
    downloaded: u64,
    uploaded: u64,
    left: u64,
    private: bool,
}

impl TransferStats {
    fn new(meta: &Metainfo, downloaded: u64, uploaded: u64) -> Self {
        Self {
            downloaded,
            uploaded,
            left: if meta.piece_count() == 0 {
                1
            } else {
                meta.total_length.saturating_sub(downloaded)
            },
            private: meta.is_private,
        }
    }
}

/// Owns discovery tasks so dropping a download also cancels DNS, tracker and
/// DHT work. The same session spans metadata resolution and file transfer.
pub(crate) struct PeerDiscovery {
    tasks: JoinSet<()>,
    peers: mpsc::Receiver<Vec<PeerAddr>>,
    stats: watch::Sender<TransferStats>,
}

impl PeerDiscovery {
    pub(crate) fn new(meta: &Metainfo, config: &Config) -> Self {
        let (tx, peers) = mpsc::channel(32);
        let (stats, rx) = watch::channel(TransferStats::new(meta, 0, 0));
        let mut tasks = JoinSet::new();
        for tier in &meta.trackers {
            tasks.spawn(announce_tier(
                tier.clone(),
                meta.info_hash,
                config.peer_id,
                config.listen_port,
                rx.clone(),
                tx.clone(),
            ));
        }
        if !meta.is_private {
            let info_hash = meta.info_hash;
            tasks.spawn(async move {
                let mut stats = rx;
                let lookup = async {
                    let mut backoff = INITIAL_BACKOFF;
                    let dht = loop {
                        match Dht::start().await {
                            Ok(dht) => break dht,
                            Err(e) => {
                                tracing::warn!("DHT start failed: {e}; retrying in {backoff:?}")
                            }
                        }
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    };
                    loop {
                        let peers = dht.get_peers(info_hash, 100).await;
                        tracing::debug!("DHT discovered {} peers", peers.len());
                        if tx.send(peers).await.is_err() {
                            return;
                        }
                        sleep(DHT_INTERVAL).await;
                    }
                };
                tokio::pin!(lookup);
                loop {
                    tokio::select! {
                        _ = &mut lookup => break,
                        changed = stats.changed() => {
                            if changed.is_err() || stats.borrow().private {
                                break;
                            }
                        }
                    }
                }
            });
        }
        Self {
            tasks,
            peers,
            stats,
        }
    }

    pub(crate) fn update(&self, meta: &Metainfo, downloaded: u64, uploaded: u64) {
        self.stats
            .send_replace(TransferStats::new(meta, downloaded, uploaded));
    }

    pub(crate) async fn recv(&mut self) -> Option<Vec<PeerAddr>> {
        // Surface unexpected task failures instead of leaving a discovery
        // source silently dead for the rest of the session.
        loop {
            tokio::select! {
                peers = self.peers.recv() => return peers,
                result = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Some(Err(e)) = result {
                        tracing::warn!("Peer discovery task failed: {e}");
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn injected(meta: &Metainfo) -> (Self, mpsc::Sender<Vec<PeerAddr>>) {
        let (tx, peers) = mpsc::channel(32);
        let (stats, _) = watch::channel(TransferStats::new(meta, 0, 0));
        (
            Self {
                tasks: JoinSet::new(),
                peers,
                stats,
            },
            tx,
        )
    }
}

async fn announce_tier(
    mut tier: Vec<String>,
    info_hash: crate::types::InfoHash,
    peer_id: PeerId,
    port: u16,
    stats: watch::Receiver<TransferStats>,
    peers: mpsc::Sender<Vec<PeerAddr>>,
) {
    let mut announced = HashSet::new();
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let mut responded = false;
        let mut delay = backoff;
        for index in 0..tier.len() {
            let url = &tier[index];
            let current = *stats.borrow();
            let req = AnnounceRequest {
                tracker_url: url,
                info_hash,
                peer_id,
                port,
                uploaded: current.uploaded,
                downloaded: current.downloaded,
                left: current.left,
                event: if announced.contains(url) {
                    None
                } else {
                    Some(Event::Started)
                },
                num_want: 200,
            };
            let response = if url.starts_with("udp://") || url.starts_with("UDP://") {
                tracker::udp::announce(&req, url).await
            } else if url.starts_with("http://") || url.starts_with("https://") {
                tracker::announce(&req).await
            } else {
                tracing::warn!("Skipping unsupported tracker scheme: {url}");
                continue;
            };
            match response {
                Ok(response) => {
                    announced.insert(url.clone());
                    responded = true;
                    delay = Duration::from_secs(response.interval.max(1));
                    backoff = INITIAL_BACKOFF;
                    if let Some(warning) = response.warning {
                        tracing::warn!("Tracker {url}: {warning}");
                    }
                    tracing::debug!(
                        "Tracker {url}: {} peers; reannounce in {delay:?}",
                        response.peers.len()
                    );
                    if peers.send(response.peers).await.is_err() {
                        return;
                    }
                    tier.rotate_left(index);
                    break;
                }
                Err(e) => tracing::warn!("Tracker {url}: {e}"),
            }
        }
        if !responded {
            tracing::warn!("No tracker in tier responded; retrying in {delay:?}");
            backoff = (backoff * 2).min(MAX_BACKOFF);
            if peers.send(Vec::new()).await.is_err() {
                return;
            }
        }
        sleep(delay).await;
    }
}

struct PeerRetry {
    next_attempt: Instant,
    backoff: Duration,
    active: bool,
}

#[derive(Default)]
pub(crate) struct PeerCandidates {
    peers: HashMap<PeerAddr, PeerRetry>,
}

impl PeerCandidates {
    pub(crate) fn add(&mut self, peers: Vec<PeerAddr>) {
        for peer in peers {
            if peer.port == 0 || peer.ip.is_unspecified() || peer.ip.is_multicast() {
                continue;
            }

            self.peers.entry(peer).or_insert(PeerRetry {
                next_attempt: Instant::now(),
                backoff: INITIAL_BACKOFF,
                active: false,
            });
        }
    }

    pub(crate) fn next(&mut self) -> Option<PeerAddr> {
        let now = Instant::now();
        let (&peer, retry) = self
            .peers
            .iter_mut()
            .filter(|(_, retry)| !retry.active && retry.next_attempt <= now)
            .min_by_key(|(_, retry)| retry.next_attempt)?;
        retry.active = true;
        Some(peer)
    }

    pub(crate) fn finished(&mut self, peer: PeerAddr) {
        if let Some(retry) = self.peers.get_mut(&peer) {
            retry.active = false;
            retry.next_attempt = Instant::now() + retry.backoff;
            retry.backoff = (retry.backoff * 2).min(MAX_BACKOFF);
        }
    }

    pub(crate) fn start_transfer(&mut self) {
        for retry in self.peers.values_mut() {
            retry.active = false;
            retry.next_attempt = Instant::now();
            retry.backoff = INITIAL_BACKOFF;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{magnet::MagnetLink, types::InfoHash};
    use tokio::net::UdpSocket;

    fn placeholder() -> Metainfo {
        let mut meta = Metainfo::placeholder_magnet(&MagnetLink {
            info_hash: InfoHash([1; 20]),
            name: None,
            trackers: Vec::new(),
        });
        meta.is_private = true;
        meta
    }

    #[tokio::test(start_paused = true)]
    async fn failed_peers_back_off_without_rediscovery_resetting_the_delay() {
        let peer = PeerAddr {
            ip: "127.0.0.1".parse().unwrap(),
            port: 6881,
        };
        let other = PeerAddr { port: 6882, ..peer };
        let mut candidates = PeerCandidates::default();
        candidates.add(vec![peer, peer]);
        assert_eq!(candidates.next(), Some(peer));
        assert!(candidates.next().is_none());
        for seconds in [30, 60, 120, 240, 480, 600, 600] {
            candidates.finished(peer);
            candidates.add(vec![peer]);
            tokio::time::advance(Duration::from_secs(seconds - 1)).await;
            assert!(candidates.next().is_none());
            tokio::time::advance(Duration::from_secs(1)).await;
            assert_eq!(candidates.next(), Some(peer));
        }
        candidates.finished(peer);
        candidates.add(vec![other]);
        assert_eq!(candidates.next(), Some(other));
    }

    #[tokio::test]
    async fn reannounces_at_tracker_interval_with_current_stats_and_no_started_event() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut meta = placeholder();
            meta.trackers = vec![vec![format!("udp://{}", socket.local_addr().unwrap())]];
            let server = tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let mut last_announce = None;
                for (left, downloaded, uploaded, event) in
                    [(1u64, 0u64, 0u64, 2u32), (60, 40, 7, 0)]
                {
                    let (_, client) = socket.recv_from(&mut buf).await.unwrap();
                    let mut response = buf[8..16].to_vec();
                    response.extend_from_slice(&1u64.to_be_bytes());
                    socket.send_to(&response, client).await.unwrap();
                    let (_, client) = socket.recv_from(&mut buf).await.unwrap();
                    assert_eq!(&buf[56..64], &downloaded.to_be_bytes());
                    assert_eq!(&buf[64..72], &left.to_be_bytes());
                    assert_eq!(&buf[72..80], &uploaded.to_be_bytes());
                    assert_eq!(&buf[80..84], &event.to_be_bytes());
                    if let Some(last) = last_announce {
                        assert!(Instant::now() - last >= Duration::from_secs(1));
                    }
                    last_announce = Some(Instant::now());
                    let mut response = buf[8..16].to_vec();
                    response.extend_from_slice(&1u32.to_be_bytes());
                    response.extend_from_slice(&0u64.to_be_bytes());
                    response.extend_from_slice(&[127, 0, 0, 1, 0x1a, 0xe1]);
                    socket.send_to(&response, client).await.unwrap();
                }
            });
            let mut discovery = PeerDiscovery::new(&meta, &Config::default());
            assert_eq!(discovery.recv().await.unwrap().len(), 1);
            meta.piece_hashes.push([0; 20]);
            meta.total_length = 100;
            discovery.update(&meta, 40, 7);
            assert_eq!(discovery.recv().await.unwrap().len(), 1);
            drop(discovery);
            server.await.unwrap();
        })
        .await
        .expect("periodic tracker announce timed out");
    }

    #[tokio::test]
    async fn failed_tracker_retries_with_exponential_backoff() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut meta = placeholder();
        meta.trackers = vec![vec![format!("udp://{}", socket.local_addr().unwrap())]];
        let mut discovery = PeerDiscovery::new(&meta, &Config::default());
        let mut buf = [0u8; 1024];
        for delay in [30, 60] {
            let (_, client) =
                tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut buf))
                    .await
                    .unwrap()
                    .unwrap();
            let mut error = 3u32.to_be_bytes().to_vec();
            error.extend_from_slice(&buf[12..16]);
            error.extend_from_slice(b"try later");
            socket.send_to(&error, client).await.unwrap();
            assert!(discovery.recv().await.unwrap().is_empty());
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(delay - 1)).await;
            assert!(matches!(
                socket.try_recv_from(&mut buf),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
            ));
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::time::resume();
        }
        tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        drop(discovery);
    }
}
