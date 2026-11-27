//! Download orchestrator.
//!
//! Coordinates tracker announces, peer connections, piece scheduling,
//! SHA-1 verification and file writing for a single torrent.
//!
//! The public surface is intentionally small: callers pass in a `Metainfo`
//! and a `Config` and receive periodic `DownloadStats` via a callback.

use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use sha1::{Digest, Sha1};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinSet,
    time::timeout,
};

use crate::{
    discovery::{PeerCandidates, PeerDiscovery},
    error::{Error, Result},
    file_writer::FileWriter,
    metainfo::Metainfo,
    peer::{
        conn::PeerConn,
        handshake,
        message::{BLOCK_SIZE, Message},
        metadata_exchange, mse,
    },
    piece_manager::PieceManager,
    resume::{self, ResumeState},
    tracker::{self, AnnounceRequest, Event},
    types::{InfoHash, PeerAddr, PeerId},
};

// Public types
/// Configuration for a single download session.
#[derive(Debug, Clone)]
pub struct Config {
    pub download_dir: std::path::PathBuf,
    /// Peer port we advertise to trackers and listen on for incoming connections.
    pub listen_port: u16,
    pub peer_id: PeerId,
    /// Pipeline depth: how many REQUEST messages to have in flight per peer.
    pub pipeline: usize,
    /// Hard cap on concurrently connected peers.
    pub max_peers: usize,
    /// Stop seeding when upload/download ratio reaches this value (None = never stop).
    pub seed_ratio: Option<f64>,
    /// Stop seeding after this duration (None = never stop from time alone).
    pub seed_time: Option<Duration>,
    /// Whether to seed after completing the download.
    pub seed: bool,
    /// Stop after this long without metadata or a verified piece (None = keep trying).
    pub idle_timeout: Option<Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            download_dir: std::path::PathBuf::from("."),
            listen_port: 6881,
            peer_id: crate::peer::id::generate(),
            pipeline: 5,
            max_peers: 50,
            seed_ratio: None,
            seed_time: None,
            seed: false,
            idle_timeout: None,
        }
    }
}

/// Snapshot of download progress sent to the progress callback.
#[derive(Debug, Clone)]
pub struct DownloadStats {
    pub downloaded: u64,
    pub uploaded: u64,
    pub total: u64,
    /// Fraction [0.0, 1.0].
    pub progress: f64,
    /// Estimated bytes/sec (exponential moving average).
    pub download_speed: f64,
    pub upload_speed: f64,
    pub peers_active: usize,
    pub pieces_done: u32,
    pub pieces_total: u32,
    pub state: DownloadState,
}

/// High-level state of the download session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadState {
    Connecting,
    Downloading,
    Complete,
}

struct IdleTimer {
    timeout: Option<Duration>,
    last_progress: tokio::time::Instant,
}

impl IdleTimer {
    fn new(timeout: Option<Duration>) -> Self {
        Self {
            timeout,
            last_progress: tokio::time::Instant::now(),
        }
    }

    fn reset(&mut self) {
        self.last_progress = tokio::time::Instant::now();
    }

    fn check(&self) -> Result<()> {
        if let Some(timeout) = self.timeout
            && self.last_progress.elapsed() >= timeout
        {
            return Err(Error::IdleTimeout(timeout));
        }
        Ok(())
    }
}

struct PeerTasks<T> {
    tasks: JoinSet<Result<T>>,
    addresses: HashMap<tokio::task::Id, PeerAddr>,
}

impl<T: Send + 'static> PeerTasks<T> {
    fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            addresses: HashMap::new(),
        }
    }

    fn len(&self) -> usize {
        self.tasks.len()
    }

    fn spawn(&mut self, peer: PeerAddr, attempt: impl Future<Output = Result<T>> + Send + 'static) {
        let task = self.tasks.spawn(attempt);
        self.addresses.insert(task.id(), peer);
    }

    async fn next(&mut self) -> Option<(PeerAddr, Result<T>)> {
        let (id, result) = match self.tasks.join_next_with_id().await? {
            Ok((id, result)) => (id, result),
            Err(e) => (e.id(), Err(Error::Peer(format!("peer task failed: {e}")))),
        };
        let peer = self
            .addresses
            .remove(&id)
            .expect("peer task has an address");
        Some((peer, result))
    }

    async fn shutdown(&mut self) {
        self.tasks.shutdown().await;
        self.addresses.clear();
    }
}

// Entry point
/// Download a single torrent, calling `on_progress` approximately once per
/// second.  Returns when all pieces have been verified and written.
pub async fn download(
    meta: Arc<Metainfo>,
    config: Config,
    on_progress: impl Fn(DownloadStats) + Send + Sync + 'static,
) -> Result<()> {
    let discovery = PeerDiscovery::new(&meta, &config);
    download_with_discovery(meta, config, on_progress, discovery).await
}

async fn download_with_discovery(
    meta: Arc<Metainfo>,
    config: Config,
    on_progress: impl Fn(DownloadStats) + Send + Sync + 'static,
    mut discovery: PeerDiscovery,
) -> Result<()> {
    if config.max_peers == 0 {
        return Err(Error::InvalidConfig(
            "max_peers must be greater than zero".into(),
        ));
    }
    let on_progress = Arc::new(on_progress);
    let mut idle = IdleTimer::new(config.idle_timeout);
    let mut candidates = PeerCandidates::default();
    // Magnet resolution
    // A placeholder Metainfo (from a magnet link) has no pieces; we must fetch
    // the info dict from peers via BEP-9 before we can start downloading.
    let meta = if meta.piece_count() == 0 {
        tracing::info!("Magnet link — fetching metadata via BEP-9...");
        let real = fetch_magnet_metadata(
            &meta,
            &config,
            &mut discovery,
            &mut candidates,
            &*on_progress,
            &idle,
        )
        .await?;
        tracing::info!("Fetched metadata: '{}'", real.name);
        idle.reset();
        Arc::new(real)
    } else {
        meta
    };

    // Resume state
    let resume_state = ResumeState::load(&config.download_dir, &meta.info_hash)
        .await
        .unwrap_or_default();

    // Shared mutable state.
    let piece_mgr = Arc::new(Mutex::new(PieceManager::new(
        meta.piece_count(),
        meta.piece_length,
        meta.total_length,
    )));

    // Restore previously completed pieces.
    {
        let mut mgr = piece_mgr.lock().await;
        resume::verify_and_resume(&resume_state, &meta, &mut mgr, &config.download_dir)
            .await
            .unwrap_or_default();
    }

    let file_writer = Arc::new(FileWriter::new(config.download_dir.clone(), &meta.files).await?);
    let uploaded = Arc::new(std::sync::atomic::AtomicU64::new(resume_state.uploaded));

    let config = Arc::new(config);

    discovery.update(
        &meta,
        piece_mgr.lock().await.downloaded,
        resume_state.uploaded,
    );
    candidates.start_transfer();
    let mut peers = PeerTasks::new();
    let mut discovery_open = true;

    let mut last_downloaded: u64 = 0;
    let mut last_uploaded: u64 = 0;
    let mut speed_ema: f64 = 0.0;
    let mut up_speed_ema: f64 = 0.0;
    let mut last_saved_pieces: u32 = piece_mgr.lock().await.done_count();
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Progress reporting loop
    loop {
        while peers.len() < config.max_peers && !piece_mgr.lock().await.is_complete() {
            let Some(peer_addr) = candidates.next() else {
                break;
            };
            let meta = meta.clone();
            let config = config.clone();
            let piece_mgr = piece_mgr.clone();
            let file_writer = file_writer.clone();
            peers.spawn(peer_addr, async move {
                run_peer(peer_addr, &meta, &config, &piece_mgr, &file_writer).await
            });
        }
        tokio::select! {
            batch = discovery.recv(), if discovery_open => {
                match batch {
                    Some(batch) => candidates.add(batch),
                    None => discovery_open = false,
                }
                continue;
            }
            Some((peer, result)) = peers.next(), if peers.len() > 0 => {
                candidates.finished(peer);
                if let Err(e) = result {
                    tracing::debug!("Peer {peer}: {e}");
                }
                continue;
            }
            _ = tick.tick() => {}
        }

        let mgr = piece_mgr.lock().await;
        let downloaded = mgr.downloaded;
        let pieces_done = mgr.done_count();
        let pieces_total = mgr.piece_count();
        let complete = mgr.is_complete();
        let checkpoint = if pieces_done > last_saved_pieces {
            let done: Vec<u32> = (0..pieces_total).filter(|&i| !mgr.needs(i)).collect();
            Some(ResumeState {
                info_hash: meta.info_hash.to_hex(),
                pieces_done: done,
                downloaded,
                uploaded: uploaded.load(std::sync::atomic::Ordering::Relaxed),
            })
        } else {
            None
        };
        drop(mgr);
        if let Some(cp) = checkpoint {
            match cp.save(&config.download_dir).await {
                Ok(()) => last_saved_pieces = pieces_done,
                Err(e) => tracing::debug!("Failed to save resume checkpoint: {e}"),
            }
        }

        let delta = downloaded.saturating_sub(last_downloaded);
        if delta > 0 {
            idle.reset();
        }
        last_downloaded = downloaded;
        let up_total = uploaded.load(std::sync::atomic::Ordering::Relaxed);
        let up_delta = up_total.saturating_sub(last_uploaded);
        last_uploaded = up_total;
        // Smooth speed estimate (EMA, α=0.3).
        speed_ema = speed_ema * 0.7 + (delta as f64 / 0.5) * 0.3;
        up_speed_ema = up_speed_ema * 0.7 + (up_delta as f64 / 0.5) * 0.3;

        let progress = if meta.total_length > 0 {
            downloaded as f64 / meta.total_length as f64
        } else {
            0.0
        };

        let peers_active = peers.len();
        let state = if complete {
            DownloadState::Complete
        } else if peers_active == 0 {
            DownloadState::Connecting
        } else {
            DownloadState::Downloading
        };

        on_progress(DownloadStats {
            downloaded,
            uploaded: up_total,
            total: meta.total_length,
            progress,
            download_speed: speed_ema,
            upload_speed: up_speed_ema,
            peers_active,
            pieces_done,
            pieces_total,
            state,
        });
        discovery.update(&meta, downloaded, up_total);

        if complete {
            // Save resume state for seeding purposes.
            let state = {
                let mgr = piece_mgr.lock().await;
                let done: Vec<u32> = (0..mgr.piece_count()).filter(|&i| !mgr.needs(i)).collect();
                ResumeState {
                    info_hash: meta.info_hash.to_hex(),
                    pieces_done: done,
                    downloaded: mgr.downloaded,
                    uploaded: uploaded.load(std::sync::atomic::Ordering::Relaxed),
                }
            };
            let _ = state.save(&config.download_dir).await;
            break;
        }

        idle.check()?;
    }

    // Let remaining peer tasks wind down naturally.
    peers.shutdown().await;
    drop(discovery);

    // Completion announce with actual transfer totals.
    let final_downloaded = piece_mgr.lock().await.downloaded;
    let final_uploaded = uploaded.load(std::sync::atomic::Ordering::Relaxed);
    announce_all_no_err(
        &meta,
        &config,
        Event::Completed,
        final_downloaded,
        final_uploaded,
    )
    .await;

    // Seeding
    if config.seed {
        tracing::info!(
            "Download complete; starting seed loop on port {}",
            config.listen_port
        );
        let seed_result = seed_loop(&meta, &config, &file_writer, &uploaded).await;
        if let Err(e) = seed_result {
            tracing::warn!("Seed loop ended: {e}");
        }
    }

    // Remove resume state on clean exit.
    if let Err(e) = ResumeState::remove(&config.download_dir, &meta.info_hash).await {
        tracing::warn!("Failed to remove resume state: {e}");
    }

    Ok(())
}

/// Reconnect to `sock_addr` over plain TCP and complete the BEP-3 handshake.
/// Used as a fallback when MSE negotiation or the encrypted handshake fail.
async fn connect_plain(
    sock_addr: SocketAddr,
    info_hash: InfoHash,
    peer_id: PeerId,
) -> Result<(handshake::HandshakeResult, PeerConn)> {
    let mut s = timeout(Duration::from_secs(10), TcpStream::connect(sock_addr))
        .await
        .map_err(|_| Error::Peer("reconnect timeout".into()))??;
    s.set_nodelay(true)?;
    let hs = timeout(
        Duration::from_secs(10),
        handshake::perform(&mut s, info_hash, peer_id),
    )
    .await
    .map_err(|_| Error::Peer("plaintext handshake timeout".into()))??;
    Ok((hs, PeerConn::new(s)))
}

// Single peer task
/// Connect to a single peer, perform the handshake, then download as many
/// pieces as the peer can provide.
async fn run_peer(
    addr: PeerAddr,
    meta: &Metainfo,
    config: &Config,
    piece_mgr: &Mutex<PieceManager>,
    file_writer: &FileWriter,
) -> Result<()> {
    let sock_addr = SocketAddr::from(addr);

    // Connect
    let stream = timeout(Duration::from_secs(10), TcpStream::connect(sock_addr))
        .await
        .map_err(|_| Error::Peer(format!("connect timeout to {addr}")))?
        .map_err(|e| Error::Peer(format!("connect to {addr}: {e}")))?;
    stream.set_nodelay(true)?;

    // Handshake (try MSE first, fall back to plaintext)
    let encrypted = timeout(Duration::from_secs(10), async {
        let mut stream = mse::perform_initiator(stream, &meta.info_hash).await?;
        let hs = handshake::perform(&mut stream, meta.info_hash, config.peer_id).await?;
        Ok::<_, Error>((hs, PeerConn::new(stream)))
    })
    .await;
    let (hs, mut conn) = match encrypted {
        Ok(Ok(connection)) => connection,
        _ => connect_plain(sock_addr, meta.info_hash, config.peer_id).await?,
    };
    tracing::debug!(
        "Handshake OK with {:?} ext={}",
        PeerId(hs.peer_id),
        hs.capabilities.extension_protocol,
    );

    // Receive initial messages (bitfield / have)
    let mut peer_bitfield = vec![false; meta.piece_count() as usize];

    // Give the peer up to 3 s to send a BITFIELD before we start.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        match tokio::time::timeout_at(deadline, conn.read_message()).await {
            Ok(Ok(Some(Message::Bitfield(bits)))) => {
                peer_bitfield = PieceManager::bitfield_to_vec(&bits, meta.piece_count());
                break;
            }
            Ok(Ok(Some(Message::Have(idx)))) => {
                if let Some(b) = peer_bitfield.get_mut(idx as usize) {
                    *b = true;
                }
            }
            Ok(Ok(Some(Message::KeepAlive))) => {}
            Ok(Ok(Some(Message::Extension { .. }))) => {}
            _ => break,
        }
    }

    // Check whether this peer has anything we need before going further.
    {
        let mgr = piece_mgr.lock().await;
        let useful = (0..meta.piece_count())
            .any(|i| peer_bitfield.get(i as usize).copied().unwrap_or(false) && mgr.needs(i));
        if !useful {
            return Ok(());
        }
    }

    // Send INTERESTED
    conn.send(&Message::Interested).await?;

    // Main download loop
    let mut choked = true;

    loop {
        // Claim a piece.
        let piece_idx = {
            let mut mgr = piece_mgr.lock().await;
            if mgr.is_complete() {
                return Ok(());
            }
            mgr.claim_piece(&peer_bitfield)
        };

        let piece_idx = match piece_idx {
            Some(i) => i,
            None => return Ok(()), // nothing left for this peer
        };

        // Wait until unchoked (may receive HAVE/BITFIELD updates while waiting).
        let unchoke_deadline = Duration::from_secs(60);
        let unchoke_start = Instant::now();
        while choked {
            if unchoke_start.elapsed() > unchoke_deadline {
                piece_mgr.lock().await.return_piece(piece_idx);
                return Err(Error::Peer("choke timeout".into()));
            }
            match conn.read_message_timeout(Duration::from_secs(5)).await {
                Ok(Some(Message::Unchoke)) => {
                    choked = false;
                }
                Ok(Some(Message::Have(idx))) => {
                    if let Some(b) = peer_bitfield.get_mut(idx as usize) {
                        *b = true;
                    }
                }
                Ok(Some(Message::Choke)) => {} // still choked
                Ok(Some(Message::KeepAlive)) | Ok(Some(Message::Extension { .. })) => {}
                Ok(None) | Err(_) => {
                    piece_mgr.lock().await.return_piece(piece_idx);
                    return Err(Error::Peer(
                        "connection lost while waiting for unchoke".into(),
                    ));
                }
                _ => {}
            }
        }

        // Download the piece.
        match download_piece(
            piece_idx,
            meta,
            config,
            &mut conn,
            &mut choked,
            &mut peer_bitfield,
        )
        .await
        {
            Ok(data) => {
                // Verify SHA-1.
                let expected = &meta.piece_hashes[piece_idx as usize];
                let mut hasher = Sha1::new();
                hasher.update(&data);
                let got = hasher.finalize();
                if got.as_slice() != expected {
                    tracing::warn!("piece {} hash mismatch, discarding", piece_idx);
                    piece_mgr.lock().await.return_piece(piece_idx);
                    return Err(Error::PieceVerification(piece_idx));
                }

                // Write to disk.
                let flat_offset = piece_idx as u64 * meta.piece_length;
                file_writer.write_piece(flat_offset, &data).await?;

                // Mark done.
                let plen = meta.piece_len(piece_idx);
                piece_mgr.lock().await.mark_done(piece_idx, plen);
                tracing::debug!("piece {} verified and written", piece_idx);
            }
            Err(e) => {
                piece_mgr.lock().await.return_piece(piece_idx);
                return Err(e);
            }
        }
    }
}

/// Download all blocks for a single piece from `conn`, handling interleaved
/// CHOKE/HAVE messages.  Returns the assembled piece data on success.
async fn download_piece(
    piece_idx: u32,
    meta: &Metainfo,
    config: &Config,
    conn: &mut PeerConn,
    choked: &mut bool,
    peer_bitfield: &mut Vec<bool>,
) -> Result<Vec<u8>> {
    let piece_len = meta.piece_len(piece_idx) as usize;
    let num_blocks = (piece_len + BLOCK_SIZE as usize - 1) / BLOCK_SIZE as usize;
    let mut piece_data = vec![0u8; piece_len];
    let mut received = vec![false; num_blocks];
    let mut blocks_done = 0usize;
    let mut next_request = 0usize; // next block index to request
    let mut deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while blocks_done < num_blocks {
        // Fill the request pipeline.
        while next_request < num_blocks && (next_request - blocks_done) < config.pipeline {
            let begin = (next_request as u32) * BLOCK_SIZE;
            let length = if next_request == num_blocks - 1 {
                (piece_len - begin as usize) as u32
            } else {
                BLOCK_SIZE
            };
            conn.send(&Message::Request {
                index: piece_idx,
                begin,
                length,
            })
            .await?;
            next_request += 1;
        }

        // Read next message.
        match tokio::time::timeout_at(deadline, conn.read_message())
            .await
            .map_err(|_| Error::Peer("no piece data received for 30 seconds".into()))??
        {
            Some(Message::Piece { index, begin, data }) => {
                if index != piece_idx {
                    // Stale response for a different piece — ignore.
                    continue;
                }
                let block_idx = begin as usize / BLOCK_SIZE as usize;
                if block_idx < num_blocks && !received[block_idx] {
                    let start = begin as usize;
                    let end = (start + data.len()).min(piece_len);
                    piece_data[start..end].copy_from_slice(&data[..end - start]);
                    received[block_idx] = true;
                    blocks_done += 1;
                    deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                }
            }
            Some(Message::Choke) => {
                *choked = true;
                return Err(Error::Peer("choked mid-piece".into()));
            }
            Some(Message::Have(idx)) => {
                if let Some(b) = peer_bitfield.get_mut(idx as usize) {
                    *b = true;
                }
            }
            Some(Message::KeepAlive) | Some(Message::Extension { .. }) => {}
            None => return Err(Error::Peer("connection closed mid-piece".into())),
            Some(_) => {}
        }
    }

    Ok(piece_data)
}

// Tracker helpers
async fn announce_all(
    meta: &Metainfo,
    config: &Config,
    event: Event,
    downloaded: u64,
    uploaded: u64,
) -> (Vec<PeerAddr>, bool) {
    let mut all_peers = Vec::new();
    let mut any_responded = false;
    let mut announces = JoinSet::new();
    let info_hash = meta.info_hash;
    let peer_id = config.peer_id;
    let port = config.listen_port;
    // Unknown magnet size is not a completed download: trackers may omit
    // seed peers when we announce left=0.
    let left = if meta.piece_count() == 0 && event == Event::Started {
        1
    } else {
        meta.total_length.saturating_sub(downloaded)
    };
    // Independent tiers run concurrently; fallback within a tier stays ordered.
    for tier in &meta.trackers {
        let tier = tier.clone();
        announces.spawn(async move {
            for url in &tier {
                let req = AnnounceRequest {
                    tracker_url: url,
                    info_hash,
                    peer_id,
                    port,
                    uploaded,
                    downloaded,
                    left,
                    event: Some(event),
                    num_want: 200,
                };
                let result = if url.starts_with("udp://") || url.starts_with("UDP://") {
                    tracker::udp::announce(&req, url).await
                } else if url.starts_with("http://") || url.starts_with("https://") {
                    tracker::announce(&req).await
                } else {
                    tracing::debug!("Skipping unsupported tracker scheme: {url}");
                    continue;
                };
                match result {
                    Ok(resp) => {
                        if let Some(w) = &resp.warning {
                            tracing::warn!("Tracker {url}: warning: {w}");
                        }
                        if resp.peers.is_empty() {
                            tracing::warn!("Tracker {url}: responded with 0 peers");
                        } else {
                            tracing::info!("Tracker {}: {} peers", url, resp.peers.len());
                        }
                        return Some(resp.peers); // BEP-12: first responsive tracker in tier
                    }
                    Err(e) => tracing::warn!("Tracker {url}: {e}"),
                }
            }
            None
        });
    }
    while let Some(result) = announces.join_next().await {
        match result {
            Ok(Some(peers)) => {
                any_responded = true;
                all_peers.extend(peers);
            }
            Ok(None) => {}
            Err(e) => tracing::warn!("Tracker announce task failed: {e}"),
        }
    }

    // Deduplicate.
    all_peers.sort_unstable();
    all_peers.dedup();
    (all_peers, any_responded)
}

/// Fetch full `Metainfo` from peers using BEP-9 (ut_metadata extension).
/// Used when only a magnet link was provided.
async fn fetch_magnet_metadata(
    placeholder: &Metainfo,
    config: &Config,
    discovery: &mut PeerDiscovery,
    candidates: &mut PeerCandidates,
    on_progress: &(dyn Fn(DownloadStats) + Send + Sync),
    idle: &IdleTimer,
) -> Result<Metainfo> {
    let concurrency = config.max_peers.clamp(1, 8);
    let mut attempts = PeerTasks::new();
    let mut discovery_open = true;
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        while attempts.len() < concurrency {
            let Some(peer) = candidates.next() else {
                break;
            };
            let peer_id = config.peer_id;
            let info_hash = placeholder.info_hash;
            attempts.spawn(peer, async move {
                metadata_exchange::fetch_from_peer(&peer, &info_hash, &peer_id).await
            });
        }
        tokio::select! {
            batch = discovery.recv(), if discovery_open => {
                match batch {
                    Some(batch) => candidates.add(batch),
                    None => discovery_open = false,
                }
            }
            Some((peer, result)) = attempts.next(), if attempts.len() > 0 => {
                candidates.finished(peer);
                match result {
                    Ok(mut meta) => {
                        tracing::info!("Fetched magnet metadata from {peer}");
                        attempts.shutdown().await;
                        meta.trackers = placeholder.trackers.clone();
                        discovery.update(&meta, 0, 0);
                        return Ok(meta);
                    }
                    Err(e) => tracing::debug!("BEP-9 from {peer}: {e}"),
                }
            }
            _ = tick.tick() => {
                on_progress(DownloadStats {
                    downloaded: 0,
                    uploaded: 0,
                    total: 0,
                    progress: 0.0,
                    download_speed: 0.0,
                    upload_speed: 0.0,
                    peers_active: attempts.len(),
                    pieces_done: 0,
                    pieces_total: 0,
                    state: DownloadState::Connecting,
                });
                idle.check()?;
            }
        }
    }
}

async fn announce_all_no_err(
    meta: &Metainfo,
    config: &Config,
    event: Event,
    downloaded: u64,
    uploaded: u64,
) {
    let _ = announce_all(meta, config, event, downloaded, uploaded).await;
}

// Seeding
/// Listen for incoming peer connections and serve pieces we already have.
async fn seed_loop(
    meta: &Arc<Metainfo>,
    config: &Arc<Config>,
    file_writer: &Arc<FileWriter>,
    uploaded: &Arc<std::sync::atomic::AtomicU64>,
) -> Result<()> {
    let bind_addr = format!("0.0.0.0:{}", config.listen_port);
    let listener = TcpListener::bind(&bind_addr).await?;
    tracing::info!("Seeding on {bind_addr}");

    let seed_start = Instant::now();
    let seed_sem = Arc::new(tokio::sync::Semaphore::new(config.max_peers));
    let mut tasks: JoinSet<()> = JoinSet::new();

    loop {
        // Check seed stop conditions.
        if let Some(max_time) = config.seed_time {
            if seed_start.elapsed() >= max_time {
                tracing::info!("Seed time limit reached");
                break;
            }
        }
        if let Some(ratio) = config.seed_ratio {
            let up = uploaded.load(std::sync::atomic::Ordering::Relaxed);
            if meta.total_length > 0 && up as f64 / meta.total_length as f64 >= ratio {
                tracing::info!("Seed ratio {ratio} reached");
                break;
            }
        }

        let accept = timeout(Duration::from_secs(1), listener.accept()).await;
        match accept {
            Ok(Ok((stream, addr))) => {
                if let Ok(permit) = seed_sem.clone().try_acquire_owned() {
                    tracing::debug!("Seed: incoming connection from {addr}");
                    let meta_c = meta.clone();
                    let config_c = config.clone();
                    let fw_c = file_writer.clone();
                    let up_c = uploaded.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        if let Err(e) = serve_peer(stream, &meta_c, &config_c, &fw_c, &up_c).await {
                            tracing::debug!("Seed peer error: {e}");
                        }
                    });
                } else {
                    tracing::debug!("Seed: connection limit reached, dropping {addr}");
                }
            }
            Ok(Err(e)) => tracing::warn!("Accept error: {e}"),
            Err(_) => {} // timeout, loop
        }
        // Reap finished tasks.
        while tasks.try_join_next().is_some() {}
    }

    tasks.shutdown().await;
    Ok(())
}

/// Serve a single incoming peer connection (upload only).
async fn serve_peer(
    mut stream: TcpStream,
    meta: &Metainfo,
    config: &Config,
    file_writer: &FileWriter,
    uploaded: &std::sync::atomic::AtomicU64,
) -> Result<()> {
    stream.set_nodelay(true)?;

    // Handshake.
    let hs = handshake::perform(&mut stream, meta.info_hash, config.peer_id).await?;
    let _ = hs; // we don't need capabilities for seeding

    let mut conn = PeerConn::new(stream);

    // Send our BITFIELD (we have everything).
    let bitfield = {
        let total_bytes = (meta.piece_count() as usize + 7) / 8;
        let mut bf = vec![0u8; total_bytes];
        for i in 0..meta.piece_count() as usize {
            bf[i / 8] |= 1 << (7 - (i % 8));
        }
        bf
    };
    conn.send(&Message::Bitfield(bitfield)).await?;
    conn.send(&Message::Unchoke).await?;

    loop {
        let msg = conn.read_message_timeout(Duration::from_secs(120)).await?;
        match msg {
            Some(Message::Request {
                index,
                begin,
                length,
            }) => {
                // Guard: valid request.
                if index >= meta.piece_count() || length == 0 || length > 1 << 17 {
                    return Err(Error::Peer("invalid REQUEST".into()));
                }
                if begin as u64 + length as u64 > meta.piece_len(index) {
                    return Err(Error::Peer("REQUEST out of piece bounds".into()));
                }
                let flat_offset = index as u64 * meta.piece_length + begin as u64;
                let data = file_writer.read_data(flat_offset, length as usize).await?;
                conn.send(&Message::Piece {
                    index,
                    begin,
                    data: bytes::Bytes::from(data),
                })
                .await?;
                uploaded.fetch_add(length as u64, std::sync::atomic::Ordering::Relaxed);
            }
            Some(Message::Interested) => {
                conn.send(&Message::Unchoke).await?;
            }
            Some(Message::KeepAlive) => {}
            Some(Message::Choke) | Some(Message::NotInterested) => break,
            None => break,
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::{io::AsyncReadExt, net::UdpSocket};

    fn test_metainfo(magnet: bool) -> Metainfo {
        if magnet {
            Metainfo::placeholder_magnet(&crate::magnet::MagnetLink {
                info_hash: InfoHash([1; 20]),
                name: None,
                trackers: Vec::new(),
            })
        } else {
            let mut info = b"d6:lengthi4e4:name8:test.txt12:piece lengthi4e6:pieces20:".to_vec();
            info.extend_from_slice(&Sha1::digest(b"test"));
            info.push(b'e');
            Metainfo::from_info_bytes(&info, &InfoHash(Sha1::digest(&info).into())).unwrap()
        }
    }

    #[tokio::test]
    async fn verified_pieces_reset_idle_timeout_during_longer_download() {
        timeout(Duration::from_secs(5), async {
            let mut info = b"d6:lengthi8e4:name8:test.txt12:piece lengthi4e6:pieces40:".to_vec();
            info.extend_from_slice(&Sha1::digest(b"test"));
            info.extend_from_slice(&Sha1::digest(b"test"));
            info.push(b'e');
            let info_hash = InfoHash(Sha1::digest(&info).into());
            let meta = Metainfo::from_info_bytes(&info, &info_hash).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                stream.read_exact(&mut [0u8; 96]).await.unwrap();
                drop(stream);
                let (mut stream, _) = listener.accept().await.unwrap();
                handshake::perform(&mut stream, info_hash, PeerId([7; 20])).await.unwrap();
                let mut conn = PeerConn::new(stream);
                conn.send(&Message::Bitfield(vec![0xc0])).await.unwrap();
                conn.send(&Message::Unchoke).await.unwrap();
                assert!(matches!(conn.read_message().await.unwrap(), Some(Message::Interested)));
                for index in 0..2 {
                    assert!(matches!(
                        conn.read_message().await.unwrap(),
                        Some(Message::Request { index: received, begin: 0, length: 4 }) if received == index
                    ));
                    tokio::time::sleep(Duration::from_millis(1100)).await;
                    conn.send(&Message::Piece {
                        index,
                        begin: 0,
                        data: Bytes::from_static(b"test"),
                    }).await.unwrap();
                }
            });
            let (discovery, sender) = PeerDiscovery::injected(&meta);
            sender.send(vec![PeerAddr { ip: addr.ip(), port: addr.port() }]).await.unwrap();
            let dir = tempfile::tempdir().unwrap();
            download_with_discovery(
                Arc::new(meta),
                Config {
                    idle_timeout: Some(Duration::from_secs(2)),
                    download_dir: dir.path().to_owned(),
                    ..Config::default()
                },
                |_| {},
                discovery,
            ).await.unwrap();
            assert_eq!(tokio::fs::read(dir.path().join("test.txt")).await.unwrap(), b"testtest");
            server.await.unwrap();
        }).await.expect("download with regular piece progress timed out");
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_stops_metadata_and_stalled_downloads_and_closes_discovery() {
        for magnet in [true, false] {
            let meta = test_metainfo(magnet);
            let dir = tempfile::tempdir().unwrap();
            let (discovery, sender) = PeerDiscovery::injected(&meta);
            let repeated = sender.clone();
            let mut source = JoinSet::new();
            source.spawn(async move {
                while repeated.send(Vec::new()).await.is_ok() {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            });
            let result = download_with_discovery(
                Arc::new(meta),
                Config {
                    idle_timeout: Some(Duration::from_secs(3)),
                    download_dir: dir.path().to_owned(),
                    ..Config::default()
                },
                |stats| {
                    assert_eq!(stats.state, DownloadState::Connecting);
                    assert_eq!(stats.downloaded, 0);
                },
                discovery,
            )
            .await;
            assert!(
                matches!(result, Err(Error::IdleTimeout(duration)) if duration == Duration::from_secs(3))
            );
            assert!(sender.is_closed());
            source.shutdown().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn default_waits_indefinitely_and_cancellation_closes_discovery() {
        for magnet in [true, false] {
            let meta = test_metainfo(magnet);
            let dir = tempfile::tempdir().unwrap();
            let (discovery, sender) = PeerDiscovery::injected(&meta);
            let reports = Arc::new(AtomicUsize::new(0));
            let on_progress = reports.clone();
            let task = tokio::spawn(download_with_discovery(
                Arc::new(meta),
                Config {
                    download_dir: dir.path().to_owned(),
                    ..Config::default()
                },
                move |_| {
                    on_progress.fetch_add(1, Ordering::Relaxed);
                },
                discovery,
            ));
            while reports.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
            tokio::time::advance(Duration::from_secs(3600)).await;
            tokio::task::yield_now().await;
            assert!(!task.is_finished());
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert!(sender.is_closed());
        }
    }

    #[tokio::test]
    async fn fetches_metadata_beyond_first_twenty_peers_without_waiting_for_silent_peer() {
        timeout(Duration::from_secs(5), async {
            let mut info = b"d6:lengthi4e4:name8:test.txt12:piece lengthi4e6:pieces20:".to_vec();
            info.extend_from_slice(&Sha1::digest(b"test"));
            info.push(b'e');
            let info_hash = InfoHash(Sha1::digest(&info).into());
            let mut peers = Vec::new();
            let mut servers = JoinSet::new();

            let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = silent.local_addr().unwrap();
            peers.push(PeerAddr {
                ip: addr.ip(),
                port: addr.port(),
            });
            let (closed, wait_for_close) = tokio::sync::oneshot::channel();
            servers.spawn(async move {
                let (mut stream, _) = silent.accept().await.unwrap();
                let mut handshake = [0u8; 68];
                stream.read_exact(&mut handshake).await.unwrap();
                // A successful attempt must cancel this stalled connection.
                assert_eq!(stream.read(&mut handshake).await.unwrap(), 0);
                closed.send(()).unwrap();
            });
            for _ in 0..21 {
                let rejected = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = rejected.local_addr().unwrap();
                peers.push(PeerAddr {
                    ip: addr.ip(),
                    port: addr.port(),
                });
                servers.spawn(async move {
                    let (mut stream, _) = rejected.accept().await.unwrap();
                    stream.read_exact(&mut [0u8; 68]).await.unwrap();
                });
            }
            let healthy = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = healthy.local_addr().unwrap();
            peers.push(PeerAddr {
                ip: addr.ip(),
                port: addr.port(),
            });
            servers.spawn(async move {
                let (mut stream, _) = healthy.accept().await.unwrap();
                handshake::perform(&mut stream, info_hash, PeerId([7; 20]))
                    .await
                    .unwrap();
                let mut conn = PeerConn::new(stream);
                assert!(matches!(
                    conn.read_message().await.unwrap(),
                    Some(Message::Extension { ext_id: 0, .. })
                ));
                conn.send(&Message::Extension {
                    ext_id: 0,
                    payload: Bytes::from(format!(
                        "d1:md11:ut_metadatai42ee13:metadata_sizei{}ee",
                        info.len()
                    )),
                })
                .await
                .unwrap();
                assert!(matches!(
                    conn.read_message().await.unwrap(),
                    Some(Message::Extension { ext_id: 42, .. })
                ));
                let mut payload = b"d8:msg_typei1e5:piecei0ee".to_vec();
                payload.extend_from_slice(&info);
                conn.send(&Message::Extension {
                    ext_id: 1,
                    payload: Bytes::from(payload),
                })
                .await
                .unwrap();
            });
            let placeholder = Metainfo::placeholder_magnet(&crate::magnet::MagnetLink {
                info_hash,
                name: None,
                trackers: Vec::new(),
            });
            let (mut discovery, sender) = PeerDiscovery::injected(&placeholder);
            sender.send(peers).await.unwrap();
            let meta = fetch_magnet_metadata(
                &placeholder,
                &Config::default(),
                &mut discovery,
                &mut PeerCandidates::default(),
                &|_| {},
                &IdleTimer::new(None),
            )
            .await
            .unwrap();
            assert_eq!(meta.info_hash, info_hash);
            assert_eq!(meta.name, "test.txt");
            assert_eq!(meta.total_length, 4);
            wait_for_close.await.unwrap();
            while let Some(result) = servers.join_next().await {
                result.unwrap();
            }
        })
        .await
        .expect("a stalled peer must not block metadata from later peers");
    }

    #[tokio::test]
    async fn announces_independent_tracker_tiers_concurrently() {
        timeout(Duration::from_secs(5), async {
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let mut trackers = Vec::new();
            let mut servers = JoinSet::new();
            for _ in 0..2 {
                let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                trackers.push(vec![format!("udp://{}", socket.local_addr().unwrap())]);
                let barrier = barrier.clone();
                servers.spawn(async move {
                    let mut buf = [0u8; 1024];
                    let (_, client) = socket.recv_from(&mut buf).await.unwrap();
                    // Neither tracker responds until both tiers have been contacted.
                    barrier.wait().await;
                    let mut response = buf[8..16].to_vec();
                    response.extend_from_slice(&1u64.to_be_bytes());
                    socket.send_to(&response, client).await.unwrap();
                    let (_, client) = socket.recv_from(&mut buf).await.unwrap();
                    let mut response = buf[8..16].to_vec();
                    response.extend_from_slice(&1800u32.to_be_bytes());
                    response.extend_from_slice(&0u64.to_be_bytes());
                    response.extend_from_slice(&[127, 0, 0, 1, 0x1a, 0xe1]);
                    socket.send_to(&response, client).await.unwrap();
                });
            }
            let mut meta = Metainfo::placeholder_magnet(&crate::magnet::MagnetLink {
                info_hash: InfoHash([1; 20]),
                name: None,
                trackers: Vec::new(),
            });
            meta.trackers = trackers;
            let (peers, responded) =
                announce_all(&meta, &Config::default(), Event::Started, 0, 0).await;
            assert!(responded);
            assert_eq!(peers.len(), 1);
            assert_eq!(
                SocketAddr::from(peers[0]),
                "127.0.0.1:6881".parse().unwrap()
            );
            while let Some(result) = servers.join_next().await {
                result.unwrap();
            }
        })
        .await
        .expect("tracker tiers were not contacted concurrently");
    }

    #[tokio::test]
    async fn falls_back_within_tier_and_stops_at_first_responsive_tracker() {
        timeout(Duration::from_secs(5), async {
            let rejected = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let responsive = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let unused = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut meta = Metainfo::placeholder_magnet(&crate::magnet::MagnetLink {
                info_hash: InfoHash([1; 20]),
                name: None,
                trackers: Vec::new(),
            });
            meta.trackers = vec![vec![
                format!("udp://{}", rejected.local_addr().unwrap()),
                format!("udp://{}", responsive.local_addr().unwrap()),
                format!("udp://{}", unused.local_addr().unwrap()),
            ]];
            let server = tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let (_, client) = rejected.recv_from(&mut buf).await.unwrap();
                let mut response = 3u32.to_be_bytes().to_vec();
                response.extend_from_slice(&buf[12..16]);
                response.extend_from_slice(b"denied");
                rejected.send_to(&response, client).await.unwrap();
                let (_, client) = responsive.recv_from(&mut buf).await.unwrap();
                let mut response = buf[8..16].to_vec();
                response.extend_from_slice(&1u64.to_be_bytes());
                responsive.send_to(&response, client).await.unwrap();
                let (_, client) = responsive.recv_from(&mut buf).await.unwrap();
                let mut response = buf[8..16].to_vec();
                response.extend_from_slice(&1800u32.to_be_bytes());
                response.extend_from_slice(&0u64.to_be_bytes());
                responsive.send_to(&response, client).await.unwrap();
            });
            // A responsive tracker with zero peers still ends tier fallback.
            let (peers, responded) =
                announce_all(&meta, &Config::default(), Event::Completed, 0, 0).await;
            assert!(responded);
            assert!(peers.is_empty());
            assert!(matches!(
                unused.try_recv_from(&mut [0u8; 1024]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
            ));
            server.await.unwrap();
        })
        .await
        .expect("tracker tier fallback timed out");
    }

    #[tokio::test]
    async fn downloads_magnet_after_empty_swarm_and_later_recovers_from_stalled_data_peer() {
        timeout(Duration::from_secs(5), async {
            let content = b"test";
            let mut info = b"d6:lengthi4e4:name8:test.txt12:piece lengthi4e6:pieces20:".to_vec();
            info.extend_from_slice(&Sha1::digest(content));
            info.push(b'e');
            let info_hash = InfoHash(Sha1::digest(&info).into());
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let peer_addr = listener.local_addr().unwrap();
            let data_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let data_addr = data_listener.local_addr().unwrap();
            let tracker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let tracker_url = format!(
                "udp://localhost:{}/announce",
                tracker.local_addr().unwrap().port()
            );

            let tracker_task = tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                for (left, event, peer) in [
                    (1u64, 2u32, None),
                    (1, 0, Some(peer_addr)),
                    (4, 0, Some(data_addr)),
                    (0, 1, Some(data_addr)),
                ] {
                    let (n, client) = tracker.recv_from(&mut buf).await.unwrap();
                    assert_eq!(n, 16);
                    assert_eq!(&buf[..8], &0x0000041727101980u64.to_be_bytes());
                    let mut response = buf[8..16].to_vec();
                    response.extend_from_slice(&1u64.to_be_bytes());
                    tracker.send_to(&response, client).await.unwrap();

                    let (n, client) = tracker.recv_from(&mut buf).await.unwrap();
                    assert_eq!(n, 98);
                    assert_eq!(&buf[16..36], info_hash.as_bytes());
                    assert_eq!(&buf[64..72], &left.to_be_bytes());
                    assert_eq!(&buf[80..84], &event.to_be_bytes());
                    let mut response = buf[8..16].to_vec();
                    response.extend_from_slice(&1u32.to_be_bytes());
                    response.extend_from_slice(&0u32.to_be_bytes());
                    response.extend_from_slice(&1u32.to_be_bytes());
                    if let Some(peer) = peer {
                        response.extend_from_slice(&[127, 0, 0, 1]);
                        response.extend_from_slice(&peer.port().to_be_bytes());
                    }
                    tracker.send_to(&response, client).await.unwrap();
                }
            });

            let peer_task = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                handshake::perform(&mut stream, info_hash, PeerId([7; 20]))
                    .await
                    .unwrap();
                let mut conn = PeerConn::new(stream);
                assert!(matches!(
                    conn.read_message().await.unwrap(),
                    Some(Message::Extension { ext_id: 0, .. })
                ));
                conn.send(&Message::Extension {
                    ext_id: 0,
                    payload: Bytes::from(format!(
                        "d1:md11:ut_metadatai42ee13:metadata_sizei{}ee",
                        info.len()
                    )),
                })
                .await
                .unwrap();
                let Some(Message::Extension { ext_id, payload }) =
                    conn.read_message().await.unwrap()
                else {
                    panic!("expected metadata request");
                };
                assert_eq!(ext_id, 42);
                assert_eq!(&payload[..], b"d8:msg_typei0e5:piecei0ee");
                let mut payload =
                    format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len()).into_bytes();
                payload.extend_from_slice(&info);
                conn.send(&Message::Extension {
                    ext_id: 1,
                    payload: Bytes::from(payload),
                })
                .await
                .unwrap();
                drop(conn);
                // This metadata peer goes offline before file transfer.
                drop(listener);

                // Reject MSE so the downloader reconnects using plaintext.
                let (mut stream, _) = data_listener.accept().await.unwrap();
                stream.read_exact(&mut [0u8; 96]).await.unwrap();
                drop(stream);
                let (mut stream, _) = data_listener.accept().await.unwrap();
                handshake::perform(&mut stream, info_hash, PeerId([7; 20]))
                    .await
                    .unwrap();
                let mut conn = PeerConn::new(stream);
                conn.send(&Message::Bitfield(vec![0x80])).await.unwrap();
                conn.send(&Message::Unchoke).await.unwrap();
                assert!(matches!(
                    conn.read_message().await.unwrap(),
                    Some(Message::Interested)
                ));
                assert!(matches!(
                    conn.read_message().await.unwrap(),
                    Some(Message::Request {
                        index: 0,
                        begin: 0,
                        length: 4
                    })
                ));
                conn.send(&Message::Piece {
                    index: 0,
                    begin: 0,
                    data: Bytes::from_static(content),
                })
                .await
                .unwrap();
            });

            let encoded_tracker: String =
                url::form_urlencoded::byte_serialize(tracker_url.as_bytes()).collect();
            let source = format!(
                "magnet:?xt=urn:btih:{}&dn=Test%20Download&tr={encoded_tracker}",
                info_hash.to_hex().to_uppercase()
            );
            let (mut placeholder, _) = crate::resolve_source(&source).await.unwrap();
            placeholder.is_private = true; // Keep this local fixture off the public DHT.
            let dir = tempfile::tempdir().unwrap();
            let config = Config {
                download_dir: dir.path().to_owned(),
                idle_timeout: Some(Duration::from_secs(2)),
                ..Config::default()
            };
            download(Arc::new(placeholder), config, |stats| {
                assert!(stats.total == 0 || stats.total == 4);
            })
            .await
            .unwrap();
            assert_eq!(
                tokio::fs::read(dir.path().join("test.txt")).await.unwrap(),
                content
            );
            tracker_task.await.unwrap();
            peer_task.await.unwrap();
        })
        .await
        .expect("local magnet download timed out");
    }
}
