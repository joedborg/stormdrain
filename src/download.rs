//! Download orchestrator.
//!
//! Coordinates tracker announces, peer connections, piece scheduling,
//! SHA-1 verification and file writing for a single torrent.
//!
//! The public surface is intentionally small: callers pass in a `Metainfo`
//! and a `Config` and receive periodic `DownloadStats` via a callback.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use sha1::{Digest, Sha1};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinSet,
    time::{sleep, timeout},
};

use crate::{
    dht,
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

// Entry point
/// Download a single torrent, calling `on_progress` approximately once per
/// second.  Returns when all pieces have been verified and written.
pub async fn download(
    meta: Arc<Metainfo>,
    config: Config,
    on_progress: impl Fn(DownloadStats) + Send + Sync + 'static,
) -> Result<()> {
    // Magnet resolution
    // A placeholder Metainfo (from a magnet link) has no pieces; we must fetch
    // the info dict from peers via BEP-9 before we can start downloading.
    let meta = if meta.piece_count() == 0 {
        tracing::info!("Magnet link — fetching metadata via BEP-9...");
        let real = fetch_magnet_metadata(&meta, &config).await?;
        tracing::info!("Fetched metadata: '{}'", real.name);
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

    let on_progress = Arc::new(on_progress);
    let config = Arc::new(config);

    // Tracker announce(s)
    let (peers, any_tracker_responded) = announce_all(&meta, &config, Event::Started, 0, 0).await;
    if peers.is_empty() {
        return Err(if any_tracker_responded {
            Error::EmptySwarm
        } else {
            Error::NoTrackers
        });
    }
    tracing::info!("Got {} peer(s) from tracker(s)", peers.len());

    // Peer task pool
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_peers));
    let mut join_set: JoinSet<()> = JoinSet::new();

    let mut last_downloaded: u64 = 0;
    let mut last_uploaded: u64 = 0;
    let mut speed_ema: f64 = 0.0;
    let mut up_speed_ema: f64 = 0.0;
    let mut last_saved_pieces: u32 = piece_mgr.lock().await.done_count();
    let active_peers = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    for peer_addr in peers.into_iter().take(config.max_peers * 3) {
        // Don't spawn more tasks than we can run concurrently.
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");

        // Short-circuit if already complete.
        if piece_mgr.lock().await.is_complete() {
            break;
        }

        let meta_c = meta.clone();
        let config_c = config.clone();
        let piece_mgr_c = piece_mgr.clone();
        let file_writer_c = file_writer.clone();
        let active_c = active_peers.clone();

        join_set.spawn(async move {
            let _permit = permit; // dropped when task ends
            active_c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let res = run_peer(peer_addr, &meta_c, &config_c, &piece_mgr_c, &file_writer_c).await;
            if let Err(e) = res {
                tracing::debug!("peer task: {e}");
            }
            active_c.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    // Progress reporting loop
    loop {
        sleep(Duration::from_millis(500)).await;

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
            let _ = cp.save(&config.download_dir).await;
            last_saved_pieces = pieces_done;
        }

        let delta = downloaded.saturating_sub(last_downloaded);
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

        let peers_active = active_peers.load(std::sync::atomic::Ordering::Relaxed);
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

        // If all spawned tasks are done but not complete, we ran out of peers.
        if join_set.is_empty() && !complete {
            return Err(Error::Stalled);
        }

        // Reap any finished tasks.
        while join_set.try_join_next().is_some() {}
    }

    // Let remaining peer tasks wind down naturally.
    join_set.shutdown().await;

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
    let _ = ResumeState::remove(&config.download_dir, &meta.info_hash).await;

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
    let hs = handshake::perform(&mut s, info_hash, peer_id).await?;
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
    let (hs, mut conn) = match mse::perform_initiator(stream, &meta.info_hash).await {
        Ok(mut mse_stream) => {
            match handshake::perform(&mut mse_stream, meta.info_hash, config.peer_id).await {
                Ok(hs) => (hs, PeerConn::new(mse_stream)),
                // MSE succeeded but BEP-3 handshake failed — fall back to plain TCP.
                Err(_) => connect_plain(sock_addr, meta.info_hash, config.peer_id).await?,
            }
        }
        // MSE negotiation failed — reconnect over plain TCP.
        Err(_) => connect_plain(sock_addr, meta.info_hash, config.peer_id).await?,
    };
    tracing::debug!(
        "Handshake OK with {:?} ext={}",
        PeerId(hs.peer_id),
        hs.capabilities.extension_protocol,
    );

    // Receive initial messages (bitfield / have)
    let mut peer_bitfield = vec![false; meta.piece_count() as usize];

    // Give the peer up to 3 s to send a BITFIELD before we start.
    let deadline = Duration::from_secs(3);
    loop {
        match conn.read_message_timeout(deadline).await {
            Ok(Some(Message::Bitfield(bits))) => {
                peer_bitfield = PieceManager::bitfield_to_vec(&bits, meta.piece_count());
                break;
            }
            Ok(Some(Message::Have(idx))) => {
                if let Some(b) = peer_bitfield.get_mut(idx as usize) {
                    *b = true;
                }
            }
            Ok(Some(Message::KeepAlive)) => {}
            Ok(Some(Message::Extension { .. })) => {} // ignore for now
            Ok(Some(_)) | Ok(None) => break,          // not a bitfield, proceed
            Err(_) => break,                          // timeout
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
        match conn.read_message_timeout(Duration::from_secs(30)).await? {
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
    let left = meta.total_length.saturating_sub(downloaded);
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

    // DHT fallback: if no peers from any tracker, query the DHT.
    if all_peers.is_empty() && event == Event::Started {
        tracing::warn!("No tracker peers; trying DHT...");
        match dht::Dht::start().await {
            Ok(dht) => {
                let peers = dht.get_peers(meta.info_hash, 50).await;
                tracing::warn!("DHT: {} peers", peers.len());
                all_peers.extend(peers);
            }
            Err(e) => tracing::warn!("DHT start failed: {e}"),
        }
    }

    // Deduplicate.
    all_peers.sort_unstable();
    all_peers.dedup();
    (all_peers, any_responded)
}

/// Fetch full `Metainfo` from peers using BEP-9 (ut_metadata extension).
/// Used when only a magnet link was provided.
async fn fetch_magnet_metadata(placeholder: &Metainfo, config: &Config) -> Result<Metainfo> {
    // Collect peers from tracker(s) embedded in the magnet link, then DHT.
    let (mut peers, _) = announce_all(placeholder, config, Event::Started, 0, 0).await;
    if peers.is_empty() {
        // Trackers might be missing (DHT-only magnet).
        match dht::Dht::start().await {
            Ok(dht) => peers.extend(dht.get_peers(placeholder.info_hash, 50).await),
            Err(e) => tracing::debug!("DHT start failed: {e}"),
        }
    }
    if peers.is_empty() {
        return Err(Error::NoTrackers);
    }
    for peer in peers.iter().take(20) {
        match metadata_exchange::fetch_from_peer(peer, &placeholder.info_hash, &config.peer_id)
            .await
        {
            Ok(mut meta) => {
                // BEP-9 transfers only the info dict, not the tracker URLs.
                meta.trackers = placeholder.trackers.clone();
                return Ok(meta);
            }
            Err(e) => tracing::debug!("BEP-9 from {peer}: {e}"),
        }
    }
    Err(Error::Peer(
        "could not fetch torrent metadata from any peer".into(),
    ))
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
    use tokio::{io::AsyncReadExt, net::UdpSocket};

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
    async fn downloads_magnet_using_hostname_tracker_after_metadata_exchange() {
        timeout(Duration::from_secs(5), async {
            let content = b"test";
            let mut info = b"d6:lengthi4e4:name8:test.txt12:piece lengthi4e6:pieces20:".to_vec();
            info.extend_from_slice(&Sha1::digest(content));
            info.push(b'e');
            let info_hash = InfoHash(Sha1::digest(&info).into());
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let peer_addr = listener.local_addr().unwrap();
            let tracker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let tracker_url = format!(
                "udp://localhost:{}/announce",
                tracker.local_addr().unwrap().port()
            );

            let tracker_task = tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                for left in [0u64, 4, 0] {
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
                    let mut response = buf[8..16].to_vec();
                    response.extend_from_slice(&1800u32.to_be_bytes());
                    response.extend_from_slice(&0u32.to_be_bytes());
                    response.extend_from_slice(&1u32.to_be_bytes());
                    response.extend_from_slice(&[127, 0, 0, 1]);
                    response.extend_from_slice(&peer_addr.port().to_be_bytes());
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

                // Reject MSE so the downloader reconnects using plaintext.
                let (mut stream, _) = listener.accept().await.unwrap();
                stream.read_exact(&mut [0u8; 96]).await.unwrap();
                drop(stream);
                let (mut stream, _) = listener.accept().await.unwrap();
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
            let (placeholder, _) = crate::resolve_source(&source).await.unwrap();
            let dir = tempfile::tempdir().unwrap();
            let config = Config {
                download_dir: dir.path().to_owned(),
                ..Config::default()
            };
            download(Arc::new(placeholder), config, |stats| {
                assert_eq!(stats.total, 4);
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
