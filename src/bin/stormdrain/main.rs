//! stormdrain CLI — download a single torrent like curl/wget.
//!
//! Usage:
//!   stormdrain [OPTIONS] <URL|FILE|MAGNET>

mod args;
mod display;

use std::{path::PathBuf, sync::Arc};

use clap::Parser;
use tokio::signal;
use tracing_subscriber::EnvFilter;

use stormdrain::{
    download::{self, Config, DownloadStats},
    peer::id as peer_id,
    resolve_source,
};

use args::Args;
use display::{build_progress_bar, human_size};

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Set up logging.
    let filter = if args.verbose {
        "stormdrain=debug"
    } else {
        "stormdrain=warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    if let Err(e) = run(args).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    // Resolve source
    if !args.quiet {
        eprint!("Resolving {}…", args.source);
    }

    let (meta, _magnet) = resolve_source(&args.source)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let meta = Arc::new(meta);

    if !args.quiet {
        eprintln!(" done");
        eprintln!("  Name:        {}", meta.name);
        eprintln!("  Size:        {}", human_size(meta.total_length));
        eprintln!(
            "  Pieces:      {} × {}",
            meta.piece_count(),
            human_size(meta.piece_length)
        );
        eprintln!("  Info hash:   {}", meta.info_hash);
        if let Some(ref by) = meta.created_by {
            eprintln!("  Created by:  {by}");
        }
    }

    let download_dir = args.download_dir.unwrap_or_else(|| PathBuf::from("."));

    // Progress bar (None in quiet mode)
    let pb_arc = if args.quiet {
        None
    } else {
        Some(build_progress_bar(meta.total_length))
    };

    // Build config
    let config = Config {
        download_dir,
        listen_port: args.port,
        peer_id: peer_id::generate(),
        pipeline: 5,
        max_peers: 50,
        seed: args.seed,
        seed_ratio: args.seed_ratio,
        seed_time: args
            .seed_time
            .map(|m| std::time::Duration::from_secs(m * 60)),
    };

    // Progress callback
    let quiet = args.quiet;
    let pb_cb = pb_arc.clone();
    let on_progress = move |stats: DownloadStats| {
        if let Some(ref pb) = pb_cb {
            pb.set_position(stats.downloaded);
            pb.set_message(format!("{} peer(s)", stats.peers_active));
        } else if !quiet {
            eprint!(
                "\rProgress: {:.1}%  {:.1} KB/s  {} peers{:20}",
                stats.progress * 100.0,
                stats.download_speed / 1024.0,
                stats.peers_active,
                "",
            );
        }
    };

    // Run download with Ctrl-C handling
    let meta_c = meta.clone();
    let download_fut = download::download(meta_c, config, on_progress);

    tokio::select! {
        result = download_fut => {
            match result {
                Ok(()) => {
                    if let Some(ref pb) = pb_arc {
                        pb.finish_with_message("complete");
                    } else if !args.quiet {
                        eprintln!("\nDownload complete.");
                    }
                }
                Err(e) => {
                    if let Some(ref pb) = pb_arc {
                        pb.abandon_with_message(format!("error: {e}"));
                    }
                    return Err(anyhow::anyhow!("{e}"));
                }
            }
        }
        _ = signal::ctrl_c() => {
            if let Some(ref pb) = pb_arc {
                pb.abandon_with_message("interrupted");
            } else {
                eprintln!("\nInterrupted.");
            }
            std::process::exit(130);
        }
    }

    Ok(())
}
