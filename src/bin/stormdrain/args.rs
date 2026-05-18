use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "stormdrain",
    about = "A curl-like BitTorrent client",
    long_about = "Download a single torrent from a .torrent file, HTTP URL, or magnet link.\n\
                  Analogous to 'curl' or 'wget' but for the BitTorrent protocol.",
    version
)]
pub struct Args {
    /// Source: path to a .torrent file, HTTP(S) URL to a .torrent, or magnet link.
    #[arg(value_name = "SOURCE")]
    pub source: String,

    /// Directory to save downloaded files [default: current directory].
    #[arg(short = 'w', long = "download-dir", value_name = "DIR")]
    pub download_dir: Option<PathBuf>,

    /// Incoming peer port advertised to trackers [default: 6881].
    #[arg(
        short = 'p',
        long = "port",
        value_name = "PORT",
        default_value_t = 6881
    )]
    pub port: u16,

    /// Maximum download speed in KB/s (0 = unlimited).
    #[arg(
        short = 'd',
        long = "max-download",
        value_name = "KBPS",
        default_value_t = 0
    )]
    pub max_download: u64,

    /// Maximum upload speed in KB/s (0 = unlimited, upload not implemented in v0.1).
    #[arg(
        short = 'u',
        long = "max-upload",
        value_name = "KBPS",
        default_value_t = 0
    )]
    pub max_upload: u64,

    /// Download pieces in sequential order (useful for streaming).
    #[arg(long = "sequential")]
    pub sequential: bool,

    /// Seed after completing the download.
    #[arg(short = 's', long = "seed")]
    pub seed: bool,

    /// Stop seeding when upload/download ratio reaches this value.
    #[arg(long = "seed-ratio", value_name = "RATIO")]
    pub seed_ratio: Option<f64>,

    /// Stop seeding after this many minutes.
    #[arg(long = "seed-time", value_name = "MINUTES")]
    pub seed_time: Option<u64>,

    /// Suppress the progress bar and only print errors.
    #[arg(short = 'q', long = "quiet")]
    pub quiet: bool,

    /// Enable verbose/debug logging (can also set RUST_LOG env var).
    #[arg(short = 'v', long = "verbose")]
    pub verbose: bool,
}
