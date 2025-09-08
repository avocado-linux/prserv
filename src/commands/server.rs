//! Server command implementation

use crate::config::SyncMode;
use crate::constants;
use crate::env_vars;
use crate::error::Result;
use crate::server::{start_server, ServerConfig};
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::info;

#[derive(Parser, Debug)]
pub struct Args {
    /// Database file path
    #[arg(
        short,
        long,
        default_value = constants::DEFAULT_DATABASE_FILE,
        env = env_vars::DATABASE
    )]
    pub database: PathBuf,

    /// Bind address for the server
    #[arg(
        short,
        long,
        default_value = constants::DEFAULT_BIND_ADDR,
        env = env_vars::BIND_ADDR
    )]
    pub bind: SocketAddr,

    /// Run in read-only mode
    #[arg(long, env = env_vars::READ_ONLY)]
    pub read_only: bool,

    /// Use no-history mode (no decrements allowed)
    #[arg(long, env = env_vars::NOHIST)]
    pub nohist: bool,

    /// Database synchronization mode: immediate, periodic:<seconds>, or on_shutdown
    #[arg(
        long,
        default_value = "immediate",
        env = env_vars::SYNC_MODE,
        value_parser = parse_sync_mode,
        help = "Database sync mode: 'immediate' (safest), 'periodic:<seconds>' (balanced), or 'on_shutdown' (fastest)"
    )]
    pub sync_mode: SyncMode,
}

/// Parse sync mode from string for clap
fn parse_sync_mode(s: &str) -> Result<SyncMode> {
    SyncMode::from_str(s)
}

pub async fn execute(args: Args) -> Result<()> {
    info!("Starting PR server...");
    info!("Database: {}", args.database.display());
    info!("Bind address: {}", args.bind);
    info!("Read-only: {}", args.read_only);
    info!("No-history mode: {}", args.nohist);
    info!("Sync mode: {:?}", args.sync_mode);

    let config = ServerConfig {
        bind_addr: args.bind,
        db_path: args.database.to_string_lossy().to_string(),
        read_only: args.read_only,
        nohist: args.nohist,
        sync_mode: args.sync_mode,
    };

    start_server(config).await?;
    Ok(())
}
