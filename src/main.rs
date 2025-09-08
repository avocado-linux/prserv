use clap::Parser;
use tracing::{debug, info};

mod args;
mod buffered_writes;
mod client;
mod commands;
mod config;
mod constants;
mod database;
mod env_vars;
mod error;
mod server;

use args::{effective_log_level, GlobalArgs};
use commands::Commands;
use config::Config;
use error::Result;

#[derive(Parser, Debug)]
#[command(name = env!("CARGO_PKG_NAME"))]
#[command(about = "Package Revision (PR) server for BitBake builds")]
#[command(version)]
struct Cli {
    #[command(flatten)]
    global: GlobalArgs,

    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("[ERROR] {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    // Initialize tracing based on effective log level
    let log_level = effective_log_level(&cli.global);
    init_tracing(log_level);

    // Load configuration
    let mut config = Config::load(&cli.global.config)?;
    config.merge_env()?;

    // Log configuration file being used
    info!("Using configuration file: {}", cli.global.config);

    debug!("CLI arguments: {:?}", cli);
    debug!("Configuration: {:?}", config);
    info!("Starting command execution.");

    match cli.command {
        Commands::Server(args) => commands::server::execute(args).await,
        Commands::Client(args) => commands::client::execute(args).await,
    }
}

fn init_tracing(log_level: args::LogLevel) {
    let filter = log_level.as_filter();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter)),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_writer(std::io::stderr)
        .compact()
        .init();

    debug!("Logging initialized at level: {}", log_level);
}
