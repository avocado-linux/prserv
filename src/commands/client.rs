//! Client command implementation

use crate::client::PrClient;
use crate::constants;
use crate::env_vars;
use crate::error::Result;
use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use tracing::info;

#[derive(Parser, Debug)]
pub struct Args {
    /// Server address to connect to
    #[arg(
        short,
        long,
        default_value = constants::DEFAULT_SERVER_ADDR,
        env = env_vars::SERVER_ADDR
    )]
    pub server: SocketAddr,

    #[command(subcommand)]
    pub command: ClientCommands,
}

#[derive(Subcommand, Debug)]
pub enum ClientCommands {
    /// Get a PR value (creates if doesn't exist)
    GetPr {
        /// Package version
        version: String,
        /// Package architecture
        pkgarch: String,
        /// Package checksum
        checksum: String,
    },

    /// Test if a PR value exists for the given checksum
    TestPr {
        /// Package version
        version: String,
        /// Package architecture
        pkgarch: String,
        /// Package checksum
        checksum: String,
    },

    /// Test if a package exists
    TestPackage {
        /// Package version
        version: String,
        /// Package architecture
        pkgarch: String,
    },

    /// Get the maximum PR value for a package
    MaxPackagePr {
        /// Package version
        version: String,
        /// Package architecture
        pkgarch: String,
    },

    /// Import a single PR entry
    ImportOne {
        /// Package version
        version: String,
        /// Package architecture
        pkgarch: String,
        /// Package checksum
        checksum: String,
        /// PR value
        value: i64,
    },

    /// Export PR data
    Export {
        /// Package version (optional)
        #[arg(long)]
        version: Option<String>,
        /// Package architecture (optional)
        #[arg(long)]
        pkgarch: Option<String>,
        /// Package checksum (optional)
        #[arg(long)]
        checksum: Option<String>,
        /// Include column info
        #[arg(long)]
        colinfo: bool,
    },

    /// Check if the server is read-only
    IsReadonly,

    /// Ping the server
    Ping,
}

pub async fn execute(args: Args) -> Result<()> {
    let mut client = PrClient::new();

    info!("Connecting to PR server at {}", args.server);
    client
        .connect(args.server)
        .await
        .map_err(|e| crate::error::Error::Generic(format!("Failed to connect to server: {e}")))?;

    let result = match args.command {
        ClientCommands::GetPr {
            version,
            pkgarch,
            checksum,
        } => {
            let value = client
                .get_pr(&version, &pkgarch, &checksum)
                .await
                .map_err(|e| crate::error::Error::Generic(format!("get-pr failed: {e}")))?;
            println!("PR value: {value}");
            Ok(())
        }

        ClientCommands::TestPr {
            version,
            pkgarch,
            checksum,
        } => {
            let value = client
                .test_pr(&version, &pkgarch, &checksum)
                .await
                .map_err(|e| crate::error::Error::Generic(format!("test-pr failed: {e}")))?;
            match value {
                Some(v) => println!("PR value exists: {v}"),
                None => println!("PR value does not exist"),
            }
            Ok(())
        }

        ClientCommands::TestPackage { version, pkgarch } => {
            let exists = client
                .test_package(&version, &pkgarch)
                .await
                .map_err(|e| crate::error::Error::Generic(format!("test-package failed: {e}")))?;
            println!("Package exists: {exists}");
            Ok(())
        }

        ClientCommands::MaxPackagePr { version, pkgarch } => {
            let value = client
                .max_package_pr(&version, &pkgarch)
                .await
                .map_err(|e| crate::error::Error::Generic(format!("max-package-pr failed: {e}")))?;
            match value {
                Some(v) => println!("Max PR value: {v}"),
                None => println!("No PR values found"),
            }
            Ok(())
        }

        ClientCommands::ImportOne {
            version,
            pkgarch,
            checksum,
            value,
        } => {
            let result = client
                .import_one(&version, &pkgarch, &checksum, value)
                .await
                .map_err(|e| crate::error::Error::Generic(format!("import-one failed: {e}")))?;
            match result {
                Some(v) => println!("Imported PR value: {v}"),
                None => println!("Import failed (server may be read-only)"),
            }
            Ok(())
        }

        ClientCommands::Export {
            version,
            pkgarch,
            checksum,
            colinfo,
        } => {
            let (metainfo, datainfo) = client
                .export(
                    version.as_deref(),
                    pkgarch.as_deref(),
                    checksum.as_deref(),
                    colinfo,
                )
                .await
                .map_err(|e| crate::error::Error::Generic(format!("export failed: {e}")))?;

            if let Some(meta) = metainfo {
                println!("Metadata: {}", serde_json::to_string_pretty(&meta).unwrap());
            }
            println!("Data: {}", serde_json::to_string_pretty(&datainfo).unwrap());
            Ok(())
        }

        ClientCommands::IsReadonly => {
            let readonly = client
                .is_readonly()
                .await
                .map_err(|e| crate::error::Error::Generic(format!("is-readonly failed: {e}")))?;
            println!("Server is read-only: {readonly}");
            Ok(())
        }

        ClientCommands::Ping => {
            let pong = client
                .ping()
                .await
                .map_err(|e| crate::error::Error::Generic(format!("ping failed: {e}")))?;
            println!("Ping successful: {pong}");
            Ok(())
        }
    };

    client
        .disconnect()
        .await
        .map_err(|e| crate::error::Error::Generic(format!("Failed to disconnect: {e}")))?;

    result
}
