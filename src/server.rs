//! PR Server implementation
//!
//! This module provides the async TCP server that handles PR service requests
//! with immediate SQLite persistence for reliability.

use crate::database::{DatabaseError, PrDatabase, PrTable};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Database error: {0}")]
    Database(#[from] DatabaseError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    #[error("Protocol error: {0}")]
    #[allow(dead_code)]
    Protocol(String),
}

pub type Result<T> = std::result::Result<T, ServerError>;

/// Connection state for BitBake asyncrpc protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    WaitingForHandshake, // Waiting for PRSERVICE 1.0 handshake
    Ready,               // Ready to handle JSON requests
}

/// PR Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub db_path: String,
    pub read_only: bool,
    pub nohist: bool,
    pub sync_mode: crate::config::SyncMode,
}

/// PR Server state
pub struct PrServer {
    config: ServerConfig,
    database: Arc<PrDatabase>,
    tables: Arc<RwLock<HashMap<String, Arc<PrTable>>>>,
}

impl PrServer {
    /// Create a new PR server
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let database = Arc::new(
            PrDatabase::new(
                &config.db_path,
                config.nohist,
                config.read_only,
                config.sync_mode.clone(),
            )
            .await?,
        );

        let tables = Arc::new(RwLock::new(HashMap::new()));

        Ok(Self {
            config,
            database,
            tables,
        })
    }

    /// Start the server
    pub async fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        info!("PR Server listening on {}", self.config.bind_addr);
        info!(
            "Database: {} (read_only: {}, nohist: {}, sync_mode: {:?})",
            self.config.db_path, self.config.read_only, self.config.nohist, self.config.sync_mode
        );

        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);

        // Spawn signal handler task
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
            let mut sigint =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    .expect("Failed to register SIGINT handler");

            tokio::select! {
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, initiating graceful shutdown...");
                }
                _ = sigint.recv() => {
                    info!("Received SIGINT (Ctrl-C), initiating graceful shutdown...");
                }
            }

            let _ = shutdown_tx_clone.send(());
        });

        loop {
            tokio::select! {
                // Handle new connections
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            debug!("New client connection from {}", addr);
                            let server = self.clone();
                            let mut client_shutdown_rx = shutdown_tx.subscribe();
                            tokio::spawn(async move {
                                tokio::select! {
                                    result = server.handle_client(stream, addr) => {
                                        if let Err(e) = result {
                                            error!("Error handling client {}: {}", addr, e);
                                        }
                                    }
                                    _ = client_shutdown_rx.recv() => {
                                        debug!("Client {} connection terminated due to shutdown", addr);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }
                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received, stopping server...");
                    break;
                }
            }
        }

        // Perform cleanup
        self.shutdown().await?;
        info!("Server shutdown complete");
        Ok(())
    }

    /// Perform graceful shutdown cleanup
    async fn shutdown(&self) -> Result<()> {
        info!("Performing graceful shutdown cleanup...");

        // Flush any buffered writes from all cached tables
        {
            let tables = self.tables.read().await;
            for (table_name, table) in tables.iter() {
                if let Err(e) = table.flush().await {
                    warn!(
                        "Error flushing table '{}' during shutdown: {}",
                        table_name, e
                    );
                } else {
                    debug!("Successfully flushed table '{}'", table_name);
                }
            }
        }

        info!("Graceful shutdown cleanup complete");
        Ok(())
    }

    /// Get or create a table instance
    async fn get_table(&self, table_name: &str) -> Result<Arc<PrTable>> {
        // Check if table already exists in cache
        {
            let tables = self.tables.read().await;
            if let Some(table) = tables.get(table_name) {
                return Ok(table.clone());
            }
        }

        // Create new table
        let table = Arc::new(self.database.get_table(table_name).await?);

        // Cache the table
        {
            let mut tables = self.tables.write().await;
            tables.insert(table_name.to_string(), table.clone());
        }

        Ok(table)
    }

    /// Handle a client connection
    async fn handle_client(&self, stream: TcpStream, addr: SocketAddr) -> Result<()> {
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();
        // Start waiting for BitBake asyncrpc handshake
        let mut connection_state = ConnectionState::WaitingForHandshake;

        loop {
            line.clear();
            match reader.read_line(&mut line).await? {
                0 => {
                    debug!("Client {} disconnected", addr);
                    break;
                }
                _ => {
                    let line = line.trim();
                    debug!("Received from {}: '{}'", addr, line);

                    match connection_state {
                        ConnectionState::WaitingForHandshake => {
                            if line.starts_with("PRSERVICE") {
                                debug!("Received BitBake asyncrpc handshake from {}", addr);
                                // BitBake asyncrpc server does NOT respond to PRSERVICE 1.0
                                continue;
                            } else if line.starts_with("needs-headers:") {
                                debug!("Received needs-headers from {}", addr);
                                // BitBake asyncrpc protocol - no response needed for needs-headers
                                continue;
                            } else if line.is_empty() {
                                debug!("Handshake complete for {}, ready for JSON requests", addr);
                                connection_state = ConnectionState::Ready;
                                continue;
                            } else {
                                return Err(ServerError::InvalidRequest(format!(
                                    "Unexpected handshake message: {line}"
                                )));
                            }
                        }
                        ConnectionState::Ready => {
                            // Skip empty lines in ready state
                            if line.is_empty() {
                                continue;
                            }

                            // Handle BitBake asyncrpc JSON requests
                            match self.process_bitbake_request(line).await {
                                Ok(response) => {
                                    debug!(
                                        "Sending BitBake asyncrpc response to {}: {}",
                                        addr, response
                                    );
                                    // Send response with newline as a single write to avoid packet fragmentation
                                    let response_with_newline = format!("{response}\n");
                                    write_half
                                        .write_all(response_with_newline.as_bytes())
                                        .await?;
                                    write_half.flush().await?;
                                }
                                Err(e) => {
                                    error!(
                                        "Error processing BitBake asyncrpc request from {}: {}",
                                        addr, e
                                    );
                                    let error_response = "{\"error\":\"invalid request\"}\n";
                                    write_half.write_all(error_response.as_bytes()).await?;
                                    write_half.flush().await?;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a BitBake asyncrpc JSON request
    async fn process_bitbake_request(&self, request_line: &str) -> Result<String> {
        // Try to parse as JSON
        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(request_line) {
            if let Some(obj) = json_value.as_object() {
                // Handle ping request
                if obj.contains_key("ping") {
                    return Ok(serde_json::to_string(&serde_json::json!({"alive": true}))?);
                }

                // Handle get-pr request
                if let Some(get_pr) = obj.get("get-pr") {
                    if let Some(params) = get_pr.as_object() {
                        let version =
                            params
                                .get("version")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing version".to_string())
                                })?;
                        let pkgarch =
                            params
                                .get("pkgarch")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing pkgarch".to_string())
                                })?;
                        let checksum =
                            params
                                .get("checksum")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing checksum".to_string())
                                })?;

                        let table = self.get_table("PRMAIN").await?;
                        let value = table.get_value(version, pkgarch, checksum).await?;
                        return Ok(serde_json::to_string(&serde_json::json!({"value": value}))?);
                    }
                }

                // Handle test-pr request
                if let Some(test_pr) = obj.get("test-pr") {
                    if let Some(params) = test_pr.as_object() {
                        let version =
                            params
                                .get("version")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing version".to_string())
                                })?;
                        let pkgarch =
                            params
                                .get("pkgarch")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing pkgarch".to_string())
                                })?;
                        let checksum =
                            params
                                .get("checksum")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing checksum".to_string())
                                })?;

                        let table = self.get_table("PRMAIN").await?;
                        let value = table.find_value(version, pkgarch, checksum).await?;
                        return Ok(serde_json::to_string(&serde_json::json!({"value": value}))?);
                    }
                }

                // Handle test-package request
                if let Some(test_package) = obj.get("test-package") {
                    if let Some(params) = test_package.as_object() {
                        let version =
                            params
                                .get("version")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing version".to_string())
                                })?;
                        let pkgarch =
                            params
                                .get("pkgarch")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing pkgarch".to_string())
                                })?;

                        let table = self.get_table("PRMAIN").await?;
                        let exists = table.test_package(version, pkgarch).await?;
                        return Ok(serde_json::to_string(
                            &serde_json::json!({"value": exists}),
                        )?);
                    }
                }

                // Handle max-package-pr request
                if let Some(max_package_pr) = obj.get("max-package-pr") {
                    if let Some(params) = max_package_pr.as_object() {
                        let version =
                            params
                                .get("version")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing version".to_string())
                                })?;
                        let pkgarch =
                            params
                                .get("pkgarch")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing pkgarch".to_string())
                                })?;

                        let table = self.get_table("PRMAIN").await?;
                        let value = table.find_max_value(version, pkgarch).await?;
                        return Ok(serde_json::to_string(&serde_json::json!({"value": value}))?);
                    }
                }

                // Handle import-one request
                if let Some(import_one) = obj.get("import-one") {
                    if let Some(params) = import_one.as_object() {
                        if self.config.read_only {
                            return Ok(serde_json::to_string(&serde_json::json!({"value": null}))?);
                        }

                        let version =
                            params
                                .get("version")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing version".to_string())
                                })?;
                        let pkgarch =
                            params
                                .get("pkgarch")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing pkgarch".to_string())
                                })?;
                        let checksum =
                            params
                                .get("checksum")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest("Missing checksum".to_string())
                                })?;
                        let value =
                            params
                                .get("value")
                                .and_then(|v| v.as_i64())
                                .ok_or_else(|| {
                                    ServerError::InvalidRequest(
                                        "Missing or invalid value".to_string(),
                                    )
                                })?;

                        let table = self.get_table("PRMAIN").await?;
                        let result = table.import_one(version, pkgarch, checksum, value).await?;
                        return Ok(serde_json::to_string(
                            &serde_json::json!({"value": result}),
                        )?);
                    }
                }

                // Handle export request
                if let Some(export) = obj.get("export") {
                    if let Some(params) = export.as_object() {
                        let version = params.get("version").and_then(|v| v.as_str());
                        let pkgarch = params.get("pkgarch").and_then(|v| v.as_str());
                        let checksum = params.get("checksum").and_then(|v| v.as_str());
                        let colinfo = params
                            .get("colinfo")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);

                        let table = self.get_table("PRMAIN").await?;
                        let (metainfo, datainfo) =
                            table.export(version, pkgarch, checksum, colinfo).await?;

                        return Ok(serde_json::to_string(&serde_json::json!({
                            "metainfo": metainfo,
                            "datainfo": datainfo
                        }))?);
                    }
                }

                // Handle is-readonly request
                if obj.contains_key("is-readonly") {
                    return Ok(serde_json::to_string(
                        &serde_json::json!({"readonly": self.config.read_only}),
                    )?);
                }
            }
        }

        Err(ServerError::InvalidRequest(format!(
            "Unknown request: {request_line}"
        )))
    }
}

impl Clone for PrServer {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            database: self.database.clone(),
            tables: self.tables.clone(),
        }
    }
}

/// Start a PR server daemon
pub async fn start_server(config: ServerConfig) -> Result<()> {
    let server = PrServer::new(config).await?;
    server.start().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn create_test_server() -> (PrServer, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();

        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            db_path: ":memory:".to_string(), // Use in-memory database for tests
            read_only: false,
            nohist: true,
            sync_mode: crate::config::SyncMode::Immediate,
        };

        let server = PrServer::new(config).await.unwrap();
        (server, temp_dir)
    }

    #[tokio::test]
    async fn test_server_creation() {
        let (_server, _temp_dir) = create_test_server().await;
        // Server creation should succeed
    }

    #[tokio::test]
    async fn test_get_table() {
        let (server, _temp_dir) = create_test_server().await;
        let _table = server.get_table("TEST").await.unwrap();
        // Table creation should succeed
    }
}
