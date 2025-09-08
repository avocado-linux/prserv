//! PR Server implementation
//!
//! This module provides the async TCP server that handles PR service requests
//! with immediate SQLite persistence for reliability.

use crate::database::{DatabaseError, PrDatabase, PrTable};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

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

/// Protocol version
const PROTOCOL_VERSION: (u32, u32) = (1, 0);

/// Request message structure
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub id: Option<String>,
    pub method: String,
    pub params: serde_json::Value,
}

/// Response message structure
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub id: Option<String>,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
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

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("New client connection from {}", addr);
                    let server = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = server.handle_client(stream, addr).await {
                            error!("Error handling client {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
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

        // Protocol handshake
        self.send_response(
            &mut write_half,
            Response {
                id: None,
                result: Some(serde_json::json!({
                    "protocol": "PRSERVICE",
                    "version": PROTOCOL_VERSION
                })),
                error: None,
            },
        )
        .await?;

        loop {
            line.clear();
            match reader.read_line(&mut line).await? {
                0 => {
                    debug!("Client {} disconnected", addr);
                    break;
                }
                _ => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    debug!("Received from {}: {}", addr, line);

                    match self.process_request(line).await {
                        Ok(response) => {
                            self.send_response(&mut write_half, response).await?;
                        }
                        Err(e) => {
                            error!("Error processing request from {}: {}", addr, e);
                            let error_response = Response {
                                id: None,
                                result: None,
                                error: Some(e.to_string()),
                            };
                            self.send_response(&mut write_half, error_response).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a JSON-RPC style request
    async fn process_request(&self, request_line: &str) -> Result<Response> {
        let request: Request = serde_json::from_str(request_line)?;

        debug!("Processing method: {}", request.method);

        let result = match request.method.as_str() {
            "get-pr" => self.handle_get_pr(request.params).await?,
            "test-pr" => self.handle_test_pr(request.params).await?,
            "test-package" => self.handle_test_package(request.params).await?,
            "max-package-pr" => self.handle_max_package_pr(request.params).await?,
            "import-one" => self.handle_import_one(request.params).await?,
            "export" => self.handle_export(request.params).await?,
            "is-readonly" => self.handle_is_readonly().await?,
            "ping" => serde_json::json!({"pong": true}),
            _ => {
                return Ok(Response {
                    id: request.id,
                    result: None,
                    error: Some(format!("Unknown method: {}", request.method)),
                });
            }
        };

        Ok(Response {
            id: request.id,
            result: Some(result),
            error: None,
        })
    }

    /// Send a response to the client
    async fn send_response(
        &self,
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
        response: Response,
    ) -> Result<()> {
        let response_json = serde_json::to_string(&response)?;
        writer.write_all(response_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        Ok(())
    }

    /// Handle get-pr request
    async fn handle_get_pr(&self, params: serde_json::Value) -> Result<serde_json::Value> {
        let version: String = params["version"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing version".to_string()))?
            .to_string();
        let pkgarch: String = params["pkgarch"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing pkgarch".to_string()))?
            .to_string();
        let checksum: String = params["checksum"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing checksum".to_string()))?
            .to_string();

        let table = self.get_table("PRMAIN").await?;
        let value = table.get_value(&version, &pkgarch, &checksum).await?;

        Ok(serde_json::json!({"value": value}))
    }

    /// Handle test-pr request
    async fn handle_test_pr(&self, params: serde_json::Value) -> Result<serde_json::Value> {
        let version: String = params["version"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing version".to_string()))?
            .to_string();
        let pkgarch: String = params["pkgarch"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing pkgarch".to_string()))?
            .to_string();
        let checksum: String = params["checksum"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing checksum".to_string()))?
            .to_string();

        let table = self.get_table("PRMAIN").await?;
        let value = table.find_value(&version, &pkgarch, &checksum).await?;

        Ok(serde_json::json!({"value": value}))
    }

    /// Handle test-package request
    async fn handle_test_package(&self, params: serde_json::Value) -> Result<serde_json::Value> {
        let version: String = params["version"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing version".to_string()))?
            .to_string();
        let pkgarch: String = params["pkgarch"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing pkgarch".to_string()))?
            .to_string();

        let table = self.get_table("PRMAIN").await?;
        let exists = table.test_package(&version, &pkgarch).await?;

        Ok(serde_json::json!({"value": exists}))
    }

    /// Handle max-package-pr request
    async fn handle_max_package_pr(&self, params: serde_json::Value) -> Result<serde_json::Value> {
        let version: String = params["version"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing version".to_string()))?
            .to_string();
        let pkgarch: String = params["pkgarch"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing pkgarch".to_string()))?
            .to_string();

        let table = self.get_table("PRMAIN").await?;
        let value = table.find_max_value(&version, &pkgarch).await?;

        Ok(serde_json::json!({"value": value}))
    }

    /// Handle import-one request
    async fn handle_import_one(&self, params: serde_json::Value) -> Result<serde_json::Value> {
        if self.config.read_only {
            return Ok(serde_json::json!({"value": null}));
        }

        let version: String = params["version"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing version".to_string()))?
            .to_string();
        let pkgarch: String = params["pkgarch"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing pkgarch".to_string()))?
            .to_string();
        let checksum: String = params["checksum"]
            .as_str()
            .ok_or_else(|| ServerError::InvalidRequest("Missing checksum".to_string()))?
            .to_string();
        let value: i64 = params["value"]
            .as_i64()
            .ok_or_else(|| ServerError::InvalidRequest("Missing or invalid value".to_string()))?;

        let table = self.get_table("PRMAIN").await?;
        let result = table
            .import_one(&version, &pkgarch, &checksum, value)
            .await?;

        Ok(serde_json::json!({"value": result}))
    }

    /// Handle export request
    async fn handle_export(&self, params: serde_json::Value) -> Result<serde_json::Value> {
        let version = params["version"].as_str();
        let pkgarch = params["pkgarch"].as_str();
        let checksum = params["checksum"].as_str();
        let colinfo = params["colinfo"].as_bool().unwrap_or(false);

        let table = self.get_table("PRMAIN").await?;
        let (metainfo, datainfo) = table.export(version, pkgarch, checksum, colinfo).await?;

        Ok(serde_json::json!({
            "metainfo": metainfo,
            "datainfo": datainfo
        }))
    }

    /// Handle is-readonly request
    async fn handle_is_readonly(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({"readonly": self.config.read_only}))
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
