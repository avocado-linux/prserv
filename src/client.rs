//! PR Client implementation
//!
//! This module provides the client for communicating with the PR server

use crate::server::{Request, Response};
use serde_json::Value;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{debug, error};
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("Server error: {0}")]
    Server(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;

/// PR Client for communicating with the PR server
pub struct PrClient {
    #[allow(dead_code)]
    stream: Option<TcpStream>,
    reader: Option<BufReader<tokio::net::tcp::OwnedReadHalf>>,
    writer: Option<tokio::net::tcp::OwnedWriteHalf>,
}

impl PrClient {
    /// Create a new PR client
    pub fn new() -> Self {
        Self {
            stream: None,
            reader: None,
            writer: None,
        }
    }

    /// Connect to a PR server
    pub async fn connect(&mut self, addr: SocketAddr) -> Result<()> {
        let stream = TcpStream::connect(addr).await?;
        debug!("Connected to PR server at {}", addr);

        let (read_half, write_half) = stream.into_split();
        let reader = BufReader::new(read_half);

        self.reader = Some(reader);
        self.writer = Some(write_half);

        // Read the initial handshake response
        let response = self.read_response().await?;
        debug!("Handshake response: {:?}", response);

        Ok(())
    }

    /// Send a request and get the response
    async fn send_request(&mut self, method: &str, params: Value) -> Result<Value> {
        let request = Request {
            id: Some(Uuid::new_v4().to_string()),
            method: method.to_string(),
            params,
        };

        let request_json = serde_json::to_string(&request)?;
        debug!("Sending request: {}", request_json);

        if let Some(writer) = &mut self.writer {
            writer.write_all(request_json.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
        } else {
            return Err(ClientError::Protocol("Not connected".to_string()));
        }

        let response = self.read_response().await?;

        if let Some(error) = response.error {
            return Err(ClientError::Server(error));
        }

        response
            .result
            .ok_or_else(|| ClientError::Protocol("No result in response".to_string()))
    }

    /// Read a response from the server
    async fn read_response(&mut self) -> Result<Response> {
        let mut line = String::new();

        if let Some(reader) = &mut self.reader {
            reader.read_line(&mut line).await?;
        } else {
            return Err(ClientError::Protocol("Not connected".to_string()));
        }

        let line = line.trim();
        debug!("Received response: {}", line);

        let response: Response = serde_json::from_str(line)?;
        Ok(response)
    }

    /// Get a PR value (creates if doesn't exist)
    pub async fn get_pr(&mut self, version: &str, pkgarch: &str, checksum: &str) -> Result<i64> {
        let params = serde_json::json!({
            "version": version,
            "pkgarch": pkgarch,
            "checksum": checksum
        });

        let result = self.send_request("get-pr", params).await?;

        result["value"]
            .as_i64()
            .ok_or_else(|| ClientError::Protocol("Invalid value in response".to_string()))
    }

    /// Test if a PR value exists for the given checksum
    pub async fn test_pr(
        &mut self,
        version: &str,
        pkgarch: &str,
        checksum: &str,
    ) -> Result<Option<i64>> {
        let params = serde_json::json!({
            "version": version,
            "pkgarch": pkgarch,
            "checksum": checksum
        });

        let result = self.send_request("test-pr", params).await?;

        Ok(result["value"].as_i64())
    }

    /// Test if a package exists
    pub async fn test_package(&mut self, version: &str, pkgarch: &str) -> Result<bool> {
        let params = serde_json::json!({
            "version": version,
            "pkgarch": pkgarch
        });

        let result = self.send_request("test-package", params).await?;

        result["value"]
            .as_bool()
            .ok_or_else(|| ClientError::Protocol("Invalid value in response".to_string()))
    }

    /// Get the maximum PR value for a package
    pub async fn max_package_pr(&mut self, version: &str, pkgarch: &str) -> Result<Option<i64>> {
        let params = serde_json::json!({
            "version": version,
            "pkgarch": pkgarch
        });

        let result = self.send_request("max-package-pr", params).await?;

        Ok(result["value"].as_i64())
    }

    /// Import a single PR entry
    pub async fn import_one(
        &mut self,
        version: &str,
        pkgarch: &str,
        checksum: &str,
        value: i64,
    ) -> Result<Option<i64>> {
        let params = serde_json::json!({
            "version": version,
            "pkgarch": pkgarch,
            "checksum": checksum,
            "value": value
        });

        let result = self.send_request("import-one", params).await?;

        Ok(result["value"].as_i64())
    }

    /// Export PR data
    pub async fn export(
        &mut self,
        version: Option<&str>,
        pkgarch: Option<&str>,
        checksum: Option<&str>,
        colinfo: bool,
    ) -> Result<(Option<Value>, Vec<Value>)> {
        let mut params = serde_json::json!({
            "colinfo": colinfo
        });

        if let Some(v) = version {
            params["version"] = Value::String(v.to_string());
        }
        if let Some(p) = pkgarch {
            params["pkgarch"] = Value::String(p.to_string());
        }
        if let Some(c) = checksum {
            params["checksum"] = Value::String(c.to_string());
        }

        let result = self.send_request("export", params).await?;

        let metainfo = result["metainfo"].clone();
        let datainfo = result["datainfo"]
            .as_array()
            .ok_or_else(|| ClientError::Protocol("Invalid datainfo in response".to_string()))?
            .clone();

        Ok((
            if metainfo.is_null() {
                None
            } else {
                Some(metainfo)
            },
            datainfo,
        ))
    }

    /// Check if the server is read-only
    pub async fn is_readonly(&mut self) -> Result<bool> {
        let result = self
            .send_request("is-readonly", serde_json::json!({}))
            .await?;

        result["readonly"]
            .as_bool()
            .ok_or_else(|| ClientError::Protocol("Invalid readonly value in response".to_string()))
    }

    /// Ping the server
    pub async fn ping(&mut self) -> Result<bool> {
        let result = self.send_request("ping", serde_json::json!({})).await?;

        result["pong"]
            .as_bool()
            .ok_or_else(|| ClientError::Protocol("Invalid pong response".to_string()))
    }

    /// Disconnect from the server
    pub async fn disconnect(&mut self) -> Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.shutdown().await?;
        }
        self.reader = None;
        Ok(())
    }
}

impl Drop for PrClient {
    fn drop(&mut self) {
        // Cleanup is handled by tokio's drop implementations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{start_server, ServerConfig};
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::time::sleep;

    async fn setup_test_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            db_path: db_path.to_string_lossy().to_string(),
            read_only: false,
            nohist: true,
            sync_mode: crate::config::SyncMode::Immediate,
        };

        let addr = config.bind_addr;
        let handle = tokio::spawn(async move {
            if let Err(e) = start_server(config).await {
                error!("Test server error: {}", e);
            }
        });

        // Give the server time to start
        sleep(Duration::from_millis(100)).await;

        (addr, handle)
    }

    #[tokio::test]
    async fn test_client_creation() {
        let _client = PrClient::new();
        // Client creation should succeed
    }

    #[tokio::test]
    #[ignore = "Requires running server"]
    async fn test_client_connect() {
        let (_addr, _handle) = setup_test_server().await;
        // Connection test would require proper server setup
    }
}
