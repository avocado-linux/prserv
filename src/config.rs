use std::fs;
use std::path::Path;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::constants;
use crate::env_vars;
use crate::error::{Error, Result};

/// Database synchronization mode
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    /// Write to disk immediately after every operation (safest, slowest)
    #[default]
    Immediate,
    /// Buffer writes and flush periodically (balanced safety/performance)
    Periodic { interval_secs: u64 },
    /// Only sync on graceful shutdown (fastest, least safe)
    OnShutdown,
}


impl SyncMode {
    /// Convert to Duration for periodic mode, None for other modes
    pub fn as_duration(&self) -> Option<Duration> {
        match self {
            SyncMode::Periodic { interval_secs } => Some(Duration::from_secs(*interval_secs)),
            _ => None,
        }
    }

    /// Check if this mode requires immediate syncing
    #[allow(dead_code)]
    pub fn is_immediate(&self) -> bool {
        matches!(self, SyncMode::Immediate)
    }

    /// Check if this mode uses periodic syncing
    #[allow(dead_code)]
    pub fn is_periodic(&self) -> bool {
        matches!(self, SyncMode::Periodic { .. })
    }

    /// Check if this mode only syncs on shutdown
    #[allow(dead_code)]
    pub fn is_on_shutdown(&self) -> bool {
        matches!(self, SyncMode::OnShutdown)
    }
}

impl FromStr for SyncMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "immediate" => Ok(SyncMode::Immediate),
            "on_shutdown" | "on-shutdown" | "shutdown" => Ok(SyncMode::OnShutdown),
            s if s.starts_with("periodic:") => {
                let interval_str = s.strip_prefix("periodic:").unwrap();
                let interval_secs = interval_str.parse::<u64>()
                    .map_err(|_| Error::Other(format!("Invalid periodic interval: {interval_str}")))?;
                Ok(SyncMode::Periodic { interval_secs })
            }
            _ => Err(Error::Other(format!(
                "Invalid sync mode: {s}. Valid options: immediate, periodic:<seconds>, on_shutdown"
            ))),
        }
    }
}

/// Server configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Database file path
    pub database: String,

    /// Server bind address
    pub bind_addr: String,

    /// Run in read-only mode
    pub read_only: bool,

    /// Use no-history mode (no decrements allowed)
    pub nohist: bool,

    /// Database synchronization mode
    pub sync_mode: SyncMode,
}

/// Client configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ClientConfig {
    /// Default server address to connect to
    pub server_addr: String,
}

/// Main configuration structure for the PR server.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct Config {
    /// Server configuration
    pub server: ServerConfig,

    /// Client configuration
    pub client: ClientConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            database: constants::DEFAULT_DATABASE_FILE.to_string(),
            bind_addr: constants::DEFAULT_BIND_ADDR.to_string(),
            read_only: false,
            nohist: false,
            sync_mode: SyncMode::default(),
        }
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_addr: constants::DEFAULT_SERVER_ADDR.to_string(),
        }
    }
}


impl Config {
    /// Loads configuration from the specified file.
    ///
    /// # Arguments
    /// * `path` - Path to configuration file
    ///
    /// # Returns
    /// * `Ok(Config)` - Loaded and validated configuration
    /// * `Err` - If loading or validation fails
    pub fn load(path: &str) -> Result<Self> {
        let config = Self::load_from_file(path)?;
        config.validate()?;
        Ok(config)
    }

    /// Loads configuration from a specific file.
    /// Automatically detects format based on file extension (.json, .yaml, .yml).
    fn load_from_file(path: &str) -> Result<Self> {
        let path = Path::new(path);

        if !path.exists() {
            debug!(
                "Configuration file not found: {}, using defaults",
                path.display()
            );
            return Ok(Self::default());
        }

        info!("Loading configuration from: {}", path.display());

        let contents = fs::read_to_string(path).map_err(Error::Io)?;

        // Detect format based on extension
        let config = match path.extension().and_then(|ext| ext.to_str()) {
            Some("json") => serde_json::from_str(&contents).map_err(Error::Json)?,
            Some("yaml" | "yml") => serde_yaml::from_str(&contents)
                .map_err(|e| Error::Other(format!("Failed to parse YAML: {e}")))?,
            _ => {
                // Default to JSON for backward compatibility
                serde_json::from_str(&contents).map_err(Error::Json)?
            }
        };

        debug!("Configuration loaded successfully");
        Ok(config)
    }

    /// Merge environment variables onto configuration.
    pub fn merge_env(&mut self) -> Result<()> {
        // Server configuration overrides
        if let Ok(val) = std::env::var(env_vars::DATABASE) {
            self.server.database = val;
        }

        if let Ok(val) = std::env::var(env_vars::BIND_ADDR) {
            self.server.bind_addr = val;
        }

        if let Ok(val) = std::env::var(env_vars::READ_ONLY) {
            self.server.read_only = val.parse().unwrap_or(false);
        }

        if let Ok(val) = std::env::var(env_vars::NOHIST) {
            self.server.nohist = val.parse().unwrap_or(false);
        }

        if let Ok(val) = std::env::var(env_vars::SYNC_MODE) {
            self.server.sync_mode = SyncMode::from_str(&val)?;
        }

        // Client configuration overrides
        if let Ok(val) = std::env::var(env_vars::SERVER_ADDR) {
            self.client.server_addr = val;
        }

        Ok(())
    }

    /// Validates the configuration.
    fn validate(&self) -> Result<()> {
        // Validate server bind address
        if let Err(e) = self.server.bind_addr.parse::<SocketAddr>() {
            return Err(Error::Other(format!(
                "Invalid bind address '{}': {}",
                self.server.bind_addr, e
            )));
        }

        // Validate client server address
        if let Err(e) = self.client.server_addr.parse::<SocketAddr>() {
            return Err(Error::Other(format!(
                "Invalid server address '{}': {}",
                self.client.server_addr, e
            )));
        }

        // Validate database path is not empty
        if self.server.database.trim().is_empty() {
            return Err(Error::Other(
                "Database path cannot be empty".to_string()
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.database, constants::DEFAULT_DATABASE_FILE);
        assert_eq!(config.server.bind_addr, constants::DEFAULT_BIND_ADDR);
        assert!(!config.server.read_only);
        assert!(!config.server.nohist);
        assert_eq!(config.server.sync_mode, SyncMode::Immediate);
        assert_eq!(config.client.server_addr, constants::DEFAULT_SERVER_ADDR);
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        // Test invalid bind address
        config.server.bind_addr = "invalid-address".to_string();
        assert!(config.validate().is_err());

        // Test empty database path
        config.server.bind_addr = constants::DEFAULT_BIND_ADDR.to_string();
        config.server.database = "".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_save_and_load_config() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test_config.json");
        let config_path_str = config_path.to_str().unwrap();

        let mut config = Config::default();
        config.server.database = "custom.db".to_string();
        config.server.read_only = true;

        let json = serde_json::to_string_pretty(&config).unwrap();
        fs::write(config_path_str, json).unwrap();

        let loaded = Config::load(config_path_str).unwrap();
        assert_eq!(loaded.server.database, "custom.db");
        assert!(loaded.server.read_only);
    }

    #[test]
    fn test_env_override() {
        std::env::set_var(env_vars::DATABASE, "/custom/prserv.db");
        std::env::set_var(env_vars::BIND_ADDR, "0.0.0.0:9999");
        std::env::set_var(env_vars::READ_ONLY, "true");
        std::env::set_var(env_vars::NOHIST, "true");
        std::env::set_var(env_vars::SYNC_MODE, "periodic:30");
        std::env::set_var(env_vars::SERVER_ADDR, "192.168.1.100:8585");

        let mut config = Config::default();
        config.merge_env().unwrap();

        assert_eq!(config.server.database, "/custom/prserv.db");
        assert_eq!(config.server.bind_addr, "0.0.0.0:9999");
        assert!(config.server.read_only);
        assert!(config.server.nohist);
        assert_eq!(config.server.sync_mode, SyncMode::Periodic { interval_secs: 30 });
        assert_eq!(config.client.server_addr, "192.168.1.100:8585");

        // Clean up
        std::env::remove_var(env_vars::DATABASE);
        std::env::remove_var(env_vars::BIND_ADDR);
        std::env::remove_var(env_vars::READ_ONLY);
        std::env::remove_var(env_vars::NOHIST);
        std::env::remove_var(env_vars::SYNC_MODE);
        std::env::remove_var(env_vars::SERVER_ADDR);
    }

    #[test]
    fn test_sync_mode_parsing() {
        assert_eq!(SyncMode::from_str("immediate").unwrap(), SyncMode::Immediate);
        assert_eq!(SyncMode::from_str("on_shutdown").unwrap(), SyncMode::OnShutdown);
        assert_eq!(SyncMode::from_str("on-shutdown").unwrap(), SyncMode::OnShutdown);
        assert_eq!(SyncMode::from_str("shutdown").unwrap(), SyncMode::OnShutdown);
        assert_eq!(SyncMode::from_str("periodic:5").unwrap(), SyncMode::Periodic { interval_secs: 5 });
        assert_eq!(SyncMode::from_str("periodic:300").unwrap(), SyncMode::Periodic { interval_secs: 300 });

        // Test invalid inputs
        assert!(SyncMode::from_str("invalid").is_err());
        assert!(SyncMode::from_str("periodic:abc").is_err());
        assert!(SyncMode::from_str("periodic").is_err());
    }

    #[test]
    fn test_sync_mode_methods() {
        let immediate = SyncMode::Immediate;
        let periodic = SyncMode::Periodic { interval_secs: 10 };
        let on_shutdown = SyncMode::OnShutdown;

        assert!(immediate.is_immediate());
        assert!(!immediate.is_periodic());
        assert!(!immediate.is_on_shutdown());
        assert_eq!(immediate.as_duration(), None);

        assert!(!periodic.is_immediate());
        assert!(periodic.is_periodic());
        assert!(!periodic.is_on_shutdown());
        assert_eq!(periodic.as_duration(), Some(Duration::from_secs(10)));

        assert!(!on_shutdown.is_immediate());
        assert!(!on_shutdown.is_periodic());
        assert!(on_shutdown.is_on_shutdown());
        assert_eq!(on_shutdown.as_duration(), None);
    }

    #[test]
    fn test_yaml_config_loading() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test_config.yaml");
        let config_path_str = config_path.to_str().unwrap();

        let yaml = r#"
server:
  database: "/var/lib/prserv/prserv.db"
  bind_addr: "0.0.0.0:8585"
  read_only: true
  nohist: false
  sync_mode: immediate
client:
  server_addr: "192.168.1.100:8585"
"#;

        fs::write(config_path_str, yaml).unwrap();

        let loaded = Config::load(config_path_str).unwrap();
        assert_eq!(loaded.server.database, "/var/lib/prserv/prserv.db");
        assert_eq!(loaded.server.bind_addr, "0.0.0.0:8585");
        assert!(loaded.server.read_only);
        assert!(!loaded.server.nohist);
        assert_eq!(loaded.server.sync_mode, SyncMode::Immediate);
        assert_eq!(loaded.client.server_addr, "192.168.1.100:8585");
    }

    #[test]
    fn test_yml_extension() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test_config.yml");
        let config_path_str = config_path.to_str().unwrap();

        let yaml = r#"
server:
  database: "test.db"
  bind_addr: "127.0.0.1:9090"
  read_only: false
  nohist: true
client:
  server_addr: "127.0.0.1:9090"
"#;

        fs::write(config_path_str, yaml).unwrap();

        let loaded = Config::load(config_path_str).unwrap();
        assert_eq!(loaded.server.database, "test.db");
        assert_eq!(loaded.server.bind_addr, "127.0.0.1:9090");
        assert!(!loaded.server.read_only);
        assert!(loaded.server.nohist);
        assert_eq!(loaded.client.server_addr, "127.0.0.1:9090");
    }
}
