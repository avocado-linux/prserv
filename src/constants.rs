//! Compile-time constants for the application.

/// Application name used in user agent strings.
#[allow(dead_code)]
pub const APP_NAME: &str = "prserv";

/// Application version from Cargo.toml.
#[allow(dead_code)]
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default configuration file name.
pub const DEFAULT_CONFIG_FILE: &str = "prserv.json";

/// Default database file name.
pub const DEFAULT_DATABASE_FILE: &str = "prserv.db";

/// Default server bind address.
pub const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8585";

/// Default client server address.
pub const DEFAULT_SERVER_ADDR: &str = "127.0.0.1:8585";
