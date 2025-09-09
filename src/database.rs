//! Database layer for the PR server
//!
//! This module provides SQLite database operations with configurable sync modes:
//! immediate persistence, periodic flushing, or sync-on-shutdown for different
//! performance/safety trade-offs.

use crate::buffered_writes::{BufferedWriteManager, PendingWrite};
use crate::config::SyncMode;
use serde::{Deserialize, Serialize};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePool},
    Row,
};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info};

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Database connection error: {0}")]
    Connection(#[from] sqlx::Error),
    #[error("Not found")]
    NotFound,
    #[error("Database integrity error: {0}")]
    Integrity(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, DatabaseError>;

/// Package revision entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrEntry {
    pub version: String,
    pub pkgarch: String,
    pub checksum: String,
    pub value: i64,
}

/// Export metadata information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportMetaInfo {
    pub tbl_name: String,
    pub core_ver: String,
    pub col_info: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub col_type: String,
    pub notnull: bool,
    pub dflt_value: Option<String>,
    pub pk: bool,
}

/// PR Table implementation with configurable SQLite persistence
pub struct PrTable {
    pool: SqlitePool,
    table_name: String,
    nohist: bool,
    read_only: bool,
    write_manager: Option<Arc<BufferedWriteManager>>,
}

impl PrTable {
    /// Create a new PR table instance
    pub async fn new(
        pool: SqlitePool,
        table_name: &str,
        nohist: bool,
        read_only: bool,
        sync_mode: SyncMode,
    ) -> Result<Self> {
        let full_table_name = if nohist {
            format!("{table_name}_nohist")
        } else {
            format!("{table_name}_hist")
        };

        // Create write manager for non-read-only tables
        let write_manager = if read_only {
            None
        } else {
            let manager = Arc::new(BufferedWriteManager::new(pool.clone(), sync_mode.clone()));
            manager.start_periodic_flush().await;
            Some(manager)
        };

        let table = Self {
            pool,
            table_name: full_table_name.clone(),
            nohist,
            read_only,
            write_manager,
        };

        if read_only {
            // Check if table exists
            let exists = table.table_exists().await?;
            if !exists {
                return Err(DatabaseError::NotFound);
            }
        } else {
            // Create table if it doesn't exist
            table.create_table().await?;
        }

        info!(
            "Initialized PR table: {} (nohist: {}, read_only: {})",
            full_table_name, nohist, read_only
        );

        Ok(table)
    }

    /// Check if the table exists
    async fn table_exists(&self) -> Result<bool> {
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?")
                .bind(&self.table_name)
                .fetch_one(&self.pool)
                .await?;

        Ok(count > 0)
    }

    /// Create the PR table
    async fn create_table(&self) -> Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                version TEXT NOT NULL,
                pkgarch TEXT NOT NULL,
                checksum TEXT NOT NULL,
                value INTEGER NOT NULL,
                PRIMARY KEY (version, pkgarch, checksum)
            )",
            self.table_name
        );

        sqlx::query(&sql).execute(&self.pool).await?;
        debug!("Created table: {}", self.table_name);
        Ok(())
    }

    /// Test if a package version exists for the given architecture
    pub async fn test_package(&self, version: &str, pkgarch: &str) -> Result<bool> {
        let sql = format!(
            "SELECT value FROM {} WHERE version=? AND pkgarch=?",
            self.table_name
        );

        let result = sqlx::query(&sql)
            .bind(version)
            .bind(pkgarch)
            .fetch_optional(&self.pool)
            .await?;

        Ok(result.is_some())
    }

    /// Test if a specific value exists for the given package and architecture
    #[allow(dead_code)]
    pub async fn test_value(&self, version: &str, pkgarch: &str, value: i64) -> Result<bool> {
        let sql = format!(
            "SELECT value FROM {} WHERE version=? AND pkgarch=? AND value=?",
            self.table_name
        );

        let result = sqlx::query(&sql)
            .bind(version)
            .bind(pkgarch)
            .bind(value)
            .fetch_optional(&self.pool)
            .await?;

        Ok(result.is_some())
    }

    /// Find the value for a specific checksum
    pub async fn find_value(
        &self,
        version: &str,
        pkgarch: &str,
        checksum: &str,
    ) -> Result<Option<i64>> {
        let sql = format!(
            "SELECT value FROM {} WHERE version=? AND pkgarch=? AND checksum=?",
            self.table_name
        );

        let result = sqlx::query_scalar::<_, i64>(&sql)
            .bind(version)
            .bind(pkgarch)
            .bind(checksum)
            .fetch_optional(&self.pool)
            .await?;

        Ok(result)
    }

    /// Find the maximum value for a given version and architecture
    pub async fn find_max_value(&self, version: &str, pkgarch: &str) -> Result<Option<i64>> {
        let sql = format!(
            "SELECT max(value) FROM {} WHERE version=? AND pkgarch=?",
            self.table_name
        );

        let result = sqlx::query_scalar::<_, Option<i64>>(&sql)
            .bind(version)
            .bind(pkgarch)
            .fetch_one(&self.pool)
            .await?;

        Ok(result)
    }

    /// Get or create a PR value (main entry point)
    pub async fn get_value(&self, version: &str, pkgarch: &str, checksum: &str) -> Result<i64> {
        if self.nohist {
            self.get_value_no_hist(version, pkgarch, checksum).await
        } else {
            self.get_value_hist(version, pkgarch, checksum).await
        }
    }

    /// Get value in history mode (allows decrements)
    async fn get_value_hist(&self, version: &str, pkgarch: &str, checksum: &str) -> Result<i64> {
        // First try to find existing value
        if let Some(value) = self.find_value(version, pkgarch, checksum).await? {
            return Ok(value);
        }

        // If read-only, calculate what the value would be
        if self.read_only {
            let next_value = self
                .find_max_value(version, pkgarch)
                .await?
                .map(|v| v + 1)
                .unwrap_or(0);
            return Ok(next_value);
        }

        // Get the next value first
        let next_value = self
            .find_max_value(version, pkgarch)
            .await?
            .map(|v| v + 1)
            .unwrap_or(0);

        // Create entry for buffered write
        let entry = PrEntry {
            version: version.to_string(),
            pkgarch: pkgarch.to_string(),
            checksum: checksum.to_string(),
            value: next_value,
        };

        // Use buffered write manager
        if let Some(write_manager) = &self.write_manager {
            let write = PendingWrite::Insert {
                table_name: self.table_name.clone(),
                entry,
            };

            write_manager
                .buffer_write(write)
                .await
                .map_err(|e| DatabaseError::Connection(sqlx::Error::Protocol(e.to_string())))?;

            debug!(
                "Buffered new PR value {} for ({}, {}, {})",
                next_value, version, pkgarch, checksum
            );

            Ok(next_value)
        } else {
            // Fallback to direct insert for read-only mode (shouldn't happen)
            Err(DatabaseError::Integrity(
                "Write attempted on read-only table".to_string(),
            ))
        }
    }

    /// Get value in no-history mode (no decrements allowed)
    async fn get_value_no_hist(&self, version: &str, pkgarch: &str, checksum: &str) -> Result<i64> {
        // First try to find existing value for exact match
        if let Some(value) = self.find_value(version, pkgarch, checksum).await? {
            return Ok(value);
        }

        // If read-only, calculate what the value would be
        if self.read_only {
            let next_value = self
                .find_max_value(version, pkgarch)
                .await?
                .map(|v| v + 1)
                .unwrap_or(0);
            return Ok(next_value);
        }

        // Get the next value first
        let next_value = self
            .find_max_value(version, pkgarch)
            .await?
            .map(|v| v + 1)
            .unwrap_or(0);

        // Create entry for buffered write
        let entry = PrEntry {
            version: version.to_string(),
            pkgarch: pkgarch.to_string(),
            checksum: checksum.to_string(),
            value: next_value,
        };

        // Use buffered write manager
        if let Some(write_manager) = &self.write_manager {
            let write = PendingWrite::Update {
                table_name: self.table_name.clone(),
                entry,
            };

            write_manager
                .buffer_write(write)
                .await
                .map_err(|e| DatabaseError::Connection(sqlx::Error::Protocol(e.to_string())))?;

            debug!(
                "Buffered PR value {} for ({}, {}, {})",
                next_value, version, pkgarch, checksum
            );

            Ok(next_value)
        } else {
            // Fallback for read-only mode (shouldn't happen)
            Err(DatabaseError::Integrity(
                "Write attempted on read-only table".to_string(),
            ))
        }
    }

    /// Import a single PR entry
    pub async fn import_one(
        &self,
        version: &str,
        pkgarch: &str,
        checksum: &str,
        value: i64,
    ) -> Result<Option<i64>> {
        if self.read_only {
            return Ok(None);
        }

        if self.nohist {
            self.import_no_hist(version, pkgarch, checksum, value).await
        } else {
            self.import_hist(version, pkgarch, checksum, value).await
        }
    }

    /// Import in history mode
    async fn import_hist(
        &self,
        version: &str,
        pkgarch: &str,
        checksum: &str,
        value: i64,
    ) -> Result<Option<i64>> {
        // Check if entry already exists
        if let Some(existing_value) = self.find_value(version, pkgarch, checksum).await? {
            return Ok(Some(existing_value));
        }

        // Create entry for buffered write
        let entry = PrEntry {
            version: version.to_string(),
            pkgarch: pkgarch.to_string(),
            checksum: checksum.to_string(),
            value,
        };

        // Use buffered write manager
        if let Some(write_manager) = &self.write_manager {
            let write = PendingWrite::Insert {
                table_name: self.table_name.clone(),
                entry,
            };

            write_manager
                .buffer_write(write)
                .await
                .map_err(|e| DatabaseError::Connection(sqlx::Error::Protocol(e.to_string())))?;

            debug!(
                "Buffered import PR value {} for ({}, {}, {})",
                value, version, pkgarch, checksum
            );

            Ok(Some(value))
        } else {
            // Fallback for read-only mode (shouldn't happen)
            Err(DatabaseError::Integrity(
                "Write attempted on read-only table".to_string(),
            ))
        }
    }

    /// Import in no-history mode
    async fn import_no_hist(
        &self,
        version: &str,
        pkgarch: &str,
        checksum: &str,
        value: i64,
    ) -> Result<Option<i64>> {
        // Create entry for buffered write
        let entry = PrEntry {
            version: version.to_string(),
            pkgarch: pkgarch.to_string(),
            checksum: checksum.to_string(),
            value,
        };

        // Use buffered write manager
        if let Some(write_manager) = &self.write_manager {
            let write = PendingWrite::Update {
                table_name: self.table_name.clone(),
                entry,
            };

            write_manager
                .buffer_write(write)
                .await
                .map_err(|e| DatabaseError::Connection(sqlx::Error::Protocol(e.to_string())))?;

            debug!(
                "Buffered import PR value {} for ({}, {}, {})",
                value, version, pkgarch, checksum
            );

            Ok(Some(value))
        } else {
            // Fallback for read-only mode (shouldn't happen)
            Err(DatabaseError::Integrity(
                "Write attempted on read-only table".to_string(),
            ))
        }
    }

    /// Export PR data
    pub async fn export(
        &self,
        version: Option<&str>,
        pkgarch: Option<&str>,
        checksum: Option<&str>,
        colinfo: bool,
    ) -> Result<(Option<ExportMetaInfo>, Vec<PrEntry>)> {
        let mut metainfo = None;

        // Get column info if requested
        if colinfo {
            let col_info_sql = format!("PRAGMA table_info({})", self.table_name);
            let rows = sqlx::query(&col_info_sql).fetch_all(&self.pool).await?;

            let mut columns = Vec::new();
            for row in rows {
                columns.push(ColumnInfo {
                    name: row.get("name"),
                    col_type: row.get("type"),
                    notnull: row.get::<i32, _>("notnull") != 0,
                    dflt_value: row.get("dflt_value"),
                    pk: row.get::<i32, _>("pk") != 0,
                });
            }

            metainfo = Some(ExportMetaInfo {
                tbl_name: self.table_name.clone(),
                core_ver: env!("CARGO_PKG_VERSION").to_string(),
                col_info: columns,
            });
        }

        // Build data query
        let mut sql = if self.nohist {
            format!(
                "SELECT T1.version, T1.pkgarch, T1.checksum, T1.value FROM {} as T1, \
                 (SELECT version, pkgarch, max(value) as maxvalue FROM {} GROUP BY version, pkgarch) as T2 \
                 WHERE T1.version=T2.version AND T1.pkgarch=T2.pkgarch AND T1.value=T2.maxvalue",
                self.table_name, self.table_name
            )
        } else {
            format!(
                "SELECT version, pkgarch, checksum, value FROM {} WHERE 1=1",
                self.table_name
            )
        };

        let mut bind_values = Vec::new();

        if let Some(v) = version {
            sql.push_str(" AND version=?");
            bind_values.push(v);
        }
        if let Some(p) = pkgarch {
            sql.push_str(" AND pkgarch=?");
            bind_values.push(p);
        }
        if let Some(c) = checksum {
            sql.push_str(" AND checksum=?");
            bind_values.push(c);
        }

        let mut query = sqlx::query(&sql);
        for value in bind_values {
            query = query.bind(value);
        }

        let rows = query.fetch_all(&self.pool).await?;
        let mut data = Vec::new();

        for row in rows {
            data.push(PrEntry {
                version: row.get("version"),
                pkgarch: row.get("pkgarch"),
                checksum: row.get("checksum"),
                value: row.get("value"),
            });
        }

        Ok((metainfo, data))
    }

    /// Dump the entire database
    #[allow(dead_code)]
    pub async fn dump_db(&self) -> Result<String> {
        // SQLite dump functionality would need to be implemented
        // For now, return a simple representation
        let (_, data) = self.export(None, None, None, false).await?;
        Ok(serde_json::to_string_pretty(&data)?)
    }

    /// Flush any pending writes to disk
    #[allow(dead_code)]
    pub async fn flush(&self) -> Result<()> {
        if let Some(write_manager) = &self.write_manager {
            write_manager
                .flush()
                .await
                .map_err(|e| DatabaseError::Connection(sqlx::Error::Protocol(e.to_string())))?;
        }
        Ok(())
    }

    /// Get the number of pending writes
    #[allow(dead_code)]
    pub async fn pending_writes_count(&self) -> usize {
        if let Some(write_manager) = &self.write_manager {
            write_manager.pending_count().await
        } else {
            0
        }
    }
}

/// PR Database wrapper
pub struct PrDatabase {
    pool: SqlitePool,
    nohist: bool,
    read_only: bool,
    sync_mode: SyncMode,
}

impl PrDatabase {
    /// Create a new PR database
    pub async fn new<P: AsRef<Path>>(
        filename: P,
        nohist: bool,
        read_only: bool,
        sync_mode: SyncMode,
    ) -> Result<Self> {
        let path = filename.as_ref();

        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let database_url = if read_only {
            format!("sqlite:{}?mode=ro", path.display())
        } else {
            format!("sqlite:{}", path.display())
        };

        info!("Opening PR database: {}", database_url);

        // Use SqliteConnectOptions to ensure database file is created if it doesn't exist
        let pool = if read_only {
            // For read-only mode, don't create the file if it doesn't exist
            SqlitePool::connect(&database_url).await?
        } else {
            // For read-write mode, create the file if it doesn't exist
            let options = SqliteConnectOptions::from_str(&database_url)?.create_if_missing(true);
            SqlitePool::connect_with(options).await?
        };

        // Configure SQLite for immediate persistence and performance
        if !read_only {
            // Enable WAL mode for better concurrency and crash safety
            sqlx::query("PRAGMA journal_mode = WAL")
                .execute(&pool)
                .await?;

            // Ensure synchronous writes for data safety
            sqlx::query("PRAGMA synchronous = FULL")
                .execute(&pool)
                .await?;

            // Set reasonable timeout for busy database
            sqlx::query("PRAGMA busy_timeout = 30000")
                .execute(&pool)
                .await?;
        }

        Ok(Self {
            pool,
            nohist,
            read_only,
            sync_mode,
        })
    }

    /// Get a table instance
    pub async fn get_table(&self, table_name: &str) -> Result<PrTable> {
        PrTable::new(
            self.pool.clone(),
            table_name,
            self.nohist,
            self.read_only,
            self.sync_mode.clone(),
        )
        .await
    }

    /// Flush any buffered writes to ensure data persistence
    #[allow(dead_code)]
    pub async fn flush(&self) -> Result<()> {
        // For now, this is a no-op since the PrDatabase doesn't directly manage
        // buffered writes. The buffering is handled at the PrTable level.
        // In a more complete implementation, we might track all tables and flush them.
        Ok(())
    }

    /// Close the database connection
    #[allow(dead_code)]
    pub async fn close(self) {
        self.pool.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_version_increment_different_checksums() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_increment.db");

        // Create database with history mode (allows increments)
        let database = PrDatabase::new(&db_path, false, false, SyncMode::Immediate)
            .await
            .unwrap();
        let table = database.get_table("TEST_INCREMENT_TABLE").await.unwrap();

        let version = "1.0.0";
        let pkgarch = "x86_64";

        // First request: new package with checksum1 should get PR value 0
        let checksum1 = "abc123def456";
        let pr_value1 = table.get_value(version, pkgarch, checksum1).await.unwrap();
        assert_eq!(pr_value1, 0, "First checksum should get PR value 0");

        // Second request: same package/arch but different checksum should get PR value 1
        let checksum2 = "789xyz012abc";
        let pr_value2 = table.get_value(version, pkgarch, checksum2).await.unwrap();
        assert_eq!(
            pr_value2, 1,
            "Different checksum should get incremented PR value"
        );

        // Third request: another different checksum should get PR value 2
        let checksum3 = "fedcba987654";
        let pr_value3 = table.get_value(version, pkgarch, checksum3).await.unwrap();
        assert_eq!(pr_value3, 2, "Third checksum should get PR value 2");

        // Requesting the same checksum again should return the same PR value
        let pr_value1_again = table.get_value(version, pkgarch, checksum1).await.unwrap();
        assert_eq!(
            pr_value1_again, 0,
            "Same checksum should return same PR value"
        );

        let pr_value2_again = table.get_value(version, pkgarch, checksum2).await.unwrap();
        assert_eq!(
            pr_value2_again, 1,
            "Same checksum should return same PR value"
        );

        // Test with different architecture - should start from 0 again
        let different_arch = "aarch64";
        let pr_value_diff_arch = table
            .get_value(version, different_arch, checksum1)
            .await
            .unwrap();
        assert_eq!(
            pr_value_diff_arch, 0,
            "Different architecture should start from 0"
        );

        // Test with different version - should start from 0 again
        let different_version = "2.0.0";
        let pr_value_diff_version = table
            .get_value(different_version, pkgarch, checksum1)
            .await
            .unwrap();
        assert_eq!(
            pr_value_diff_version, 0,
            "Different version should start from 0"
        );
    }

    #[tokio::test]
    async fn test_find_value_existing_entries() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_find.db");

        let database = PrDatabase::new(&db_path, false, false, SyncMode::Immediate)
            .await
            .unwrap();
        let table = database.get_table("TEST_FIND_TABLE").await.unwrap();

        let version = "1.0.0";
        let pkgarch = "x86_64";
        let checksum = "test_checksum";

        // Initially should find nothing
        let initial_find = table.find_value(version, pkgarch, checksum).await.unwrap();
        assert_eq!(initial_find, None, "Should find no value initially");

        // Create an entry
        let pr_value = table.get_value(version, pkgarch, checksum).await.unwrap();
        assert_eq!(pr_value, 0, "First entry should get PR value 0");

        // Now find_value should return the created value
        let found_value = table.find_value(version, pkgarch, checksum).await.unwrap();
        assert_eq!(found_value, Some(0), "Should find the created value");

        // find_max_value should also work
        let max_value = table.find_max_value(version, pkgarch).await.unwrap();
        assert_eq!(max_value, Some(0), "Max value should be 0");
    }

    #[tokio::test]
    async fn test_nohist_mode_behavior() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_nohist.db");

        // Create database with nohist=true (no decrements allowed)
        let database = PrDatabase::new(&db_path, true, false, SyncMode::Immediate)
            .await
            .unwrap();
        let table = database.get_table("TEST_NOHIST_TABLE").await.unwrap();

        let version = "1.0.0";
        let pkgarch = "x86_64";

        // First checksum should get PR value 0
        let checksum1 = "checksum1";
        let pr_value1 = table.get_value(version, pkgarch, checksum1).await.unwrap();
        assert_eq!(pr_value1, 0, "First checksum in nohist mode should get 0");

        // Different checksum should get incremented value
        let checksum2 = "checksum2";
        let pr_value2 = table.get_value(version, pkgarch, checksum2).await.unwrap();
        assert_eq!(
            pr_value2, 1,
            "Different checksum in nohist mode should increment"
        );

        // Same checksum should return same value
        let pr_value1_again = table.get_value(version, pkgarch, checksum1).await.unwrap();
        assert_eq!(
            pr_value1_again, 0,
            "Same checksum should return same value in nohist mode"
        );
    }
}
