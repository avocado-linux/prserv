//! Buffered write mechanism for database operations
//!
//! This module provides a buffered write system that can batch database operations
//! and flush them periodically or on demand, improving performance for non-immediate
//! sync modes while maintaining data integrity.

use crate::config::SyncMode;
use crate::database::{DatabaseError, PrEntry};
use sqlx::sqlite::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

pub type Result<T> = std::result::Result<T, DatabaseError>;

/// Represents a pending database write operation
#[derive(Debug, Clone)]
pub enum PendingWrite {
    Insert { table_name: String, entry: PrEntry },
    Update { table_name: String, entry: PrEntry },
}

/// Buffered write manager that handles batching and periodic flushing
pub struct BufferedWriteManager {
    pool: SqlitePool,
    sync_mode: SyncMode,
    pending_writes: Arc<Mutex<Vec<PendingWrite>>>,
    last_flush: Arc<RwLock<Instant>>,
    flush_in_progress: Arc<Mutex<bool>>,
}

impl BufferedWriteManager {
    /// Create a new buffered write manager
    pub fn new(pool: SqlitePool, sync_mode: SyncMode) -> Self {
        Self {
            pool,
            sync_mode,
            pending_writes: Arc::new(Mutex::new(Vec::new())),
            last_flush: Arc::new(RwLock::new(Instant::now())),
            flush_in_progress: Arc::new(Mutex::new(false)),
        }
    }

    /// Start the periodic flush task if in periodic mode
    pub async fn start_periodic_flush(&self) {
        if let Some(flush_interval) = self.sync_mode.as_duration() {
            let pending_writes = self.pending_writes.clone();
            let last_flush = self.last_flush.clone();
            let flush_in_progress = self.flush_in_progress.clone();
            let pool = self.pool.clone();

            tokio::spawn(async move {
                let mut interval_timer = interval(flush_interval);

                loop {
                    interval_timer.tick().await;

                    // Check if we need to flush
                    let should_flush = {
                        let last_flush_time = *last_flush.read().await;
                        let pending_count = pending_writes.lock().await.len();

                        pending_count > 0 && last_flush_time.elapsed() >= flush_interval
                    };

                    if should_flush {
                        if let Err(e) = Self::flush_pending_writes(
                            &pool,
                            &pending_writes,
                            &last_flush,
                            &flush_in_progress,
                        )
                        .await
                        {
                            error!("Failed to flush pending writes: {}", e);
                        }
                    }
                }
            });

            info!(
                "Started periodic flush task with interval: {:?}",
                flush_interval
            );
        }
    }

    /// Add a write operation to the buffer
    pub async fn buffer_write(&self, write: PendingWrite) -> Result<()> {
        match &self.sync_mode {
            SyncMode::Immediate => {
                // Execute immediately
                self.execute_write(&write).await?;
            }
            SyncMode::Periodic { .. } | SyncMode::OnShutdown => {
                // Add to buffer
                let mut pending = self.pending_writes.lock().await;
                pending.push(write);
                debug!("Buffered write operation, {} pending", pending.len());
            }
        }
        Ok(())
    }

    /// Force flush all pending writes
    #[allow(dead_code)]
    pub async fn flush(&self) -> Result<()> {
        Self::flush_pending_writes(
            &self.pool,
            &self.pending_writes,
            &self.last_flush,
            &self.flush_in_progress,
        )
        .await
    }

    /// Internal method to flush pending writes
    async fn flush_pending_writes(
        pool: &SqlitePool,
        pending_writes: &Arc<Mutex<Vec<PendingWrite>>>,
        last_flush: &Arc<RwLock<Instant>>,
        flush_in_progress: &Arc<Mutex<bool>>,
    ) -> Result<()> {
        // Prevent concurrent flushes
        let mut flush_guard = flush_in_progress.lock().await;
        if *flush_guard {
            debug!("Flush already in progress, skipping");
            return Ok(());
        }
        *flush_guard = true;
        drop(flush_guard);

        let writes_to_flush = {
            let mut pending = pending_writes.lock().await;
            if pending.is_empty() {
                let mut flush_guard = flush_in_progress.lock().await;
                *flush_guard = false;
                return Ok(());
            }

            let writes = pending.clone();
            pending.clear();
            writes
        };

        let write_count = writes_to_flush.len();
        debug!("Flushing {} pending writes", write_count);

        // Group writes by table for batch operations
        let mut writes_by_table: HashMap<String, Vec<&PendingWrite>> = HashMap::new();
        for write in &writes_to_flush {
            let table_name = match write {
                PendingWrite::Insert { table_name, .. } => table_name,
                PendingWrite::Update { table_name, .. } => table_name,
            };
            writes_by_table
                .entry(table_name.clone())
                .or_default()
                .push(write);
        }

        // Execute writes in batches per table
        for (table_name, table_writes) in writes_by_table {
            // Clone the writes before passing to avoid move issues
            let writes_clone: Vec<PendingWrite> = table_writes.iter().map(|&w| w.clone()).collect();

            if let Err(e) = Self::execute_batch_writes(pool, &table_name, &table_writes).await {
                error!(
                    "Failed to execute batch writes for table {}: {}",
                    table_name, e
                );

                // Re-add failed writes to the buffer
                let mut pending = pending_writes.lock().await;
                for write in writes_clone {
                    pending.push(write);
                }

                let mut flush_guard = flush_in_progress.lock().await;
                *flush_guard = false;
                return Err(e);
            }
        }

        // Update last flush time
        {
            let mut last_flush_time = last_flush.write().await;
            *last_flush_time = Instant::now();
        }

        let mut flush_guard = flush_in_progress.lock().await;
        *flush_guard = false;

        info!("Successfully flushed {} writes to database", write_count);
        Ok(())
    }

    /// Execute a batch of writes for a specific table
    async fn execute_batch_writes(
        pool: &SqlitePool,
        table_name: &str,
        writes: &[&PendingWrite],
    ) -> Result<()> {
        let mut tx = pool.begin().await?;
        let write_count = writes.len();

        for write in writes {
            match write {
                PendingWrite::Insert { entry, .. } => {
                    let sql = format!(
                        "INSERT OR IGNORE INTO {table_name} (version, pkgarch, checksum, value) VALUES (?, ?, ?, ?)"
                    );
                    sqlx::query(&sql)
                        .bind(&entry.version)
                        .bind(&entry.pkgarch)
                        .bind(&entry.checksum)
                        .bind(entry.value)
                        .execute(&mut *tx)
                        .await?;
                }
                PendingWrite::Update { entry, .. } => {
                    let sql = format!(
                        "INSERT OR REPLACE INTO {table_name} (version, pkgarch, checksum, value) VALUES (?, ?, ?, ?)"
                    );
                    sqlx::query(&sql)
                        .bind(&entry.version)
                        .bind(&entry.pkgarch)
                        .bind(&entry.checksum)
                        .bind(entry.value)
                        .execute(&mut *tx)
                        .await?;
                }
            }
        }

        tx.commit().await?;
        debug!(
            "Executed batch of {} writes for table {}",
            write_count, table_name
        );
        Ok(())
    }

    /// Execute a single write immediately
    async fn execute_write(&self, write: &PendingWrite) -> Result<()> {
        match write {
            PendingWrite::Insert { table_name, entry } => {
                let sql = format!(
                    "INSERT OR IGNORE INTO {table_name} (version, pkgarch, checksum, value) VALUES (?, ?, ?, ?)"
                );
                sqlx::query(&sql)
                    .bind(&entry.version)
                    .bind(&entry.pkgarch)
                    .bind(&entry.checksum)
                    .bind(entry.value)
                    .execute(&self.pool)
                    .await?;
            }
            PendingWrite::Update { table_name, entry } => {
                let sql = format!(
                    "INSERT OR REPLACE INTO {table_name} (version, pkgarch, checksum, value) VALUES (?, ?, ?, ?)"
                );
                sqlx::query(&sql)
                    .bind(&entry.version)
                    .bind(&entry.pkgarch)
                    .bind(&entry.checksum)
                    .bind(entry.value)
                    .execute(&self.pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Get the number of pending writes
    #[allow(dead_code)]
    pub async fn pending_count(&self) -> usize {
        self.pending_writes.lock().await.len()
    }

    /// Check if there are any pending writes
    #[allow(dead_code)]
    pub async fn has_pending_writes(&self) -> bool {
        !self.pending_writes.lock().await.is_empty()
    }
}

impl Drop for BufferedWriteManager {
    fn drop(&mut self) {
        // Note: We can't use async in Drop, so we just warn about unflushed writes
        // The server should call flush() explicitly during shutdown
        warn!("BufferedWriteManager dropped - ensure flush() was called during shutdown");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SyncMode;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap();

        // Create test table
        sqlx::query(
            "CREATE TABLE test_table (
                version TEXT NOT NULL,
                pkgarch TEXT NOT NULL,
                checksum TEXT NOT NULL,
                value INTEGER NOT NULL,
                PRIMARY KEY (version, pkgarch, checksum)
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        pool
    }

    #[tokio::test]
    async fn test_immediate_mode() {
        let pool = create_test_pool().await;
        let manager = BufferedWriteManager::new(pool.clone(), SyncMode::Immediate);

        let write = PendingWrite::Insert {
            table_name: "test_table".to_string(),
            entry: PrEntry {
                version: "1.0".to_string(),
                pkgarch: "x86_64".to_string(),
                checksum: "abc123".to_string(),
                value: 1,
            },
        };

        manager.buffer_write(write).await.unwrap();

        // Should be written immediately
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_table")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(manager.pending_count().await, 0);
    }

    #[tokio::test]
    async fn test_periodic_mode() {
        let pool = create_test_pool().await;
        let manager =
            BufferedWriteManager::new(pool.clone(), SyncMode::Periodic { interval_secs: 1 });

        let write = PendingWrite::Insert {
            table_name: "test_table".to_string(),
            entry: PrEntry {
                version: "1.0".to_string(),
                pkgarch: "x86_64".to_string(),
                checksum: "abc123".to_string(),
                value: 1,
            },
        };

        manager.buffer_write(write).await.unwrap();

        // Should be buffered, not written yet
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_table")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
        assert_eq!(manager.pending_count().await, 1);

        // Manual flush should write it
        manager.flush().await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_table")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(manager.pending_count().await, 0);
    }

    #[tokio::test]
    async fn test_on_shutdown_mode() {
        let pool = create_test_pool().await;
        let manager = BufferedWriteManager::new(pool.clone(), SyncMode::OnShutdown);

        let write = PendingWrite::Insert {
            table_name: "test_table".to_string(),
            entry: PrEntry {
                version: "1.0".to_string(),
                pkgarch: "x86_64".to_string(),
                checksum: "abc123".to_string(),
                value: 1,
            },
        };

        manager.buffer_write(write).await.unwrap();

        // Should be buffered
        assert_eq!(manager.pending_count().await, 1);

        // Manual flush should work
        manager.flush().await.unwrap();
        assert_eq!(manager.pending_count().await, 0);
    }
}
