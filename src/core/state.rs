//! Persists internal recovery state independently of external log destinations.

// Rust guideline compliant 2026-02-21
use crate::{
    core::{errors::DistillError, registry::ChannelLifecycle},
    domain::{AggregatedLog, ChannelDefinition, LogEvent, SinkConfig},
};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{fs::File, path::Path, sync::Arc, time::Duration};
use uuid::Uuid;

/// Keeps the process lease alive until every state-store handle is dropped.
#[derive(Clone, Debug)]
pub(crate) struct StateStore {
    pool: SqlitePool,
    _lease: Arc<File>,
}

#[derive(Clone, Debug)]
pub(crate) struct StoredChannel {
    pub(crate) generation: Uuid,
    pub(crate) definition: ChannelDefinition,
    pub(crate) lifecycle: ChannelLifecycle,
    pub(crate) active: bool,
}

impl StateStore {
    /// Opens protected local state and refuses concurrent process ownership.
    ///
    /// Use a persistent local filesystem. SQLite WAL on network storage is not
    /// supported. The catalog contains credentials and must remain private.
    ///
    /// # Errors
    /// Returns a safe error for unsafe paths, ownership conflicts, unsupported
    /// schema versions, corrupt state, or unavailable storage.
    #[cfg(test)]
    pub(crate) async fn open(path: &Path) -> Result<Self, DistillError> {
        Self::open_with_limit(path, 1024 * 1024 * 1024).await
    }

    pub(crate) async fn open_with_limit(path: &Path, max_bytes: u64) -> Result<Self, DistillError> {
        let path = path.to_path_buf();
        let lease_path = path.clone();
        let lease = tokio::task::spawn_blocking(move || protect_files(&lease_path))
            .await
            .map_err(|_| unavailable())??;
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(false)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|_| unavailable())?;
        let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
            .fetch_one(&pool)
            .await
            .map_err(|_| unavailable())?;
        let max_pages = max_bytes / u64::try_from(page_size).map_err(|_| unavailable())?;
        if max_pages == 0 {
            return Err(unavailable());
        }
        let actual: i64 = sqlx::query_scalar(&format!("PRAGMA max_page_count = {max_pages}"))
            .fetch_one(&pool)
            .await
            .map_err(|_| unavailable())?;
        if u64::try_from(actual).map_err(|_| unavailable())? > max_pages {
            return Err(unavailable());
        }
        let integrity: String = sqlx::query_scalar("PRAGMA quick_check")
            .fetch_one(&pool)
            .await
            .map_err(|_| unavailable())?;
        if integrity != "ok" {
            return Err(unavailable());
        }
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await
            .map_err(|_| unavailable())?;
        if version > 4 {
            return Err(DistillError::Persistence(
                "internal state schema is newer than this server supports".to_string(),
            ));
        }
        let mut transaction = pool.begin().await.map_err(|_| unavailable())?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS channel_generations (
                generation TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                definition TEXT NOT NULL,
                lifecycle TEXT NOT NULL,
                active INTEGER NOT NULL CHECK(active IN (0, 1))
            )",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        if version < 2 {
            sqlx::query("ALTER TABLE channel_generations ADD COLUMN configuration_version TEXT NOT NULL DEFAULT ''")
                .execute(&mut *transaction).await.map_err(|_| unavailable())?;
        }
        for statement in [
            "CREATE TABLE IF NOT EXISTS registered_sinks (sink_id TEXT PRIMARY KEY, config TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS processed_events (generation TEXT NOT NULL, event_id TEXT NOT NULL, fingerprint BLOB NOT NULL, PRIMARY KEY(generation, event_id))",
            "CREATE TABLE IF NOT EXISTS aggregate_checkpoints (generation TEXT PRIMARY KEY, checkpoint TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS pending_aggregates (output_id TEXT PRIMARY KEY, generation TEXT NOT NULL, aggregate TEXT NOT NULL)",
        ] {
            sqlx::query(statement)
                .execute(&mut *transaction)
                .await
                .map_err(|_| unavailable())?;
        }
        sqlx::query("PRAGMA user_version = 4")
            .execute(&mut *transaction)
            .await
            .map_err(|_| unavailable())?;
        transaction.commit().await.map_err(|_| unavailable())?;
        Ok(Self {
            pool,
            _lease: Arc::new(lease),
        })
    }

    /// Restores every accepted-work destination before broker consumption starts.
    ///
    /// # Errors
    /// Returns an error when stored configuration cannot be decoded or read.
    pub(crate) async fn channels(&self) -> Result<Vec<StoredChannel>, DistillError> {
        let rows = sqlx::query(
            "SELECT generation, definition, lifecycle, active FROM channel_generations
             ORDER BY active, generation",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|_| unavailable())?;
        rows.into_iter()
            .map(|row| {
                let generation: String = row.get("generation");
                let definition: String = row.get("definition");
                let lifecycle: String = row.get("lifecycle");
                Ok(StoredChannel {
                    generation: Uuid::parse_str(&generation).map_err(|_| unavailable())?,
                    definition: serde_json::from_str(&definition).map_err(|_| unavailable())?,
                    lifecycle: serde_json::from_str(&lifecycle).map_err(|_| unavailable())?,
                    active: row.get::<i64, _>("active") != 0,
                })
            })
            .collect()
    }

    /// Commits a prepared generation and retains its predecessors for recovery.
    ///
    /// # Errors
    /// Returns an error when serialization or the durable transaction fails.
    pub(crate) async fn activate(&self, record: &StoredChannel) -> Result<(), DistillError> {
        let definition = serde_json::to_string(&record.definition).map_err(|_| unavailable())?;
        let lifecycle = serde_json::to_string(&record.lifecycle).map_err(|_| unavailable())?;
        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        sqlx::query("UPDATE channel_generations SET active = 0 WHERE channel_id = ? AND configuration_version = ?")
            .bind(&record.definition.channel_id)
            .bind(record.definition.version.as_deref().unwrap_or_default())
            .execute(&mut *transaction)
            .await
            .map_err(|_| unavailable())?;
        sqlx::query(
            "INSERT INTO channel_generations (generation, channel_id, definition, lifecycle, active, configuration_version)
             VALUES (?, ?, ?, ?, 1, ?)
             ON CONFLICT(generation) DO UPDATE SET definition = excluded.definition,
                 lifecycle = excluded.lifecycle, active = 1",
        )
        .bind(record.generation.to_string())
        .bind(&record.definition.channel_id)
        .bind(definition)
        .bind(lifecycle)
        .bind(record.definition.version.as_deref().unwrap_or_default())
        .execute(&mut *transaction).await.map_err(|_| unavailable())?;
        transaction.commit().await.map_err(|_| unavailable())
    }

    pub(crate) async fn sinks(&self) -> Result<Vec<(String, SinkConfig)>, DistillError> {
        let rows = sqlx::query("SELECT sink_id, config FROM registered_sinks ORDER BY sink_id")
            .fetch_all(&self.pool)
            .await
            .map_err(|_| unavailable())?;
        rows.into_iter()
            .map(|row| {
                let json: String = row.get("config");
                Ok((
                    row.get("sink_id"),
                    serde_json::from_str(&json).map_err(|_| unavailable())?,
                ))
            })
            .collect()
    }

    pub(crate) async fn register_sink(
        &self,
        sink_id: &str,
        config: &SinkConfig,
    ) -> Result<(), DistillError> {
        let canonical = serde_json::to_value(config)
            .map_err(|_| unavailable())?
            .to_string();
        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        sqlx::query("INSERT INTO registered_sinks (sink_id, config) VALUES (?, ?) ON CONFLICT(sink_id) DO NOTHING")
            .bind(sink_id).bind(&canonical).execute(&mut *transaction).await.map_err(|_| unavailable())?;
        let current: String =
            sqlx::query_scalar("SELECT config FROM registered_sinks WHERE sink_id = ?")
                .bind(sink_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(|_| unavailable())?;
        if current != canonical {
            return Err(DistillError::ConfigurationConflict);
        }
        transaction.commit().await.map_err(|_| unavailable())
    }

    /// Loads an aggregate checkpoint without reconstructing state from sink rows.
    pub(crate) async fn checkpoint<T: DeserializeOwned>(
        &self,
        generation: Uuid,
    ) -> Result<Option<T>, DistillError> {
        let value: Option<String> =
            sqlx::query_scalar("SELECT checkpoint FROM aggregate_checkpoints WHERE generation = ?")
                .bind(generation.to_string())
                .fetch_optional(&self.pool)
                .await
                .map_err(|_| unavailable())?;
        value
            .map(|json| serde_json::from_str(&json).map_err(|_| unavailable()))
            .transpose()
    }

    /// Distinguishes retries from conflicting reuse of a source identity.
    pub(crate) async fn duplicate(
        &self,
        generation: Uuid,
        event: &LogEvent,
    ) -> Result<bool, DistillError> {
        let previous: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT fingerprint FROM processed_events WHERE generation = ? AND event_id = ?",
        )
        .bind(generation.to_string())
        .bind(event.event_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| unavailable())?;
        if let Some(previous) = previous {
            if previous != fingerprint(event)? {
                return Err(DistillError::ConfigurationConflict);
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// Commits duplicate protection, aggregate state, and optional output together.
    pub(crate) async fn commit_input<T: Serialize>(
        &self,
        generation: Uuid,
        event: &LogEvent,
        checkpoint: &T,
    ) -> Result<(), DistillError> {
        let value = serde_json::to_string(checkpoint).map_err(|_| unavailable())?;
        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        sqlx::query(
            "INSERT INTO processed_events (generation, event_id, fingerprint) VALUES (?, ?, ?)",
        )
        .bind(generation.to_string())
        .bind(event.event_id.to_string())
        .bind(fingerprint(event)?)
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        sqlx::query("INSERT INTO aggregate_checkpoints (generation, checkpoint) VALUES (?, ?) ON CONFLICT(generation) DO UPDATE SET checkpoint = excluded.checkpoint")
            .bind(generation.to_string()).bind(value).execute(&mut *transaction).await.map_err(|_| unavailable())?;
        transaction.commit().await.map_err(|_| unavailable())
    }

    /// Makes a completed window recoverable before resetting its in-memory state.
    pub(crate) async fn commit_output<T: Serialize>(
        &self,
        generation: Uuid,
        checkpoint: &T,
        aggregate: &AggregatedLog,
    ) -> Result<(), DistillError> {
        let value = serde_json::to_string(checkpoint).map_err(|_| unavailable())?;
        let payload = serde_json::to_string(aggregate).map_err(|_| unavailable())?;
        let output_id = output_id(aggregate);
        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        sqlx::query(
            "INSERT INTO pending_aggregates (output_id, generation, aggregate) VALUES (?, ?, ?)",
        )
        .bind(output_id)
        .bind(generation.to_string())
        .bind(payload)
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        sqlx::query("INSERT INTO aggregate_checkpoints (generation, checkpoint) VALUES (?, ?) ON CONFLICT(generation) DO UPDATE SET checkpoint = excluded.checkpoint")
            .bind(generation.to_string()).bind(value).execute(&mut *transaction).await.map_err(|_| unavailable())?;
        transaction.commit().await.map_err(|_| unavailable())
    }

    /// Returns a bounded batch of pending publications in commit order.
    pub(crate) async fn pending(
        &self,
        generation: Uuid,
    ) -> Result<Vec<AggregatedLog>, DistillError> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT aggregate FROM pending_aggregates WHERE generation = ? ORDER BY rowid LIMIT 64",
        )
        .bind(generation.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|_| unavailable())?;
        rows.into_iter()
            .map(|json| serde_json::from_str(&json).map_err(|_| unavailable()))
            .collect()
    }

    /// Removes output only after its broker publication has been confirmed.
    pub(crate) async fn published(&self, aggregate: &AggregatedLog) -> Result<(), DistillError> {
        sqlx::query("DELETE FROM pending_aggregates WHERE output_id = ?")
            .bind(output_id(aggregate))
            .execute(&self.pool)
            .await
            .map_err(|_| unavailable())?;
        Ok(())
    }

    /// Updates lifecycle without changing accepted-work generation identity.
    ///
    /// # Errors
    /// Returns an error if the generation is missing or storage is unavailable.
    pub(crate) async fn set_lifecycle(
        &self,
        generation: Uuid,
        lifecycle: ChannelLifecycle,
    ) -> Result<(), DistillError> {
        let value = serde_json::to_string(&lifecycle).map_err(|_| unavailable())?;
        let result =
            sqlx::query("UPDATE channel_generations SET lifecycle = ? WHERE generation = ?")
                .bind(value)
                .bind(generation.to_string())
                .execute(&self.pool)
                .await
                .map_err(|_| unavailable())?;
        if result.rows_affected() != 1 {
            return Err(unavailable());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn close(&self) {
        self.pool.close().await;
    }
}

fn unavailable() -> DistillError {
    DistillError::Persistence(
        "internal state unavailable or capacity exhausted; check DISTILL_STATE_PATH, DISTILL_STATE_MAX_BYTES and storage".to_string(),
    )
}

fn protect_files(path: &Path) -> Result<File, DistillError> {
    use std::fs::{self, OpenOptions};
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(unavailable)?;
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(parent).map_err(|_| unavailable())?;
    if fs::symlink_metadata(parent)
        .map_err(|_| unavailable())?
        .file_type()
        .is_symlink()
    {
        return Err(unavailable());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // The catalog and its SQLite journal can contain submitted sink secrets.
        if fs::metadata(parent)
            .map_err(|_| unavailable())?
            .permissions()
            .mode()
            & 0o077
            != 0
        {
            return Err(DistillError::Persistence(
                "DISTILL_STATE_PATH requires a private directory (mode 0700)".to_string(),
            ));
        }
    }
    let open = |path: &Path| -> Result<File, DistillError> {
        if fs::symlink_metadata(path)
            .is_ok_and(|metadata| !metadata.is_file() || metadata.file_type().is_symlink())
        {
            return Err(unavailable());
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path).map_err(|_| unavailable())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|_| unavailable())?;
        }
        Ok(file)
    };
    let lease = open(&path.with_extension("lock"))?;
    lease.try_lock().map_err(|_| {
        DistillError::Persistence(
            "internal state is already owned by another Distill instance".to_string(),
        )
    })?;
    drop(open(path)?);
    Ok(lease)
}

fn fingerprint(event: &LogEvent) -> Result<Vec<u8>, DistillError> {
    // Value canonicalizes object keys, including metadata HashMaps, before hashing.
    let canonical = serde_json::to_value(event).map_err(|_| unavailable())?;
    Ok(Sha256::digest(canonical.to_string().as_bytes()).to_vec())
}

fn output_id(aggregate: &AggregatedLog) -> String {
    format!("{}:{}", aggregate.aggregate_id, aggregate.update_revision)
}

#[cfg(test)]
mod tests {
    use super::StateStore;

    #[tokio::test]
    async fn state_is_empty_and_exclusively_owned() {
        let directory =
            std::env::temp_dir().join(format!("distill-state-{}", uuid::Uuid::new_v4()));
        let path = directory.join("state.sqlite3");
        let store = StateStore::open(&path).await.unwrap();
        assert!(store.channels().await.unwrap().is_empty());
        assert!(StateStore::open(&path).await.is_err());
        store.pool.close().await;
        drop(store);
        let reopened = StateStore::open(&path).await.unwrap();
        assert!(reopened.channels().await.unwrap().is_empty());
        reopened.pool.close().await;
        drop(reopened);
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[tokio::test]
    async fn configuration_versions_and_sinks_survive_restart() {
        use super::{ChannelLifecycle, StoredChannel};
        let directory =
            std::env::temp_dir().join(format!("distill-catalog-{}", uuid::Uuid::new_v4()));
        let path = directory.join("state.sqlite3");
        let store = StateStore::open(&path).await.unwrap();
        let config = crate::domain::SinkConfig {
            driver: "http".to_string(),
            connection_string: "https://receiver.test/records".to_string(),
            headers: std::collections::HashMap::new(),
        };
        store.register_sink("receiver@1", &config).await.unwrap();
        store.register_sink("receiver@1", &config).await.unwrap();
        let mut conflict = config.clone();
        conflict.connection_string = "https://different.test".to_string();
        assert!(store.register_sink("receiver@1", &conflict).await.is_err());
        for version in ["1", "2"] {
            let definition = serde_json::from_value(serde_json::json!({
                "channel_id": "catalog.test", "version": version, "severity_threshold": "error",
                "rabbitmq_config": {"connection_string": "amqp://unused", "exchange_name": "unused"},
                "sink_config": config, "aggregator": null,
                "buffer_bytes": 1024, "mailbox_capacity": 8, "ttl_seconds": 3600
            })).unwrap();
            store
                .activate(&StoredChannel {
                    definition,
                    generation: uuid::Uuid::new_v4(),
                    lifecycle: ChannelLifecycle::Active,
                    active: true,
                })
                .await
                .unwrap();
        }
        store.close().await;
        drop(store);
        let store = StateStore::open(&path).await.unwrap();
        let channels = store.channels().await.unwrap();
        assert_eq!(channels.len(), 2);
        assert!(channels.iter().all(|record| record.active));
        assert_eq!(store.sinks().await.unwrap().len(), 1);
        store.close().await;
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn capacity_failure_recovers_without_losing_or_duplicating_input() {
        let directory =
            std::env::temp_dir().join(format!("distill-capacity-{}", uuid::Uuid::new_v4()));
        let path = directory.join("state.sqlite3");
        let store = StateStore::open_with_limit(&path, 1024 * 1024)
            .await
            .unwrap();
        let event: crate::domain::LogEvent = serde_json::from_value(serde_json::json!({
            "event_id": uuid::Uuid::new_v4(), "channel_id": "capacity.test", "timestamp": chrono::Utc::now(),
            "severity_level": "info", "payload": {}
        })).unwrap();
        let generation = uuid::Uuid::new_v4();
        let checkpoint = serde_json::json!({"state": "x".repeat(2 * 1024 * 1024)});
        assert!(
            store
                .commit_input(generation, &event, &checkpoint)
                .await
                .is_err()
        );
        assert!(!store.duplicate(generation, &event).await.unwrap());
        assert!(
            store
                .checkpoint::<serde_json::Value>(generation)
                .await
                .unwrap()
                .is_none()
        );
        store.close().await;
        drop(store);
        let store = StateStore::open_with_limit(&path, 4 * 1024 * 1024)
            .await
            .unwrap();
        store
            .commit_input(generation, &event, &checkpoint)
            .await
            .unwrap();
        assert!(store.duplicate(generation, &event).await.unwrap());
        assert_eq!(
            store
                .checkpoint::<serde_json::Value>(generation)
                .await
                .unwrap(),
            Some(checkpoint)
        );
        store.close().await;
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn failed_checkpoint_does_not_record_an_event_as_processed() {
        let directory =
            std::env::temp_dir().join(format!("distill-rollback-{}", uuid::Uuid::new_v4()));
        let store = StateStore::open(&directory.join("state.sqlite3"))
            .await
            .unwrap();
        let event: crate::domain::LogEvent = serde_json::from_value(serde_json::json!({
            "event_id": uuid::Uuid::new_v4(), "channel_id": "rollback.test", "timestamp": chrono::Utc::now(),
            "severity_level": "info", "payload": {}
        })).unwrap();
        let generation = uuid::Uuid::new_v4();
        sqlx::query("DROP TABLE aggregate_checkpoints")
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(
            store
                .commit_input(generation, &event, &serde_json::json!({"count": 1}))
                .await
                .is_err()
        );
        assert!(!store.duplicate(generation, &event).await.unwrap());
        store.close().await;
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
