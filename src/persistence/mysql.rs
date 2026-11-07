//! Implements one external persistence driver.

// Rust guideline compliant 2026-02-21
use super::{
    AggregatedLog, DistillError, LogEvent, MySqlPool, MySqlPoolOptions, Sink, async_trait,
    count_as_i64, event_fingerprint, snapshot_fingerprint, snapshot_revision,
};

#[derive(Clone, Debug)]
pub struct MySqlSink {
    pool: MySqlPool,
}

impl MySqlSink {
    pub async fn connect(database_url: &str, auto_migrate: bool) -> Result<Self, DistillError> {
        let pool = MySqlPoolOptions::new()
            .max_connections(16)
            .connect(database_url)
            .await?;
        let sink = Self { pool };
        if auto_migrate {
            sink.initialize().await?;
        }
        sink.verify_schema().await?;
        Ok(sink)
    }

    pub(super) fn restore(database_url: &str) -> Result<Self, DistillError> {
        Ok(Self {
            pool: MySqlPoolOptions::new()
                .max_connections(16)
                .connect_lazy(database_url)?,
        })
    }

    async fn verify_schema(&self) -> Result<(), DistillError> {
        let version: Option<i64> =
            sqlx::query_scalar("SELECT MAX(version) FROM distill_schema_migrations")
                .fetch_one(&self.pool)
                .await
                .map_err(|_| super::migration_required())?;
        if version != Some(1) {
            return Err(super::migration_required());
        }
        Ok(())
    }

    async fn initialize(&self) -> Result<(), DistillError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS distill_events (
                event_id CHAR(36) PRIMARY KEY,
                channel_id VARCHAR(255) NOT NULL,
                event_timestamp DATETIME(6) NOT NULL,
                severity SMALLINT NOT NULL,
                payload JSON NOT NULL,
                metadata JSON NOT NULL,
                event_fingerprint VARCHAR(64) NOT NULL DEFAULT '',
                configuration_version VARCHAR(64)
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS distill_aggregates (
                aggregate_id CHAR(36) PRIMARY KEY,
                channel_id VARCHAR(255) NOT NULL,
                window_start DATETIME(6) NOT NULL,
                window_end DATETIME(6) NOT NULL,
                aggregator_type VARCHAR(100) NOT NULL,
                aggregated_value DOUBLE NOT NULL,
                event_count BIGINT NOT NULL,
                late_event_count BIGINT NOT NULL,
                rolling BOOLEAN NOT NULL DEFAULT FALSE,
                custom_metadata JSON NOT NULL,
                update_revision BIGINT NOT NULL DEFAULT 0,
                snapshot_fingerprint VARCHAR(64) NOT NULL DEFAULT '',
                configuration_version VARCHAR(64)
            )",
        )
        .execute(&self.pool)
        .await?;
        for (column, definition) in [
            ("update_revision", "BIGINT NOT NULL DEFAULT 0"),
            ("snapshot_fingerprint", "VARCHAR(64) NOT NULL DEFAULT ''"),
            ("configuration_version", "VARCHAR(64) NULL"),
        ] {
            let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'distill_aggregates' AND COLUMN_NAME = ?")
                .bind(column).fetch_one(&self.pool).await?;
            if exists == 0 {
                sqlx::query(&format!(
                    "ALTER TABLE distill_aggregates ADD COLUMN {column} {definition}"
                ))
                .execute(&self.pool)
                .await?;
            }
        }
        for (column, definition) in [
            ("event_fingerprint", "VARCHAR(64) NOT NULL DEFAULT ''"),
            ("configuration_version", "VARCHAR(64) NULL"),
        ] {
            let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'distill_events' AND COLUMN_NAME = ?")
                .bind(column).fetch_one(&self.pool).await?;
            if exists == 0 {
                sqlx::query(&format!(
                    "ALTER TABLE distill_events ADD COLUMN {column} {definition}"
                ))
                .execute(&self.pool)
                .await?;
            }
        }
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS distill_schema_migrations (version INT PRIMARY KEY)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query("INSERT IGNORE INTO distill_schema_migrations (version) VALUES (1)")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for MySqlSink {
    async fn persist_event(&self, event: &LogEvent) -> Result<(), DistillError> {
        let fingerprint = event_fingerprint(event)?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO distill_events
                (event_id, channel_id, event_timestamp, severity, payload, metadata, event_fingerprint, configuration_version)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE event_id = VALUES(event_id)",
        )
        .bind(event.event_id.to_string()).bind(&event.channel_id).bind(event.timestamp).bind(event.severity_level as i16)
        .bind(&event.payload).bind(serde_json::to_value(&event.metadata).map_err(|_| DistillError::ConfigurationConflict)?)
        .bind(&fingerprint).bind(&event.version).execute(&mut *transaction).await?;
        let current: String = sqlx::query_scalar(
            "SELECT event_fingerprint FROM distill_events WHERE event_id = ? FOR UPDATE",
        )
        .bind(event.event_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if current != fingerprint {
            return Err(DistillError::ConfigurationConflict);
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn persist_aggregate(&self, aggregate: &AggregatedLog) -> Result<(), DistillError> {
        let revision = snapshot_revision(aggregate)?;
        let fingerprint = snapshot_fingerprint(aggregate)?;
        let metadata = serde_json::to_value(&aggregate.custom_metadata)
            .map_err(|_| DistillError::ConfigurationConflict)?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO distill_aggregates
                (aggregate_id, channel_id, window_start, window_end, aggregator_type,
                 aggregated_value, event_count, late_event_count, rolling, custom_metadata,
                 update_revision, snapshot_fingerprint, configuration_version)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                window_end = IF(update_revision < VALUES(update_revision), VALUES(window_end), window_end),
                aggregated_value = IF(update_revision < VALUES(update_revision), VALUES(aggregated_value), aggregated_value),
                event_count = IF(update_revision < VALUES(update_revision), VALUES(event_count), event_count),
                late_event_count = IF(update_revision < VALUES(update_revision), VALUES(late_event_count), late_event_count),
                custom_metadata = IF(update_revision < VALUES(update_revision), VALUES(custom_metadata), custom_metadata),
                snapshot_fingerprint = IF(update_revision < VALUES(update_revision), VALUES(snapshot_fingerprint), snapshot_fingerprint),
                update_revision = GREATEST(update_revision, VALUES(update_revision))",
        )
        .bind(aggregate.aggregate_id.to_string()).bind(&aggregate.channel_id)
        .bind(aggregate.window_start).bind(aggregate.window_end)
        .bind(format!("{:?}", aggregate.aggregator_type)).bind(aggregate.aggregated_value)
        .bind(count_as_i64(aggregate.count)).bind(count_as_i64(aggregate.late_event_count))
        .bind(aggregate.rolling).bind(metadata).bind(revision).bind(&fingerprint).bind(&aggregate.version)
        .execute(&mut *transaction).await?;
        let current: (i64, String) = sqlx::query_as("SELECT update_revision, snapshot_fingerprint FROM distill_aggregates WHERE aggregate_id = ? FOR UPDATE")
            .bind(aggregate.aggregate_id.to_string()).fetch_one(&mut *transaction).await?;
        if current.0 == revision && current.1 != fingerprint {
            return Err(DistillError::ConfigurationConflict);
        }
        transaction.commit().await?;
        Ok(())
    }
}
