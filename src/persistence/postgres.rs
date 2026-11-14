//! Implements one external persistence driver.

// Rust guideline compliant 2026-02-21
use super::{
    AggregatedLog, DistillError, LogEvent, PgPool, PgPoolOptions, Sink, async_trait, count_as_i64,
    event_fingerprint, snapshot_fingerprint, snapshot_revision,
};

#[derive(Clone)]
pub struct PostgresSink {
    pool: PgPool,
}

impl PostgresSink {
    pub async fn connect(database_url: &str, auto_migrate: bool) -> Result<Self, DistillError> {
        let pool = PgPoolOptions::new()
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
            pool: PgPoolOptions::new()
                .max_connections(16)
                .connect_lazy(database_url)?,
        })
    }

    async fn verify_schema(&self) -> Result<(), DistillError> {
        let version: Option<i32> =
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
                event_id UUID PRIMARY KEY,
                channel_id TEXT NOT NULL,
                event_timestamp TIMESTAMPTZ NOT NULL,
                severity SMALLINT NOT NULL,
                payload JSONB NOT NULL,
                metadata JSONB NOT NULL,
                event_fingerprint TEXT NOT NULL DEFAULT '',
                configuration_version TEXT
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS distill_aggregates (
                aggregate_id UUID PRIMARY KEY,
                channel_id TEXT NOT NULL,
                window_start TIMESTAMPTZ NOT NULL,
                window_end TIMESTAMPTZ NOT NULL,
                aggregator_type TEXT NOT NULL,
                aggregated_value DOUBLE PRECISION NOT NULL,
                event_count BIGINT NOT NULL,
                late_event_count BIGINT NOT NULL,
                rolling BOOLEAN NOT NULL DEFAULT FALSE,
                custom_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                update_revision BIGINT NOT NULL DEFAULT 0,
                snapshot_fingerprint TEXT NOT NULL DEFAULT '',
                configuration_version TEXT
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "ALTER TABLE distill_aggregates
                ADD COLUMN IF NOT EXISTS rolling BOOLEAN NOT NULL DEFAULT FALSE",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "ALTER TABLE distill_aggregates
                ADD COLUMN IF NOT EXISTS custom_metadata JSONB NOT NULL DEFAULT '{}'::jsonb",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query("ALTER TABLE distill_events ADD COLUMN IF NOT EXISTS event_fingerprint TEXT NOT NULL DEFAULT '', ADD COLUMN IF NOT EXISTS configuration_version TEXT")
            .execute(&self.pool).await?;
        sqlx::query("ALTER TABLE distill_aggregates ADD COLUMN IF NOT EXISTS update_revision BIGINT NOT NULL DEFAULT 0, ADD COLUMN IF NOT EXISTS snapshot_fingerprint TEXT NOT NULL DEFAULT '', ADD COLUMN IF NOT EXISTS configuration_version TEXT")
            .execute(&self.pool).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS distill_schema_migrations (version INT PRIMARY KEY)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "INSERT INTO distill_schema_migrations (version) VALUES (1) ON CONFLICT DO NOTHING",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for PostgresSink {
    async fn persist_event(&self, event: &LogEvent) -> Result<(), DistillError> {
        let fingerprint = event_fingerprint(event)?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO distill_events
                (event_id, channel_id, event_timestamp, severity, payload, metadata, event_fingerprint, configuration_version)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(event.event_id).bind(&event.channel_id).bind(event.timestamp).bind(event.severity_level as i16)
        .bind(&event.payload).bind(serde_json::to_value(&event.metadata).map_err(|_| DistillError::ConfigurationConflict)?)
        .bind(&fingerprint).bind(&event.version).execute(&mut *transaction).await?;
        let current: String = sqlx::query_scalar(
            "SELECT event_fingerprint FROM distill_events WHERE event_id = $1 FOR UPDATE",
        )
        .bind(event.event_id)
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
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
             ON CONFLICT (aggregate_id) DO UPDATE SET
                window_end = EXCLUDED.window_end,
                aggregated_value = EXCLUDED.aggregated_value,
                event_count = EXCLUDED.event_count,
                late_event_count = EXCLUDED.late_event_count,
                custom_metadata = EXCLUDED.custom_metadata,
                update_revision = EXCLUDED.update_revision,
                snapshot_fingerprint = EXCLUDED.snapshot_fingerprint
             WHERE distill_aggregates.update_revision < EXCLUDED.update_revision",
        )
        .bind(aggregate.aggregate_id)
        .bind(&aggregate.channel_id)
        .bind(aggregate.window_start)
        .bind(aggregate.window_end)
        .bind(format!("{:?}", aggregate.aggregator_type))
        .bind(aggregate.aggregated_value)
        .bind(count_as_i64(aggregate.count))
        .bind(count_as_i64(aggregate.late_event_count))
        .bind(aggregate.rolling)
        .bind(metadata)
        .bind(revision)
        .bind(&fingerprint)
        .bind(&aggregate.version)
        .execute(&mut *transaction)
        .await?;
        let current: (i64, String) = sqlx::query_as("SELECT update_revision, snapshot_fingerprint FROM distill_aggregates WHERE aggregate_id = $1 FOR UPDATE")
            .bind(aggregate.aggregate_id).fetch_one(&mut *transaction).await?;
        if current.0 == revision && current.1 != fingerprint {
            return Err(DistillError::ConfigurationConflict);
        }
        transaction.commit().await?;
        Ok(())
    }
}
