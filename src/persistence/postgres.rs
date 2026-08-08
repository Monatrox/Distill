//! Implements one external persistence driver.

// Rust guideline compliant 2026-02-21
use super::{
    AggregatedLog, DistillError, LogEvent, PgPool, PgPoolOptions, Sink, async_trait, count_as_i64,
    merge_aggregate_values,
};
use serde_json::Value;

#[derive(Clone)]
pub struct PostgresSink {
    pool: PgPool,
}

impl PostgresSink {
    pub async fn connect(database_url: &str) -> Result<Self, DistillError> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(database_url)
            .await?;
        let sink = Self { pool };
        sink.initialize().await?;
        Ok(sink)
    }

    async fn initialize(&self) -> Result<(), DistillError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS distill_events (
                event_id UUID PRIMARY KEY,
                channel_id TEXT NOT NULL,
                event_timestamp TIMESTAMPTZ NOT NULL,
                severity SMALLINT NOT NULL,
                payload JSONB NOT NULL,
                metadata JSONB NOT NULL
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
                custom_metadata JSONB NOT NULL DEFAULT '{}'::jsonb
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
        Ok(())
    }
}

#[async_trait]
impl Sink for PostgresSink {
    async fn persist_event(&self, event: &LogEvent) -> Result<(), DistillError> {
        sqlx::query(
            "INSERT INTO distill_events
                (event_id, channel_id, event_timestamp, severity, payload, metadata)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(event.event_id)
        .bind(&event.channel_id)
        .bind(event.timestamp)
        .bind(event.severity_level as i16)
        .bind(&event.payload)
        .bind(
            serde_json::to_value(&event.metadata)
                .map_err(|error| DistillError::Persistence(error.to_string()))?,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "The transactional rolling merge keeps the persistence invariant in one operation"
    )]
    async fn persist_aggregate(&self, aggregate: &AggregatedLog) -> Result<(), DistillError> {
        let metadata = serde_json::to_value(&aggregate.custom_metadata)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        if !aggregate.rolling {
            sqlx::query(
                "INSERT INTO distill_aggregates
                    (aggregate_id, channel_id, window_start, window_end, aggregator_type,
                     aggregated_value, event_count, late_event_count, rolling, custom_metadata)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, FALSE, $9)
                 ON CONFLICT (aggregate_id) DO NOTHING",
            )
            .bind(aggregate.aggregate_id)
            .bind(&aggregate.channel_id)
            .bind(aggregate.window_start)
            .bind(aggregate.window_end)
            .bind(format!("{:?}", aggregate.aggregator_type))
            .bind(aggregate.aggregated_value)
            .bind(count_as_i64(aggregate.count))
            .bind(count_as_i64(aggregate.late_event_count))
            .bind(metadata)
            .execute(&self.pool)
            .await?;
            return Ok(());
        }

        let mut transaction = self.pool.begin().await?;
        let existing = sqlx::query_as::<
            _,
            (
                f64,
                i64,
                i64,
                String,
                Value,
                chrono::DateTime<chrono::Utc>,
                chrono::DateTime<chrono::Utc>,
                bool,
            ),
        >(
            "SELECT aggregated_value, event_count, late_event_count, aggregator_type,
                    custom_metadata, window_start, window_end, rolling
             FROM distill_aggregates
             WHERE aggregate_id = $1
             FOR UPDATE",
        )
        .bind(aggregate.aggregate_id)
        .fetch_optional(&mut *transaction)
        .await?;

        let incoming_type = format!("{:?}", aggregate.aggregator_type);
        let (value, count, late_count, custom_metadata, window_start) = if let Some((
            existing_value,
            existing_count,
            existing_late,
            existing_type,
            existing_metadata,
            existing_start,
            _,
            existing_rolling,
        )) = existing
        {
            if existing_type != incoming_type || !existing_rolling {
                return Err(DistillError::Persistence(
                    "rolling aggregate identity conflicts with an existing aggregate".to_string(),
                ));
            }
            let (merged_value, merged_metadata) = merge_aggregate_values(
                aggregate.aggregator_type,
                existing_value,
                existing_count,
                existing_metadata,
                aggregate.aggregated_value,
                aggregate.count,
                metadata,
            )
            .map_err(DistillError::Persistence)?;
            (
                merged_value,
                existing_count.saturating_add(count_as_i64(aggregate.count)),
                existing_late.saturating_add(count_as_i64(aggregate.late_event_count)),
                merged_metadata,
                existing_start,
            )
        } else {
            (
                aggregate.aggregated_value,
                count_as_i64(aggregate.count),
                count_as_i64(aggregate.late_event_count),
                metadata,
                aggregate.window_start,
            )
        };

        sqlx::query(
            "INSERT INTO distill_aggregates
                (aggregate_id, channel_id, window_start, window_end, aggregator_type,
                 aggregated_value, event_count, late_event_count, rolling, custom_metadata)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE, $9)
             ON CONFLICT (aggregate_id) DO UPDATE SET
                window_end = EXCLUDED.window_end,
                aggregated_value = EXCLUDED.aggregated_value,
                event_count = EXCLUDED.event_count,
                late_event_count = EXCLUDED.late_event_count,
                custom_metadata = EXCLUDED.custom_metadata",
        )
        .bind(aggregate.aggregate_id)
        .bind(&aggregate.channel_id)
        .bind(window_start)
        .bind(aggregate.window_end)
        .bind(incoming_type)
        .bind(value)
        .bind(count)
        .bind(late_count)
        .bind(custom_metadata)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}
