//! Implements one external persistence driver.

// Rust guideline compliant 2026-02-21
use super::{
    AggregatedLog, DistillError, LogEvent, MySqlPool, MySqlPoolOptions, Sink, async_trait,
    count_as_i64,
};

#[derive(Clone, Debug)]
pub struct MySqlSink {
    pool: MySqlPool,
}

impl MySqlSink {
    pub async fn connect(database_url: &str) -> Result<Self, DistillError> {
        let pool = MySqlPoolOptions::new()
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
                event_id CHAR(36) PRIMARY KEY,
                channel_id VARCHAR(255) NOT NULL,
                event_timestamp DATETIME(6) NOT NULL,
                severity SMALLINT NOT NULL,
                payload JSON NOT NULL,
                metadata JSON NOT NULL
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
                custom_metadata JSON NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for MySqlSink {
    async fn persist_event(&self, event: &LogEvent) -> Result<(), DistillError> {
        sqlx::query(
            "INSERT IGNORE INTO distill_events
                (event_id, channel_id, event_timestamp, severity, payload, metadata)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(event.event_id.to_string())
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

    async fn persist_aggregate(&self, aggregate: &AggregatedLog) -> Result<(), DistillError> {
        let metadata = serde_json::to_value(&aggregate.custom_metadata)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        sqlx::query(
            "INSERT INTO distill_aggregates
                (aggregate_id, channel_id, window_start, window_end, aggregator_type,
                 aggregated_value, event_count, late_event_count, rolling, custom_metadata)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                window_end = VALUES(window_end),
                aggregated_value = VALUES(aggregated_value),
                event_count = VALUES(event_count),
                late_event_count = VALUES(late_event_count),
                rolling = VALUES(rolling),
                custom_metadata = VALUES(custom_metadata)",
        )
        .bind(aggregate.aggregate_id.to_string())
        .bind(&aggregate.channel_id)
        .bind(aggregate.window_start)
        .bind(aggregate.window_end)
        .bind(format!("{:?}", aggregate.aggregator_type))
        .bind(aggregate.aggregated_value)
        .bind(count_as_i64(aggregate.count))
        .bind(count_as_i64(aggregate.late_event_count))
        .bind(aggregate.rolling)
        .bind(metadata)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
