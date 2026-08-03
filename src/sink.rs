//! Provides `PostgreSQL` persistence and `RabbitMQ` delivery primitives.

use crate::{
    domain::{AggregatedLog, LogEvent},
    errors::DistillError,
    metrics::Metrics,
};
use async_trait::async_trait;
use futures_util::StreamExt;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
        ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{MySqlPool, PgPool, mysql::MySqlPoolOptions, postgres::PgPoolOptions};
use std::{collections::HashMap, sync::Arc};
use tiberius::{Client, Config};
use tokio::time::{Duration, sleep};
use tokio::{
    net::TcpStream,
    sync::{Mutex, RwLock},
};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{error, warn};

const DELIVERY_QUEUE: &str = "distill.persistence";
const DEAD_LETTER_EXCHANGE: &str = "distill.dead-letter";
const MAX_RETRIES: u32 = 10;
// A bounded timeout prevents an unavailable HTTP receiver from holding a delivery forever.
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[async_trait]
pub trait Sink: Send + Sync {
    async fn persist_event(&self, event: &LogEvent) -> Result<(), DistillError>;
    async fn persist_aggregate(&self, aggregate: &AggregatedLog) -> Result<(), DistillError>;
}

/// Creates a configured sink for one channel.
pub async fn build_sink(config: &crate::domain::SinkConfig) -> Result<Arc<dyn Sink>, DistillError> {
    match config.driver.as_str() {
        "postgres" => Ok(Arc::new(
            PostgresSink::connect(&config.connection_string).await?,
        )),
        "mysql" => Ok(Arc::new(
            MySqlSink::connect(&config.connection_string).await?,
        )),
        "sqlserver" | "mssql" => Ok(Arc::new(
            SqlServerSink::connect(&config.connection_string).await?,
        )),
        "http" => Ok(Arc::new(HttpSink::connect(config)?)),
        driver => Err(DistillError::InvalidConfiguration(format!(
            "unsupported sink driver: {driver}"
        ))),
    }
}

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
                &incoming_type,
                existing_value,
                existing_count,
                existing_metadata,
                aggregate.aggregated_value,
                aggregate.count,
                metadata,
            )?;
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

#[derive(Clone, Debug)]
pub struct HttpSink {
    client: reqwest::Client,
    endpoint: String,
}

impl HttpSink {
    pub fn connect(config: &crate::domain::SinkConfig) -> Result<Self, DistillError> {
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in &config.headers {
            let name = reqwest::header::HeaderName::try_from(name).map_err(|error| {
                DistillError::InvalidConfiguration(format!("invalid HTTP header name: {error}"))
            })?;
            let value = reqwest::header::HeaderValue::try_from(value).map_err(|error| {
                DistillError::InvalidConfiguration(format!("invalid HTTP header value: {error}"))
            })?;
            headers.insert(name, value);
        }
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(HTTP_REQUEST_TIMEOUT)
            .build()
            .map_err(|error| DistillError::InvalidConfiguration(error.to_string()))?;
        reqwest::Url::parse(&config.connection_string).map_err(|error| {
            DistillError::InvalidConfiguration(format!("invalid HTTP sink URL: {error}"))
        })?;
        Ok(Self {
            client,
            endpoint: config.connection_string.clone(),
        })
    }

    async fn send(
        &self,
        kind: &str,
        id: String,
        value: &impl Serialize,
    ) -> Result<(), DistillError> {
        let response = self
            .client
            .post(&self.endpoint)
            .header("X-Distill-Record-Type", kind)
            .header("X-Distill-Record-Id", id)
            .json(value)
            .send()
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        if response.status().is_success() || response.status() == reqwest::StatusCode::CONFLICT {
            return Ok(());
        }
        Err(DistillError::Persistence(format!(
            "HTTP sink returned status {}",
            response.status()
        )))
    }
}

#[async_trait]
impl Sink for HttpSink {
    async fn persist_event(&self, event: &LogEvent) -> Result<(), DistillError> {
        self.send("event", event.event_id.to_string(), event).await
    }

    async fn persist_aggregate(&self, aggregate: &AggregatedLog) -> Result<(), DistillError> {
        self.send("aggregate", aggregate.aggregate_id.to_string(), aggregate)
            .await
    }
}

type SqlServerConnection = Compat<TcpStream>;

#[derive(Clone)]
pub struct SqlServerSink {
    client: Arc<Mutex<Client<SqlServerConnection>>>,
}

impl SqlServerSink {
    pub async fn connect(connection_string: &str) -> Result<Self, DistillError> {
        let config = Config::from_ado_string(connection_string).map_err(|error| {
            DistillError::InvalidConfiguration(format!(
                "invalid SQL Server connection string: {error}"
            ))
        })?;
        let tcp = TcpStream::connect(config.get_addr())
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        tcp.set_nodelay(true)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        let client = Client::connect(config, tcp.compat_write())
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        let sink = Self {
            client: Arc::new(Mutex::new(client)),
        };
        sink.initialize().await?;
        Ok(sink)
    }

    async fn initialize(&self) -> Result<(), DistillError> {
        let mut client = self.client.lock().await;
        client
            .execute(
                "IF OBJECT_ID(N'distill_events', N'U') IS NULL
                 BEGIN
                    CREATE TABLE distill_events (
                        event_id NVARCHAR(36) NOT NULL PRIMARY KEY,
                        channel_id NVARCHAR(255) NOT NULL,
                        event_timestamp DATETIME2 NOT NULL,
                        severity SMALLINT NOT NULL,
                        payload NVARCHAR(MAX) NOT NULL,
                        metadata NVARCHAR(MAX) NOT NULL
                    )
                 END;
                 IF OBJECT_ID(N'distill_aggregates', N'U') IS NULL
                 BEGIN
                    CREATE TABLE distill_aggregates (
                        aggregate_id NVARCHAR(36) NOT NULL PRIMARY KEY,
                        channel_id NVARCHAR(255) NOT NULL,
                        window_start DATETIME2 NOT NULL,
                        window_end DATETIME2 NOT NULL,
                        aggregator_type NVARCHAR(100) NOT NULL,
                        aggregated_value FLOAT NOT NULL,
                        event_count BIGINT NOT NULL,
                        late_event_count BIGINT NOT NULL,
                        rolling BIT NOT NULL DEFAULT 0,
                        custom_metadata NVARCHAR(MAX) NOT NULL
                    )
                 END",
                &[],
            )
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl Sink for SqlServerSink {
    async fn persist_event(&self, event: &LogEvent) -> Result<(), DistillError> {
        let event_id = event.event_id.to_string();
        let timestamp = event.timestamp.naive_utc();
        let payload = serde_json::to_string(&event.payload)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        let metadata = serde_json::to_string(&event.metadata)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        let severity = event.severity_level as i16;
        let params: [&dyn tiberius::ToSql; 6] = [
            &event_id,
            &event.channel_id,
            &timestamp,
            &severity,
            &payload,
            &metadata,
        ];
        self.client
            .lock()
            .await
            .execute(
                "IF NOT EXISTS (SELECT 1 FROM distill_events WHERE event_id = @P1)
                 INSERT INTO distill_events
                    (event_id, channel_id, event_timestamp, severity, payload, metadata)
                 VALUES (@P1, @P2, @P3, @P4, @P5, @P6)",
                &params,
            )
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        Ok(())
    }

    async fn persist_aggregate(&self, aggregate: &AggregatedLog) -> Result<(), DistillError> {
        let aggregate_id = aggregate.aggregate_id.to_string();
        let channel_id = aggregate.channel_id.clone();
        let window_start = aggregate.window_start.naive_utc();
        let window_end = aggregate.window_end.naive_utc();
        let aggregator_type = format!("{:?}", aggregate.aggregator_type);
        let value = aggregate.aggregated_value;
        let count = count_as_i64(aggregate.count);
        let late_count = count_as_i64(aggregate.late_event_count);
        let rolling = aggregate.rolling;
        let metadata = serde_json::to_string(&aggregate.custom_metadata)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        let params: [&dyn tiberius::ToSql; 10] = [
            &aggregate_id,
            &channel_id,
            &window_start,
            &window_end,
            &aggregator_type,
            &value,
            &count,
            &late_count,
            &rolling,
            &metadata,
        ];
        self.client
            .lock()
            .await
            .execute(
                "MERGE distill_aggregates AS target
                 USING (VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10))
                    AS source (aggregate_id, channel_id, window_start, window_end,
                               aggregator_type, aggregated_value, event_count,
                               late_event_count, rolling, custom_metadata)
                 ON target.aggregate_id = source.aggregate_id
                 WHEN MATCHED THEN UPDATE SET
                    window_end = source.window_end,
                    aggregated_value = source.aggregated_value,
                    event_count = source.event_count,
                    late_event_count = source.late_event_count,
                    rolling = source.rolling,
                    custom_metadata = source.custom_metadata
                 WHEN NOT MATCHED THEN INSERT
                    (aggregate_id, channel_id, window_start, window_end, aggregator_type,
                     aggregated_value, event_count, late_event_count, rolling, custom_metadata)
                 VALUES (source.aggregate_id, source.channel_id, source.window_start,
                    source.window_end, source.aggregator_type, source.aggregated_value,
                    source.event_count, source.late_event_count, source.rolling,
                    source.custom_metadata);",
                &params,
            )
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        Ok(())
    }
}

fn count_as_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn merge_aggregate_values(
    aggregator_type: &str,
    existing_value: f64,
    existing_count: i64,
    existing_metadata: Value,
    incoming_value: f64,
    incoming_count: u64,
    incoming_metadata: Value,
) -> Result<(f64, Value), DistillError> {
    let mut existing_metadata: std::collections::HashMap<String, Value> =
        serde_json::from_value(existing_metadata)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
    let incoming_metadata_map: std::collections::HashMap<String, Value> =
        serde_json::from_value(incoming_metadata)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;

    let value = match aggregator_type {
        "Average" => {
            let total_count = existing_count.saturating_add(count_as_i64(incoming_count));
            if total_count == 0 {
                0.0
            } else {
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "aggregate counts are represented as f64 in the wire format"
                )]
                let existing_count_f64 = existing_count as f64;
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "aggregate counts are represented as f64 in the wire format"
                )]
                let incoming_count_f64 = count_as_i64(incoming_count) as f64;
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "aggregate counts are represented as f64 in the wire format"
                )]
                let total_count_f64 = total_count as f64;
                (existing_value * existing_count_f64 + incoming_value * incoming_count_f64)
                    / total_count_f64
            }
        }
        "AverageDurationBetweenEvents" => {
            let existing_sum = metadata_f64(&existing_metadata, "interval_sum_seconds");
            let existing_intervals = metadata_u64(&existing_metadata, "interval_count");
            let incoming_sum = metadata_f64(&incoming_metadata_map, "interval_sum_seconds");
            let incoming_intervals = metadata_u64(&incoming_metadata_map, "interval_count");
            let boundary = match (
                metadata_timestamp(&existing_metadata, "last_event_timestamp"),
                metadata_timestamp(&incoming_metadata_map, "first_event_timestamp"),
            ) {
                (Some(previous), Some(next)) => next
                    .signed_duration_since(previous)
                    .to_std()
                    .map_or(0.0, |duration| duration.as_secs_f64()),
                _ => 0.0,
            };
            let total_intervals = existing_intervals
                .saturating_add(incoming_intervals)
                .saturating_add(u64::from(boundary > 0.0));
            let total_sum = existing_sum + incoming_sum + boundary;
            existing_metadata.insert(
                "interval_count".to_string(),
                serde_json::json!(total_intervals),
            );
            existing_metadata.insert(
                "interval_sum_seconds".to_string(),
                serde_json::json!(total_sum),
            );
            if let Some(last) = incoming_metadata_map.get("last_event_timestamp") {
                existing_metadata.insert("last_event_timestamp".to_string(), last.clone());
            }
            if total_intervals == 0 {
                0.0
            } else {
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "duration interval counts are represented as f64 in the aggregate"
                )]
                let total_intervals_f64 = total_intervals as f64;
                total_sum / total_intervals_f64
            }
        }
        "Count" | "Sum" => existing_value + incoming_value,
        "Min" => existing_value.min(incoming_value),
        "Max" => existing_value.max(incoming_value),
        _ => {
            return Err(DistillError::Persistence(
                "unsupported rolling aggregator type".to_string(),
            ));
        }
    };
    if aggregator_type != "AverageDurationBetweenEvents" {
        existing_metadata.extend(incoming_metadata_map);
    }
    Ok((
        value,
        serde_json::to_value(existing_metadata)
            .map_err(|error| DistillError::Persistence(error.to_string()))?,
    ))
}

fn metadata_f64(metadata: &std::collections::HashMap<String, Value>, key: &str) -> f64 {
    metadata.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

fn metadata_u64(metadata: &std::collections::HashMap<String, Value>, key: &str) -> u64 {
    metadata.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn metadata_timestamp(
    metadata: &std::collections::HashMap<String, Value>,
    key: &str,
) -> Option<chrono::DateTime<chrono::Utc>> {
    metadata
        .get(key)
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
}

#[derive(Clone)]
pub struct RabbitPublisher {
    _connection: Arc<Connection>,
    channel: Arc<Channel>,
}

impl RabbitPublisher {
    pub async fn connect(uri: &str) -> Result<Self, DistillError> {
        let connection = Connection::connect(uri, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;
        Ok(Self {
            _connection: Arc::new(connection),
            channel: Arc::new(channel),
        })
    }

    pub async fn publish<T: Serialize>(
        &self,
        exchange: &str,
        routing_key: &str,
        message: &T,
    ) -> Result<(), DistillError> {
        self.channel
            .exchange_declare(
                exchange,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        let envelope = DeliveryEnvelope {
            payload: serde_json::to_value(message)
                .map_err(|error| DistillError::Persistence(error.to_string()))?,
            retry_count: 0,
        };
        let payload = serde_json::to_vec(&envelope)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        self.channel
            .basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2),
            )
            .await?
            .await?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct DeliveryEnvelope {
    payload: serde_json::Value,
    retry_count: u32,
}

pub struct RabbitConsumer {
    _connection: Arc<Connection>,
    channel: Arc<Channel>,
    sinks: Arc<RwLock<HashMap<String, Arc<dyn Sink>>>>,
}

impl RabbitConsumer {
    pub async fn connect(uri: &str, metrics: Arc<Metrics>) -> Result<Self, DistillError> {
        let connection = Connection::connect(uri, ConnectionProperties::default()).await?;
        let channel = Arc::new(connection.create_channel().await?);
        declare_consumer_queue(&channel).await?;
        let sinks = Arc::new(RwLock::new(HashMap::new()));
        let consumer = Self {
            _connection: Arc::new(connection),
            channel,
            sinks: Arc::clone(&sinks),
        };
        let consumer_channel = Arc::clone(&consumer.channel);
        let consumer_sinks = Arc::clone(&sinks);
        tokio::spawn(async move {
            if let Err(error) = consume(consumer_channel, consumer_sinks, metrics).await {
                error!(event.name = "distill.rabbit.consumer.stopped", error.message = %error, "RabbitMQ consumer stopped");
            }
        });
        Ok(consumer)
    }

    pub async fn register_exchange(&self, exchange: &str) -> Result<(), DistillError> {
        declare_topology(&self.channel, exchange).await
    }

    pub async fn register_sink(&self, channel_id: String, sink: Arc<dyn Sink>) {
        self.sinks.write().await.insert(channel_id, sink);
    }
}

async fn declare_consumer_queue(channel: &Channel) -> Result<(), DistillError> {
    channel
        .exchange_declare(
            DEAD_LETTER_EXCHANGE,
            ExchangeKind::Fanout,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_declare(
            DELIVERY_QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_declare(
            "distill.dead-letter.queue",
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_bind(
            "distill.dead-letter.queue",
            DEAD_LETTER_EXCHANGE,
            "#",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    Ok(())
}

async fn declare_topology(channel: &Channel, exchange: &str) -> Result<(), DistillError> {
    channel
        .exchange_declare(
            exchange,
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_bind(
            DELIVERY_QUEUE,
            exchange,
            "#",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    Ok(())
}

async fn consume(
    channel: Arc<Channel>,
    sinks: Arc<RwLock<HashMap<String, Arc<dyn Sink>>>>,
    metrics: Arc<Metrics>,
) -> Result<(), DistillError> {
    channel.basic_qos(1, BasicQosOptions::default()).await?;
    let mut consumer = channel
        .basic_consume(
            DELIVERY_QUEUE,
            "distill-persistence",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        let result = process_delivery(
            &channel,
            &sinks,
            &delivery.data,
            delivery.routing_key.as_str(),
            &metrics,
        )
        .await;
        match result {
            Ok(()) => delivery.ack(BasicAckOptions::default()).await?,
            Err(error) => {
                warn!(event.name = "distill.rabbit.delivery.failure", error.message = %error, "RabbitMQ delivery failed");
                if let Err(retry_error) = retry_delivery(
                    &channel,
                    &delivery.data,
                    delivery.exchange.as_str(),
                    delivery.routing_key.as_str(),
                )
                .await
                {
                    delivery
                        .nack(lapin::options::BasicNackOptions {
                            requeue: true,
                            ..Default::default()
                        })
                        .await?;
                    warn!(event.name = "distill.rabbit.retry.failure", error.message = %retry_error, "RabbitMQ retry publication failed");
                } else {
                    delivery.ack(BasicAckOptions::default()).await?;
                }
            }
        }
    }
    Ok(())
}

async fn process_delivery(
    channel: &Channel,
    sinks: &RwLock<HashMap<String, Arc<dyn Sink>>>,
    data: &[u8],
    routing_key: &str,
    metrics: &Metrics,
) -> Result<(), DistillError> {
    let envelope: DeliveryEnvelope = serde_json::from_slice(data)
        .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
    if routing_key.ends_with("aggregate") {
        let aggregate: AggregatedLog = serde_json::from_value(envelope.payload)
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
        let sink = sinks
            .read()
            .await
            .get(&aggregate.channel_id)
            .cloned()
            .ok_or_else(|| DistillError::ChannelNotFound(aggregate.channel_id.clone()))?;
        sink.persist_aggregate(&aggregate).await?;
        metrics.aggregate_persisted();
    } else {
        let event: LogEvent = serde_json::from_value(envelope.payload)
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
        let sink = sinks
            .read()
            .await
            .get(&event.channel_id)
            .cloned()
            .ok_or_else(|| DistillError::ChannelNotFound(event.channel_id.clone()))?;
        sink.persist_event(&event).await?;
        metrics.persisted();
    }
    let _ = channel;
    Ok(())
}

async fn retry_delivery(
    channel: &Channel,
    data: &[u8],
    exchange: &str,
    routing_key: &str,
) -> Result<(), DistillError> {
    let mut envelope: DeliveryEnvelope = serde_json::from_slice(data)
        .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
    if envelope.retry_count >= MAX_RETRIES {
        channel
            .basic_publish(
                DEAD_LETTER_EXCHANGE,
                routing_key,
                BasicPublishOptions::default(),
                data,
                BasicProperties::default().with_delivery_mode(2),
            )
            .await?
            .await?;
        return Ok(());
    }
    envelope.retry_count += 1;
    let delay = 2_u64.pow(envelope.retry_count.min(6)) - 1;
    sleep(Duration::from_secs(delay)).await;
    let payload = serde_json::to_vec(&envelope)
        .map_err(|error| DistillError::Persistence(error.to_string()))?;
    channel
        .basic_publish(
            exchange,
            routing_key,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default().with_delivery_mode(2),
        )
        .await?
        .await?;
    Ok(())
}
