//! Provides external sink implementations and persistence dispatch.

use crate::{
    domain::{AggregatedLog, LogEvent},
    errors::DistillError,
};
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use sqlx::{MySqlPool, PgPool, mysql::MySqlPoolOptions, postgres::PgPoolOptions};
use std::sync::Arc;
use tiberius::{Client, Config};
use tokio::time::Duration;
use tokio::{net::TcpStream, sync::Mutex};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

// A bounded timeout prevents an unavailable HTTP receiver from holding a delivery forever.
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

mod http;
mod mysql;
mod postgres;
mod sql_server;

use http::HttpSink;
use mysql::MySqlSink;
use postgres::PostgresSink;
use sql_server::SqlServerSink;

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
