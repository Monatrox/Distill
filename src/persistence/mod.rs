//! Provides external sink implementations and persistence dispatch.

// Rust guideline compliant 2026-02-21
use crate::{
    core::aggregation::count_as_i64,
    core::errors::DistillError,
    domain::{AggregatedLog, LogEvent},
};
use async_trait::async_trait;
use serde::Serialize;
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
mod policy;
mod postgres;
mod sql_server;

pub use policy::SinkAllowlist;

use http::HttpSink;
use mysql::MySqlSink;
use postgres::PostgresSink;
use sql_server::SqlServerSink;

#[allow(
    clippy::double_must_use,
    reason = "async_trait generates must_use on already must-use boxed futures"
)]
#[async_trait]
pub trait Sink: Send + Sync {
    async fn persist_event(&self, event: &LogEvent) -> Result<(), DistillError>;
    async fn persist_aggregate(&self, aggregate: &AggregatedLog) -> Result<(), DistillError>;
}

/// Creates a configured sink for one channel.
///
/// The sink allowlist governs the destinations HTTP sinks may reach; other
/// drivers ignore it.
pub async fn build_sink(
    config: &crate::domain::SinkConfig,
    sink_allowlist: &SinkAllowlist,
    auto_migrate: bool,
) -> Result<Arc<dyn Sink>, DistillError> {
    match config.driver.as_str() {
        "postgres" => Ok(Arc::new(
            PostgresSink::connect(&config.connection_string, auto_migrate).await?,
        )),
        "mysql" => Ok(Arc::new(
            MySqlSink::connect(&config.connection_string, auto_migrate).await?,
        )),
        "sqlserver" | "mssql" => Ok(Arc::new(
            SqlServerSink::connect(&config.connection_string, auto_migrate).await?,
        )),
        "http" => {
            let config = config.clone();
            let allowlist = sink_allowlist.clone();
            let sink = tokio::task::spawn_blocking(move || HttpSink::connect(&config, &allowlist))
                .await
                .map_err(|_| {
                    DistillError::Persistence("HTTP destination preparation failed".to_string())
                })??;
            Ok(Arc::new(sink))
        }
        driver => Err(DistillError::InvalidConfiguration(format!(
            "unsupported sink driver: {driver}"
        ))),
    }
}

/// Restores known destinations without requiring every destination to be online.
pub(crate) fn restore_sink(
    config: &crate::domain::SinkConfig,
    allowlist: &SinkAllowlist,
) -> Result<Arc<dyn Sink>, DistillError> {
    match config.driver.as_str() {
        "postgres" => Ok(Arc::new(PostgresSink::restore(&config.connection_string)?)),
        "mysql" => Ok(Arc::new(MySqlSink::restore(&config.connection_string)?)),
        "sqlserver" | "mssql" => Ok(Arc::new(SqlServerSink::restore(&config.connection_string)?)),
        "http" => Ok(Arc::new(HttpSink::connect(config, allowlist)?)),
        _ => Err(DistillError::InvalidConfiguration(
            "unsupported restored sink driver".to_string(),
        )),
    }
}

fn migration_required() -> DistillError {
    DistillError::InvalidConfiguration("database sink requires schema version 1; run distill sink migrate with migration credentials before registration".to_string())
}

fn event_fingerprint(event: &LogEvent) -> Result<String, DistillError> {
    use sha2::{Digest, Sha256};
    let value = serde_json::to_value(event).map_err(|_| DistillError::ConfigurationConflict)?;
    Ok(format!(
        "{:x}",
        Sha256::digest(value.to_string().as_bytes())
    ))
}

/// Hashes the complete snapshot, including its configuration and update revision.
fn snapshot_fingerprint(aggregate: &AggregatedLog) -> Result<String, DistillError> {
    use sha2::{Digest, Sha256};
    let value = serde_json::to_value(aggregate).map_err(|_| DistillError::ConfigurationConflict)?;
    Ok(format!(
        "{:x}",
        Sha256::digest(value.to_string().as_bytes())
    ))
}

fn snapshot_revision(aggregate: &AggregatedLog) -> Result<i64, DistillError> {
    if aggregate.count > i64::MAX as u64 || aggregate.late_event_count > i64::MAX as u64 {
        return Err(DistillError::InvalidRequest(
            "snapshot count exceeds supported range".to_string(),
        ));
    }
    if aggregate.rolling && aggregate.update_revision == 0 {
        return Err(DistillError::InvalidRequest(
            "legacy rolling deltas require migration before replay".to_string(),
        ));
    }
    i64::try_from(aggregate.update_revision)
        .map_err(|_| DistillError::InvalidRequest("snapshot revision is too large".to_string()))
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use crate::domain::AggregationFunction;
    use chrono::Utc;
    use uuid::Uuid;

    fn snapshot() -> AggregatedLog {
        AggregatedLog {
            aggregate_id: Uuid::new_v4(),
            update_revision: 1,
            channel_id: "contract.rolling".to_string(),
            version: Some("1".to_string()),
            window_start: Utc::now(),
            window_end: Utc::now(),
            aggregator_type: AggregationFunction::Count,
            aggregated_value: 2.0,
            count: 2,
            late_event_count: 0,
            rolling: true,
            custom_metadata: std::collections::HashMap::default(),
        }
    }

    async fn exercise(sink: &dyn Sink) -> AggregatedLog {
        let event: LogEvent = serde_json::from_value(serde_json::json!({
            "event_id": Uuid::new_v4(), "channel_id": "contract.direct", "version": "1", "timestamp": Utc::now(),
            "severity_level": "info", "payload": {"value": 3}
        })).unwrap();
        sink.persist_event(&event)
            .await
            .expect("first event must persist");
        sink.persist_event(&event)
            .await
            .expect("identical event retry must succeed");
        let mut conflict = event.clone();
        conflict.payload = serde_json::json!({"value": 4});
        assert!(
            sink.persist_event(&conflict).await.is_err(),
            "conflicting event identity must fail"
        );
        sink.persist_event(&event)
            .await
            .expect("event conflict must not damage later delivery");
        let first = snapshot();
        sink.persist_aggregate(&first)
            .await
            .expect("first snapshot must persist");
        sink.persist_aggregate(&first)
            .await
            .expect("identical retry must succeed");
        let mut second = first.clone();
        second.update_revision = 2;
        second.count = 4;
        second.aggregated_value = 4.0;
        sink.persist_aggregate(&second)
            .await
            .expect("new revision must persist");
        sink.persist_aggregate(&first)
            .await
            .expect("stale retry must be harmless");
        let mut conflict = second.clone();
        conflict.count = 999;
        assert!(
            sink.persist_aggregate(&conflict).await.is_err(),
            "conflicting equal revision must fail"
        );
        sink.persist_aggregate(&second)
            .await
            .expect("conflict must not damage later delivery");
        second
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL; run scripts/test-sink-contracts.sh"]
    async fn postgres_snapshot_contract() {
        let uri = std::env::var("DISTILL_TEST_POSTGRES_URI").unwrap();
        let sink = PostgresSink::connect(&uri, true).await.unwrap();
        let result = exercise(&sink).await;
        let pool = PgPoolOptions::new().connect(&uri).await.unwrap();
        let row: (i64, i64) = sqlx::query_as(
            "SELECT event_count, update_revision FROM distill_aggregates WHERE aggregate_id = $1",
        )
        .bind(result.aggregate_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row, (4, 2));
    }

    #[tokio::test]
    #[ignore = "requires MySQL; run scripts/test-sink-contracts.sh"]
    async fn mysql_snapshot_contract() {
        let uri = std::env::var("DISTILL_TEST_MYSQL_URI").unwrap();
        let sink = MySqlSink::connect(&uri, true).await.unwrap();
        let result = exercise(&sink).await;
        let pool = MySqlPoolOptions::new().connect(&uri).await.unwrap();
        let row: (i64, i64) = sqlx::query_as(
            "SELECT event_count, update_revision FROM distill_aggregates WHERE aggregate_id = ?",
        )
        .bind(result.aggregate_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row, (4, 2));
    }

    #[tokio::test]
    #[ignore = "requires SQL Server; run scripts/test-sink-contracts.sh"]
    async fn sql_server_snapshot_contract() {
        let uri = std::env::var("DISTILL_TEST_SQLSERVER_URI").unwrap();
        let sink = SqlServerSink::connect(&uri, true).await.unwrap();
        let result = exercise(&sink).await;
        let config = Config::from_ado_string(&uri).unwrap();
        let tcp = TcpStream::connect(config.get_addr()).await.unwrap();
        let mut client = Client::connect(config, tcp.compat_write()).await.unwrap();
        let identity = result.aggregate_id.to_string();
        let row = client.query("SELECT event_count, update_revision FROM distill_aggregates WHERE aggregate_id = @P1", &[&identity])
            .await.unwrap().into_row().await.unwrap().unwrap();
        assert_eq!(row.get::<i64, _>("event_count"), Some(4));
        assert_eq!(row.get::<i64, _>("update_revision"), Some(2));
    }

    #[tokio::test]
    #[ignore = "requires reference HTTP receiver; run scripts/test-sink-contracts.sh"]
    async fn http_snapshot_contract() {
        let endpoint = std::env::var("DISTILL_TEST_HTTP_URL").unwrap();
        let port = reqwest::Url::parse(&endpoint).unwrap().port().unwrap();
        let allowlist = SinkAllowlist::parse(&format!("127.0.0.1/32:{port}")).unwrap();
        let config = crate::domain::SinkConfig {
            driver: "http".to_string(),
            connection_string: endpoint.clone(),
            headers: [(
                "Authorization".to_string(),
                "Bearer contract-token".to_string(),
            )]
            .into_iter()
            .collect(),
        };
        let sink = HttpSink::connect(&config, &allowlist).unwrap();
        let result = exercise(&sink).await;
        let record: AggregatedLog = reqwest::Client::new()
            .get(format!("{}/{}", endpoint, result.aggregate_id))
            .bearer_auth("contract-token")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!((record.count, record.update_revision), (4, 2));
    }
}
