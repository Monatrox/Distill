//! Provides external sink implementations and persistence dispatch.

// Rust guideline compliant 2026-02-21
use crate::{
    core::aggregation::{count_as_i64, merge_aggregate_values},
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
) -> Result<Arc<dyn Sink>, DistillError> {
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
        "http" => Ok(Arc::new(HttpSink::connect(config, sink_allowlist)?)),
        driver => Err(DistillError::InvalidConfiguration(format!(
            "unsupported sink driver: {driver}"
        ))),
    }
}
