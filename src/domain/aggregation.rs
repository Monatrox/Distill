//! Defines aggregate functions and persisted aggregate values.

// Rust guideline compliant 2026-02-21
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::AggregateId;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AggregatorSpec {
    pub function: AggregationFunction,
    pub granularity: Granularity,
    #[serde(default)]
    pub parameter: Option<u64>,
    #[serde(default)]
    pub rolling: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AggregationFunction {
    Average,
    #[serde(rename = "average_duration_between_events")]
    AverageDurationBetweenEvents,
    Max,
    Min,
    Sum,
    Count,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Granularity {
    PerNEvents,
    PerTTime,
    Overall,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingMode {
    FifoRequired,
    #[default]
    FirstComeFirstServed,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AggregatedLog {
    pub aggregate_id: AggregateId,
    pub channel_id: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub aggregator_type: AggregationFunction,
    pub aggregated_value: f64,
    pub count: u64,
    pub late_event_count: u64,
    pub rolling: bool,
    pub custom_metadata: HashMap<String, serde_json::Value>,
}
