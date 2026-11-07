//! Defines the small Protobuf envelope accepted by the ingest endpoint.

// Rust guideline compliant 2026-02-21
use crate::{
    core::errors::DistillError,
    domain::{LogEvent, SeverityLevel},
};
use base64::Engine;
use chrono::{DateTime, Utc};
use prost::Message;
use serde_json::json;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Message)]
pub struct IngestRequest {
    #[prost(message, repeated, tag = "1")]
    pub events: Vec<ProtoLogEvent>,
}

#[derive(Message)]
pub struct ProtoLogEvent {
    #[prost(string, tag = "1")]
    pub event_id: String,
    #[prost(int64, tag = "2")]
    pub timestamp_millis: i64,
    #[prost(string, tag = "3")]
    pub channel_id: String,
    #[prost(int32, tag = "4")]
    pub severity_level: i32,
    #[prost(bytes = "vec", tag = "5")]
    pub payload: Vec<u8>,
    #[prost(map = "string, string", tag = "6")]
    pub metadata: HashMap<String, String>,
    #[prost(string, optional, tag = "7")]
    pub version: Option<String>,
}

impl TryFrom<ProtoLogEvent> for LogEvent {
    type Error = DistillError;

    fn try_from(value: ProtoLogEvent) -> Result<Self, Self::Error> {
        let event_id = if value.event_id.is_empty() {
            Uuid::new_v4()
        } else {
            Uuid::parse_str(&value.event_id)
                .map_err(|error| DistillError::InvalidRequest(error.to_string()))?
        };
        let timestamp = DateTime::<Utc>::from_timestamp_millis(value.timestamp_millis)
            .ok_or_else(|| DistillError::InvalidRequest("invalid event timestamp".to_string()))?;
        let severity_level = match value.severity_level {
            1 => SeverityLevel::Trace,
            2 => SeverityLevel::Debug,
            3 => SeverityLevel::Info,
            4 => SeverityLevel::Warning,
            5 => SeverityLevel::Error,
            6 => SeverityLevel::Critical,
            _ => SeverityLevel::Unknown,
        };
        let payload = serde_json::from_slice(&value.payload)
            .unwrap_or_else(|_| {
                json!({"raw_payload_base64": base64::engine::general_purpose::STANDARD.encode(&value.payload)})
            });
        Ok(LogEvent {
            version: value.version,
            event_id,
            timestamp,
            channel_id: value.channel_id,
            severity_level,
            payload,
            payload_bytes: Some(value.payload),
            metadata: value.metadata,
        })
    }
}
