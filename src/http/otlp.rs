//! Converts OTLP log payloads into validated Distill events.

// Rust guideline compliant 2026-02-21
use super::{DistillError, LogEvent, SeverityLevel};
use chrono::Utc;
use serde_json::{Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

pub(super) fn otlp_events(
    request: &Value,
    default_channel: Option<&str>,
) -> Result<Vec<LogEvent>, DistillError> {
    let resource_logs = request
        .get("resourceLogs")
        .and_then(Value::as_array)
        .ok_or_else(|| DistillError::InvalidRequest("OTLP resourceLogs is required".to_string()))?;
    let mut events = Vec::new();
    for resource_log in resource_logs {
        let resource_attributes = otlp_attributes(
            resource_log
                .get("resource")
                .and_then(|resource| resource.get("attributes")),
        );
        let channel = default_channel
            .map(str::to_string)
            .or_else(|| {
                resource_attributes
                    .get("distill.channel")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .ok_or_else(|| {
                DistillError::InvalidRequest(
                    "OTLP logs require X-Distill-Channel or resource attribute distill.channel"
                        .to_string(),
                )
            })?;
        let scope_logs = resource_log
            .get("scopeLogs")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                DistillError::InvalidRequest("OTLP scopeLogs is required".to_string())
            })?;
        for scope_log in scope_logs {
            let records = scope_log
                .get("logRecords")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    DistillError::InvalidRequest("OTLP logRecords is required".to_string())
                })?;
            for record in records {
                let timestamp = otlp_timestamp(record).ok_or_else(|| {
                    DistillError::InvalidRequest(
                        "OTLP log record requires timeUnixNano or observedTimeUnixNano".to_string(),
                    )
                })?;
                let mut attributes = resource_attributes.clone();
                attributes.extend(otlp_attributes(record.get("attributes")));
                let payload = otlp_payload(record.get("body"), &attributes);
                let mut metadata = HashMap::new();
                for (key, value) in &attributes {
                    if let Some(value) = value.as_str() {
                        metadata.insert(key.clone(), value.to_string());
                    }
                }
                if let Some(trace_id) = record.get("traceId").and_then(Value::as_str) {
                    metadata.insert("trace_id".to_string(), trace_id.to_string());
                }
                if let Some(span_id) = record.get("spanId").and_then(Value::as_str) {
                    metadata.insert("span_id".to_string(), span_id.to_string());
                }
                events.push(LogEvent {
                    event_id: Uuid::new_v4(),
                    timestamp,
                    channel_id: channel.clone(),
                    severity_level: otlp_severity(
                        record.get("severityNumber").and_then(Value::as_i64),
                    ),
                    payload,
                    payload_bytes: None,
                    metadata,
                });
            }
        }
    }
    if events.is_empty() {
        return Err(DistillError::InvalidRequest(
            "OTLP request contains no log records".to_string(),
        ));
    }
    Ok(events)
}

fn otlp_attributes(value: Option<&Value>) -> HashMap<String, Value> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|attribute| {
            Some((
                attribute.get("key")?.as_str()?.to_string(),
                otlp_any_value(attribute.get("value")?),
            ))
        })
        .collect()
}

fn otlp_any_value(value: &Value) -> Value {
    value
        .get("stringValue")
        .or_else(|| value.get("boolValue"))
        .or_else(|| value.get("intValue"))
        .or_else(|| value.get("doubleValue"))
        .or_else(|| value.get("bytesValue"))
        .cloned()
        .unwrap_or(Value::Null)
}

fn otlp_payload(body: Option<&Value>, attributes: &HashMap<String, Value>) -> Value {
    let body = body.map_or(Value::Null, otlp_any_value);
    let mut payload = match body {
        Value::Object(object) => object,
        value => {
            let mut object = Map::new();
            object.insert("body".to_string(), value);
            object
        }
    };
    payload.insert(
        "attributes".to_string(),
        serde_json::to_value(attributes).unwrap_or(Value::Object(Map::new())),
    );
    Value::Object(payload)
}

fn otlp_timestamp(record: &Value) -> Option<chrono::DateTime<Utc>> {
    let nanos = record
        .get("timeUnixNano")
        .or_else(|| record.get("observedTimeUnixNano"))
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))?;
    let seconds = i64::try_from(nanos / 1_000_000_000).ok()?;
    let subsec_nanos = u32::try_from(nanos % 1_000_000_000).ok()?;
    chrono::DateTime::from_timestamp(seconds, subsec_nanos)
}

fn otlp_severity(number: Option<i64>) -> SeverityLevel {
    match number.unwrap_or(0) {
        1..=4 => SeverityLevel::Trace,
        5..=8 => SeverityLevel::Debug,
        9..=12 => SeverityLevel::Info,
        13..=16 => SeverityLevel::Warning,
        17..=20 => SeverityLevel::Error,
        21..=24 => SeverityLevel::Critical,
        _ => SeverityLevel::Unknown,
    }
}
