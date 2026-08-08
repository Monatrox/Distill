//! Computes bounded rolling aggregates from channel-local events.

// Rust guideline compliant 2026-02-21
use crate::domain::{AggregatedLog, AggregationFunction, AggregatorSpec, LogEvent};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug)]
pub struct AggregatorState {
    spec: AggregatorSpec,
    channel_id: String,
    events: Vec<LogEvent>,
    sum: f64,
    count: u64,
    minimum: Option<f64>,
    maximum: Option<f64>,
    late_event_count: u64,
}

impl AggregatorState {
    pub fn new(channel_id: String, spec: AggregatorSpec) -> Self {
        Self {
            spec,
            channel_id,
            events: Vec::new(),
            sum: 0.0,
            count: 0,
            minimum: None,
            maximum: None,
            late_event_count: 0,
        }
    }

    pub fn update(&mut self, event: LogEvent) -> Result<(), String> {
        if matches!(
            self.spec.function,
            AggregationFunction::Count | AggregationFunction::AverageDurationBetweenEvents
        ) {
            self.count = self
                .count
                .checked_add(1)
                .ok_or_else(|| "count overflow".to_string())?;
        } else {
            let value = event
                .payload
                .get("value")
                .and_then(serde_json::Value::as_f64)
                .ok_or_else(|| "aggregated payload must contain numeric value".to_string())?;
            if !value.is_finite() {
                return Err("aggregated value must be finite".to_string());
            }
            let next_sum = self.sum + value;
            if !next_sum.is_finite() {
                return Err("aggregate sum overflow".to_string());
            }
            self.sum = next_sum;
            self.count = self
                .count
                .checked_add(1)
                .ok_or_else(|| "count overflow".to_string())?;
            self.minimum = Some(self.minimum.map_or(value, |minimum| minimum.min(value)));
            self.maximum = Some(self.maximum.map_or(value, |maximum| maximum.max(value)));
        }

        if matches!(self.spec.granularity, crate::domain::Granularity::PerTTime)
            || matches!(
                self.spec.function,
                AggregationFunction::Average | AggregationFunction::AverageDurationBetweenEvents
            )
        {
            self.events.push(event);
        }
        Ok(())
    }

    pub fn mark_late_event(&mut self) {
        self.late_event_count = self.late_event_count.saturating_add(1);
    }

    pub fn should_flush(&self, first_timestamp: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
        match self.spec.granularity {
            crate::domain::Granularity::PerNEvents => self
                .spec
                .parameter
                .is_some_and(|parameter| self.count >= parameter),
            crate::domain::Granularity::PerTTime => self
                .spec
                .parameter
                .zip(first_timestamp)
                .is_some_and(|(seconds, first)| {
                    now.signed_duration_since(first).num_seconds()
                        >= i64::try_from(seconds).unwrap_or(i64::MAX)
                }),
            crate::domain::Granularity::Overall => false,
        }
    }

    pub fn snapshot(
        &mut self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> AggregatedLog {
        if !self.events.is_empty() {
            self.events.sort_by(|left, right| {
                left.timestamp
                    .cmp(&right.timestamp)
                    .then_with(|| left.event_id.cmp(&right.event_id))
            });
        }
        let value = match self.spec.function {
            AggregationFunction::Average => {
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "the wire format represents aggregates as f64"
                )]
                let count = self.count.max(1) as f64;
                self.sum / count
            }
            AggregationFunction::Max => self.maximum.unwrap_or(0.0),
            AggregationFunction::Min => self.minimum.unwrap_or(0.0),
            AggregationFunction::Sum => self.sum,
            AggregationFunction::AverageDurationBetweenEvents => {
                average_duration_seconds(&self.events)
            }
            AggregationFunction::Count => {
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "the wire format represents counts in the aggregate value as f64"
                )]
                {
                    self.count as f64
                }
            }
        };
        AggregatedLog {
            aggregate_id: aggregate_id(&self.channel_id, &self.spec),
            channel_id: self.channel_id.clone(),
            window_start,
            window_end,
            aggregator_type: self.spec.function,
            aggregated_value: value,
            count: self.count,
            late_event_count: self.late_event_count,
            rolling: self.spec.rolling,
            custom_metadata: duration_metadata(&self.events, self.spec.function),
        }
    }

    pub fn flush(
        &mut self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> AggregatedLog {
        let result = self.snapshot(window_start, window_end);
        self.reset();
        result
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "The rollup merge contract is exercised by integration paths in the next slice"
        )
    )]
    pub fn merge(&mut self, other: Self) -> Result<(), String> {
        if self.spec.function != other.spec.function {
            return Err("aggregators with different functions cannot merge".to_string());
        }
        let next_sum = self.sum + other.sum;
        if !next_sum.is_finite() {
            return Err("aggregate sum overflow".to_string());
        }
        self.sum = next_sum;
        self.count = self
            .count
            .checked_add(other.count)
            .ok_or_else(|| "count overflow".to_string())?;
        self.minimum = match (self.minimum, other.minimum) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        self.maximum = match (self.maximum, other.maximum) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        self.late_event_count = self.late_event_count.saturating_add(other.late_event_count);
        self.events.extend(other.events);
        Ok(())
    }

    fn reset(&mut self) {
        self.events.clear();
        self.sum = 0.0;
        self.count = 0;
        self.minimum = None;
        self.maximum = None;
        self.late_event_count = 0;
    }
}

fn average_duration_seconds(events: &[LogEvent]) -> f64 {
    if events.len() < 2 {
        return 0.0;
    }

    let mut total_seconds = 0.0;
    for pair in events.windows(2) {
        let duration = pair[1].timestamp.signed_duration_since(pair[0].timestamp);
        total_seconds += duration.to_std().map_or(0.0, |value| value.as_secs_f64());
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "the aggregate wire format represents durations as f64 seconds"
    )]
    let interval_count = (events.len() - 1) as f64;
    total_seconds / interval_count
}

fn aggregate_id(channel_id: &str, spec: &AggregatorSpec) -> Uuid {
    if spec.rolling {
        let identity = serde_json::to_string(&(channel_id, spec))
            .expect("aggregator specification is always serializable");
        Uuid::new_v5(&Uuid::NAMESPACE_URL, identity.as_bytes())
    } else {
        Uuid::new_v4()
    }
}

fn duration_metadata(events: &[LogEvent], function: AggregationFunction) -> HashMap<String, Value> {
    if function != AggregationFunction::AverageDurationBetweenEvents || events.is_empty() {
        return HashMap::new();
    }
    let interval_count = events.len().saturating_sub(1);
    let interval_sum_seconds = events
        .windows(2)
        .map(|pair| {
            pair[1]
                .timestamp
                .signed_duration_since(pair[0].timestamp)
                .to_std()
                .map_or(0.0, |duration| duration.as_secs_f64())
        })
        .sum::<f64>();
    HashMap::from([
        (
            "first_event_timestamp".to_string(),
            json!(events[0].timestamp),
        ),
        (
            "last_event_timestamp".to_string(),
            json!(events[events.len() - 1].timestamp),
        ),
        ("interval_count".to_string(), json!(interval_count)),
        (
            "interval_sum_seconds".to_string(),
            json!(interval_sum_seconds),
        ),
    ])
}

/// Merges a persisted rolling aggregate with the incoming window's aggregate.
pub(crate) fn merge_aggregate_values(
    aggregator_type: AggregationFunction,
    existing_value: f64,
    existing_count: i64,
    existing_metadata: Value,
    incoming_value: f64,
    incoming_count: u64,
    incoming_metadata: Value,
) -> Result<(f64, Value), String> {
    let mut existing_metadata: HashMap<String, Value> =
        serde_json::from_value(existing_metadata).map_err(|error| error.to_string())?;
    let incoming_metadata_map: HashMap<String, Value> =
        serde_json::from_value(incoming_metadata).map_err(|error| error.to_string())?;

    let value = match aggregator_type {
        AggregationFunction::Average => {
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
        AggregationFunction::AverageDurationBetweenEvents => {
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
            existing_metadata.insert("interval_count".to_string(), json!(total_intervals));
            existing_metadata.insert("interval_sum_seconds".to_string(), json!(total_sum));
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
        AggregationFunction::Count | AggregationFunction::Sum => existing_value + incoming_value,
        AggregationFunction::Min => existing_value.min(incoming_value),
        AggregationFunction::Max => existing_value.max(incoming_value),
    };
    if aggregator_type != AggregationFunction::AverageDurationBetweenEvents {
        existing_metadata.extend(incoming_metadata_map);
    }
    Ok((
        value,
        serde_json::to_value(existing_metadata).map_err(|error| error.to_string())?,
    ))
}

fn metadata_f64(metadata: &HashMap<String, Value>, key: &str) -> f64 {
    metadata.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

fn metadata_u64(metadata: &HashMap<String, Value>, key: &str) -> u64 {
    metadata.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn metadata_timestamp(metadata: &HashMap<String, Value>, key: &str) -> Option<DateTime<Utc>> {
    metadata
        .get(key)
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
}

pub(crate) fn count_as_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Granularity;
    use serde_json::json;

    #[test]
    fn merges_average_state_without_confusing_sum_and_average() {
        let spec = AggregatorSpec {
            function: AggregationFunction::Average,
            granularity: Granularity::Overall,
            parameter: None,
            rolling: false,
        };
        let mut first = AggregatorState::new("app.test".to_string(), spec.clone());
        first.update(event(2.0)).expect("valid event");
        let mut second = AggregatorState::new("app.test".to_string(), spec);
        second.update(event(4.0)).expect("valid event");
        first.merge(second).expect("compatible aggregators");
        let result = first.flush(Utc::now(), Utc::now());
        assert!((result.aggregated_value - 3.0).abs() < f64::EPSILON);
        assert_eq!(result.count, 2);
    }

    #[test]
    fn averages_duration_between_ordered_event_timestamps() {
        let spec = AggregatorSpec {
            function: AggregationFunction::AverageDurationBetweenEvents,
            granularity: Granularity::Overall,
            parameter: None,
            rolling: false,
        };
        let start = Utc::now();
        let mut state = AggregatorState::new("app.test".to_string(), spec);
        state.update(event_at(start)).expect("valid event");
        state
            .update(event_at(start + chrono::Duration::seconds(4)))
            .expect("valid event");
        state
            .update(event_at(start + chrono::Duration::seconds(10)))
            .expect("valid event");

        let result = state.flush(start, start + chrono::Duration::seconds(10));
        assert!((result.aggregated_value - 5.0).abs() < f64::EPSILON);
        assert_eq!(result.count, 3);
    }

    fn event(value: f64) -> LogEvent {
        let mut event = event_at(Utc::now());
        event.payload = json!({"value": value});
        event
    }

    fn event_at(timestamp: chrono::DateTime<Utc>) -> LogEvent {
        LogEvent {
            event_id: Uuid::new_v4(),
            timestamp,
            channel_id: "app.test".to_string(),
            severity_level: crate::domain::SeverityLevel::Info,
            payload: json!({"value": 1.0}),
            payload_bytes: None,
            metadata: std::collections::HashMap::new(),
        }
    }
}
