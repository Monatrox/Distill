//! Computes bounded rolling aggregates from channel-local events.

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
