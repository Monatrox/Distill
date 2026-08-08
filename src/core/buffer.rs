//! Implements safe owner-local byte-bounded event buffering.

// Rust guideline compliant 2026-02-21
use crate::domain::LogEvent;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct MemoryBudget {
    max_bytes: usize,
    resident_bytes: AtomicUsize,
}

impl MemoryBudget {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            resident_bytes: AtomicUsize::new(0),
        }
    }

    pub fn reserve(&self, bytes: usize) -> bool {
        self.resident_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= self.max_bytes)
            })
            .is_ok()
    }

    pub fn release(&self, bytes: usize) {
        let _ = self
            .resident_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
    }
}

#[derive(Debug)]
pub struct EventBuffer {
    capacity_bytes: usize,
    resident_bytes: usize,
    entries: VecDeque<LogEvent>,
    budget: Arc<MemoryBudget>,
}

impl EventBuffer {
    #[cfg(test)]
    pub fn new(capacity_bytes: usize) -> Self {
        Self::with_budget(
            capacity_bytes,
            Arc::new(MemoryBudget::new(
                crate::domain::DEFAULT_PROCESS_MEMORY_BYTES,
            )),
        )
    }

    pub fn with_budget(capacity_bytes: usize, budget: Arc<MemoryBudget>) -> Self {
        Self {
            capacity_bytes,
            resident_bytes: 0,
            entries: VecDeque::new(),
            budget,
        }
    }

    pub fn push(&mut self, event: LogEvent) -> Result<(), Box<LogEvent>> {
        let event_bytes = estimate_event_bytes(&event);
        let Some(next_bytes) = self.resident_bytes.checked_add(event_bytes) else {
            return Err(Box::new(event));
        };
        if next_bytes > self.capacity_bytes {
            return Err(Box::new(event));
        }
        if !self.budget.reserve(event_bytes) {
            return Err(Box::new(event));
        }
        self.resident_bytes = next_bytes;
        self.entries.push_back(event);
        Ok(())
    }

    pub fn pop_back(&mut self) -> Option<LogEvent> {
        let event = self.entries.pop_back()?;
        let bytes = estimate_event_bytes(&event);
        self.resident_bytes = self.resident_bytes.saturating_sub(bytes);
        self.budget.release(bytes);
        Some(event)
    }

    pub fn drain(&mut self) -> Vec<LogEvent> {
        let drained = self.entries.drain(..).collect();
        self.budget.release(self.resident_bytes);
        self.resident_bytes = 0;
        drained
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn estimate_event_bytes(event: &LogEvent) -> usize {
    event.channel_id.len()
        + event
            .metadata
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>()
        + event.payload.to_string().len()
        + std::mem::size_of::<LogEvent>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::SeverityLevel;
    use chrono::Utc;
    use serde_json::json;

    fn event() -> LogEvent {
        LogEvent {
            event_id: uuid::Uuid::new_v4(),
            timestamp: Utc::now(),
            channel_id: "app.test".to_string(),
            severity_level: SeverityLevel::Info,
            payload: json!({"value": 1}),
            payload_bytes: None,
            metadata: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn rejects_events_over_capacity_without_dropping_existing_data() {
        let mut buffer = EventBuffer::new(1);
        let rejected = buffer
            .push(event())
            .expect_err("event must exceed capacity");
        assert_eq!(rejected.channel_id, "app.test");
        assert!(buffer.is_empty());
    }
}
