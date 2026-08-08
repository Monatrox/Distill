//! Collects low-overhead process metrics for the Prometheus endpoint.

// Rust guideline compliant 2026-02-21
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    events_received: AtomicU64,
    events_persisted: AtomicU64,
    aggregates_persisted: AtomicU64,
    errors: AtomicU64,
}

impl Metrics {
    pub fn received(&self) {
        self.events_received.fetch_add(1, Ordering::Relaxed);
    }
    pub fn persisted(&self) {
        self.events_persisted.fetch_add(1, Ordering::Relaxed);
    }
    pub fn aggregate_persisted(&self) {
        self.aggregates_persisted.fetch_add(1, Ordering::Relaxed);
    }
    pub fn prometheus(&self) -> String {
        format!(
            "# TYPE distill_events_received_total counter\ndistill_events_received_total {}\n# TYPE distill_events_persisted_total counter\ndistill_events_persisted_total {}\n# TYPE distill_aggregates_persisted_total counter\ndistill_aggregates_persisted_total {}\n# TYPE distill_errors_total counter\ndistill_errors_total {}\n",
            self.events_received.load(Ordering::Relaxed),
            self.events_persisted.load(Ordering::Relaxed),
            self.aggregates_persisted.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
        )
    }
}
