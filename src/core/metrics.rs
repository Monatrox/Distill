//! Collects low-overhead process metrics for the Prometheus endpoint.

// Rust guideline compliant 2026-02-21
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    events_received: AtomicU64,
    events_persisted: AtomicU64,
    aggregates_persisted: AtomicU64,
    errors: AtomicU64,
    admitted: AtomicU64,
    retries: AtomicU64,
    dead_letters: AtomicU64,
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
    pub fn admitted(&self) {
        self.admitted.fetch_add(1, Ordering::Relaxed);
    }
    pub fn delivery_failed(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn retried(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dead_lettered(&self) {
        self.dead_letters.fetch_add(1, Ordering::Relaxed);
    }
    pub fn prometheus(&self) -> String {
        format!(
            "# TYPE distill_events_received_total counter\ndistill_events_received_total {}\n# TYPE distill_event_sink_confirmations_total counter\ndistill_event_sink_confirmations_total {}\n# TYPE distill_aggregate_sink_confirmations_total counter\ndistill_aggregate_sink_confirmations_total {}\n# TYPE distill_errors_total counter\ndistill_errors_total {}\n# TYPE distill_events_admitted_total counter\ndistill_events_admitted_total {}\n# TYPE distill_retries_total counter\ndistill_retries_total {}\n# TYPE distill_dead_letters_total counter\ndistill_dead_letters_total {}\n",
            self.events_received.load(Ordering::Relaxed),
            self.events_persisted.load(Ordering::Relaxed),
            self.aggregates_persisted.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
            self.admitted.load(Ordering::Relaxed),
            self.retries.load(Ordering::Relaxed),
            self.dead_letters.load(Ordering::Relaxed),
        )
    }
}
