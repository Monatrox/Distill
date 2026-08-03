//! Coordinates configuration, event processing, and bounded runtime state.

// Rust guideline compliant 2026-02-21
pub(crate) mod aggregation;
pub(crate) mod buffer;
pub(crate) mod channel;
pub(crate) mod cli;
pub(crate) mod config;
pub(crate) mod errors;
pub(crate) mod metrics;
pub(crate) mod proto;
pub(crate) mod routing;
pub(crate) mod schema;
