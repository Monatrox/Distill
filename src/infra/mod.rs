//! Provides process-level configuration and CLI plumbing.
//!
//! The `infra` module owns how the Distill process is started and configured:
//! environment and file configuration loading (`config`) and the declarative
//! administration CLI (`cli`). It does not implement ingestion, aggregation,
//! or persistence behavior.

// Rust guideline compliant 2026-02-21
pub(crate) mod cli;
pub(crate) mod config;
