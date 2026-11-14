//! Contains the application core: channel actors, aggregation, and shared contracts.
//!
//! The `core` module implements what Distill does with events after they arrive:
//! per-channel actor orchestration (`channel`), event classification (`routing`),
//! bounded buffering (`buffer`), aggregate computation (`aggregation`), runtime
//! schema validation (`schema`), the ingest wire formats (`proto`), process
//! metrics (`metrics`), and the stable error contract (`errors`). The channel
//! registry (`registry`) composes these with the broker and persistence layers.
//!
//! Core modules may use `domain` types and `infra` configuration; they are the
//! application's behavior rather than its process plumbing.

// Rust guideline compliant 2026-02-21
pub(crate) mod aggregation;
pub(crate) mod buffer;
pub(crate) mod channel;
pub(crate) mod errors;
pub(crate) mod metrics;
pub(crate) mod proto;
pub(crate) mod registry;
pub(crate) mod routing;
pub(crate) mod schema;
pub(crate) mod state;
