//! Starts the Distill logging aggregation service.

// Rust guideline compliant 2026-02-21
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    distill::run().await
}
