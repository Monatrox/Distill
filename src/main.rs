//! Starts the Distill logging aggregation service.

// Rust guideline compliant 2026-02-21
use anyhow::Context;
use clap::Parser;
use mimalloc::MiMalloc;
use tokio::net::TcpListener;
use tracing::info;

mod aggregation;
mod buffer;
mod channel;
mod cli;
mod config;
mod domain;
mod errors;
mod http;
mod metrics;
mod proto;
mod routing;
mod schema;
mod sink;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let command = cli::Cli::parse();
    if command.command.is_some() {
        return command.run();
    }

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("distill=info")),
        )
        .init();

    let settings = config::Settings::from_env().context("loading Distill configuration")?;
    let listener = TcpListener::bind(settings.http.bind_address)
        .await
        .context("binding HTTP listener")?;
    let state = http::AppState::connect(settings).await?;
    let app = http::router(state);

    info!(
        event.name = "distill.http.started",
        address = %listener.local_addr()?,
        "Distill HTTP server started"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("running HTTP server")?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("installing Ctrl-C handler is a process setup invariant");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing SIGTERM handler is a process setup invariant")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => info!(event.name = "distill.shutdown.requested", "shutdown requested"),
        () = terminate => info!(event.name = "distill.shutdown.requested", "shutdown requested"),
    }
}
