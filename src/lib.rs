//! Contains the Distill application and its internal components.
//!
//! The binary crate supplies process-level configuration, while this crate
//! composes the CLI and HTTP application. Modules are grouped by concern:
//! `domain` holds pure types, `core` holds application behavior, `infra`
//! holds process configuration and CLI plumbing, and `broker`, `persistence`,
//! and `http` wrap the external surfaces.

// Rust guideline compliant 2026-02-21
use anyhow::Context;
use clap::Parser;
use tokio::{net::TcpListener, sync::watch};
use tracing::info;

mod broker;
mod core;
mod domain;
mod http;
mod infra;
mod persistence;

/// Runs the Distill command-line application.
///
/// Without a subcommand, this starts the configured HTTP server.
///
/// # Errors
/// Returns an error when CLI execution, configuration, listener setup, or the HTTP server fails.
pub async fn run() -> anyhow::Result<()> {
    let command = infra::cli::Cli::parse();
    if command.command.is_some() {
        return command.run();
    }
    let http_limits = command.http_limits();

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("distill=info")),
        )
        .init();

    let settings =
        infra::config::Settings::from_env(http_limits).context("loading Distill configuration")?;
    let listener = TcpListener::bind(settings.http.bind_address)
        .await
        .context("binding HTTP listener")?;
    let admin_listener = TcpListener::bind(settings.http.admin_bind_address)
        .await
        .context("binding administrative HTTP listener")?;
    let state = http::AppState::connect(settings).await?;
    let app = http::public_router(state.clone());
    let admin_app = http::admin_router(state.clone());

    info!(
        event.name = "distill.http.started",
        address = %listener.local_addr()?,
        "Distill public HTTP server started"
    );
    info!(
        event.name = "distill.http.admin_started",
        address = %admin_listener.local_addr()?,
        "Distill loopback administrative HTTP server started"
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let servers = async {
        tokio::try_join!(
            axum::serve(listener, app)
                .with_graceful_shutdown(wait_for_shutdown(shutdown_rx.clone())),
            axum::serve(admin_listener, admin_app)
                .with_graceful_shutdown(wait_for_shutdown(shutdown_rx)),
        )
    };
    tokio::pin!(servers);
    let result = tokio::select! {
        result = &mut servers => result,
        () = shutdown_signal() => {
            state.stop_admission();
            let _ = shutdown_tx.send(true);
            match tokio::time::timeout(state.shutdown_timeout(), &mut servers).await {
                Ok(result) => result,
                Err(_) => Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "HTTP graceful shutdown exceeded the configured timeout",
                )),
            }
        }
    };
    state.shutdown().await.context("flushing channel buffers")?;
    result.context("running HTTP servers")?;

    Ok(())
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
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
