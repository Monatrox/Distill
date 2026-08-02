//! Loads process and channel configuration from environment variables and JSON files.

// Rust guideline compliant 2026-02-21

use crate::{domain::ChannelDefinition, errors::DistillError};
use anyhow::Context;
use serde::Deserialize;
use std::{env, fmt, net::SocketAddr, path::PathBuf};

#[derive(Clone)]
pub struct Settings {
    pub http: HttpSettings,
    pub rabbitmq_uri: String,
    pub api_key: String,
    pub config_path: Option<PathBuf>,
    pub default_sink_connection_string: Option<String>,
    pub exchange_name_prefix: String,
}

impl fmt::Debug for Settings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Settings")
            .field("http", &self.http)
            .field("rabbitmq_uri", &"[REDACTED]")
            .field("api_key", &"[REDACTED]")
            .field("config_path", &self.config_path)
            .field(
                "default_sink_connection_string",
                &self
                    .default_sink_connection_string
                    .as_ref()
                    .map(|_| "[REDACTED]"),
            )
            .field("exchange_name_prefix", &self.exchange_name_prefix)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct HttpSettings {
    pub bind_address: SocketAddr,
}

impl Settings {
    pub fn from_env() -> anyhow::Result<Self> {
        let bind_address = env::var("DISTILL_BIND_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
            .parse()
            .context("DISTILL_BIND_ADDRESS must be a socket address")?;
        let rabbitmq_uri = env::var("RABBITMQ_URI").context("RABBITMQ_URI is required")?;
        let api_key = env::var("DISTILL_API_KEY").context("DISTILL_API_KEY is required")?;
        let default_sink_connection_string =
            env::var("DISTILL_DEFAULT_SINK_CONNECTION_STRING").ok();
        let exchange_name_prefix =
            env::var("DISTILL_EXCHANGE_NAME_PREFIX").unwrap_or_else(|_| "distill".to_string());

        Ok(Self {
            http: HttpSettings { bind_address },
            rabbitmq_uri,
            api_key,
            config_path: env::var_os("DISTILL_CONFIG_PATH").map(PathBuf::from),
            default_sink_connection_string,
            exchange_name_prefix,
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct ChannelFile {
    pub channels: Vec<ChannelDefinition>,
}

pub fn parse_channels(contents: &str) -> Result<Vec<ChannelDefinition>, DistillError> {
    let configuration: ChannelFile = serde_json::from_str(contents)
        .map_err(|error| DistillError::InvalidConfiguration(error.to_string()))?;
    for channel in &configuration.channels {
        channel
            .validate()
            .map_err(DistillError::InvalidConfiguration)?;
    }
    Ok(configuration.channels)
}
