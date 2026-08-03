//! Loads process and channel configuration from environment variables and JSON files.

// Rust guideline compliant 2026-02-21

use crate::{
    domain::{ChannelDefinition, SinkConfig, SinkDefinition},
    errors::DistillError,
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{env, fmt, net::SocketAddr, path::PathBuf};

#[derive(Clone)]
pub struct Settings {
    pub http: HttpSettings,
    pub rabbitmq_uri: String,
    pub api_key: String,
    pub config_path: Option<PathBuf>,
    pub sinks_config_path: Option<PathBuf>,
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
            .field("sinks_config_path", &self.sinks_config_path)
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
            sinks_config_path: env::var_os("DISTILL_SINKS_CONFIG_PATH").map(PathBuf::from),
            default_sink_connection_string,
            exchange_name_prefix,
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct ChannelFile {
    pub channels: Vec<ChannelDefinition>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct SinkFile {
    #[serde(default)]
    pub default_sink_id: Option<String>,
    #[serde(default)]
    pub sinks: Vec<SinkDefinition>,
}

pub fn parse_channels_with_default(
    contents: &str,
    default_sink_id: Option<&str>,
) -> Result<Vec<ChannelDefinition>, DistillError> {
    let configuration: ChannelFile = serde_json::from_str(contents)
        .map_err(|error| DistillError::InvalidConfiguration(error.to_string()))?;
    let mut channels = configuration.channels;
    for channel in &mut channels {
        if channel.sink_id.is_none() {
            channel.sink_id = default_sink_id.map(ToOwned::to_owned);
        }
        channel
            .validate()
            .map_err(DistillError::InvalidConfiguration)?;
    }
    Ok(channels)
}

pub fn parse_sinks(contents: &str) -> Result<SinkFile, DistillError> {
    let configuration: SinkFile = serde_json::from_str(contents)
        .map_err(|error| DistillError::InvalidConfiguration(error.to_string()))?;
    let mut ids = std::collections::HashSet::new();
    for sink in &configuration.sinks {
        if sink.sink_id.is_empty() || !ids.insert(&sink.sink_id) {
            return Err(DistillError::InvalidConfiguration(
                "sink IDs must be non-empty and unique".to_string(),
            ));
        }
        if sink.connection_string.is_none()
            && sink.connection_string_env.is_none()
            && sink.connection_string_file.is_none()
        {
            return Err(DistillError::InvalidConfiguration(format!(
                "sink '{}' must specify connection_string, connection_string_env, or connection_string_file",
                sink.sink_id
            )));
        }
        if sink.driver != "http" && sink.connection_string.is_some() {
            return Err(DistillError::InvalidConfiguration(format!(
                "database sink '{}' must use connection_string_env or connection_string_file; raw connection strings are not allowed in sink manifests",
                sink.sink_id
            )));
        }
        if sink.driver != "http" && sink.bearer_token_env.is_some() {
            return Err(DistillError::InvalidConfiguration(format!(
                "bearer_token_env is only valid for HTTP sink '{}'",
                sink.sink_id
            )));
        }
        if sink.connection_string.is_some()
            && (sink.connection_string_env.is_some() || sink.connection_string_file.is_some())
        {
            return Err(DistillError::InvalidConfiguration(format!(
                "sink '{}' must specify only one connection string source",
                sink.sink_id
            )));
        }
        let config = resolve_sink(sink)?;
        config
            .validate()
            .map_err(DistillError::InvalidConfiguration)?;
    }
    if let Some(default_id) = &configuration.default_sink_id
        && !configuration
            .sinks
            .iter()
            .any(|sink| &sink.sink_id == default_id)
    {
        return Err(DistillError::InvalidConfiguration(format!(
            "default sink '{default_id}' is not defined"
        )));
    }
    Ok(configuration)
}

pub fn resolve_sink(definition: &SinkDefinition) -> Result<SinkConfig, DistillError> {
    let connection_string = if let Some(value) = &definition.connection_string {
        value.clone()
    } else if let Some(variable) = &definition.connection_string_env {
        env::var(variable).map_err(|_| {
            DistillError::InvalidConfiguration(format!(
                "environment variable '{variable}' for sink '{}' is not set",
                definition.sink_id
            ))
        })?
    } else if let Some(path) = &definition.connection_string_file {
        std::fs::read_to_string(path)
            .map_err(|error| {
                DistillError::InvalidConfiguration(format!(
                    "reading sink secret file {}: {error}",
                    path.display()
                ))
            })?
            .trim()
            .to_string()
    } else {
        return Err(DistillError::InvalidConfiguration(format!(
            "sink '{}' has no connection string source",
            definition.sink_id
        )));
    };
    let mut headers = definition.headers.clone();
    if let Some(variable) = &definition.bearer_token_env {
        let token = env::var(variable).map_err(|_| {
            DistillError::InvalidConfiguration(format!(
                "environment variable '{variable}' for sink '{}' is not set",
                definition.sink_id
            ))
        })?;
        headers.insert("Authorization".to_string(), format!("Bearer {token}"));
    }
    Ok(SinkConfig {
        driver: definition.driver.clone(),
        connection_string,
        headers,
    })
}
