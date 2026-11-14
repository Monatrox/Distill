//! Loads process and channel configuration from environment variables and JSON files.

// Rust guideline compliant 2026-02-21

use crate::{
    core::errors::DistillError,
    domain::{ChannelDefinition, SinkConfig, SinkDefinition},
    persistence::SinkAllowlist,
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    env, fmt,
    net::SocketAddr,
    path::PathBuf,
    time::Duration,
};

/// API versions that requests may pin via the `X-API-VERSION` header.
///
/// The contract advances only on breaking changes; new endpoints and additive
/// fields extend the current version without adding a new entry here.
pub const SUPPORTED_API_VERSIONS: [&str; 1] = ["1"];

/// Default maximum number of concurrently registered channels.
///
/// Each channel spawns an actor and registers `RabbitMQ` topology; the cap
/// bounds those resources. Operators may raise it with `DISTILL_MAX_CHANNELS`.
const DEFAULT_MAX_CHANNELS: usize = 500;
/// Default time allowed for confirmed aggregate flushing during shutdown.
const DEFAULT_SHUTDOWN_TIMEOUT_SECONDS: u64 = 30;

type EnvironmentSinks = Vec<(String, SinkConfig)>;

#[derive(Clone)]
pub struct Settings {
    pub http: HttpSettings,
    pub rabbitmq_uri: String,
    pub ingest_api_key: String,
    pub admin_api_key: String,
    pub config_path: Option<PathBuf>,
    pub state_path: PathBuf,
    pub state_max_bytes: u64,
    pub auto_migrate: bool,
    pub sinks_config_path: Option<PathBuf>,
    pub(crate) environment_sinks: EnvironmentSinks,
    pub(crate) environment_default_sink_id: Option<String>,
    pub default_sink_connection_string: Option<String>,
    pub exchange_name_prefix: String,
    pub api_default_version: String,
    pub max_channels: usize,
    pub max_sinks: usize,
    pub sink_allowlist: SinkAllowlist,
    pub shutdown_timeout: Duration,
}

impl fmt::Debug for Settings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Settings")
            .field("http", &self.http)
            .field("rabbitmq_uri", &"[REDACTED]")
            .field("ingest_api_key", &"[REDACTED]")
            .field("admin_api_key", &"[REDACTED]")
            .field("config_path", &self.config_path)
            .field("state_path", &self.state_path)
            .field("state_max_bytes", &self.state_max_bytes)
            .field("auto_migrate", &self.auto_migrate)
            .field("sinks_config_path", &self.sinks_config_path)
            .field("environment_sink_count", &self.environment_sinks.len())
            .field(
                "environment_default_sink_id",
                &self.environment_default_sink_id,
            )
            .field(
                "default_sink_connection_string",
                &self
                    .default_sink_connection_string
                    .as_ref()
                    .map(|_| "[REDACTED]"),
            )
            .field("exchange_name_prefix", &self.exchange_name_prefix)
            .field("api_default_version", &self.api_default_version)
            .field("max_channels", &self.max_channels)
            .field("max_sinks", &self.max_sinks)
            .field("sink_allowlist", &self.sink_allowlist)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct HttpSettings {
    pub bind_address: SocketAddr,
    pub admin_bind_address: SocketAddr,
    pub limits: HttpLimits,
}

#[derive(Debug, Clone)]
pub struct HttpLimits {
    pub request_body_limit_bytes: Option<usize>,
    pub request_timeout: Option<Duration>,
    pub max_concurrent_requests: Option<usize>,
    pub max_requests_per_minute: Option<usize>,
}

impl Settings {
    pub fn from_env(limits: HttpLimits) -> anyhow::Result<Self> {
        let bind_address = env::var("DISTILL_BIND_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
            .parse()
            .context("DISTILL_BIND_ADDRESS must be a socket address")?;
        let admin_bind_address = parse_admin_bind_address(
            &env::var("DISTILL_ADMIN_BIND_ADDRESS")
                .unwrap_or_else(|_| "127.0.0.1:8081".to_string()),
            env::var("DISTILL_ALLOW_ADMIN_NETWORK").as_deref() == Ok("true"),
        )?;
        let rabbitmq_uri = env::var("RABBITMQ_URI").context("RABBITMQ_URI is required")?;
        let ingest_api_key =
            env::var("DISTILL_INGEST_API_KEY").context("DISTILL_INGEST_API_KEY is required")?;
        let admin_api_key =
            env::var("DISTILL_ADMIN_API_KEY").unwrap_or_else(|_| ingest_api_key.clone());
        validate_api_keys(&ingest_api_key, &admin_api_key)?;
        let default_sink_connection_string =
            env::var("DISTILL_DEFAULT_SINK_CONNECTION_STRING").ok();
        let exchange_name_prefix =
            env::var("DISTILL_EXCHANGE_NAME_PREFIX").unwrap_or_else(|_| "distill".to_string());
        let api_default_version =
            env::var("DISTILL_API_DEFAULT_VERSION").unwrap_or_else(|_| "1".to_string());
        validate_default_api_version(&api_default_version)?;
        let max_channels = env::var("DISTILL_MAX_CHANNELS")
            .unwrap_or_else(|_| DEFAULT_MAX_CHANNELS.to_string())
            .parse()
            .context("DISTILL_MAX_CHANNELS must be a non-negative integer")?;
        let max_sinks = env::var("DISTILL_MAX_SINKS")
            .unwrap_or_else(|_| DEFAULT_MAX_CHANNELS.to_string())
            .parse()
            .context("DISTILL_MAX_SINKS must be a non-negative integer")?;
        let shutdown_timeout_seconds = env::var("DISTILL_SHUTDOWN_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| DEFAULT_SHUTDOWN_TIMEOUT_SECONDS.to_string())
            .parse::<u64>()
            .context("DISTILL_SHUTDOWN_TIMEOUT_SECONDS must be a positive integer")?;
        if shutdown_timeout_seconds == 0 {
            anyhow::bail!("DISTILL_SHUTDOWN_TIMEOUT_SECONDS must be positive");
        }
        let sink_allowlist =
            SinkAllowlist::parse(&env::var("DISTILL_SINK_ALLOWLIST").unwrap_or_default())
                .map_err(|error| anyhow::anyhow!("DISTILL_SINK_ALLOWLIST is invalid: {error}"))?;
        let (environment_sinks, environment_default_sink_id) =
            parse_environment_sinks(env::vars())?;

        let state_max_bytes = env::var("DISTILL_STATE_MAX_BYTES")
            .unwrap_or_else(|_| "1073741824".to_string())
            .parse::<u64>()
            .context("DISTILL_STATE_MAX_BYTES must be an integer")?;
        if state_max_bytes < 1024 * 1024 {
            anyhow::bail!("DISTILL_STATE_MAX_BYTES must be at least 1048576");
        }
        let auto_migrate = match env::var("DISTILL_AUTO_MIGRATE").as_deref() {
            Ok("true") => true,
            Ok("false") | Err(_) => false,
            Ok(_) => anyhow::bail!("DISTILL_AUTO_MIGRATE must be true or false"),
        };
        Ok(Self {
            auto_migrate,
            state_max_bytes,
            http: HttpSettings {
                bind_address,
                admin_bind_address,
                limits,
            },
            rabbitmq_uri,
            ingest_api_key,
            admin_api_key,
            config_path: env::var_os("DISTILL_CONFIG_PATH").map(PathBuf::from),
            state_path: env::var_os("DISTILL_STATE_PATH")
                .map_or_else(|| PathBuf::from("data/state.sqlite3"), PathBuf::from),
            sinks_config_path: env::var_os("DISTILL_SINKS_CONFIG_PATH").map(PathBuf::from),
            environment_sinks,
            environment_default_sink_id,
            default_sink_connection_string,
            exchange_name_prefix,
            api_default_version,
            max_channels,
            max_sinks,
            sink_allowlist,
            shutdown_timeout: Duration::from_secs(shutdown_timeout_seconds),
        })
    }
}

#[derive(Default)]
struct EnvironmentSink {
    driver: Option<String>,
    connection_string: Option<String>,
    url: Option<String>,
    bearer_token: Option<String>,
}

fn parse_environment_sinks<I>(variables: I) -> anyhow::Result<(EnvironmentSinks, Option<String>)>
where
    I: IntoIterator<Item = (String, String)>,
{
    let mut definitions = BTreeMap::<String, EnvironmentSink>::new();
    let mut default_sink_id = None;
    for (name, value) in variables {
        if name == "DISTILL_DEFAULT_SINK" {
            if value.is_empty() {
                anyhow::bail!("DISTILL_DEFAULT_SINK must not be empty");
            }
            default_sink_id = Some(value.to_ascii_lowercase());
            continue;
        }
        let Some((sink_id, property)) = name
            .strip_prefix("DISTILL_SINK__")
            .and_then(|value| value.split_once("__"))
        else {
            continue;
        };
        if sink_id.is_empty() {
            anyhow::bail!("DISTILL_SINK__{{NAME}}__* requires a sink name");
        }
        let definition = definitions.entry(sink_id.to_ascii_lowercase()).or_default();
        match property {
            "DRIVER" => definition.driver = Some(value),
            "CONNECTION_STRING" => definition.connection_string = Some(value),
            "URL" => definition.url = Some(value),
            "BEARER_TOKEN" => definition.bearer_token = Some(value),
            _ => anyhow::bail!("unsupported environment sink property: {property}"),
        }
    }

    definitions
        .into_iter()
        .map(|(sink_id, definition)| {
            let driver = definition
                .driver
                .ok_or_else(|| anyhow::anyhow!("environment sink '{sink_id}' requires DRIVER"))?;
            let connection_string = match (definition.connection_string, definition.url) {
                (Some(connection_string), None) | (None, Some(connection_string)) => {
                    connection_string
                }
                (None, None) => {
                    anyhow::bail!("environment sink '{sink_id}' requires CONNECTION_STRING or URL")
                }
                (Some(_), Some(_)) => anyhow::bail!(
                    "environment sink '{sink_id}' cannot set both CONNECTION_STRING and URL"
                ),
            };
            if driver != "http" && definition.bearer_token.is_some() {
                anyhow::bail!("environment sink '{sink_id}' only supports BEARER_TOKEN for http");
            }
            let mut headers = HashMap::new();
            if let Some(token) = definition.bearer_token {
                headers.insert("Authorization".to_string(), format!("Bearer {token}"));
            }
            let config = SinkConfig {
                driver,
                connection_string,
                headers,
            };
            config.validate().map_err(|error| {
                anyhow::anyhow!("environment sink '{sink_id}' is invalid: {error}")
            })?;
            Ok((sink_id, config))
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map(|sinks| (sinks, default_sink_id))
}

fn validate_default_api_version(value: &str) -> anyhow::Result<()> {
    if !SUPPORTED_API_VERSIONS.contains(&value) {
        anyhow::bail!(
            "DISTILL_API_DEFAULT_VERSION must be one of: {}",
            SUPPORTED_API_VERSIONS.join(", ")
        );
    }
    Ok(())
}

fn parse_admin_bind_address(value: &str, allow_network: bool) -> anyhow::Result<SocketAddr> {
    let address: SocketAddr = value
        .parse()
        .context("DISTILL_ADMIN_BIND_ADDRESS must be a socket address")?;
    if !allow_network && !address.ip().is_loopback() {
        anyhow::bail!(
            "DISTILL_ADMIN_BIND_ADDRESS must use a loopback address unless DISTILL_ALLOW_ADMIN_NETWORK=true"
        );
    }
    Ok(address)
}

fn validate_api_keys(ingest_api_key: &str, admin_api_key: &str) -> anyhow::Result<()> {
    if ingest_api_key.trim().is_empty() || admin_api_key.trim().is_empty() {
        anyhow::bail!("DISTILL_INGEST_API_KEY and DISTILL_ADMIN_API_KEY must not be empty");
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::{
        parse_admin_bind_address, parse_environment_sinks, validate_api_keys,
        validate_default_api_version,
    };

    #[test]
    fn administrative_listener_requires_explicit_network_opt_in() {
        assert!(parse_admin_bind_address("127.0.0.1:8081", false).is_ok());
        assert!(parse_admin_bind_address("[::1]:8081", false).is_ok());
        assert!(parse_admin_bind_address("0.0.0.0:8081", false).is_err());
        assert!(parse_admin_bind_address("0.0.0.0:8081", true).is_ok());
    }

    #[test]
    fn shared_or_separate_api_keys_are_supported() {
        assert!(validate_api_keys("ingest", "admin").is_ok());
        assert!(validate_api_keys("same", "same").is_ok());
    }

    #[test]
    fn default_api_version_must_be_supported() {
        assert!(validate_default_api_version("1").is_ok());
        assert!(validate_default_api_version("2").is_err());
        assert!(validate_default_api_version("").is_err());
    }

    #[test]
    fn parses_aspire_sink_environment() {
        let (sinks, default_sink_id) = parse_environment_sinks(
            [
                ("DISTILL_SINK__PRIMARY__DRIVER", "postgres"),
                (
                    "DISTILL_SINK__PRIMARY__CONNECTION_STRING",
                    "postgresql://distill:secret@postgres/logs",
                ),
                ("DISTILL_DEFAULT_SINK", "primary"),
            ]
            .map(|(name, value)| (name.to_string(), value.to_string())),
        )
        .expect("Aspire sink environment should parse");

        assert_eq!(default_sink_id.as_deref(), Some("primary"));
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].0, "primary");
        assert_eq!(sinks[0].1.driver, "postgres");
        assert_eq!(
            sinks[0].1.connection_string,
            "postgresql://distill:secret@postgres/logs"
        );
    }
}
