//! Provides the local, declarative Distill administration CLI.

use crate::{
    config::{HttpLimits, SinkFile},
    domain::SinkDefinition,
};
use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand};
use std::{collections::HashMap, fs, path::PathBuf, time::Duration};

/// Default maximum HTTP request body size in bytes.
///
/// Two MiB accommodates normal structured log batches while limiting memory retained by a request.
const DEFAULT_REQUEST_BODY_LIMIT_BYTES: usize = 2 * 1024 * 1024;
/// Default maximum duration for an HTTP request.
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 30;
/// Default number of concurrent HTTP requests handled by Distill.
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 128;
/// Default process-wide request allowance per rolling minute.
const DEFAULT_MAX_REQUESTS_PER_MINUTE: usize = 6_000;

#[derive(Debug, Parser)]
#[command(
    name = "distill",
    version,
    about = "Distill logging service and administration CLI"
)]
pub struct Cli {
    #[command(flatten)]
    pub http: HttpOptions,
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Configures process-wide HTTP resource controls.
#[derive(Debug, Args, Clone)]
pub struct HttpOptions {
    /// Maximum request body size in bytes; use 0 to disable this limit.
    #[arg(long, env = "DISTILL_REQUEST_BODY_LIMIT_BYTES", default_value_t = DEFAULT_REQUEST_BODY_LIMIT_BYTES)]
    request_body_limit_bytes: usize,
    /// Maximum request duration in seconds; use 0 to disable this limit.
    #[arg(long, env = "DISTILL_REQUEST_TIMEOUT_SECONDS", default_value_t = DEFAULT_REQUEST_TIMEOUT_SECONDS)]
    request_timeout_seconds: u64,
    /// Maximum simultaneous requests; use 0 to disable this limit.
    #[arg(long, env = "DISTILL_MAX_CONCURRENT_REQUESTS", default_value_t = DEFAULT_MAX_CONCURRENT_REQUESTS)]
    max_concurrent_requests: usize,
    /// Maximum requests per rolling minute; use 0 to disable this limit.
    #[arg(long, env = "DISTILL_MAX_REQUESTS_PER_MINUTE", default_value_t = DEFAULT_MAX_REQUESTS_PER_MINUTE)]
    max_requests_per_minute: usize,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Manage named persistence sinks.
    Sink {
        #[command(subcommand)]
        command: SinkCommand,
    },
    /// Create a local secret file or dotenv-style environment file.
    Secret {
        #[command(subcommand)]
        command: SecretCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum SecretCommand {
    /// Prompt for a secret and write it to a file without echoing it.
    FileWrite(WriteSecretFile),
    /// Prompt for a secret and write NAME=value to a local environment file.
    EnvWrite(WriteSecretEnv),
}

#[derive(Debug, Args)]
pub struct WriteSecretFile {
    #[arg(long)]
    pub file: PathBuf,
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub struct WriteSecretEnv {
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub file: PathBuf,
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Subcommand)]
pub enum SinkCommand {
    /// Create or replace a sink definition in a manifest.
    Create(CreateSink),
    /// List sink definitions without resolving or displaying secrets.
    List(ListSinks),
}

#[derive(Debug, Args)]
pub struct CreateSink {
    /// Stable name referenced by channel definitions.
    pub sink_id: String,
    /// Built-in driver: postgres, mysql, sqlserver, or http.
    #[arg(long)]
    pub driver: String,
    /// Non-secret endpoint, used by the HTTP driver.
    #[arg(long)]
    pub url: Option<String>,
    /// Environment variable containing a database connection string or HTTP bearer token.
    #[arg(long)]
    pub connection_string_env: Option<String>,
    /// File containing a database connection string.
    #[arg(long)]
    pub connection_string_file: Option<PathBuf>,
    /// Environment variable containing an HTTP bearer token.
    #[arg(long)]
    pub bearer_token_env: Option<String>,
    /// Static HTTP header in NAME=VALUE form. Do not use this for secrets.
    #[arg(long = "header", value_parser = parse_header)]
    pub headers: Vec<(String, String)>,
    /// Sink manifest to create or update.
    #[arg(long, default_value = "sinks.json")]
    pub file: PathBuf,
    /// Make this sink the default destination.
    #[arg(long)]
    pub default: bool,
}

#[derive(Debug, Args)]
pub struct ListSinks {
    /// Sink manifest to inspect.
    #[arg(long, default_value = "sinks.json")]
    pub file: PathBuf,
}

impl Cli {
    pub fn http_limits(&self) -> HttpLimits {
        HttpLimits {
            request_body_limit_bytes: (self.http.request_body_limit_bytes != 0)
                .then_some(self.http.request_body_limit_bytes),
            request_timeout: (self.http.request_timeout_seconds != 0)
                .then(|| Duration::from_secs(self.http.request_timeout_seconds)),
            max_concurrent_requests: (self.http.max_concurrent_requests != 0)
                .then_some(self.http.max_concurrent_requests),
            max_requests_per_minute: (self.http.max_requests_per_minute != 0)
                .then_some(self.http.max_requests_per_minute),
        }
    }

    pub fn run(self) -> anyhow::Result<()> {
        match self.command.context("a CLI command is required")? {
            Command::Sink { command } => match command {
                SinkCommand::Create(command) => command.run(),
                SinkCommand::List(command) => command.run(),
            },
            Command::Secret { command } => match command {
                SecretCommand::FileWrite(command) => command.run(),
                SecretCommand::EnvWrite(command) => command.run(),
            },
        }
    }
}

impl WriteSecretFile {
    fn run(self) -> anyhow::Result<()> {
        let secret = rpassword::prompt_password("Secret value: ")?;
        write_secret(&self.file, &secret, self.force)
    }
}

impl WriteSecretEnv {
    fn run(self) -> anyhow::Result<()> {
        if self.name.is_empty()
            || !self
                .name
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            bail!("environment variable names must contain only letters, digits, and underscores");
        }
        let secret = rpassword::prompt_password("Secret value: ")?;
        write_secret(&self.file, &format!("{}={secret}\n", self.name), self.force)
    }
}

fn write_secret(path: &PathBuf, contents: &str, force: bool) -> anyhow::Result<()> {
    if path.exists() && !force {
        bail!(
            "refusing to overwrite {}; use --force explicitly",
            path.display()
        );
    }
    fs::write(path, contents).with_context(|| format!("writing secret file {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("restricting permissions on {}", path.display()))?;
    }
    Ok(())
}

impl CreateSink {
    fn run(self) -> anyhow::Result<()> {
        let driver = self.driver.to_ascii_lowercase();
        if !matches!(
            driver.as_str(),
            "postgres" | "mysql" | "sqlserver" | "mssql" | "http"
        ) {
            bail!("unsupported sink driver '{driver}'");
        }
        if self.url.is_some() && driver != "http" {
            bail!("--url is only valid with the http driver");
        }
        if driver == "http" && self.url.is_none() {
            bail!("the http driver requires --url");
        }
        if driver != "http"
            && self.connection_string_env.is_none()
            && self.connection_string_file.is_none()
        {
            bail!("database sinks require --connection-string-env or --connection-string-file");
        }
        if self.connection_string_env.is_some() && self.connection_string_file.is_some() {
            bail!("choose only one of --connection-string-env and --connection-string-file");
        }
        if driver != "http" && self.bearer_token_env.is_some() {
            bail!("--bearer-token-env is only valid with the http driver");
        }
        let mut file = read_manifest(&self.file)?;
        let sink = SinkDefinition {
            sink_id: self.sink_id.clone(),
            driver,
            connection_string: self.url,
            connection_string_env: self.connection_string_env,
            connection_string_file: self.connection_string_file,
            bearer_token_env: self.bearer_token_env,
            headers: self.headers.into_iter().collect::<HashMap<_, _>>(),
        };
        if let Some(existing) = file
            .sinks
            .iter_mut()
            .find(|existing| existing.sink_id == sink.sink_id)
        {
            *existing = sink;
        } else {
            file.sinks.push(sink);
        }
        if self.default {
            file.default_sink_id = Some(self.sink_id);
        }
        write_manifest(&self.file, &file)?;
        println!("sink manifest updated: {}", self.file.display());
        Ok(())
    }
}

impl ListSinks {
    fn run(self) -> anyhow::Result<()> {
        let file = read_manifest(&self.file)?;
        for sink in file.sinks {
            let marker = if file.default_sink_id.as_deref() == Some(sink.sink_id.as_str()) {
                " (default)"
            } else {
                ""
            };
            println!("{}\t{}{}", sink.sink_id, sink.driver, marker);
        }
        Ok(())
    }
}

fn parse_header(value: &str) -> anyhow::Result<(String, String)> {
    let (name, value) = value
        .split_once('=')
        .context("headers must use NAME=VALUE syntax")?;
    if name.is_empty() || value.is_empty() {
        bail!("header names and values must not be empty");
    }
    Ok((name.to_string(), value.to_string()))
}

fn read_manifest(path: &PathBuf) -> anyhow::Result<SinkFile> {
    if !path.exists() {
        return Ok(SinkFile::default());
    }
    let contents = fs::read_to_string(path)
        .with_context(|| format!("reading sink manifest {}", path.display()))?;
    serde_json::from_str(&contents)
        .with_context(|| format!("parsing sink manifest {}", path.display()))
}

fn write_manifest(path: &PathBuf, manifest: &SinkFile) -> anyhow::Result<()> {
    let contents = serde_json::to_string_pretty(manifest).context("serializing sink manifest")?;
    fs::write(path, format!("{contents}\n"))
        .with_context(|| format!("writing sink manifest {}", path.display()))
}
