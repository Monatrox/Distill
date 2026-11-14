//! Defines external persistence configuration.

// Rust guideline compliant 2026-02-21
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt, path::PathBuf};
use utoipa::ToSchema;

#[derive(Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct SinkConfig {
    pub driver: String,
    pub connection_string: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

impl fmt::Debug for SinkConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SinkConfig")
            .field("driver", &self.driver)
            .field("connection_string", &"[REDACTED]")
            .field("headers", &"[REDACTED]")
            .finish()
    }
}

impl SinkConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if !matches!(
            self.driver.as_str(),
            "postgres" | "mysql" | "sqlserver" | "mssql" | "http"
        ) {
            return Err(format!(
                "unsupported sink driver '{}'; supported drivers are postgres, mysql, sqlserver, mssql, and http",
                self.driver
            ));
        }
        if self.connection_string.is_empty() {
            return Err("sink connection_string must not be empty".to_string());
        }
        if self.driver == "http" {
            let scheme = self.connection_string.split(':').next().unwrap_or_default();
            if !matches!(scheme, "http" | "https") {
                return Err(
                    "http sink connection_string must use the http or https scheme".to_string(),
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct SinkDefinition {
    pub sink_id: String,
    pub driver: String,
    #[serde(default)]
    pub connection_string: Option<String>,
    #[serde(default)]
    pub connection_string_env: Option<String>,
    #[serde(default)]
    pub connection_string_file: Option<PathBuf>,
    #[serde(default)]
    pub bearer_token_env: Option<String>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

impl fmt::Debug for SinkDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SinkDefinition")
            .field("sink_id", &self.sink_id)
            .field("driver", &self.driver)
            .field(
                "connection_string",
                &self.connection_string.as_ref().map(|_| "[REDACTED]"),
            )
            .field("connection_string_env", &self.connection_string_env)
            .field("connection_string_file", &self.connection_string_file)
            .field("bearer_token_env", &self.bearer_token_env)
            .field("headers", &"[REDACTED]")
            .finish()
    }
}
