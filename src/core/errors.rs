//! Defines stable errors returned by the Distill application boundary.

// Rust guideline compliant 2026-02-21
use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use std::fmt;
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Error)]
pub enum DistillError {
    #[error("invalid API key")]
    Unauthorized,
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("configuration conflicts with an existing version")]
    ConfigurationConflict,
    #[error("channel not found: {0}")]
    ChannelNotFound(String),
    #[error("channel configuration is invalid: {0}")]
    InvalidConfiguration(String),
    #[error("schema validation failed: {0}")]
    SchemaValidation(String),
    #[error("buffer admission rejected for channel {0}")]
    BufferFull(String),
    #[error("request throttled")]
    RequestThrottled,
    #[error("persistence delivery failed: {0}")]
    Persistence(String),
    #[error("unsupported API version: {0}")]
    UnsupportedApiVersion(String),
}

/// Stable machine-readable error codes exposed by the API contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    Unauthorized,
    InvalidRequest,
    ChannelNotFound,
    ConfigurationConflict,
    InvalidConfiguration,
    SchemaValidation,
    BufferFull,
    ServiceUnavailable,
    RequestThrottled,
    UnsupportedApiVersion,
}

impl ErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unauthorized => "UNAUTHORIZED",
            Self::InvalidRequest => "INVALID_REQUEST",
            Self::ChannelNotFound => "CHANNEL_NOT_FOUND",
            Self::ConfigurationConflict => "CONFIGURATION_CONFLICT",
            Self::InvalidConfiguration => "INVALID_CONFIGURATION",
            Self::SchemaValidation => "SCHEMA_VALIDATION",
            Self::BufferFull => "BUFFER_FULL",
            Self::ServiceUnavailable => "SERVICE_UNAVAILABLE",
            Self::RequestThrottled => "REQUEST_THROTTLED",
            Self::UnsupportedApiVersion => "UNSUPPORTED_API_VERSION",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorEnvelope {
    pub error_code: ErrorCode,
    pub message: String,
}

impl DistillError {
    /// Returns the HTTP status code for this error.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::ConfigurationConflict => StatusCode::CONFLICT,
            Self::InvalidRequest(_)
            | Self::SchemaValidation(_)
            | Self::UnsupportedApiVersion(_) => StatusCode::BAD_REQUEST,
            Self::ChannelNotFound(_) => StatusCode::NOT_FOUND,
            Self::InvalidConfiguration(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::BufferFull(_) | Self::Persistence(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::RequestThrottled => StatusCode::TOO_MANY_REQUESTS,
        }
    }

    /// Returns the stable machine-readable code for this error.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::Unauthorized => ErrorCode::Unauthorized,
            Self::ConfigurationConflict => ErrorCode::ConfigurationConflict,
            Self::InvalidRequest(_) => ErrorCode::InvalidRequest,
            Self::ChannelNotFound(_) => ErrorCode::ChannelNotFound,
            Self::InvalidConfiguration(_) => ErrorCode::InvalidConfiguration,
            Self::SchemaValidation(_) => ErrorCode::SchemaValidation,
            Self::BufferFull(_) => ErrorCode::BufferFull,
            Self::Persistence(_) => ErrorCode::ServiceUnavailable,
            Self::RequestThrottled => ErrorCode::RequestThrottled,
            Self::UnsupportedApiVersion(_) => ErrorCode::UnsupportedApiVersion,
        }
    }

    /// Returns the message that is safe to expose to API clients.
    pub fn client_message(&self) -> String {
        if matches!(self, Self::Persistence(_)) {
            "external persistence is temporarily unavailable".to_string()
        } else {
            self.to_string()
        }
    }
}

impl IntoResponse for DistillError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status_code(),
            Json(ErrorEnvelope {
                error_code: self.error_code(),
                message: self.client_message(),
            }),
        )
            .into_response()
    }
}

impl From<sqlx::Error> for DistillError {
    fn from(value: sqlx::Error) -> Self {
        Self::Persistence(value.to_string())
    }
}

impl From<lapin::Error> for DistillError {
    fn from(value: lapin::Error) -> Self {
        Self::Persistence(value.to_string())
    }
}
