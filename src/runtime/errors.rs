//! Defines stable errors returned by the Distill application boundary.

use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DistillError {
    #[error("invalid API key")]
    Unauthorized,
    #[error("invalid request: {0}")]
    InvalidRequest(String),
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
}

#[derive(Debug, Serialize)]
pub struct ErrorEnvelope {
    pub error_code: &'static str,
    pub message: String,
}

impl IntoResponse for DistillError {
    fn into_response(self) -> axum::response::Response {
        let (status, code) = match &self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "UNAUTHORIZED"),
            Self::InvalidRequest(_) | Self::SchemaValidation(_) => {
                (StatusCode::BAD_REQUEST, "INVALID_REQUEST")
            }
            Self::ChannelNotFound(_) => (StatusCode::NOT_FOUND, "CHANNEL_NOT_FOUND"),
            Self::InvalidConfiguration(_) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "INVALID_CONFIGURATION")
            }
            Self::BufferFull(_) | Self::Persistence(_) => {
                (StatusCode::SERVICE_UNAVAILABLE, "SERVICE_UNAVAILABLE")
            }
            Self::RequestThrottled => (StatusCode::TOO_MANY_REQUESTS, "REQUEST_THROTTLED"),
        };

        let message = if matches!(self, Self::Persistence(_)) {
            "external persistence is temporarily unavailable".to_string()
        } else {
            self.to_string()
        };

        (
            status,
            Json(ErrorEnvelope {
                error_code: code,
                message,
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
