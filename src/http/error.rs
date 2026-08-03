//! Maps internal errors to stable HTTP response details.

// Rust guideline compliant 2026-02-21
use super::DistillError;

pub(super) fn error_code(error: &DistillError) -> &'static str {
    match error {
        DistillError::InvalidRequest(_) | DistillError::SchemaValidation(_) => "INVALID_REQUEST",
        DistillError::ChannelNotFound(_) => "CHANNEL_NOT_FOUND",
        DistillError::InvalidConfiguration(_) => "INVALID_CONFIGURATION",
        DistillError::BufferFull(_) | DistillError::Persistence(_) => "SERVICE_UNAVAILABLE",
    }
}

pub(super) fn client_message(error: &DistillError) -> String {
    if matches!(error, DistillError::Persistence(_)) {
        "external persistence is temporarily unavailable".to_string()
    } else {
        error.to_string()
    }
}
