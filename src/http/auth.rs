//! Contains HTTP endpoint handlers.

// Rust guideline compliant 2026-02-21
use super::{AppState, ConstantTimeEq, DistillError, HeaderMap};

pub(super) fn authorize(headers: &HeaderMap, state: &AppState) -> Result<(), DistillError> {
    let provided = headers
        .get("X-API-Key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !bool::from(provided.as_bytes().ct_eq(state.api_key.as_bytes())) {
        return Err(DistillError::InvalidRequest("invalid API key".to_string()));
    }
    Ok(())
}
