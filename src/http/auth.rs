//! Contains HTTP endpoint handlers.

// Rust guideline compliant 2026-02-21
use super::{
    ConstantTimeEq, DistillError, HeaderMap, IntoResponse, Next, Request, Response, State,
};
use std::sync::Arc;

fn authorize_with_key(headers: &HeaderMap, expected: &str) -> Result<(), DistillError> {
    let provided = headers
        .get("X-API-Key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !bool::from(provided.as_bytes().ct_eq(expected.as_bytes())) {
        return Err(DistillError::Unauthorized);
    }
    Ok(())
}

pub(super) async fn authenticate(
    State(expected): State<Arc<String>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    if let Err(error) = authorize_with_key(&headers, expected.as_str()) {
        return error.into_response();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::{authenticate, authorize_with_key};
    use crate::errors::DistillError;
    use axum::{
        Router,
        body::{Body, Bytes},
        http::{HeaderMap, HeaderValue, Request, StatusCode},
        response::IntoResponse,
        routing::post,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tower::ServiceExt;
    use tower_http::limit::RequestBodyLimitLayer;

    fn api_key_headers(key: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-API-Key",
            HeaderValue::from_str(key).expect("test key is valid"),
        );
        headers
    }

    #[test]
    fn ingest_key_does_not_authorize_administration() {
        let headers = api_key_headers("ingest-key");
        assert!(authorize_with_key(&headers, "admin-key").is_err());
    }

    #[test]
    fn admin_key_does_not_authorize_ingestion() {
        let headers = api_key_headers("admin-key");
        assert!(authorize_with_key(&headers, "ingest-key").is_err());
    }

    #[test]
    fn rejected_keys_return_unauthorized() {
        let response = DistillError::Unauthorized.into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejected_key_does_not_invoke_body_handler() {
        let invoked = Arc::new(AtomicBool::new(false));
        let handler_invoked = Arc::clone(&invoked);
        let app = Router::new()
            .route(
                "/",
                post(move |_: Bytes| async move {
                    handler_invoked.store(true, Ordering::Relaxed);
                    StatusCode::NO_CONTENT
                }),
            )
            .route_layer(axum::middleware::from_fn_with_state(
                Arc::new("expected".to_string()),
                authenticate,
            ));
        let request = Request::builder()
            .method("POST")
            .uri("/")
            .header("X-API-Key", "wrong")
            .body(Body::from("body"))
            .expect("test request should build");
        let response = app
            .oneshot(request)
            .await
            .expect("test router should respond");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(!invoked.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn authentication_precedes_body_limit_for_rejected_keys() {
        let app = Router::new()
            .route("/", post(|_: Bytes| async { StatusCode::NO_CONTENT }))
            .layer(RequestBodyLimitLayer::new(1))
            .layer(axum::middleware::from_fn_with_state(
                Arc::new("expected".to_string()),
                authenticate,
            ));
        let request = Request::builder()
            .method("POST")
            .uri("/")
            .header("X-API-Key", "wrong")
            .body(Body::from("body larger than one byte"))
            .expect("test request should build");
        let response = app
            .oneshot(request)
            .await
            .expect("test router should respond");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
