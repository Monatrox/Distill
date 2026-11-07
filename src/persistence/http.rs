//! Implements one external persistence driver.

// Rust guideline compliant 2026-02-21
use super::{
    AggregatedLog, DistillError, HTTP_REQUEST_TIMEOUT, LogEvent, Serialize, Sink, SinkAllowlist,
    async_trait,
};

#[derive(Clone)]
pub struct HttpSink {
    client: reqwest::Client,
    endpoint: String,
}

impl std::fmt::Debug for HttpSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("HttpSink").finish_non_exhaustive()
    }
}

impl HttpSink {
    pub fn connect(
        config: &crate::domain::SinkConfig,
        sink_allowlist: &SinkAllowlist,
    ) -> Result<Self, DistillError> {
        let destination =
            super::policy::resolve_sink_url(&config.connection_string, sink_allowlist)
                .map_err(DistillError::InvalidConfiguration)?;
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in &config.headers {
            let name = reqwest::header::HeaderName::try_from(name).map_err(|error| {
                DistillError::InvalidConfiguration(format!("invalid HTTP header name: {error}"))
            })?;
            let mut value = reqwest::header::HeaderValue::try_from(value).map_err(|error| {
                DistillError::InvalidConfiguration(format!("invalid HTTP header value: {error}"))
            })?;
            value.set_sensitive(true);
            headers.insert(name, value);
        }
        let builder = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(HTTP_REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .dns_resolver(std::sync::Arc::new(destination));
        let client = builder
            .build()
            .map_err(|error| DistillError::InvalidConfiguration(error.to_string()))?;
        Ok(Self {
            client,
            endpoint: config.connection_string.clone(),
        })
    }

    async fn send(
        &self,
        kind: &str,
        id: String,
        value: &impl Serialize,
        revision: Option<u64>,
    ) -> Result<(), DistillError> {
        let mut request = self
            .client
            .post(&self.endpoint)
            .header("X-Distill-Record-Type", kind)
            .header("X-Distill-Record-Id", id)
            .header("X-Distill-Contract-Version", "1")
            .json(value);
        if let Some(revision) = revision {
            request = request.header("X-Distill-Update-Revision", revision.to_string());
        }
        let response = request
            .send()
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        if response.status().is_success() {
            return Ok(());
        }
        if response.status() == reqwest::StatusCode::CONFLICT {
            return Err(DistillError::ConfigurationConflict);
        }
        if response.status().is_client_error()
            && !matches!(
                response.status(),
                reqwest::StatusCode::REQUEST_TIMEOUT | reqwest::StatusCode::TOO_MANY_REQUESTS
            )
        {
            return Err(DistillError::InvalidRequest(
                "HTTP receiver rejected the record".to_string(),
            ));
        }
        Err(DistillError::Persistence(format!(
            "HTTP sink returned status {}",
            response.status()
        )))
    }
}

#[async_trait]
impl Sink for HttpSink {
    async fn persist_event(&self, event: &LogEvent) -> Result<(), DistillError> {
        self.send("event", event.event_id.to_string(), event, None)
            .await
    }

    async fn persist_aggregate(&self, aggregate: &AggregatedLog) -> Result<(), DistillError> {
        super::snapshot_revision(aggregate)?;
        self.send(
            "aggregate",
            aggregate.aggregate_id.to_string(),
            aggregate,
            Some(aggregate.update_revision),
        )
        .await
    }
}
