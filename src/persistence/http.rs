//! Implements one external persistence driver.

// Rust guideline compliant 2026-02-21
use super::{
    AggregatedLog, DistillError, HTTP_REQUEST_TIMEOUT, LogEvent, Serialize, Sink, async_trait,
};

#[derive(Clone, Debug)]
pub struct HttpSink {
    client: reqwest::Client,
    endpoint: String,
}

impl HttpSink {
    pub fn connect(config: &crate::domain::SinkConfig) -> Result<Self, DistillError> {
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in &config.headers {
            let name = reqwest::header::HeaderName::try_from(name).map_err(|error| {
                DistillError::InvalidConfiguration(format!("invalid HTTP header name: {error}"))
            })?;
            let value = reqwest::header::HeaderValue::try_from(value).map_err(|error| {
                DistillError::InvalidConfiguration(format!("invalid HTTP header value: {error}"))
            })?;
            headers.insert(name, value);
        }
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(HTTP_REQUEST_TIMEOUT)
            .build()
            .map_err(|error| DistillError::InvalidConfiguration(error.to_string()))?;
        reqwest::Url::parse(&config.connection_string).map_err(|error| {
            DistillError::InvalidConfiguration(format!("invalid HTTP sink URL: {error}"))
        })?;
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
    ) -> Result<(), DistillError> {
        let response = self
            .client
            .post(&self.endpoint)
            .header("X-Distill-Record-Type", kind)
            .header("X-Distill-Record-Id", id)
            .json(value)
            .send()
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        if response.status().is_success() || response.status() == reqwest::StatusCode::CONFLICT {
            return Ok(());
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
        self.send("event", event.event_id.to_string(), event).await
    }

    async fn persist_aggregate(&self, aggregate: &AggregatedLog) -> Result<(), DistillError> {
        self.send("aggregate", aggregate.aggregate_id.to_string(), aggregate)
            .await
    }
}
