# Distill

Distill is a bounded Rust service for routing, aggregating, and persisting structured log events.

The v1 service uses Tokio channel actors and RabbitMQ as a durable delivery boundary. Persisted events and aggregates are written directly to the external sink configured for each channel.

Events and completed aggregates are accepted only after RabbitMQ confirms publication. An internal consumer writes them idempotently to each channel's configured external sink and acknowledges messages only after the write succeeds. Transient failures are retried with bounded backoff; exhausted messages are published to the durable `distill.dead-letter` exchange.

## Local configuration

Set `RABBITMQ_URI` and `DISTILL_API_KEY` before starting the service. For a local broker environment, run `docker compose up --build`; it starts Distill and RabbitMQ. Each channel creation request supplies its external sink connection string in `sink_config.connection_string`; Distill connects to and initializes that sink when the channel is registered.

Run `cargo test` for the local domain and aggregation tests.
