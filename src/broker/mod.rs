//! Provides `RabbitMQ` publishing, consumption, and retry handling.

// Rust guideline compliant 2026-02-21
use crate::{
    core::{channel::ChannelHandle, errors::DistillError, metrics::Metrics},
    domain::{AggregatedLog, LogEvent},
    persistence::Sink,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream::FuturesUnordered};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicNackOptions,
        BasicPublishOptions, BasicQosOptions, ConfirmSelectOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    publisher_confirm::Confirmation,
    types::{AMQPValue, FieldTable},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::RwLock,
    time::{Duration, sleep},
};
use tracing::{error, warn};
use uuid::Uuid;

const DELIVERY_QUEUE: &str = "distill.persistence";
const AGGREGATE_INPUT_EXCHANGE: &str = "distill.aggregate-input";
const DEAD_LETTER_EXCHANGE: &str = "distill.dead-letter";
const DEAD_LETTER_QUEUE: &str = "distill.dead-letter.queue";
const RETRY_EXCHANGE: &str = "distill.retry";
/// Stops repeated sink failures from retrying forever.
const MAX_RETRIES: u32 = 10;
/// Identifies compatible broker payload encoding.
const ENVELOPE_VERSION: u8 = 1;
/// Identifies envelopes written before explicit versioning was introduced.
const LEGACY_ENVELOPE_VERSION: u8 = 1;
/// `RabbitMQ` queue cap chosen for bounded outage backlogs.
const MAX_QUEUE_MESSAGES: u32 = 1_000_000;
/// `RabbitMQ` queue byte cap: one GiB prevents broker-disk exhaustion.
const MAX_QUEUE_BYTES: u32 = 1_073_741_824;
/// Prevents failure diagnostics from becoming an unbounded secondary payload.
const MAX_FAILURE_REASON_BYTES: usize = 256;
const ROUTED_EVENT_SUFFIX: &str = "routed-event-v1";
const ROUTED_AGGREGATE_SUFFIX: &str = "routed-aggregate-v1";
/// Bounds unacknowledged aggregate inputs while allowing per-event windows to fill.
const CONSUMER_PREFETCH: u16 = 256;
/// Prevents failed consumers from hot-looping while retaining automatic recovery.
const CONSUMER_RESTART_DELAY: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct RabbitPublisher {
    _connection: Arc<Connection>,
    channel: Arc<Channel>,
}

impl RabbitPublisher {
    pub async fn connect(uri: &str) -> Result<Self, DistillError> {
        let connection = Connection::connect(uri, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await?;
        declare_exchange(&channel, AGGREGATE_INPUT_EXCHANGE).await?;
        Ok(Self {
            _connection: Arc::new(connection),
            channel: Arc::new(channel),
        })
    }

    pub async fn publish<T: Serialize>(
        &self,
        exchange: &str,
        routing_key: &str,
        message: &T,
    ) -> Result<(), DistillError> {
        self.channel
            .exchange_declare(
                exchange,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        let envelope = DeliveryEnvelope {
            version: ENVELOPE_VERSION,
            payload: serde_json::to_value(message)
                .map_err(|error| DistillError::Persistence(error.to_string()))?,
            retry_count: 0,
        };
        let payload = serde_json::to_vec(&envelope)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        publish_confirmed(&self.channel, exchange, routing_key, &payload).await
    }

    pub(crate) async fn publish_routed_event(
        &self,
        exchange: &str,
        routing_key_prefix: &str,
        event: LogEvent,
        generation: Uuid,
        persist: bool,
        aggregate: bool,
    ) -> Result<(), DistillError> {
        let message = RoutedEvent {
            event,
            generation,
            persist,
            aggregate,
        };
        let destination = if aggregate {
            AGGREGATE_INPUT_EXCHANGE
        } else {
            exchange
        };
        let routing_key = if aggregate {
            generation.to_string()
        } else {
            format!("{routing_key_prefix}{ROUTED_EVENT_SUFFIX}")
        };
        self.publish(destination, &routing_key, &message).await
    }

    pub(crate) async fn publish_routed_aggregate(
        &self,
        exchange: &str,
        routing_key_prefix: &str,
        aggregate: AggregatedLog,
        generation: Uuid,
    ) -> Result<(), DistillError> {
        self.publish(
            exchange,
            &format!("{routing_key_prefix}{ROUTED_AGGREGATE_SUFFIX}"),
            &RoutedAggregate {
                aggregate,
                generation,
            },
        )
        .await
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct DeliveryEnvelope {
    #[serde(default = "default_envelope_version")]
    version: u8,
    payload: serde_json::Value,
    retry_count: u32,
}

#[derive(Debug, Deserialize, Serialize)]
struct DeadLetterMessage {
    metadata: DeadLetterMetadata,
    original_data_base64: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct DeadLetterMetadata {
    id: Uuid,
    envelope_version: Option<u8>,
    failure_category: String,
    failure_reason: String,
    retry_count: Option<u32>,
    original_message_id: Option<Uuid>,
    original_exchange: String,
    original_routing_key: String,
    failed_at: DateTime<Utc>,
}

fn default_envelope_version() -> u8 {
    LEGACY_ENVELOPE_VERSION
}

fn parse_envelope(data: &[u8]) -> Result<DeliveryEnvelope, DistillError> {
    let envelope: DeliveryEnvelope = serde_json::from_slice(data)
        .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
    if envelope.version != ENVELOPE_VERSION {
        return Err(DistillError::InvalidRequest(format!(
            "unsupported broker envelope version {}",
            envelope.version
        )));
    }
    Ok(envelope)
}

fn queue_arguments() -> FieldTable {
    let mut arguments = FieldTable::default();
    arguments.insert(
        "x-max-length".into(),
        AMQPValue::LongUInt(MAX_QUEUE_MESSAGES),
    );
    arguments.insert(
        "x-max-length-bytes".into(),
        AMQPValue::LongUInt(MAX_QUEUE_BYTES),
    );
    arguments.insert(
        "x-overflow".into(),
        AMQPValue::LongString("reject-publish".into()),
    );
    arguments
}

#[derive(Debug, Deserialize, Serialize)]
struct RoutedEvent {
    event: LogEvent,
    generation: Uuid,
    persist: bool,
    aggregate: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct RoutedAggregate {
    aggregate: AggregatedLog,
    generation: Uuid,
}

#[derive(Clone)]
struct SinkBinding {
    sink: Arc<dyn Sink>,
}

#[derive(Clone)]
struct AggregateBinding {
    generation: Uuid,
}

#[derive(Clone)]
struct GenerationSink {
    channel_id: String,
    sink: Arc<dyn Sink>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DeadLetterRecord {
    pub(crate) id: Uuid,
    pub(crate) envelope_version: Option<u8>,
    pub(crate) failure_category: String,
    pub(crate) failure_reason: String,
    pub(crate) retry_count: Option<u32>,
    pub(crate) original_message_id: Option<Uuid>,
    pub(crate) original_exchange: String,
    pub(crate) original_routing_key: String,
    pub(crate) failed_at: DateTime<Utc>,
}

pub(crate) struct DeadLetterReplaySummary {
    pub(crate) replayed: u32,
    pub(crate) retained: u32,
    pub(crate) invalid: u32,
}

pub(crate) struct DeadLetterClient {
    _connection: Connection,
    channel: Channel,
}

pub struct RabbitConsumer {
    uri: Arc<str>,
    connection: Arc<Connection>,
    channel: Arc<Channel>,
    sinks: Arc<RwLock<HashMap<String, SinkBinding>>>,
    generation_sinks: Arc<RwLock<HashMap<Uuid, GenerationSink>>>,
    aggregators: Arc<RwLock<HashMap<String, AggregateBinding>>>,
    aggregate_handles: Arc<RwLock<HashMap<Uuid, ChannelHandle>>>,
    metrics: Arc<Metrics>,
}

pub(crate) struct PreparedChannel {
    channel: Option<Arc<Channel>>,
    channel_id: String,
    generation: Uuid,
    sink: Arc<dyn Sink>,
    handle: ChannelHandle,
}

impl DeadLetterClient {
    pub(crate) async fn connect(uri: &str) -> Result<Self, DistillError> {
        let connection = Connection::connect(uri, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await?;
        Ok(Self {
            _connection: connection,
            channel,
        })
    }

    pub(crate) async fn count(&self) -> Result<u32, DistillError> {
        Ok(self
            .channel
            .queue_declare(
                DEAD_LETTER_QUEUE,
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?
            .message_count())
    }

    pub(crate) async fn list(&self, limit: u16) -> Result<Vec<DeadLetterRecord>, DistillError> {
        let mut records = Vec::new();
        let mut last_unacknowledged = None;
        for _ in 0..limit {
            let Some(delivery) = self
                .channel
                .basic_get(DEAD_LETTER_QUEUE, BasicGetOptions { no_ack: false })
                .await?
            else {
                break;
            };
            if let Ok(message) = parse_dead_letter(&delivery.data) {
                records.push(dead_letter_record(message));
            }
            last_unacknowledged = Some(delivery.delivery_tag);
        }
        requeue_unacknowledged(&self.channel, last_unacknowledged).await?;
        Ok(records)
    }

    pub(crate) async fn replay(
        &self,
        id: Option<Uuid>,
    ) -> Result<DeadLetterReplaySummary, DistillError> {
        let limit = self.count().await?;
        let mut replayed = 0;
        let mut retained = 0;
        let mut invalid = 0;
        let mut last_unacknowledged = None;
        for _ in 0..limit {
            let Some(delivery) = self
                .channel
                .basic_get(DEAD_LETTER_QUEUE, BasicGetOptions { no_ack: false })
                .await?
            else {
                break;
            };
            let Ok(message) = parse_dead_letter(&delivery.data) else {
                invalid += 1;
                last_unacknowledged = Some(delivery.delivery_tag);
                continue;
            };
            if id.is_some_and(|selected| selected != message.metadata.id) {
                last_unacknowledged = Some(delivery.delivery_tag);
                continue;
            }
            let Ok(payload) = STANDARD.decode(&message.original_data_base64) else {
                invalid += 1;
                last_unacknowledged = Some(delivery.delivery_tag);
                if id.is_some() {
                    break;
                }
                continue;
            };
            if !replay_eligible(&message, &payload) {
                retained += 1;
                last_unacknowledged = Some(delivery.delivery_tag);
                if id.is_some() {
                    break;
                }
                continue;
            }
            if let Err(error) = publish_confirmed(
                &self.channel,
                &message.metadata.original_exchange,
                &message.metadata.original_routing_key,
                &payload,
            )
            .await
            {
                last_unacknowledged = Some(delivery.delivery_tag);
                let _ = requeue_unacknowledged(&self.channel, last_unacknowledged).await;
                return Err(DistillError::Persistence(format!(
                    "DLQ replay stopped after {replayed} confirmed messages ({})",
                    error.error_code()
                )));
            }
            delivery.ack(BasicAckOptions::default()).await?;
            replayed += 1;
            if id.is_some() {
                break;
            }
        }
        requeue_unacknowledged(&self.channel, last_unacknowledged).await?;
        Ok(DeadLetterReplaySummary {
            replayed,
            retained,
            invalid,
        })
    }
}

async fn requeue_unacknowledged(
    channel: &Channel,
    last_delivery_tag: Option<u64>,
) -> Result<(), DistillError> {
    if let Some(delivery_tag) = last_delivery_tag {
        channel
            .basic_nack(
                delivery_tag,
                BasicNackOptions {
                    multiple: true,
                    requeue: true,
                },
            )
            .await?;
    }
    Ok(())
}

fn replay_eligible(message: &DeadLetterMessage, payload: &[u8]) -> bool {
    message.metadata.failure_category == "retries_exhausted"
        && message.metadata.original_exchange != AGGREGATE_INPUT_EXCHANGE
        && validate_delivery(payload, &message.metadata.original_routing_key).is_ok()
}

fn parse_dead_letter(data: &[u8]) -> Result<DeadLetterMessage, DistillError> {
    serde_json::from_slice(data).map_err(|error| DistillError::InvalidRequest(error.to_string()))
}

fn dead_letter_record(message: DeadLetterMessage) -> DeadLetterRecord {
    DeadLetterRecord {
        id: message.metadata.id,
        envelope_version: message.metadata.envelope_version,
        failure_category: message.metadata.failure_category,
        failure_reason: message.metadata.failure_reason,
        retry_count: message.metadata.retry_count,
        original_message_id: message.metadata.original_message_id,
        original_exchange: message.metadata.original_exchange,
        original_routing_key: message.metadata.original_routing_key,
        failed_at: message.metadata.failed_at,
    }
}

impl RabbitConsumer {
    pub async fn connect(uri: &str, metrics: Arc<Metrics>) -> Result<Self, DistillError> {
        let connection = Connection::connect(uri, ConnectionProperties::default()).await?;
        let channel = Arc::new(connection.create_channel().await?);
        declare_consumer_queue(&channel).await?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await?;
        let sinks = Arc::new(RwLock::new(HashMap::new()));
        let aggregators = Arc::new(RwLock::new(HashMap::new()));
        let generation_sinks = Arc::new(RwLock::new(HashMap::new()));
        let aggregate_handles = Arc::new(RwLock::new(HashMap::new()));
        let consumer = Self {
            uri: Arc::from(uri),
            connection: Arc::new(connection),
            channel,
            sinks: Arc::clone(&sinks),
            generation_sinks: Arc::clone(&generation_sinks),
            aggregators: Arc::clone(&aggregators),
            aggregate_handles,
            metrics: Arc::clone(&metrics),
        };
        let consumer_channel = Arc::clone(&consumer.channel);
        let consumer_sinks = Arc::clone(&sinks);
        let consumer_generation_sinks = Arc::clone(&generation_sinks);
        let consumer_aggregators = Arc::clone(&aggregators);
        let persistence_aggregators = Arc::clone(&consumer_aggregators);
        let persistence_metrics = Arc::clone(&metrics);
        let consumer_uri = Arc::clone(&consumer.uri);
        tokio::spawn(supervise_persistence(
            consumer_uri,
            consumer_channel,
            consumer_sinks,
            consumer_generation_sinks,
            persistence_aggregators,
            persistence_metrics,
        ));
        Ok(consumer)
    }

    pub async fn register_exchange(&self, exchange: &str) -> Result<(), DistillError> {
        declare_topology(&self.channel, exchange).await
    }

    pub(crate) async fn prepare_channel(
        &self,
        channel_id: String,
        generation: Uuid,
        sink: Arc<dyn Sink>,
        handle: ChannelHandle,
        aggregate_enabled: bool,
    ) -> Result<PreparedChannel, DistillError> {
        let channel = if aggregate_enabled {
            let channel = Arc::new(self.connection.create_channel().await?);
            declare_generation_queue(&channel, &channel_id, generation).await?;
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await?;
            Some(channel)
        } else {
            None
        };
        Ok(PreparedChannel {
            channel,
            channel_id,
            generation,
            sink,
            handle,
        })
    }

    pub(crate) async fn activate_channel(&self, prepared: PreparedChannel) -> ChannelHandle {
        let PreparedChannel {
            channel,
            channel_id,
            generation,
            sink,
            handle,
        } = prepared;
        self.sinks.write().await.insert(
            channel_id.clone(),
            SinkBinding {
                sink: Arc::clone(&sink),
            },
        );
        self.generation_sinks.write().await.insert(
            generation,
            GenerationSink {
                channel_id: channel_id.clone(),
                sink: Arc::clone(&sink),
            },
        );
        if let Some(channel) = channel {
            self.aggregators
                .write()
                .await
                .insert(channel_id.clone(), AggregateBinding { generation });
            self.aggregate_handles
                .write()
                .await
                .insert(generation, handle.clone());
            let metrics = Arc::clone(&self.metrics);
            let consumer_handle = handle.clone();
            tokio::spawn(supervise_generation(
                Arc::clone(&self.uri),
                channel,
                channel_id,
                generation,
                sink,
                consumer_handle,
                metrics,
            ));
        }
        handle
    }

    pub(crate) async fn flush_aggregators(&self) -> Result<(), DistillError> {
        let handles = self
            .aggregate_handles
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for handle in handles {
            handle.flush().await?;
        }
        Ok(())
    }
}

async fn supervise_persistence(
    uri: Arc<str>,
    initial_channel: Arc<Channel>,
    sinks: Arc<RwLock<HashMap<String, SinkBinding>>>,
    generation_sinks: Arc<RwLock<HashMap<Uuid, GenerationSink>>>,
    aggregators: Arc<RwLock<HashMap<String, AggregateBinding>>>,
    metrics: Arc<Metrics>,
) {
    let mut channel = Some(initial_channel);
    loop {
        let result = if let Some(initial) = channel.take() {
            consume_persistence(
                initial,
                Arc::clone(&sinks),
                Arc::clone(&generation_sinks),
                Arc::clone(&aggregators),
                Arc::clone(&metrics),
            )
            .await
        } else {
            consume_persistence_connection(
                &uri,
                Arc::clone(&sinks),
                Arc::clone(&generation_sinks),
                Arc::clone(&aggregators),
                Arc::clone(&metrics),
            )
            .await
        };
        let error_code = result
            .err()
            .map_or("CONSUMER_ENDED", |error| error.error_code().as_str());
        error!(
            event.name = "distill.rabbit.consumer.restarting",
            error.code = error_code,
            retry.seconds = CONSUMER_RESTART_DELAY.as_secs(),
            "RabbitMQ consumer stopped; retrying with a fresh connection"
        );
        sleep(CONSUMER_RESTART_DELAY).await;
    }
}

async fn consume_persistence_connection(
    uri: &str,
    sinks: Arc<RwLock<HashMap<String, SinkBinding>>>,
    generation_sinks: Arc<RwLock<HashMap<Uuid, GenerationSink>>>,
    aggregators: Arc<RwLock<HashMap<String, AggregateBinding>>>,
    metrics: Arc<Metrics>,
) -> Result<(), DistillError> {
    let connection = Connection::connect(uri, ConnectionProperties::default()).await?;
    let channel = Arc::new(connection.create_channel().await?);
    declare_consumer_queue(&channel).await?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await?;
    let result = consume_persistence(channel, sinks, generation_sinks, aggregators, metrics).await;
    drop(connection);
    result
}

async fn supervise_generation(
    uri: Arc<str>,
    initial_channel: Arc<Channel>,
    channel_id: String,
    generation: Uuid,
    sink: Arc<dyn Sink>,
    handle: ChannelHandle,
    metrics: Arc<Metrics>,
) {
    let mut channel = Some(initial_channel);
    loop {
        let result = if let Some(initial) = channel.take() {
            consume_generation(
                initial,
                channel_id.clone(),
                generation,
                Arc::clone(&sink),
                handle.clone(),
                Arc::clone(&metrics),
            )
            .await
        } else {
            consume_generation_connection(
                &uri,
                channel_id.clone(),
                generation,
                Arc::clone(&sink),
                handle.clone(),
                Arc::clone(&metrics),
            )
            .await
        };
        let error_code = result
            .err()
            .map_or("CONSUMER_ENDED", |error| error.error_code().as_str());
        error!(
            event.name = "distill.rabbit.aggregate_consumer.restarting",
            channel.id = %channel_id,
            generation.id = %generation,
            error.code = error_code,
            retry.seconds = CONSUMER_RESTART_DELAY.as_secs(),
            "RabbitMQ aggregate consumer stopped; retrying with a fresh connection"
        );
        sleep(CONSUMER_RESTART_DELAY).await;
    }
}

async fn consume_generation_connection(
    uri: &str,
    channel_id: String,
    generation: Uuid,
    sink: Arc<dyn Sink>,
    handle: ChannelHandle,
    metrics: Arc<Metrics>,
) -> Result<(), DistillError> {
    let connection = Connection::connect(uri, ConnectionProperties::default()).await?;
    let channel = Arc::new(connection.create_channel().await?);
    declare_generation_queue(&channel, &channel_id, generation).await?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await?;
    let result = consume_generation(channel, channel_id, generation, sink, handle, metrics).await;
    drop(connection);
    result
}

async fn declare_exchange(channel: &Channel, exchange: &str) -> Result<(), DistillError> {
    channel
        .exchange_declare(
            exchange,
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    Ok(())
}

async fn declare_generation_queue(
    channel: &Channel,
    channel_id: &str,
    generation: Uuid,
) -> Result<(), DistillError> {
    declare_exchange(channel, AGGREGATE_INPUT_EXCHANGE).await?;
    let queue = generation_queue(channel_id, generation);
    channel
        .queue_declare(
            &queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            queue_arguments(),
        )
        .await?;
    channel
        .queue_bind(
            &queue,
            AGGREGATE_INPUT_EXCHANGE,
            &generation.to_string(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    Ok(())
}

fn generation_queue(channel_id: &str, generation: Uuid) -> String {
    format!("distill.aggregate.{channel_id}.{}", generation.simple())
}

async fn declare_consumer_queue(channel: &Channel) -> Result<(), DistillError> {
    channel
        .exchange_declare(
            DEAD_LETTER_EXCHANGE,
            ExchangeKind::Fanout,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .exchange_declare(
            RETRY_EXCHANGE,
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_declare(
            DELIVERY_QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            queue_arguments(),
        )
        .await?;
    channel
        .queue_declare(
            DEAD_LETTER_QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            queue_arguments(),
        )
        .await?;
    channel
        .queue_bind(
            DEAD_LETTER_QUEUE,
            DEAD_LETTER_EXCHANGE,
            "#",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    Ok(())
}

async fn declare_topology(channel: &Channel, exchange: &str) -> Result<(), DistillError> {
    channel
        .exchange_declare(
            exchange,
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_bind(
            DELIVERY_QUEUE,
            exchange,
            "#",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    Ok(())
}

async fn consume_persistence(
    channel: Arc<Channel>,
    sinks: Arc<RwLock<HashMap<String, SinkBinding>>>,
    generation_sinks: Arc<RwLock<HashMap<Uuid, GenerationSink>>>,
    aggregators: Arc<RwLock<HashMap<String, AggregateBinding>>>,
    metrics: Arc<Metrics>,
) -> Result<(), DistillError> {
    channel.basic_qos(1, BasicQosOptions::default()).await?;
    let mut consumer = channel
        .basic_consume(
            DELIVERY_QUEUE,
            "distill-persistence",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        if let Err(error) = validate_delivery(&delivery.data, delivery.routing_key.as_str()) {
            warn!(
                event.name = "distill.rabbit.delivery.poison",
                "dead-lettering malformed RabbitMQ delivery"
            );
            if let Err(handoff_error) = dead_letter(
                &channel,
                &delivery.data,
                delivery.exchange.as_str(),
                delivery.routing_key.as_str(),
                "invalid_envelope",
                &error.to_string(),
            )
            .await
            {
                delivery
                    .nack(BasicNackOptions {
                        requeue: true,
                        ..Default::default()
                    })
                    .await?;
                return Err(DistillError::Persistence(format!(
                    "dead-letter handoff unavailable ({})",
                    handoff_error.error_code()
                )));
            }
            delivery.ack(BasicAckOptions::default()).await?;
            continue;
        }
        if delivery.routing_key.as_str().ends_with("aggregate-input") {
            let result =
                migrate_legacy_aggregate_input(&channel, &aggregators, &delivery.data).await;
            settle_delivery(&channel, delivery, result).await?;
            continue;
        }
        let result = process_delivery(
            &channel,
            &sinks,
            &generation_sinks,
            &delivery.data,
            delivery.routing_key.as_str(),
            &metrics,
        )
        .await;
        match result {
            Ok(()) => delivery.ack(BasicAckOptions::default()).await?,
            Err(error) => {
                warn!(
                    event.name = "distill.rabbit.delivery.failure",
                    error.code = %error.error_code(),
                    "RabbitMQ delivery failed"
                );
                if let Err(retry_error) = retry_delivery(
                    &channel,
                    &delivery.data,
                    delivery.exchange.as_str(),
                    delivery.routing_key.as_str(),
                )
                .await
                {
                    delivery
                        .nack(lapin::options::BasicNackOptions {
                            requeue: true,
                            ..Default::default()
                        })
                        .await?;
                    warn!(
                        event.name = "distill.rabbit.retry.failure",
                        error.code = %retry_error.error_code(),
                        "RabbitMQ retry publication failed"
                    );
                    return Err(DistillError::Persistence(
                        "retry handoff unavailable".to_string(),
                    ));
                }
                delivery.ack(BasicAckOptions::default()).await?;
            }
        }
    }
    Ok(())
}

async fn consume_generation(
    channel: Arc<Channel>,
    channel_id: String,
    generation: Uuid,
    sink: Arc<dyn Sink>,
    handle: ChannelHandle,
    metrics: Arc<Metrics>,
) -> Result<(), DistillError> {
    channel
        .basic_qos(CONSUMER_PREFETCH, BasicQosOptions::default())
        .await?;
    let mut consumer = channel
        .basic_consume(
            &generation_queue(&channel_id, generation),
            &format!("distill-aggregation-{generation}"),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    let mut pending = FuturesUnordered::new();
    loop {
        tokio::select! {
            completion = pending.next(), if !pending.is_empty() => {
                if let Some((delivery, result)) = completion {
                    settle_delivery(&channel, delivery, result).await?;
                }
            }
            delivery = consumer.next() => {
                let Some(delivery) = delivery else {
                    break;
                };
                let delivery = delivery?;
                let routed = match routed_event(&delivery.data) {
                    Ok(routed)
                        if routed.aggregate
                            && routed.generation == generation
                            && routed.event.channel_id == channel_id => routed,
                    Ok(_) => {
                        settle_delivery(
                            &channel,
                            delivery,
                            Err(DistillError::InvalidRequest(
                                "aggregate queue received a non-aggregate event".to_string(),
                            )),
                        )
                        .await?;
                        continue;
                    }
                    Err(error) => {
                        warn!(event.name = "distill.rabbit.delivery.poison", "dead-lettering malformed RabbitMQ delivery");
                        if let Err(handoff_error) = dead_letter(
                            &channel,
                            &delivery.data,
                            delivery.exchange.as_str(),
                            delivery.routing_key.as_str(),
                            "invalid_aggregate_input",
                            &error.to_string(),
                        )
                        .await
                        {
                            delivery
                                .nack(BasicNackOptions {
                                    requeue: true,
                                    ..Default::default()
                                })
                                .await?;
                            return Err(DistillError::Persistence(format!(
                                "dead-letter handoff unavailable ({})",
                                handoff_error.error_code()
                            )));
                        }
                        delivery.ack(BasicAckOptions::default()).await?;
                        continue;
                    }
                };
                let result = prepare_generation_input(&sink, &handle, &metrics, routed).await;
                match result {
                    Ok(receiver) => pending.push(async move {
                        let result = receiver.await.unwrap_or_else(|_| {
                            Err(DistillError::Persistence(
                                "aggregate actor dropped response".to_string(),
                            ))
                        });
                        (delivery, result)
                    }),
                    Err(error) => settle_delivery(&channel, delivery, Err(error)).await?,
                }
            }
        }
    }
    while let Some(completion) = pending.next().await {
        let (delivery, result) = completion;
        settle_delivery(&channel, delivery, result).await?;
    }
    Ok(())
}

fn validate_delivery(data: &[u8], routing_key: &str) -> Result<(), DistillError> {
    let envelope = parse_envelope(data)?;
    if routing_key.ends_with(ROUTED_EVENT_SUFFIX) {
        serde_json::from_value::<RoutedEvent>(envelope.payload)
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
    } else if routing_key.ends_with(ROUTED_AGGREGATE_SUFFIX) {
        serde_json::from_value::<RoutedAggregate>(envelope.payload)
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
    } else if routing_key.ends_with("aggregate") {
        serde_json::from_value::<AggregatedLog>(envelope.payload)
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
    } else {
        serde_json::from_value::<LogEvent>(envelope.payload)
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
    }
    Ok(())
}

async fn process_delivery(
    channel: &Channel,
    sinks: &RwLock<HashMap<String, SinkBinding>>,
    generation_sinks: &RwLock<HashMap<Uuid, GenerationSink>>,
    data: &[u8],
    routing_key: &str,
    metrics: &Metrics,
) -> Result<(), DistillError> {
    let envelope = parse_envelope(data)?;
    if routing_key.ends_with(ROUTED_EVENT_SUFFIX) {
        let routed: RoutedEvent = serde_json::from_value(envelope.payload)
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
        if routed.aggregate || !routed.persist {
            return Err(DistillError::InvalidRequest(
                "persistence queue received invalid route targets".to_string(),
            ));
        }
        let sink = generation_sink(
            generation_sinks,
            &routed.event.channel_id,
            routed.generation,
        )
        .await?;
        sink.persist_event(&routed.event).await?;
        metrics.persisted();
    } else if routing_key.ends_with(ROUTED_AGGREGATE_SUFFIX) {
        let routed: RoutedAggregate = serde_json::from_value(envelope.payload)
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
        let sink = generation_sink(
            generation_sinks,
            &routed.aggregate.channel_id,
            routed.generation,
        )
        .await?;
        sink.persist_aggregate(&routed.aggregate).await?;
        metrics.aggregate_persisted();
    } else if routing_key.ends_with("aggregate") {
        let aggregate: AggregatedLog = serde_json::from_value(envelope.payload)
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
        let sink = sinks
            .read()
            .await
            .get(&aggregate.channel_id)
            .map(|binding| Arc::clone(&binding.sink))
            .ok_or_else(|| DistillError::ChannelNotFound(aggregate.channel_id.clone()))?;
        sink.persist_aggregate(&aggregate).await?;
        metrics.aggregate_persisted();
    } else {
        let event: LogEvent = serde_json::from_value(envelope.payload)
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
        let sink = sinks
            .read()
            .await
            .get(&event.channel_id)
            .map(|binding| Arc::clone(&binding.sink))
            .ok_or_else(|| DistillError::ChannelNotFound(event.channel_id.clone()))?;
        sink.persist_event(&event).await?;
        metrics.persisted();
    }
    let _ = channel;
    Ok(())
}

async fn generation_sink(
    sinks: &RwLock<HashMap<Uuid, GenerationSink>>,
    channel_id: &str,
    generation: Uuid,
) -> Result<Arc<dyn Sink>, DistillError> {
    let binding = sinks
        .read()
        .await
        .get(&generation)
        .cloned()
        .ok_or_else(|| {
            DistillError::InvalidConfiguration(format!(
                "channel '{channel_id}' delivery generation is unavailable"
            ))
        })?;
    if binding.channel_id != channel_id {
        return Err(DistillError::InvalidConfiguration(
            "delivery generation does not match its channel".to_string(),
        ));
    }
    Ok(binding.sink)
}

fn routed_event(data: &[u8]) -> Result<RoutedEvent, DistillError> {
    let envelope = parse_envelope(data)?;
    serde_json::from_value(envelope.payload)
        .map_err(|error| DistillError::InvalidRequest(error.to_string()))
}

async fn prepare_generation_input(
    sink: &Arc<dyn Sink>,
    handle: &ChannelHandle,
    metrics: &Metrics,
    routed: RoutedEvent,
) -> Result<tokio::sync::oneshot::Receiver<Result<(), DistillError>>, DistillError> {
    if routed.persist {
        sink.persist_event(&routed.event).await?;
        metrics.persisted();
    }
    handle.aggregate_input(routed.event).await
}

async fn migrate_legacy_aggregate_input(
    channel: &Channel,
    aggregators: &RwLock<HashMap<String, AggregateBinding>>,
    data: &[u8],
) -> Result<(), DistillError> {
    let envelope = parse_envelope(data)?;
    let event: LogEvent = serde_json::from_value(envelope.payload)
        .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
    let binding = aggregators
        .read()
        .await
        .get(&event.channel_id)
        .cloned()
        .ok_or_else(|| DistillError::ChannelNotFound(event.channel_id.clone()))?;
    let routed = DeliveryEnvelope {
        version: ENVELOPE_VERSION,
        payload: serde_json::to_value(RoutedEvent {
            event,
            generation: binding.generation,
            persist: false,
            aggregate: true,
        })
        .map_err(|error| DistillError::Persistence(error.to_string()))?,
        retry_count: envelope.retry_count,
    };
    let payload = serde_json::to_vec(&routed)
        .map_err(|error| DistillError::Persistence(error.to_string()))?;
    publish_confirmed(
        channel,
        AGGREGATE_INPUT_EXCHANGE,
        &binding.generation.to_string(),
        &payload,
    )
    .await
}

async fn settle_delivery(
    channel: &Channel,
    delivery: Delivery,
    result: Result<(), DistillError>,
) -> Result<(), DistillError> {
    match result {
        Ok(()) => delivery.ack(BasicAckOptions::default()).await?,
        Err(error) => {
            warn!(
                event.name = "distill.rabbit.delivery.failure",
                error.code = %error.error_code(),
                "RabbitMQ delivery failed"
            );
            if let Err(retry_error) = retry_delivery(
                channel,
                &delivery.data,
                delivery.exchange.as_str(),
                delivery.routing_key.as_str(),
            )
            .await
            {
                delivery
                    .nack(lapin::options::BasicNackOptions {
                        requeue: true,
                        ..Default::default()
                    })
                    .await?;
                warn!(
                    event.name = "distill.rabbit.retry.failure",
                    error.code = %retry_error.error_code(),
                    "RabbitMQ retry publication failed"
                );
                return Err(DistillError::Persistence(
                    "retry handoff unavailable".to_string(),
                ));
            }
            delivery.ack(BasicAckOptions::default()).await?;
        }
    }
    Ok(())
}

async fn dead_letter(
    channel: &Channel,
    data: &[u8],
    exchange: &str,
    routing_key: &str,
    failure_category: &'static str,
    failure_reason: &str,
) -> Result<(), DistillError> {
    let parsed = serde_json::from_slice::<DeliveryEnvelope>(data).ok();
    let message = DeadLetterMessage {
        metadata: DeadLetterMetadata {
            id: Uuid::new_v4(),
            envelope_version: parsed.as_ref().map(|envelope| envelope.version),
            failure_category: failure_category.to_string(),
            failure_reason: sanitized_reason(failure_reason),
            retry_count: parsed.as_ref().map(|envelope| envelope.retry_count),
            original_message_id: parsed.as_ref().and_then(original_message_id),
            original_exchange: exchange.to_string(),
            original_routing_key: routing_key.to_string(),
            failed_at: Utc::now(),
        },
        original_data_base64: STANDARD.encode(data),
    };
    let payload = serde_json::to_vec(&message)
        .map_err(|error| DistillError::Persistence(error.to_string()))?;
    publish_confirmed(channel, DEAD_LETTER_EXCHANGE, routing_key, &payload).await
}

fn original_message_id(envelope: &DeliveryEnvelope) -> Option<Uuid> {
    let payload = &envelope.payload;
    [
        payload.get("event_id"),
        payload.get("aggregate_id"),
        payload.get("event").and_then(|event| event.get("event_id")),
        payload
            .get("aggregate")
            .and_then(|aggregate| aggregate.get("aggregate_id")),
    ]
    .into_iter()
    .flatten()
    .find_map(|value| value.as_str().and_then(|value| value.parse().ok()))
}

fn sanitized_reason(reason: &str) -> String {
    let mut sanitized = String::with_capacity(reason.len().min(MAX_FAILURE_REASON_BYTES));
    for character in reason.chars().filter(|character| !character.is_control()) {
        if sanitized.len() + character.len_utf8() > MAX_FAILURE_REASON_BYTES {
            break;
        }
        sanitized.push(character);
    }
    sanitized
}

async fn retry_delivery(
    channel: &Channel,
    data: &[u8],
    exchange: &str,
    routing_key: &str,
) -> Result<(), DistillError> {
    let mut envelope = parse_envelope(data)?;
    if envelope.retry_count >= MAX_RETRIES {
        return dead_letter(
            channel,
            data,
            exchange,
            routing_key,
            "retries_exhausted",
            "delivery failed after maximum retry attempts",
        )
        .await;
    }
    envelope.retry_count += 1;
    declare_retry_queue(channel, exchange, routing_key, envelope.retry_count).await?;
    let payload = serde_json::to_vec(&envelope)
        .map_err(|error| DistillError::Persistence(error.to_string()))?;
    publish_confirmed(
        channel,
        RETRY_EXCHANGE,
        &retry_routing_key(exchange, routing_key, envelope.retry_count),
        &payload,
    )
    .await
}

async fn declare_retry_queue(
    channel: &Channel,
    exchange: &str,
    routing_key: &str,
    retry_count: u32,
) -> Result<(), DistillError> {
    let delay_ms = retry_delay_seconds(retry_count).saturating_mul(1_000);
    let arguments = retry_queue_arguments(delay_ms, exchange, routing_key);
    let queue = retry_queue(exchange, routing_key, retry_count);
    let retry_key = retry_routing_key(exchange, routing_key, retry_count);
    channel
        .queue_declare(
            &queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            arguments,
        )
        .await?;
    channel
        .queue_bind(
            &queue,
            RETRY_EXCHANGE,
            &retry_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    Ok(())
}

fn retry_queue_arguments(delay_ms: u32, exchange: &str, routing_key: &str) -> FieldTable {
    let mut arguments = queue_arguments();
    arguments.insert("x-message-ttl".into(), AMQPValue::LongUInt(delay_ms));
    arguments.insert(
        "x-queue-type".into(),
        AMQPValue::LongString("quorum".into()),
    );
    arguments.insert(
        "x-dead-letter-strategy".into(),
        AMQPValue::LongString("at-least-once".into()),
    );
    arguments.insert(
        "x-dead-letter-exchange".into(),
        AMQPValue::LongString(exchange.into()),
    );
    arguments.insert(
        "x-dead-letter-routing-key".into(),
        AMQPValue::LongString(routing_key.into()),
    );
    arguments
}

fn retry_delay_seconds(retry_count: u32) -> u32 {
    // Retry delay tops out at 63 seconds so failed destinations back off without starving others.
    2_u32.pow(retry_count.min(6)) - 1
}

fn retry_routing_key(exchange: &str, routing_key: &str, retry_count: u32) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("{exchange}|{routing_key}|{retry_count}").as_bytes(),
    )
    .simple()
    .to_string()
}

fn retry_queue(exchange: &str, routing_key: &str, retry_count: u32) -> String {
    format!(
        "distill.retry.{}",
        retry_routing_key(exchange, routing_key, retry_count)
    )
}

async fn publish_confirmed(
    channel: &Channel,
    exchange: &str,
    routing_key: &str,
    data: &[u8],
) -> Result<(), DistillError> {
    let confirmation = channel
        .basic_publish(
            exchange,
            routing_key,
            BasicPublishOptions {
                mandatory: true,
                ..Default::default()
            },
            data,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2),
        )
        .await?
        .await?;
    validate_confirmation(&confirmation)
}

fn validate_confirmation(confirmation: &Confirmation) -> Result<(), DistillError> {
    match confirmation {
        Confirmation::Ack(None) => Ok(()),
        Confirmation::Ack(Some(_)) => Err(DistillError::Persistence(
            "RabbitMQ returned an unroutable publication".to_string(),
        )),
        Confirmation::Nack(_) => Err(DistillError::Persistence(
            "RabbitMQ rejected a publication".to_string(),
        )),
        Confirmation::NotRequested => Err(DistillError::Persistence(
            "RabbitMQ publisher confirms are not enabled".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AGGREGATE_INPUT_EXCHANGE, AMQPValue, DeadLetterMessage, DeadLetterMetadata,
        ENVELOPE_VERSION, LEGACY_ENVELOPE_VERSION, MAX_FAILURE_REASON_BYTES, MAX_QUEUE_BYTES,
        MAX_QUEUE_MESSAGES, generation_queue, parse_envelope, queue_arguments, replay_eligible,
        retry_delay_seconds, retry_queue, retry_queue_arguments, sanitized_reason,
        validate_confirmation, validate_delivery,
    };
    use chrono::Utc;
    use lapin::publisher_confirm::Confirmation;
    use uuid::Uuid;

    #[test]
    fn publication_requires_an_explicit_broker_acknowledgement() {
        assert!(validate_confirmation(&Confirmation::Ack(None)).is_ok());
        assert!(validate_confirmation(&Confirmation::Nack(None)).is_err());
        assert!(validate_confirmation(&Confirmation::NotRequested).is_err());
    }

    #[test]
    fn malformed_deliveries_are_rejected_before_retry() {
        assert!(validate_delivery(b"not-json", "orders.event").is_err());
        assert!(validate_delivery(br#"{"payload":{},"retry_count":0}"#, "orders.event").is_err());
    }

    #[test]
    fn broker_envelopes_reject_unsupported_versions() {
        assert!(parse_envelope(br#"{"version":1,"payload":{},"retry_count":0}"#).is_ok());
        assert!(parse_envelope(br#"{"version":2,"payload":{},"retry_count":0}"#).is_err());
        assert_eq!(ENVELOPE_VERSION, 1);
        assert_eq!(LEGACY_ENVELOPE_VERSION, 1);
    }

    #[test]
    fn retry_queues_are_broker_delayed_and_destination_specific() {
        assert_eq!(retry_delay_seconds(1), 1);
        assert_eq!(retry_delay_seconds(6), 63);
        assert_eq!(retry_delay_seconds(20), 63);
        assert_ne!(
            retry_queue("distill.orders", "orders.event", 1),
            retry_queue("distill.orders", "orders.event", 2)
        );
    }

    #[test]
    fn application_queues_have_broker_enforced_capacity() {
        let arguments = queue_arguments();
        assert_eq!(
            arguments.inner().get("x-max-length"),
            Some(&AMQPValue::LongUInt(MAX_QUEUE_MESSAGES))
        );
        assert_eq!(
            arguments.inner().get("x-max-length-bytes"),
            Some(&AMQPValue::LongUInt(MAX_QUEUE_BYTES))
        );
    }

    #[test]
    fn retry_queues_use_confirmed_quorum_dead_lettering() {
        let arguments = retry_queue_arguments(1_000, "distill.orders", "orders.event");
        assert_eq!(
            arguments.inner().get("x-queue-type"),
            Some(&AMQPValue::LongString("quorum".into()))
        );
        assert_eq!(
            arguments.inner().get("x-dead-letter-strategy"),
            Some(&AMQPValue::LongString("at-least-once".into()))
        );
        assert_eq!(
            arguments.inner().get("x-overflow"),
            Some(&AMQPValue::LongString("reject-publish".into()))
        );
    }

    #[test]
    fn failure_reasons_are_bounded_by_utf8_bytes() {
        let reason = "é".repeat(MAX_FAILURE_REASON_BYTES);
        let sanitized = sanitized_reason(&reason);
        assert!(sanitized.len() <= MAX_FAILURE_REASON_BYTES);
        assert!(sanitized.is_char_boundary(sanitized.len()));
    }

    #[test]
    fn replay_excludes_poison_and_aggregate_input() {
        let payload = br#"{"version":1,"payload":{"event_id":"00000000-0000-0000-0000-000000000001","timestamp":"2026-08-30T00:00:00Z","channel_id":"orders","severity_level":"info","payload":{},"metadata":{}},"retry_count":10}"#;
        let mut message = DeadLetterMessage {
            metadata: DeadLetterMetadata {
                id: Uuid::from_u128(2),
                envelope_version: Some(1),
                failure_category: "retries_exhausted".to_string(),
                failure_reason: "maximum retries reached".to_string(),
                retry_count: Some(10),
                original_message_id: Some(Uuid::from_u128(1)),
                original_exchange: "distill.orders".to_string(),
                original_routing_key: "orders.event".to_string(),
                failed_at: Utc::now(),
            },
            original_data_base64: String::new(),
        };
        assert!(replay_eligible(&message, payload));
        message.metadata.failure_category = "invalid_envelope".to_string();
        assert!(!replay_eligible(&message, payload));
        message.metadata.failure_category = "retries_exhausted".to_string();
        message.metadata.original_exchange = AGGREGATE_INPUT_EXCHANGE.to_string();
        assert!(!replay_eligible(&message, payload));
    }

    #[test]
    fn aggregate_queues_are_isolated_by_channel_generation() {
        let first = generation_queue("orders", Uuid::from_u128(1));
        let second = generation_queue("orders", Uuid::from_u128(2));
        assert_ne!(first, second);
        assert!(first.len() < 256);
    }
}
