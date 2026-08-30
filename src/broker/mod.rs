//! Provides `RabbitMQ` publishing, consumption, and retry handling.

// Rust guideline compliant 2026-02-21
use crate::{
    core::{channel::ChannelHandle, errors::DistillError, metrics::Metrics},
    domain::{AggregatedLog, LogEvent},
    persistence::Sink,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
        ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    publisher_confirm::Confirmation,
    types::FieldTable,
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
const MAX_RETRIES: u32 = 10;
const ROUTED_EVENT_SUFFIX: &str = "routed-event-v1";
const ROUTED_AGGREGATE_SUFFIX: &str = "routed-aggregate-v1";
/// Bounds unacknowledged aggregate inputs while allowing per-event windows to fill.
const CONSUMER_PREFETCH: u16 = 256;

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
    payload: serde_json::Value,
    retry_count: u32,
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

pub struct RabbitConsumer {
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
        tokio::spawn(async move {
            if let Err(error) = consume_persistence(
                consumer_channel,
                consumer_sinks,
                consumer_generation_sinks,
                persistence_aggregators,
                persistence_metrics,
            )
            .await
            {
                error!(event.name = "distill.rabbit.consumer.stopped", error.message = %error, "RabbitMQ consumer stopped");
            }
        });
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
            tokio::spawn(async move {
                if let Err(error) = consume_generation(
                    channel,
                    channel_id,
                    generation,
                    sink,
                    consumer_handle,
                    metrics,
                )
                .await
                {
                    error!(event.name = "distill.rabbit.aggregate_consumer.stopped", error.message = %error, "RabbitMQ aggregate consumer stopped");
                }
            });
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
            FieldTable::default(),
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
        .queue_declare(
            DELIVERY_QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_declare(
            "distill.dead-letter.queue",
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_bind(
            "distill.dead-letter.queue",
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
            warn!(event.name = "distill.rabbit.delivery.poison", error.message = %error, "Dead-lettering malformed RabbitMQ delivery");
            dead_letter(&channel, &delivery.data, delivery.routing_key.as_str()).await?;
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
                warn!(event.name = "distill.rabbit.delivery.failure", error.message = %error, "RabbitMQ delivery failed");
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
                    warn!(event.name = "distill.rabbit.retry.failure", error.message = %retry_error, "RabbitMQ retry publication failed");
                } else {
                    delivery.ack(BasicAckOptions::default()).await?;
                }
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
                        warn!(event.name = "distill.rabbit.delivery.poison", error.message = %error, "Dead-lettering malformed RabbitMQ delivery");
                        dead_letter(&channel, &delivery.data, delivery.routing_key.as_str()).await?;
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
    let envelope: DeliveryEnvelope = serde_json::from_slice(data)
        .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
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
    let envelope: DeliveryEnvelope = serde_json::from_slice(data)
        .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
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
    let envelope: DeliveryEnvelope = serde_json::from_slice(data)
        .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
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
    let envelope: DeliveryEnvelope = serde_json::from_slice(data)
        .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
    let event: LogEvent = serde_json::from_value(envelope.payload)
        .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
    let binding = aggregators
        .read()
        .await
        .get(&event.channel_id)
        .cloned()
        .ok_or_else(|| DistillError::ChannelNotFound(event.channel_id.clone()))?;
    let routed = DeliveryEnvelope {
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
            warn!(event.name = "distill.rabbit.delivery.failure", error.message = %error, "RabbitMQ delivery failed");
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
                warn!(event.name = "distill.rabbit.retry.failure", error.message = %retry_error, "RabbitMQ retry publication failed");
            } else {
                delivery.ack(BasicAckOptions::default()).await?;
            }
        }
    }
    Ok(())
}

async fn dead_letter(
    channel: &Channel,
    data: &[u8],
    routing_key: &str,
) -> Result<(), DistillError> {
    publish_confirmed(channel, DEAD_LETTER_EXCHANGE, routing_key, data).await
}

async fn retry_delivery(
    channel: &Channel,
    data: &[u8],
    exchange: &str,
    routing_key: &str,
) -> Result<(), DistillError> {
    let mut envelope: DeliveryEnvelope = serde_json::from_slice(data)
        .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
    if envelope.retry_count >= MAX_RETRIES {
        return dead_letter(channel, data, routing_key).await;
    }
    envelope.retry_count += 1;
    let delay = 2_u64.pow(envelope.retry_count.min(6)) - 1;
    sleep(Duration::from_secs(delay)).await;
    let payload = serde_json::to_vec(&envelope)
        .map_err(|error| DistillError::Persistence(error.to_string()))?;
    publish_confirmed(channel, exchange, routing_key, &payload).await
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
    use super::{generation_queue, validate_confirmation, validate_delivery};
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
    fn aggregate_queues_are_isolated_by_channel_generation() {
        let first = generation_queue("orders", Uuid::from_u128(1));
        let second = generation_queue("orders", Uuid::from_u128(2));
        assert_ne!(first, second);
        assert!(first.len() < 256);
    }
}
