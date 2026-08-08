//! Provides `RabbitMQ` publishing, consumption, and retry handling.

// Rust guideline compliant 2026-02-21
use crate::{
    core::{errors::DistillError, metrics::Metrics},
    domain::{AggregatedLog, LogEvent},
    persistence::Sink,
};
use futures_util::StreamExt;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
        ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::RwLock,
    time::{Duration, sleep},
};
use tracing::{error, warn};

const DELIVERY_QUEUE: &str = "distill.persistence";
const DEAD_LETTER_EXCHANGE: &str = "distill.dead-letter";
const MAX_RETRIES: u32 = 10;

#[derive(Clone)]
pub struct RabbitPublisher {
    _connection: Arc<Connection>,
    channel: Arc<Channel>,
}

impl RabbitPublisher {
    pub async fn connect(uri: &str) -> Result<Self, DistillError> {
        let connection = Connection::connect(uri, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;
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
        self.channel
            .basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2),
            )
            .await?
            .await?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct DeliveryEnvelope {
    payload: serde_json::Value,
    retry_count: u32,
}

pub struct RabbitConsumer {
    _connection: Arc<Connection>,
    channel: Arc<Channel>,
    sinks: Arc<RwLock<HashMap<String, Arc<dyn Sink>>>>,
}

impl RabbitConsumer {
    pub async fn connect(uri: &str, metrics: Arc<Metrics>) -> Result<Self, DistillError> {
        let connection = Connection::connect(uri, ConnectionProperties::default()).await?;
        let channel = Arc::new(connection.create_channel().await?);
        declare_consumer_queue(&channel).await?;
        let sinks = Arc::new(RwLock::new(HashMap::new()));
        let consumer = Self {
            _connection: Arc::new(connection),
            channel,
            sinks: Arc::clone(&sinks),
        };
        let consumer_channel = Arc::clone(&consumer.channel);
        let consumer_sinks = Arc::clone(&sinks);
        tokio::spawn(async move {
            if let Err(error) = consume(consumer_channel, consumer_sinks, metrics).await {
                error!(event.name = "distill.rabbit.consumer.stopped", error.message = %error, "RabbitMQ consumer stopped");
            }
        });
        Ok(consumer)
    }

    pub async fn register_exchange(&self, exchange: &str) -> Result<(), DistillError> {
        declare_topology(&self.channel, exchange).await
    }

    pub async fn register_sink(&self, channel_id: String, sink: Arc<dyn Sink>) {
        self.sinks.write().await.insert(channel_id, sink);
    }
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

async fn consume(
    channel: Arc<Channel>,
    sinks: Arc<RwLock<HashMap<String, Arc<dyn Sink>>>>,
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
        let result = process_delivery(
            &channel,
            &sinks,
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

async fn process_delivery(
    channel: &Channel,
    sinks: &RwLock<HashMap<String, Arc<dyn Sink>>>,
    data: &[u8],
    routing_key: &str,
    metrics: &Metrics,
) -> Result<(), DistillError> {
    let envelope: DeliveryEnvelope = serde_json::from_slice(data)
        .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
    if routing_key.ends_with("aggregate") {
        let aggregate: AggregatedLog = serde_json::from_value(envelope.payload)
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?;
        let sink = sinks
            .read()
            .await
            .get(&aggregate.channel_id)
            .cloned()
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
            .cloned()
            .ok_or_else(|| DistillError::ChannelNotFound(event.channel_id.clone()))?;
        sink.persist_event(&event).await?;
        metrics.persisted();
    }
    let _ = channel;
    Ok(())
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
        channel
            .basic_publish(
                DEAD_LETTER_EXCHANGE,
                routing_key,
                BasicPublishOptions::default(),
                data,
                BasicProperties::default().with_delivery_mode(2),
            )
            .await?
            .await?;
        return Ok(());
    }
    envelope.retry_count += 1;
    let delay = 2_u64.pow(envelope.retry_count.min(6)) - 1;
    sleep(Duration::from_secs(delay)).await;
    let payload = serde_json::to_vec(&envelope)
        .map_err(|error| DistillError::Persistence(error.to_string()))?;
    channel
        .basic_publish(
            exchange,
            routing_key,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default().with_delivery_mode(2),
        )
        .await?
        .await?;
    Ok(())
}
