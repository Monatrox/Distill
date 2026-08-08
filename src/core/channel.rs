//! Owns channel actors and routes events through their bounded state.

// Rust guideline compliant 2026-02-21
use crate::{
    broker::RabbitPublisher,
    core::{
        aggregation::AggregatorState,
        buffer::{EventBuffer, MemoryBudget},
        errors::DistillError,
        metrics::Metrics,
        routing::{RouteDecision, classify},
    },
    domain::{ChannelDefinition, LogEvent, OrderingMode},
};
use chrono::{DateTime, Duration, Utc};
use std::{sync::Arc, time::Duration as StdDuration};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

#[derive(Clone)]
pub struct ChannelHandle {
    sender: mpsc::Sender<Command>,
}

enum Command {
    Event {
        event: LogEvent,
        response: oneshot::Sender<Result<(), DistillError>>,
    },
    Flush {
        response: oneshot::Sender<Result<(), DistillError>>,
    },
}

pub fn spawn(
    definition: ChannelDefinition,
    rabbit: RabbitPublisher,
    metrics: Arc<Metrics>,
    budget: Arc<MemoryBudget>,
) -> ChannelHandle {
    let (sender, receiver) = mpsc::channel(definition.mailbox_capacity);
    let handle = ChannelHandle { sender };
    tokio::spawn(run_actor(definition, receiver, rabbit, metrics, budget));
    handle
}

impl ChannelHandle {
    pub async fn send(&self, event: LogEvent) -> Result<(), DistillError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .try_send(Command::Event { event, response })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    DistillError::BufferFull("channel mailbox".to_string())
                }
                mpsc::error::TrySendError::Closed(_) => {
                    DistillError::Persistence("channel actor is stopped".to_string())
                }
            })?;
        receiver
            .await
            .map_err(|_| DistillError::Persistence("channel actor dropped response".to_string()))?
    }

    pub async fn flush(&self) -> Result<(), DistillError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(Command::Flush { response })
            .await
            .map_err(|_| DistillError::Persistence("channel actor is stopped".to_string()))?;
        receiver
            .await
            .map_err(|_| DistillError::Persistence("channel actor dropped response".to_string()))?
    }
}

async fn run_actor(
    definition: ChannelDefinition,
    mut receiver: mpsc::Receiver<Command>,
    rabbit: RabbitPublisher,
    metrics: Arc<Metrics>,
    budget: Arc<MemoryBudget>,
) {
    let mut buffer = EventBuffer::with_budget(definition.buffer_bytes, budget);
    let mut aggregator = definition
        .aggregator
        .clone()
        .map(|spec| AggregatorState::new(definition.channel_id.clone(), spec));
    let mut first_timestamp = None;
    let mut ticker = tokio::time::interval(StdDuration::from_secs(1));

    loop {
        tokio::select! {
            command = receiver.recv() => match command {
                Some(Command::Event { event, response }) => {
                    let result = handle_event(
                        &definition,
                        event,
                        &mut buffer,
                        &mut aggregator,
                        &mut first_timestamp,
                        &rabbit,
                        &metrics,
                    ).await;
                    let _ = response.send(result);
                }
                Some(Command::Flush { response }) => {
                    let result = flush_aggregate(
                        &definition,
                        &mut buffer,
                        &mut aggregator,
                        &mut first_timestamp,
                        &rabbit,
                        &metrics,
                    ).await;
                    let _ = response.send(result);
                }
                None => break,
            },
            _ = ticker.tick(), if aggregator.is_some() => {
                let now = Utc::now();
                let should_flush = aggregator.as_ref().is_some_and(|state| {
                    state.should_flush(first_timestamp, now)
                        || first_timestamp.is_some_and(|start| {
                            now.signed_duration_since(start)
                                >= Duration::seconds(i64::try_from(definition.ttl_seconds).unwrap_or(i64::MAX))
                        })
                });
                if should_flush
                    && let Err(error) = flush_aggregate(
                        &definition,
                        &mut buffer,
                        &mut aggregator,
                        &mut first_timestamp,
                        &rabbit,
                        &metrics,
                    ).await
                {
                    warn!(event.name = "distill.aggregate.flush.failure", error.message = %error, "aggregate flush failed");
                }
            }
        }
    }

    info!(event.name = "distill.channel.stopped", channel.id = %definition.channel_id, "channel actor stopped");
}

async fn handle_event(
    definition: &ChannelDefinition,
    event: LogEvent,
    buffer: &mut EventBuffer,
    aggregator: &mut Option<AggregatorState>,
    first_timestamp: &mut Option<DateTime<Utc>>,
    rabbit: &RabbitPublisher,
    metrics: &Metrics,
) -> Result<(), DistillError> {
    event.validate().map_err(DistillError::InvalidRequest)?;
    metrics.received();
    let decision = classify(&event, definition);
    if matches!(
        decision,
        RouteDecision::DirectPersist | RouteDecision::PersistAndAggregate
    ) {
        rabbit
            .publish(
                &definition.rabbitmq_config.exchange_name,
                &format!("{}event", definition.rabbitmq_config.routing_key_prefix),
                &event,
            )
            .await?;
        metrics.persisted();
    }
    if matches!(
        decision,
        RouteDecision::Aggregate | RouteDecision::PersistAndAggregate
    ) && aggregator.is_some()
    {
        let is_late = matches!(definition.ordering, OrderingMode::FifoRequired)
            && first_timestamp.is_some_and(|first| {
                event.timestamp < first
                    && first
                        .signed_duration_since(event.timestamp)
                        .num_milliseconds()
                        > i64::try_from(definition.allowed_lateness_ms).unwrap_or(i64::MAX)
            });
        if buffer.push(event.clone()).is_err() {
            flush_aggregate(
                definition,
                buffer,
                aggregator,
                first_timestamp,
                rabbit,
                metrics,
            )
            .await?;
            buffer
                .push(event.clone())
                .map_err(|_| DistillError::BufferFull(definition.channel_id.clone()))?;
        }
        if first_timestamp.is_none() {
            *first_timestamp = Some(event.timestamp);
        }
        if is_late && let Some(state) = aggregator.as_mut() {
            state.mark_late_event();
        }
        if let Some(state) = aggregator.as_mut()
            && let Err(error) = state.update(event)
        {
            let _ = buffer.pop_back();
            return Err(DistillError::SchemaValidation(error));
        }
        let should_flush = aggregator
            .as_ref()
            .is_some_and(|state| state.should_flush(*first_timestamp, Utc::now()));
        if should_flush {
            flush_aggregate(
                definition,
                buffer,
                aggregator,
                first_timestamp,
                rabbit,
                metrics,
            )
            .await?;
        }
    }
    Ok(())
}

async fn flush_aggregate(
    definition: &ChannelDefinition,
    buffer: &mut EventBuffer,
    aggregator: &mut Option<AggregatorState>,
    first_timestamp: &mut Option<DateTime<Utc>>,
    rabbit: &RabbitPublisher,
    metrics: &Metrics,
) -> Result<(), DistillError> {
    if buffer.is_empty() {
        return Ok(());
    }
    let Some(state) = aggregator.as_mut() else {
        return Ok(());
    };
    let start = first_timestamp.unwrap_or_else(Utc::now);
    let aggregate = state.snapshot(start, Utc::now());
    rabbit
        .publish(
            &definition.rabbitmq_config.exchange_name,
            &format!("{}aggregate", definition.rabbitmq_config.routing_key_prefix),
            &aggregate,
        )
        .await?;
    state.flush(start, aggregate.window_end);
    buffer.drain();
    *first_timestamp = None;
    metrics.aggregate_persisted();
    Ok(())
}
