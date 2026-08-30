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
use chrono::{DateTime, Utc};
use std::{sync::Arc, time::Duration as StdDuration};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct ChannelHandle {
    sender: mpsc::Sender<Command>,
    aggregate_sender: mpsc::Sender<AggregateCommand>,
    generation: Uuid,
}

enum Command {
    Event {
        event: LogEvent,
        response: oneshot::Sender<Result<(), DistillError>>,
    },
    Stop {
        response: oneshot::Sender<()>,
    },
}

enum AggregateCommand {
    Event {
        event: LogEvent,
        response: oneshot::Sender<Result<(), DistillError>>,
    },
    Flush {
        response: oneshot::Sender<Result<(), DistillError>>,
    },
}

pub(crate) fn spawn(
    definition: ChannelDefinition,
    generation: Option<Uuid>,
    rabbit: RabbitPublisher,
    metrics: Arc<Metrics>,
    budget: Arc<MemoryBudget>,
) -> ChannelHandle {
    let generation = generation.unwrap_or_else(|| generation_for(&definition));
    let (sender, receiver) = mpsc::channel(definition.mailbox_capacity);
    let (aggregate_sender, aggregate_receiver) = mpsc::channel(definition.mailbox_capacity);
    let handle = ChannelHandle {
        sender,
        aggregate_sender,
        generation,
    };
    tokio::spawn(run_actor(
        definition.clone(),
        generation,
        receiver,
        rabbit.clone(),
        Arc::clone(&metrics),
    ));
    tokio::spawn(run_aggregator(
        definition,
        generation,
        aggregate_receiver,
        rabbit,
        metrics,
        budget,
    ));
    handle
}

impl ChannelHandle {
    pub(crate) fn generation(&self) -> Uuid {
        self.generation
    }

    pub(crate) async fn send(&self, event: LogEvent) -> Result<(), DistillError> {
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

    pub(crate) async fn aggregate_input(
        &self,
        event: LogEvent,
    ) -> Result<oneshot::Receiver<Result<(), DistillError>>, DistillError> {
        let (response, receiver) = oneshot::channel();
        self.aggregate_sender
            .send(AggregateCommand::Event { event, response })
            .await
            .map_err(|_| DistillError::Persistence("aggregate actor is stopped".to_string()))?;
        Ok(receiver)
    }

    pub(crate) async fn flush(&self) -> Result<(), DistillError> {
        let (response, receiver) = oneshot::channel();
        self.aggregate_sender
            .send(AggregateCommand::Flush { response })
            .await
            .map_err(|_| DistillError::Persistence("aggregate actor is stopped".to_string()))?;
        receiver.await.map_err(|_| {
            DistillError::Persistence("aggregate actor dropped response".to_string())
        })?
    }

    pub(crate) async fn stop_event_actor(&self) -> Result<(), DistillError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(Command::Stop { response })
            .await
            .map_err(|_| DistillError::Persistence("channel actor is stopped".to_string()))?;
        receiver.await.map_err(|_| {
            DistillError::Persistence("channel actor dropped stop response".to_string())
        })
    }
}

async fn run_actor(
    definition: ChannelDefinition,
    generation: Uuid,
    mut receiver: mpsc::Receiver<Command>,
    rabbit: RabbitPublisher,
    metrics: Arc<Metrics>,
) {
    while let Some(command) = receiver.recv().await {
        match command {
            Command::Event { event, response } => {
                let result = handle_event(&definition, generation, event, &rabbit, &metrics).await;
                let _ = response.send(result);
            }
            Command::Stop { response } => {
                let _ = response.send(());
                break;
            }
        }
    }
    info!(event.name = "distill.channel.stopped", channel.id = %definition.channel_id, "channel actor stopped");
}

#[expect(
    clippy::too_many_lines,
    reason = "the actor loop keeps aggregate state and command ordering in one owner"
)]
async fn run_aggregator(
    definition: ChannelDefinition,
    generation: Uuid,
    mut receiver: mpsc::Receiver<AggregateCommand>,
    rabbit: RabbitPublisher,
    metrics: Arc<Metrics>,
    budget: Arc<MemoryBudget>,
) {
    let mut buffer = EventBuffer::with_budget(definition.buffer_bytes, budget);
    let mut aggregator = definition
        .aggregator
        .clone()
        .map(|spec| AggregatorState::new(definition.channel_id.clone(), spec));
    let mut acknowledgements = Vec::new();
    let mut first_timestamp = None;
    let mut buffered_since = None;
    let mut ticker = tokio::time::interval(StdDuration::from_secs(1));

    loop {
        tokio::select! {
            command = receiver.recv() => match command {
                Some(AggregateCommand::Event { event, response }) => {
                    let result = if let Some(state) = aggregator.as_mut() {
                        handle_aggregate_event(
                            &definition,
                            generation,
                            event,
                            &mut buffer,
                            state,
                            &mut first_timestamp,
                            &mut buffered_since,
                            &mut acknowledgements,
                            &rabbit,
                            &metrics,
                        ).await
                    } else {
                        Err(DistillError::Persistence("aggregate input sent to a channel without an aggregator".to_string()))
                    };
                    match result {
                        Ok(should_flush) => {
                            acknowledgements.push(response);
                            if should_flush
                                && let Err(error) = flush_aggregate(
                                    &definition,
                                    generation,
                                    &mut buffer,
                                    aggregator.as_mut().expect("aggregate state exists"),
                                    &mut first_timestamp,
                                    &mut buffered_since,
                                    &mut acknowledgements,
                                    &rabbit,
                                    &metrics,
                                ).await
                            {
                                warn!(event.name = "distill.aggregate.flush.failure", error.code = %error.error_code(), "aggregate flush failed");
                            }
                        }
                        Err(error) => {
                            let _ = response.send(Err(error));
                        }
                    }
                }
                Some(AggregateCommand::Flush { response }) => {
                    let result = match aggregator.as_mut() {
                        Some(state) => flush_aggregate(
                            &definition,
                            generation,
                            &mut buffer,
                            state,
                            &mut first_timestamp,
                            &mut buffered_since,
                            &mut acknowledgements,
                            &rabbit,
                            &metrics,
                        ).await,
                        None => Ok(()),
                    };
                    let _ = response.send(result);
                }
                None => break,
            },
            _ = ticker.tick(), if aggregator.is_some() => {
                let now = Utc::now();
                let should_flush = aggregator.as_ref().is_some_and(|state| {
                    state.should_flush(first_timestamp, now)
                            || buffered_since.is_some_and(|start: tokio::time::Instant| {
                                start.elapsed() >= StdDuration::from_secs(definition.ttl_seconds)
                            })
                });
                if should_flush
                    && let Err(error) = flush_aggregate(
                        &definition,
                        generation,
                        &mut buffer,
                        aggregator.as_mut().expect("aggregate state exists"),
                        &mut first_timestamp,
                        &mut buffered_since,
                        &mut acknowledgements,
                        &rabbit,
                        &metrics,
                    ).await
                {
                    warn!(event.name = "distill.aggregate.flush.failure", error.code = %error.error_code(), "aggregate flush failed");
                }
            }
        }
    }

    info!(event.name = "distill.aggregate.stopped", channel.id = %definition.channel_id, "aggregate actor stopped");
}

async fn handle_event(
    definition: &ChannelDefinition,
    generation: Uuid,
    event: LogEvent,
    rabbit: &RabbitPublisher,
    metrics: &Metrics,
) -> Result<(), DistillError> {
    event.validate().map_err(DistillError::InvalidRequest)?;
    metrics.received();
    let decision = classify(&event, definition);
    let persist = matches!(
        decision,
        RouteDecision::DirectPersist | RouteDecision::PersistAndAggregate
    );
    let aggregate = matches!(
        decision,
        RouteDecision::Aggregate | RouteDecision::PersistAndAggregate
    );
    rabbit
        .publish_routed_event(
            &definition.rabbitmq_config.exchange_name,
            &definition.rabbitmq_config.routing_key_prefix,
            event,
            generation,
            persist,
            aggregate,
        )
        .await?;
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "aggregation state is intentionally owner-local to preserve acknowledgement ordering"
)]
async fn handle_aggregate_event(
    definition: &ChannelDefinition,
    generation: Uuid,
    event: LogEvent,
    buffer: &mut EventBuffer,
    aggregator: &mut AggregatorState,
    first_timestamp: &mut Option<DateTime<Utc>>,
    buffered_since: &mut Option<tokio::time::Instant>,
    acknowledgements: &mut Vec<oneshot::Sender<Result<(), DistillError>>>,
    rabbit: &RabbitPublisher,
    metrics: &Metrics,
) -> Result<bool, DistillError> {
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
            generation,
            buffer,
            aggregator,
            first_timestamp,
            buffered_since,
            acknowledgements,
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
        *buffered_since = Some(tokio::time::Instant::now());
    }
    if let Err(error) = aggregator.update(event) {
        let _ = buffer.pop_back();
        if buffer.is_empty() {
            *first_timestamp = None;
            *buffered_since = None;
        }
        return Err(DistillError::SchemaValidation(error));
    }
    if is_late {
        aggregator.mark_late_event();
    }
    Ok(aggregator.should_flush(*first_timestamp, Utc::now()))
}

#[expect(
    clippy::too_many_arguments,
    reason = "flush owns coordinated buffer, aggregate, acknowledgement, and broker state"
)]
async fn flush_aggregate(
    definition: &ChannelDefinition,
    generation: Uuid,
    buffer: &mut EventBuffer,
    aggregator: &mut AggregatorState,
    first_timestamp: &mut Option<DateTime<Utc>>,
    buffered_since: &mut Option<tokio::time::Instant>,
    acknowledgements: &mut Vec<oneshot::Sender<Result<(), DistillError>>>,
    rabbit: &RabbitPublisher,
    metrics: &Metrics,
) -> Result<(), DistillError> {
    if buffer.is_empty() {
        return Ok(());
    }
    let start = first_timestamp.unwrap_or_else(Utc::now);
    let aggregate = aggregator.snapshot(start, Utc::now());
    rabbit
        .publish_routed_aggregate(
            &definition.rabbitmq_config.exchange_name,
            &definition.rabbitmq_config.routing_key_prefix,
            aggregate.clone(),
            generation,
        )
        .await?;
    aggregator.flush(start, aggregate.window_end);
    buffer.drain();
    *first_timestamp = None;
    *buffered_since = None;
    metrics.aggregate_persisted();
    for acknowledgement in acknowledgements.drain(..) {
        let _ = acknowledgement.send(Ok(()));
    }
    Ok(())
}

fn generation_for(definition: &ChannelDefinition) -> Uuid {
    let identity = format!(
        "{}|{}|{}|{:?}|{}|{}|{}|{}|{:?}",
        definition.channel_id,
        definition.rabbitmq_config.exchange_name,
        definition.rabbitmq_config.routing_key_prefix,
        definition.aggregator,
        definition.sink_id.as_deref().unwrap_or_default(),
        definition.sink_config.driver,
        definition.buffer_bytes,
        definition.ttl_seconds,
        (
            definition.mailbox_capacity,
            definition.ordering,
            definition.allowed_lateness_ms
        ),
    );
    Uuid::new_v5(&Uuid::NAMESPACE_OID, identity.as_bytes())
}
