//! Owns channel actors and routes events through their bounded state.

// Rust guideline compliant 2026-02-21
use crate::{
    broker::RabbitPublisher,
    core::{
        aggregation::{AggregatorState, count_as_i64, merge_aggregate_values},
        buffer::{EventBuffer, MemoryBudget},
        errors::DistillError,
        metrics::Metrics,
        routing::{RouteDecision, classify},
        state::StateStore,
    },
    domain::{AggregatedLog, ChannelDefinition, LogEvent, OrderingMode},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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

pub(crate) async fn spawn(
    definition: ChannelDefinition,
    generation: Option<Uuid>,
    rabbit: RabbitPublisher,
    metrics: Arc<Metrics>,
    budget: Arc<MemoryBudget>,
    store: StateStore,
) -> Result<ChannelHandle, DistillError> {
    let generation = generation.unwrap_or_else(|| generation_for(&definition));
    let checkpoint = store.checkpoint::<DurableWindow>(generation).await?;
    let recovered_bytes = checkpoint
        .as_ref()
        .map(|state| serde_json::to_vec(state).map(|bytes| bytes.len()))
        .transpose()
        .map_err(|_| {
            DistillError::Persistence("aggregate checkpoint cannot be restored".to_string())
        })?
        .unwrap_or_default();
    if !budget.reserve(recovered_bytes) {
        return Err(DistillError::BufferFull(
            "recovered aggregate state".to_string(),
        ));
    }
    let recovered = RecoveredBudget {
        budget: Arc::clone(&budget),
        bytes: recovered_bytes,
    };
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
        store,
        checkpoint,
        recovered,
    ));
    Ok(handle)
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
        }
    }
    info!(event.name = "distill.channel.stopped", channel.id = %definition.channel_id, "channel actor stopped");
}

#[derive(Clone, Deserialize, Serialize)]
struct DurableWindow {
    aggregator: AggregatorState,
    first_timestamp: Option<DateTime<Utc>>,
    buffered_since: Option<DateTime<Utc>>,
    buffered_bytes: usize,
    cumulative: Option<AggregatedLog>,
    revision: u64,
}

struct RecoveredBudget {
    budget: Arc<MemoryBudget>,
    bytes: usize,
}
impl Drop for RecoveredBudget {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the actor owns coordinated durable state and bounded memory"
)]
async fn run_aggregator(
    definition: ChannelDefinition,
    generation: Uuid,
    mut receiver: mpsc::Receiver<AggregateCommand>,
    rabbit: RabbitPublisher,
    _metrics: Arc<Metrics>,
    budget: Arc<MemoryBudget>,
    store: StateStore,
    checkpoint: Option<DurableWindow>,
    mut recovered: RecoveredBudget,
) {
    let mut buffer = EventBuffer::with_budget(definition.buffer_bytes, budget);
    let mut state = checkpoint.or_else(|| {
        definition.aggregator.clone().map(|spec| DurableWindow {
            aggregator: AggregatorState::new(definition.channel_id.clone(), spec),
            first_timestamp: None,
            buffered_since: None,
            buffered_bytes: 0,
            cumulative: None,
            revision: 0,
        })
    });
    let mut ticker = tokio::time::interval(StdDuration::from_secs(1));
    loop {
        tokio::select! {
            command = receiver.recv() => match command {
                Some(AggregateCommand::Event { event, response }) => {
                    let result = match state.as_mut() {
                        Some(state) => apply_input(&definition, generation, &store, state, &mut buffer, &mut recovered, event).await,
                        None => Err(DistillError::InvalidRequest("channel has no aggregator".to_string())),
                    };
                    // SQLite owns the contribution before its source is acknowledged.
                    // Failed output publication remains recoverable in the local outbox.
                    let _ = response.send(result);
                }
                Some(AggregateCommand::Flush { response }) => {
                    let result = match state.as_mut() {
                        Some(state) => flush_window(&definition, generation, &store, state, &mut buffer, &mut recovered).await,
                        None => Ok(()),
                    };
                    let result = match result {
                        Ok(()) => publish_pending(&definition, generation, &store, &rabbit).await,
                        Err(error) => Err(error),
                    };
                    let _ = response.send(result);
                }
                None => break,
            },
            _ = ticker.tick() => {
                if let Some(state) = state.as_mut() {
                    let due = state.buffered_since.is_some_and(|start| Utc::now().signed_duration_since(start).num_seconds() >= i64::try_from(definition.ttl_seconds).unwrap_or(i64::MAX));
                    if (due || state.aggregator.should_flush(state.first_timestamp, Utc::now()))
                        && let Err(error) = flush_window(&definition, generation, &store, state, &mut buffer, &mut recovered).await {
                        warn!(event.name = "distill.aggregate.checkpoint.failure", error.code = %error.error_code(), "aggregate checkpoint failed; input remains recoverable");
                    }
                }
            }
        }
        if let Err(error) = publish_pending(&definition, generation, &store, &rabbit).await {
            warn!(event.name = "distill.aggregate.outbox.deferred", error.code = %error.error_code(), "aggregate publication remains pending");
        }
    }
}

async fn apply_input(
    definition: &ChannelDefinition,
    generation: Uuid,
    store: &StateStore,
    state: &mut DurableWindow,
    buffer: &mut EventBuffer,
    recovered: &mut RecoveredBudget,
    event: LogEvent,
) -> Result<(), DistillError> {
    if store.duplicate(generation, &event).await? {
        return Ok(());
    }
    let bytes = super::buffer::estimate_event_bytes(&event);
    if state.buffered_bytes.saturating_add(bytes) > definition.buffer_bytes {
        flush_window(definition, generation, store, state, buffer, recovered).await?;
    }
    buffer
        .push(event.clone())
        .map_err(|_| DistillError::BufferFull(definition.channel_id.clone()))?;
    let mut next = state.clone();
    let late = matches!(definition.ordering, OrderingMode::FifoRequired)
        && next.first_timestamp.is_some_and(|first| {
            first
                .signed_duration_since(event.timestamp)
                .num_milliseconds()
                > i64::try_from(definition.allowed_lateness_ms).unwrap_or(i64::MAX)
        });
    let timestamp = event.timestamp;
    if let Err(error) = next.aggregator.update(&event) {
        buffer.pop_back();
        return Err(DistillError::SchemaValidation(error));
    }
    if late {
        next.aggregator.mark_late_event();
    }
    next.first_timestamp.get_or_insert(timestamp);
    next.buffered_since.get_or_insert_with(Utc::now);
    next.buffered_bytes = next.buffered_bytes.saturating_add(bytes);
    if let Err(error) = store.commit_input(generation, &event, &next).await {
        buffer.pop_back();
        return Err(error);
    }
    *state = next;
    // Flushing failure cannot undo a committed input. The ticker retries it.
    if state
        .aggregator
        .should_flush(state.first_timestamp, Utc::now())
    {
        let _ = flush_window(definition, generation, store, state, buffer, recovered).await;
    }
    Ok(())
}

async fn flush_window(
    definition: &ChannelDefinition,
    generation: Uuid,
    store: &StateStore,
    state: &mut DurableWindow,
    buffer: &mut EventBuffer,
    recovered: &mut RecoveredBudget,
) -> Result<(), DistillError> {
    let Some(start) = state.first_timestamp else {
        return Ok(());
    };
    let mut next = state.clone();
    let mut aggregate = next.aggregator.flush(start, Utc::now());
    next.revision = next
        .revision
        .checked_add(1)
        .ok_or_else(|| DistillError::Persistence("aggregate revision exhausted".to_string()))?;
    aggregate.update_revision = next.revision;
    aggregate.version.clone_from(&definition.version);
    let identity = if aggregate.rolling {
        "rolling".to_string()
    } else {
        next.revision.to_string()
    };
    aggregate.aggregate_id = Uuid::new_v5(&generation, identity.as_bytes());
    if aggregate.rolling {
        if let Some(previous) = &state.cumulative {
            let (value, metadata) = merge_aggregate_values(
                aggregate.aggregator_type,
                previous.aggregated_value,
                count_as_i64(previous.count),
                serde_json::to_value(&previous.custom_metadata)
                    .map_err(|_| DistillError::ConfigurationConflict)?,
                aggregate.aggregated_value,
                aggregate.count,
                serde_json::to_value(&aggregate.custom_metadata)
                    .map_err(|_| DistillError::ConfigurationConflict)?,
            )
            .map_err(DistillError::SchemaValidation)?;
            if !value.is_finite() {
                return Err(DistillError::SchemaValidation(
                    "rolling aggregate overflow".to_string(),
                ));
            }
            aggregate.aggregated_value = value;
            aggregate.custom_metadata = serde_json::from_value(metadata)
                .map_err(|_| DistillError::ConfigurationConflict)?;
            aggregate.count = previous.count.checked_add(aggregate.count).ok_or_else(|| {
                DistillError::SchemaValidation("rolling count overflow".to_string())
            })?;
            aggregate.late_event_count = previous
                .late_event_count
                .saturating_add(aggregate.late_event_count);
            aggregate.window_start = previous.window_start;
        }
        next.cumulative = Some(aggregate.clone());
    }
    next.first_timestamp = None;
    next.buffered_since = None;
    next.buffered_bytes = 0;
    store.commit_output(generation, &next, &aggregate).await?;
    *state = next;
    buffer.drain();
    recovered.budget.release(recovered.bytes);
    recovered.bytes = 0;
    Ok(())
}

async fn publish_pending(
    definition: &ChannelDefinition,
    generation: Uuid,
    store: &StateStore,
    rabbit: &RabbitPublisher,
) -> Result<(), DistillError> {
    for aggregate in store.pending(generation).await? {
        rabbit
            .publish_routed_aggregate(
                &definition.rabbitmq_config.exchange_name,
                &definition.rabbitmq_config.routing_key_prefix,
                aggregate.clone(),
                generation,
            )
            .await?;
        store.published(&aggregate).await?;
    }
    Ok(())
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
    metrics.admitted();
    Ok(())
}

fn generation_for(definition: &ChannelDefinition) -> Uuid {
    let identity = format!(
        "{}|{}|{}|{}|{:?}|{}|{}|{}|{}|{:?}",
        definition.channel_id,
        definition.version.as_deref().unwrap_or_default(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{AggregationFunction, SeverityLevel};
    use serde_json::json;

    fn definition(rolling: bool) -> ChannelDefinition {
        serde_json::from_value(json!({
            "channel_id": "durable.test", "version": "1", "severity_threshold": "error",
            "rabbitmq_config": {"connection_string": "amqp://unused", "exchange_name": "unused"},
            "sink_config": {"driver": "http", "connection_string": "https://unused.test"},
            "aggregator": {"function": "count", "granularity": "per_n_events", "parameter": 2, "rolling": rolling},
            "buffer_bytes": 8192, "ttl_seconds": 3600, "mailbox_capacity": 8
        })).unwrap()
    }

    fn event() -> LogEvent {
        LogEvent {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            channel_id: "durable.test".to_string(),
            version: Some("1".to_string()),
            severity_level: SeverityLevel::Info,
            payload: json!({}),
            payload_bytes: None,
            metadata: std::collections::HashMap::default(),
        }
    }

    fn window(definition: &ChannelDefinition) -> DurableWindow {
        DurableWindow {
            aggregator: AggregatorState::new(
                definition.channel_id.clone(),
                definition.aggregator.clone().unwrap(),
            ),
            first_timestamp: None,
            buffered_since: None,
            buffered_bytes: 0,
            cumulative: None,
            revision: 0,
        }
    }

    #[tokio::test]
    async fn restart_and_redelivery_preserve_exact_contributions() {
        let directory = std::env::temp_dir().join(format!("distill-checkpoint-{}", Uuid::new_v4()));
        let path = directory.join("state.sqlite3");
        let definition = definition(false);
        let generation = Uuid::new_v4();
        let mut first = event();
        let store = StateStore::open(&path).await.unwrap();
        let budget = Arc::new(MemoryBudget::new(8192));
        let mut buffer = EventBuffer::with_budget(8192, Arc::clone(&budget));
        let mut recovered = RecoveredBudget {
            budget: Arc::clone(&budget),
            bytes: 0,
        };
        let mut state = window(&definition);
        apply_input(
            &definition,
            generation,
            &store,
            &mut state,
            &mut buffer,
            &mut recovered,
            first.clone(),
        )
        .await
        .unwrap();
        store.close().await;
        drop(store);
        drop(buffer);
        let store = StateStore::open(&path).await.unwrap();
        let mut state: DurableWindow = store.checkpoint(generation).await.unwrap().unwrap();
        let mut buffer = EventBuffer::with_budget(8192, Arc::clone(&budget));
        apply_input(
            &definition,
            generation,
            &store,
            &mut state,
            &mut buffer,
            &mut recovered,
            first.clone(),
        )
        .await
        .unwrap();
        assert!(store.pending(generation).await.unwrap().is_empty());
        let second = event();
        apply_input(
            &definition,
            generation,
            &store,
            &mut state,
            &mut buffer,
            &mut recovered,
            second.clone(),
        )
        .await
        .unwrap();
        let pending = store.pending(generation).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].count, 2);
        assert_eq!(pending[0].update_revision, 1);
        // Simulate lost publication confirmation: reloading retains identical output.
        let reloaded = store.pending(generation).await.unwrap();
        assert_eq!(
            serde_json::to_value(&pending).unwrap(),
            serde_json::to_value(reloaded).unwrap()
        );
        store.published(&pending[0]).await.unwrap();
        apply_input(
            &definition,
            generation,
            &store,
            &mut state,
            &mut buffer,
            &mut recovered,
            second,
        )
        .await
        .unwrap();
        assert!(store.pending(generation).await.unwrap().is_empty());
        first.payload = json!({"different": true});
        assert!(matches!(
            apply_input(
                &definition,
                generation,
                &store,
                &mut state,
                &mut buffer,
                &mut recovered,
                first
            )
            .await,
            Err(DistillError::ConfigurationConflict)
        ));
        store.close().await;
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn rolling_snapshots_are_cumulative_and_duplicate_safe() {
        let directory = std::env::temp_dir().join(format!("distill-rolling-{}", Uuid::new_v4()));
        let store = StateStore::open(&directory.join("state.sqlite3"))
            .await
            .unwrap();
        let definition = definition(true);
        let generation = Uuid::new_v4();
        let budget = Arc::new(MemoryBudget::new(8192));
        let mut buffer = EventBuffer::with_budget(8192, Arc::clone(&budget));
        let mut recovered = RecoveredBudget { budget, bytes: 0 };
        let mut state = window(&definition);
        for _ in 0..4 {
            let input = event();
            apply_input(
                &definition,
                generation,
                &store,
                &mut state,
                &mut buffer,
                &mut recovered,
                input.clone(),
            )
            .await
            .unwrap();
            apply_input(
                &definition,
                generation,
                &store,
                &mut state,
                &mut buffer,
                &mut recovered,
                input,
            )
            .await
            .unwrap();
        }
        let pending = store.pending(generation).await.unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].aggregate_id, pending[1].aggregate_id);
        assert_eq!((pending[0].count, pending[1].count), (2, 4));
        assert_eq!(
            (pending[0].update_revision, pending[1].update_revision),
            (1, 2)
        );
        assert_eq!(pending[1].aggregator_type, AggregationFunction::Count);
        store.close().await;
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
