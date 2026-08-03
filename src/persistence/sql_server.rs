//! Implements one external persistence driver.

// Rust guideline compliant 2026-02-21
use super::{
    AggregatedLog, Arc, Client, Compat, Config, DistillError, LogEvent, Mutex, Sink, TcpStream,
    TokioAsyncWriteCompatExt, async_trait, count_as_i64,
};

type SqlServerConnection = Compat<TcpStream>;

#[derive(Clone)]
pub struct SqlServerSink {
    client: Arc<Mutex<Client<SqlServerConnection>>>,
}

impl SqlServerSink {
    pub async fn connect(connection_string: &str) -> Result<Self, DistillError> {
        let config = Config::from_ado_string(connection_string).map_err(|error| {
            DistillError::InvalidConfiguration(format!(
                "invalid SQL Server connection string: {error}"
            ))
        })?;
        let tcp = TcpStream::connect(config.get_addr())
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        tcp.set_nodelay(true)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        let client = Client::connect(config, tcp.compat_write())
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        let sink = Self {
            client: Arc::new(Mutex::new(client)),
        };
        sink.initialize().await?;
        Ok(sink)
    }

    async fn initialize(&self) -> Result<(), DistillError> {
        let mut client = self.client.lock().await;
        client
            .execute(
                "IF OBJECT_ID(N'distill_events', N'U') IS NULL
                 BEGIN
                    CREATE TABLE distill_events (
                        event_id NVARCHAR(36) NOT NULL PRIMARY KEY,
                        channel_id NVARCHAR(255) NOT NULL,
                        event_timestamp DATETIME2 NOT NULL,
                        severity SMALLINT NOT NULL,
                        payload NVARCHAR(MAX) NOT NULL,
                        metadata NVARCHAR(MAX) NOT NULL
                    )
                 END;
                 IF OBJECT_ID(N'distill_aggregates', N'U') IS NULL
                 BEGIN
                    CREATE TABLE distill_aggregates (
                        aggregate_id NVARCHAR(36) NOT NULL PRIMARY KEY,
                        channel_id NVARCHAR(255) NOT NULL,
                        window_start DATETIME2 NOT NULL,
                        window_end DATETIME2 NOT NULL,
                        aggregator_type NVARCHAR(100) NOT NULL,
                        aggregated_value FLOAT NOT NULL,
                        event_count BIGINT NOT NULL,
                        late_event_count BIGINT NOT NULL,
                        rolling BIT NOT NULL DEFAULT 0,
                        custom_metadata NVARCHAR(MAX) NOT NULL
                    )
                 END",
                &[],
            )
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl Sink for SqlServerSink {
    async fn persist_event(&self, event: &LogEvent) -> Result<(), DistillError> {
        let event_id = event.event_id.to_string();
        let timestamp = event.timestamp.naive_utc();
        let payload = serde_json::to_string(&event.payload)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        let metadata = serde_json::to_string(&event.metadata)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        let severity = event.severity_level as i16;
        let params: [&dyn tiberius::ToSql; 6] = [
            &event_id,
            &event.channel_id,
            &timestamp,
            &severity,
            &payload,
            &metadata,
        ];
        self.client
            .lock()
            .await
            .execute(
                "IF NOT EXISTS (SELECT 1 FROM distill_events WHERE event_id = @P1)
                 INSERT INTO distill_events
                    (event_id, channel_id, event_timestamp, severity, payload, metadata)
                 VALUES (@P1, @P2, @P3, @P4, @P5, @P6)",
                &params,
            )
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        Ok(())
    }

    async fn persist_aggregate(&self, aggregate: &AggregatedLog) -> Result<(), DistillError> {
        let aggregate_id = aggregate.aggregate_id.to_string();
        let channel_id = aggregate.channel_id.clone();
        let window_start = aggregate.window_start.naive_utc();
        let window_end = aggregate.window_end.naive_utc();
        let aggregator_type = format!("{:?}", aggregate.aggregator_type);
        let value = aggregate.aggregated_value;
        let count = count_as_i64(aggregate.count);
        let late_count = count_as_i64(aggregate.late_event_count);
        let rolling = aggregate.rolling;
        let metadata = serde_json::to_string(&aggregate.custom_metadata)
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        let params: [&dyn tiberius::ToSql; 10] = [
            &aggregate_id,
            &channel_id,
            &window_start,
            &window_end,
            &aggregator_type,
            &value,
            &count,
            &late_count,
            &rolling,
            &metadata,
        ];
        self.client
            .lock()
            .await
            .execute(
                "MERGE distill_aggregates AS target
                 USING (VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10))
                    AS source (aggregate_id, channel_id, window_start, window_end,
                               aggregator_type, aggregated_value, event_count,
                               late_event_count, rolling, custom_metadata)
                 ON target.aggregate_id = source.aggregate_id
                 WHEN MATCHED THEN UPDATE SET
                    window_end = source.window_end,
                    aggregated_value = source.aggregated_value,
                    event_count = source.event_count,
                    late_event_count = source.late_event_count,
                    rolling = source.rolling,
                    custom_metadata = source.custom_metadata
                 WHEN NOT MATCHED THEN INSERT
                    (aggregate_id, channel_id, window_start, window_end, aggregator_type,
                     aggregated_value, event_count, late_event_count, rolling, custom_metadata)
                 VALUES (source.aggregate_id, source.channel_id, source.window_start,
                    source.window_end, source.aggregator_type, source.aggregated_value,
                    source.event_count, source.late_event_count, source.rolling,
                    source.custom_metadata);",
                &params,
            )
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        Ok(())
    }
}
