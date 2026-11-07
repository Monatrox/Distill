//! Implements one external persistence driver.

// Rust guideline compliant 2026-02-21
use super::{
    AggregatedLog, Arc, Client, Compat, Config, DistillError, LogEvent, Mutex, Sink, TcpStream,
    TokioAsyncWriteCompatExt, async_trait, count_as_i64, event_fingerprint, snapshot_fingerprint,
    snapshot_revision,
};

type SqlServerConnection = Compat<TcpStream>;

#[derive(Clone)]
pub struct SqlServerSink {
    client: Arc<Mutex<Option<Client<SqlServerConnection>>>>,
    connection_string: Arc<str>,
}

impl SqlServerSink {
    pub async fn connect(
        connection_string: &str,
        auto_migrate: bool,
    ) -> Result<Self, DistillError> {
        let sink = Self::restore(connection_string)?;
        if auto_migrate {
            sink.initialize().await?;
        }
        sink.verify_schema().await?;
        Ok(sink)
    }

    pub(super) fn restore(connection_string: &str) -> Result<Self, DistillError> {
        Config::from_ado_string(connection_string).map_err(|_| {
            DistillError::InvalidConfiguration("invalid SQL Server connection string".to_string())
        })?;
        Ok(Self {
            client: Arc::new(Mutex::new(None)),
            connection_string: Arc::from(connection_string),
        })
    }

    async fn connection(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, Option<Client<SqlServerConnection>>>, DistillError>
    {
        let mut client = self.client.lock().await;
        if client.is_none() {
            let config = Config::from_ado_string(&self.connection_string).map_err(|_| {
                DistillError::InvalidConfiguration(
                    "invalid SQL Server connection string".to_string(),
                )
            })?;
            let tcp = TcpStream::connect(config.get_addr())
                .await
                .map_err(|error| DistillError::Persistence(error.to_string()))?;
            tcp.set_nodelay(true)
                .map_err(|error| DistillError::Persistence(error.to_string()))?;
            *client = Some(
                Client::connect(config, tcp.compat_write())
                    .await
                    .map_err(|error| DistillError::Persistence(error.to_string()))?,
            );
        }
        Ok(client)
    }

    async fn execute(
        &self,
        sql: &str,
        params: &[&dyn tiberius::ToSql],
    ) -> Result<(), DistillError> {
        let mut guard = self.connection().await?;
        // Keep the cached slot empty until completion. Cancelling this future drops
        // the connection instead of reusing a session with an unfinished transaction.
        let mut connection = guard.take().expect("connection has been initialized");
        connection
            .execute(sql, params)
            .await
            .map_err(|error| DistillError::Persistence(error.to_string()))?;
        *guard = Some(connection);
        Ok(())
    }

    async fn verify_schema(&self) -> Result<(), DistillError> {
        let mut client = self.connection().await?;
        let row = client
            .as_mut()
            .expect("connection has been initialized")
            .simple_query("SELECT MAX(version) AS version FROM distill_schema_migrations")
            .await
            .map_err(|_| super::migration_required())?
            .into_row()
            .await
            .map_err(|_| super::migration_required())?;
        if row.and_then(|row| row.get::<i32, _>("version")) != Some(1) {
            return Err(super::migration_required());
        }
        Ok(())
    }

    async fn initialize(&self) -> Result<(), DistillError> {
        self.execute(
                "IF OBJECT_ID(N'distill_events', N'U') IS NULL
                 BEGIN
                    CREATE TABLE distill_events (
                        event_id NVARCHAR(36) NOT NULL PRIMARY KEY,
                        channel_id NVARCHAR(255) NOT NULL,
                        event_timestamp DATETIME2 NOT NULL,
                        severity SMALLINT NOT NULL,
                        payload NVARCHAR(MAX) NOT NULL,
                        metadata NVARCHAR(MAX) NOT NULL,
                        event_fingerprint NVARCHAR(64) NOT NULL DEFAULT '',
                        configuration_version NVARCHAR(64)
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
                        custom_metadata NVARCHAR(MAX) NOT NULL,
                        update_revision BIGINT NOT NULL DEFAULT 0,
                        snapshot_fingerprint NVARCHAR(64) NOT NULL DEFAULT '',
                        configuration_version NVARCHAR(64)
                    )
                 END;
                 IF COL_LENGTH('distill_events', 'event_fingerprint') IS NULL
                     ALTER TABLE distill_events ADD event_fingerprint NVARCHAR(64) NOT NULL DEFAULT '';
                 IF COL_LENGTH('distill_events', 'configuration_version') IS NULL
                     ALTER TABLE distill_events ADD configuration_version NVARCHAR(64) NULL;
                 IF COL_LENGTH('distill_aggregates', 'update_revision') IS NULL
                     ALTER TABLE distill_aggregates ADD update_revision BIGINT NOT NULL DEFAULT 0;
                 IF COL_LENGTH('distill_aggregates', 'snapshot_fingerprint') IS NULL
                     ALTER TABLE distill_aggregates ADD snapshot_fingerprint NVARCHAR(64) NOT NULL DEFAULT '';
                 IF COL_LENGTH('distill_aggregates', 'configuration_version') IS NULL
                     ALTER TABLE distill_aggregates ADD configuration_version NVARCHAR(64) NULL;
                 IF OBJECT_ID(N'distill_schema_migrations', N'U') IS NULL
                     CREATE TABLE distill_schema_migrations (version INT PRIMARY KEY);
                 IF NOT EXISTS (SELECT 1 FROM distill_schema_migrations WHERE version = 1)
                     INSERT INTO distill_schema_migrations (version) VALUES (1);",
                &[],
            )
            .await?;
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
        let fingerprint = event_fingerprint(event)?;
        let params: [&dyn tiberius::ToSql; 8] = [
            &event_id,
            &event.channel_id,
            &timestamp,
            &severity,
            &payload,
            &metadata,
            &fingerprint,
            &event.version,
        ];
        self.execute(
                "SET XACT_ABORT ON;
                 BEGIN TRANSACTION;
                 IF EXISTS (SELECT 1 FROM distill_events WITH (UPDLOCK, HOLDLOCK) WHERE event_id = @P1 AND event_fingerprint <> @P7)
                     THROW 50002, 'Conflicting event identity', 1;
                 IF NOT EXISTS (SELECT 1 FROM distill_events WHERE event_id = @P1)
                 INSERT INTO distill_events
                    (event_id, channel_id, event_timestamp, severity, payload, metadata, event_fingerprint, configuration_version)
                 VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8);
                 COMMIT TRANSACTION;",
                &params,
            )
            .await?;
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
        let revision = snapshot_revision(aggregate)?;
        let fingerprint = snapshot_fingerprint(aggregate)?;
        let params: [&dyn tiberius::ToSql; 13] = [
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
            &revision,
            &fingerprint,
            &aggregate.version,
        ];
        self.execute(
                "SET XACT_ABORT ON;
                 BEGIN TRANSACTION;
                 IF EXISTS (SELECT 1 FROM distill_aggregates WITH (UPDLOCK, HOLDLOCK)
                    WHERE aggregate_id = @P1 AND update_revision = @P11 AND snapshot_fingerprint <> @P12)
                    THROW 50001, 'Conflicting aggregate snapshot revision', 1;
                 MERGE distill_aggregates WITH (HOLDLOCK) AS target
                 USING (VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10, @P11, @P12, @P13))
                    AS source (aggregate_id, channel_id, window_start, window_end,
                               aggregator_type, aggregated_value, event_count,
                               late_event_count, rolling, custom_metadata, update_revision, snapshot_fingerprint, configuration_version)
                 ON target.aggregate_id = source.aggregate_id
                 WHEN MATCHED AND source.update_revision > target.update_revision THEN UPDATE SET
                    window_end = source.window_end,
                    aggregated_value = source.aggregated_value,
                    event_count = source.event_count,
                    late_event_count = source.late_event_count,
                    rolling = source.rolling,
                    custom_metadata = source.custom_metadata,
                    update_revision = source.update_revision,
                    snapshot_fingerprint = source.snapshot_fingerprint
                 WHEN NOT MATCHED THEN INSERT
                    (aggregate_id, channel_id, window_start, window_end, aggregator_type,
                     aggregated_value, event_count, late_event_count, rolling, custom_metadata, update_revision, snapshot_fingerprint, configuration_version)
                 VALUES (source.aggregate_id, source.channel_id, source.window_start,
                    source.window_end, source.aggregator_type, source.aggregated_value,
                    source.event_count, source.late_event_count, source.rolling,
                    source.custom_metadata, source.update_revision, source.snapshot_fingerprint, source.configuration_version);
                 COMMIT TRANSACTION;",
                &params,
            )
            .await?;
        Ok(())
    }
}
