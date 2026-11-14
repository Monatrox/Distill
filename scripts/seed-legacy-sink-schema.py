#!/usr/bin/env python3
"""Emit a pre-V1 schema and sentinel row for an isolated migration fixture."""
import sys

kind = sys.argv[1]
types = {
    'postgres': ('UUID', 'TEXT', 'TIMESTAMPTZ', 'JSONB', 'DOUBLE PRECISION', 'BOOLEAN', 'FALSE'),
    'mysql': ('CHAR(36)', 'VARCHAR(255)', 'DATETIME(6)', 'JSON', 'DOUBLE', 'BOOLEAN', 'FALSE'),
    'sqlserver': ('NVARCHAR(36)', 'NVARCHAR(255)', 'DATETIME2', 'NVARCHAR(MAX)', 'FLOAT', 'BIT', '0'),
}
identity, text, timestamp, document, number, boolean, false = types[kind]
print(f'''
CREATE TABLE distill_events (
    event_id {identity} PRIMARY KEY,
    channel_id {text} NOT NULL,
    event_timestamp {timestamp} NOT NULL,
    severity SMALLINT NOT NULL,
    payload {document} NOT NULL,
    metadata {document} NOT NULL
);
CREATE TABLE distill_aggregates (
    aggregate_id {identity} PRIMARY KEY,
    channel_id {text} NOT NULL,
    window_start {timestamp} NOT NULL,
    window_end {timestamp} NOT NULL,
    aggregator_type {text} NOT NULL,
    aggregated_value {number} NOT NULL,
    event_count BIGINT NOT NULL,
    late_event_count BIGINT NOT NULL,
    rolling {boolean} NOT NULL DEFAULT {false},
    custom_metadata {document} NOT NULL
);
INSERT INTO distill_events(event_id,channel_id,event_timestamp,severity,payload,metadata)
VALUES('00000000-0000-0000-0000-000000000099','migration.legacy','2026-01-01T00:00:00',2,'{{"legacy":true}}','{{}}');
''')
