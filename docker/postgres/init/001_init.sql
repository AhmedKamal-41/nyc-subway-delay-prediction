-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create schema
CREATE SCHEMA IF NOT EXISTS mta;

-- Table: ingest_runs
CREATE TABLE IF NOT EXISTS mta.ingest_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ NULL,
    status TEXT NOT NULL CHECK (status IN ('running','success','failed')),
    source TEXT NOT NULL,
    notes TEXT NULL
);

-- Table: raw_events
CREATE TABLE IF NOT EXISTS mta.raw_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES mta.ingest_runs(run_id) ON DELETE CASCADE,
    feed_type TEXT NOT NULL CHECK (feed_type IN ('service_alerts','trip_updates','vehicle_positions')),
    entity_id TEXT NULL,
    event_ts TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    line_id TEXT NULL,
    stop_id TEXT NULL,
    trip_id TEXT NULL,
    payload JSONB NOT NULL
);

-- Indexes for raw_events
CREATE INDEX IF NOT EXISTS idx_raw_events_event_ts ON mta.raw_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_raw_events_feed_type ON mta.raw_events(feed_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_line_id ON mta.raw_events(line_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_stop_id ON mta.raw_events(stop_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_payload_gin ON mta.raw_events USING GIN(payload);

-- Table: station_minute_facts
CREATE TABLE IF NOT EXISTS mta.station_minute_facts (
    fact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bucket_start TIMESTAMPTZ NOT NULL,
    bucket_size_seconds INT NOT NULL CHECK (bucket_size_seconds IN (60,300)),
    line_id TEXT NULL,
    stop_id TEXT NULL,
    alerts_count INT NOT NULL DEFAULT 0,
    major_alerts_count INT NOT NULL DEFAULT 0,
    trip_updates_count INT NOT NULL DEFAULT 0,
    vehicle_positions_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(bucket_start, bucket_size_seconds, line_id, stop_id)
);

-- Indexes for station_minute_facts
CREATE INDEX IF NOT EXISTS idx_station_facts_bucket ON mta.station_minute_facts(bucket_start);
CREATE INDEX IF NOT EXISTS idx_station_facts_line_stop ON mta.station_minute_facts(line_id, stop_id);

