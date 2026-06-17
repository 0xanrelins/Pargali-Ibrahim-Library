# PHASE 3 IMPLEMENTATION — MARKET DATA PLATFORM MONITORING & METADATA SYSTEM

### Context

Phase 1 and Phase 2 already exist.

### Phase 1

Binance Futures WebSocket collection using UBWA.

Collected streams:

* aggTrade
* forceOrder (liquidations)

Output:

* raw JSONL files

### Phase 2

Raw JSONL files are converted into Parquet datasets.

Output:

* trades parquet datasets
* liquidations parquet datasets

DuckDB is used for querying.

The goal of this task is to implement Phase 3.

---

# Objective

Build a complete Monitoring, Metadata and Control Center for the Market Data Platform.

The implementation should use:

* Grafana
* Prometheus
* Loki
* DuckDB

The dashboard must become the single operational interface for the entire platform.

---

# SECTION 1 — COLLECTOR HEALTH

Source:

UBWA monitoring APIs.

Implement metrics collection for:

## Stream lifecycle

* CONNECT
* FIRST_RECEIVED_DATA
* DISCONNECT
* STOP
* STREAM_UNREPAIRABLE

## Buffer metrics

* stream_buffer_length
* stream_buffer_bytes
* stream_buffer_maxlen

## System metrics

* active_streams
* crashed_streams
* restarting_streams
* reconnects
* uptime
* received_mb
* receives_per_second

## Throughput

* bytes_per_second
* messages_per_second

## Errors

* endpoint errors
* endpoint responses

## Stream details

* stream_id
* stream label
* subscriptions

## Process metrics

* cpu usage
* memory usage
* thread count

Expose all metrics to Prometheus.

---

# SECTION 2 — PIPELINE HEALTH

Monitor the complete data flow.

Pipeline:

UBWA
→ raw JSONL
→ Parquet conversion
→ Metadata catalog

Metrics:

## Raw Writer

* last flush timestamp
* files written
* bytes written
* write duration

## Parquet Converter

* last batch run
* processed files
* conversion duration
* failed conversions

## Catalog Sync

* last sync timestamp
* datasets discovered
* metadata extraction failures

---

# SECTION 3 — STORAGE HEALTH

Monitor storage growth.

Metrics:

* total storage used
* free disk space
* daily growth
* parquet file count
* raw file count
* largest datasets

Alerts:

* disk usage > 85%
* free space below threshold

---

# SECTION 4 — DATA COVERAGE

Create dataset coverage tracking.

Requirements:

For each dataset:

* first timestamp
* last timestamp
* expected coverage
* missing days
* missing hours

Dashboard views:

* calendar heatmap
* missing data table
* coverage summary

Examples:

BTCUSDT trades
BTCUSDT liquidations

---

# SECTION 5 — DATA QUALITY

Implement validation layer.

Checks:

Trades:

* monotonic timestamps
* duplicates
* null fields

Liquidations:

* positive quantity
* positive price

Future datasets must be able to register custom validators.

Metrics:

* validation failures
* quality score

---

# SECTION 6 — METADATA CATALOG

Create catalog.duckdb.

Required tables:

datasets

* dataset_id
* dataset_type
* exchange
* symbol
* path

file_stats

* dataset_id
* rows
* file_size
* start_timestamp
* end_timestamp

schema_registry

* dataset_id
* schema_version
* column_name
* data_type

ai_metadata

* dataset_id
* summary
* tags
* use_cases

The catalog must automatically update when new datasets appear.

---

# SECTION 7 — AI METADATA LAYER

Prepare metadata for future AI usage.

Requirements:

* expose catalog records
* track indexing status
* track indexed dataset count

No LLM implementation required yet.

Only provide infrastructure and monitoring.

---

# SECTION 8 — GRAFANA DASHBOARD

Create the following dashboard groups:

1. Overview
2. Collector Health
3. Pipeline Health
4. Storage Health
5. Data Coverage
6. Data Quality
7. Metadata Catalog
8. AI Layer

Overview dashboard should show:

* collector status
* active streams
* throughput
* storage usage
* total datasets
* quality score
* last catalog sync

---

# ALERTING

Implement alert rules for:

* STREAM_UNREPAIRABLE
* collector disconnected
* buffer near capacity
* failed parquet conversion
* failed catalog sync
* disk usage > 85%
* missing data coverage
* schema change detected

Alerts should be configurable.

---

# NON-GOALS

Do not modify Phase 1.

Do not modify Phase 2.

Only build monitoring, metadata and operational visibility.

---

# SUCCESS CRITERIA

A single Grafana installation must allow an operator to answer:

* Is data currently being collected?
* Is any collector unhealthy?
* Is data being converted successfully?
* Is storage healthy?
* Are datasets complete?
* Are there missing days or hours?
* What datasets exist?
* What schemas exist?
* Is metadata up to date?

without opening source code or log files.
