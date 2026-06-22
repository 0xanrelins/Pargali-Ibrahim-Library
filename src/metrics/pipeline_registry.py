#!/usr/bin/env python3
"""Prometheus metric definitions for pipeline health (Phase 3, Section 2)."""

from __future__ import annotations

from prometheus_client import Counter, Gauge

# --- Raw writer ---

raw_last_flush_timestamp = Gauge(
    "mdp_pipeline_raw_last_flush_timestamp_seconds",
    "Unix timestamp of the last raw JSONL flush.",
)

raw_last_flush_duration_seconds = Gauge(
    "mdp_pipeline_raw_last_flush_duration_seconds",
    "Duration of the last raw JSONL flush in seconds.",
)

raw_files_written_total = Counter(
    "mdp_pipeline_raw_files_written_total",
    "Total raw JSONL files written.",
    ["category"],
)

raw_bytes_written_total = Counter(
    "mdp_pipeline_raw_bytes_written_total",
    "Total bytes written to raw JSONL files.",
    ["category"],
)

# --- Normalizer batch writer ---

parquet_last_batch_timestamp = Gauge(
    "mdp_pipeline_parquet_last_batch_timestamp_seconds",
    "Unix timestamp of the last parquet conversion batch.",
)

parquet_last_batch_duration_seconds = Gauge(
    "mdp_pipeline_parquet_last_batch_duration_seconds",
    "Duration of the last parquet conversion batch in seconds.",
)

parquet_last_batch_files_processed = Gauge(
    "mdp_pipeline_parquet_last_batch_files_processed",
    "Raw files processed in the last parquet conversion batch.",
)

parquet_files_processed_total = Counter(
    "mdp_pipeline_parquet_files_processed_total",
    "Total raw files successfully converted to parquet.",
)

parquet_failed_conversions_total = Counter(
    "mdp_pipeline_parquet_failed_conversions_total",
    "Total parquet conversion failures.",
    ["reason"],
)

parquet_raw_backlog_files = Gauge(
    "mdp_pipeline_parquet_raw_backlog_files",
    "Unprocessed raw JSONL files waiting for parquet conversion.",
    ["category"],
)
