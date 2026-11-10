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

# --- Raw retention (optional normalizer poller) ---

raw_retention_last_run_timestamp = Gauge(
    "mdp_raw_retention_last_run_timestamp_seconds",
    "Unix timestamp of the last raw retention run.",
)

raw_retention_would_delete_files = Gauge(
    "mdp_raw_retention_would_delete_files",
    "Raw JSONL files that would be deleted on the last dry-run retention pass.",
)

raw_retention_would_delete_bytes = Gauge(
    "mdp_raw_retention_would_delete_bytes",
    "Bytes that would be freed on the last dry-run retention pass.",
)

raw_retention_files_deleted_total = Counter(
    "mdp_raw_retention_files_deleted_total",
    "Total raw JSONL files deleted by live retention runs.",
)

raw_retention_bytes_freed_total = Counter(
    "mdp_raw_retention_bytes_freed_total",
    "Total bytes freed by live raw retention runs.",
)

raw_retention_run_skipped = Gauge(
    "mdp_raw_retention_run_skipped",
    "1 when the last retention attempt was skipped entirely (interval throttle).",
    ["reason"],
)

raw_retention_skipped_files_last_run = Gauge(
    "mdp_raw_retention_skipped_files_last_run",
    "Files skipped on the last retention run, by reason, exchange, and category.",
    ["reason", "exchange", "category"],
)

raw_retention_backlog_files = Gauge(
    "mdp_raw_retention_backlog_files",
    "Unprocessed raw JSONL files seen by the last retention run, by category (informational; never blocks deletion).",
    ["category"],
)
