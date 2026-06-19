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

# --- Parquet converter ---

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

# --- Raw cleanup ---

raw_cleanup_files_deleted_total = Counter(
    "mdp_pipeline_raw_cleanup_files_deleted_total",
    "Total processed raw JSONL files deleted by cleanup.",
)

raw_cleanup_bytes_freed_total = Counter(
    "mdp_pipeline_raw_cleanup_bytes_freed_total",
    "Total bytes freed by raw JSONL cleanup.",
)

raw_last_cleanup_timestamp = Gauge(
    "mdp_pipeline_raw_last_cleanup_timestamp_seconds",
    "Unix timestamp of the last raw JSONL cleanup run.",
)

# --- Catalog sync (Section 6 — stub metrics until catalog exists) ---

catalog_last_sync_timestamp = Gauge(
    "mdp_pipeline_catalog_last_sync_timestamp_seconds",
    "Unix timestamp of the last metadata catalog sync.",
)

catalog_datasets_discovered = Gauge(
    "mdp_pipeline_catalog_datasets_discovered",
    "Number of datasets currently registered in the catalog.",
)

catalog_metadata_extraction_failures_total = Counter(
    "mdp_pipeline_catalog_metadata_extraction_failures_total",
    "Total metadata extraction failures during catalog sync.",
)
