#!/usr/bin/env python3
"""Prometheus metric definitions for storage health (Phase 3, Section 3)."""

from __future__ import annotations

from prometheus_client import Gauge

storage_total_bytes = Gauge(
    "mdp_storage_total_bytes",
    "Total bytes used by raw and parquet data directories.",
    ["path"],
)

storage_free_bytes = Gauge(
    "mdp_storage_free_bytes",
    "Free disk space on the volume hosting project data.",
)

storage_used_percent = Gauge(
    "mdp_storage_used_percent",
    "Disk usage percent on the volume hosting project data.",
)

storage_daily_growth_bytes = Gauge(
    "mdp_storage_daily_growth_bytes",
    "Estimated storage growth in bytes over the last 24 hours.",
    ["path"],
)

storage_raw_file_count = Gauge(
    "mdp_storage_raw_file_count",
    "Number of raw JSONL files under raw/.",
)

storage_parquet_file_count = Gauge(
    "mdp_storage_parquet_file_count",
    "Number of parquet files under data/.",
)

storage_largest_dataset_bytes = Gauge(
    "mdp_storage_largest_dataset_bytes",
    "Size in bytes of the largest symbol dataset directory.",
    ["category", "symbol"],
)
