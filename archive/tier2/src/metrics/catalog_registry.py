#!/usr/bin/env python3
"""Prometheus metric definitions for metadata catalog (Phase 3, Section 6)."""

from __future__ import annotations

from prometheus_client import Counter, Gauge

catalog_schema_changes_total = Counter(
    "mdp_catalog_schema_changes_total",
    "Total parquet schema changes detected during catalog sync.",
    ["dataset_id"],
)

catalog_dataset_files = Gauge(
    "mdp_catalog_dataset_files",
    "Number of parquet files for a registered dataset.",
    ["dataset_id", "category", "symbol"],
)

catalog_dataset_rows = Gauge(
    "mdp_catalog_dataset_rows",
    "Total rows across parquet files for a registered dataset.",
    ["dataset_id", "category", "symbol"],
)

catalog_schema_columns = Gauge(
    "mdp_catalog_schema_columns",
    "Registered schema column for a dataset (value=1).",
    ["dataset_id", "column_name", "data_type"],
)
