#!/usr/bin/env python3
"""Prometheus metric definitions for the AI metadata layer (Phase 3, Section 7)."""

from __future__ import annotations

from prometheus_client import Gauge

ai_datasets_total = Gauge(
    "mdp_ai_datasets_total",
    "Total datasets registered for AI metadata indexing.",
)

ai_datasets_indexed = Gauge(
    "mdp_ai_datasets_indexed",
    "Number of datasets with indexing_status=indexed.",
)

ai_indexing_status = Gauge(
    "mdp_ai_indexing_status",
    "Current AI indexing status for a dataset (1=active status).",
    ["dataset_id", "status"],
)

ai_last_index_run_timestamp_seconds = Gauge(
    "mdp_ai_last_index_run_timestamp_seconds",
    "Unix timestamp of the last AI indexing run (0 until implemented).",
)
