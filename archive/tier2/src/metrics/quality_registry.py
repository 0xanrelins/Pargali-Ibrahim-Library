#!/usr/bin/env python3
"""Prometheus metric definitions for data quality (Phase 3, Section 5)."""

from __future__ import annotations

from prometheus_client import Gauge

quality_validation_failures = Gauge(
    "mdp_quality_validation_failures",
    "Count of validation failures from the latest quality scan.",
    ["category", "symbol", "check"],
)

quality_score = Gauge(
    "mdp_quality_score",
    "Data quality score from 0 to 100 for a dataset.",
    ["category", "symbol"],
)

quality_rows_validated = Gauge(
    "mdp_quality_rows_validated",
    "Number of rows validated in the latest quality scan.",
    ["category", "symbol"],
)
