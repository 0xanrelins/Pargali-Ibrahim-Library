#!/usr/bin/env python3
"""Prometheus metric definitions for data coverage (Phase 3, Section 4)."""

from __future__ import annotations

from prometheus_client import Gauge

coverage_first_timestamp_seconds = Gauge(
    "mdp_coverage_first_timestamp_seconds",
    "Earliest event timestamp in the dataset (Unix seconds).",
    ["category", "symbol"],
)

coverage_last_timestamp_seconds = Gauge(
    "mdp_coverage_last_timestamp_seconds",
    "Latest event timestamp in the dataset (Unix seconds).",
    ["category", "symbol"],
)

coverage_missing_days_total = Gauge(
    "mdp_coverage_missing_days_total",
    "Number of calendar days with no parquet file in the expected range.",
    ["category", "symbol"],
)

coverage_missing_hours_total = Gauge(
    "mdp_coverage_missing_hours_total",
    "Number of hours with no events in the expected range.",
    ["category", "symbol"],
)

coverage_percent = Gauge(
    "mdp_coverage_percent",
    "Percent of expected hours that contain at least one event.",
    ["category", "symbol"],
)

coverage_day_present = Gauge(
    "mdp_coverage_day_present",
    "Whether a calendar day has parquet data (1) or is missing (0).",
    ["category", "symbol", "day"],
)

coverage_hour_present = Gauge(
    "mdp_coverage_hour_present",
    "Whether an hour bucket contains at least one event (1) or is missing (0).",
    ["category", "symbol", "day", "hour"],
)
