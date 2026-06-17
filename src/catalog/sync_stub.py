#!/usr/bin/env python3
"""Stub catalog sync metrics until Section 6 implements catalog.duckdb."""

from __future__ import annotations

import logging

from metrics.pipeline_registry import (
    catalog_datasets_discovered,
    catalog_last_sync_timestamp,
)

logger = logging.getLogger(__name__)


def publish_stub_metrics() -> None:
    """Expose zeroed catalog metrics so dashboards/alerts can be wired early."""
    catalog_last_sync_timestamp.set(0)
    catalog_datasets_discovered.set(0)
    logger.info("Catalog sync stub active (Section 6 will replace this)")
