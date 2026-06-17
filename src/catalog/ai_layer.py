#!/usr/bin/env python3
"""AI metadata layer stub — indexing status and Prometheus metrics (Phase 3, Section 7)."""

from __future__ import annotations

import logging
import time
from typing import Literal

from catalog.db import CatalogConfig, connect_catalog, load_catalog_config
from metrics.ai_registry import (
    ai_datasets_indexed,
    ai_datasets_total,
    ai_indexing_status,
    ai_last_index_run_timestamp_seconds,
)

logger = logging.getLogger(__name__)

IndexingStatus = Literal["pending", "indexed", "failed"]
INDEXING_STATUSES: tuple[IndexingStatus, ...] = ("pending", "indexed", "failed")
DEFAULT_INDEXING_STATUS: IndexingStatus = "pending"

_AI_SCHEMA_MIGRATION = """
ALTER TABLE ai_metadata ADD COLUMN IF NOT EXISTS indexing_status VARCHAR DEFAULT 'pending'
"""


def upgrade_ai_metadata_schema(connection) -> None:
    connection.execute(_AI_SCHEMA_MIGRATION)
    connection.execute(
        """
        UPDATE ai_metadata
        SET indexing_status = 'pending'
        WHERE indexing_status IS NULL
        """
    )


def ensure_dataset_ai_metadata(connection, dataset_id: str) -> None:
    """Ensure a dataset has an ai_metadata row with pending indexing status."""
    upgrade_ai_metadata_schema(connection)
    connection.execute(
        """
        INSERT INTO ai_metadata (dataset_id, summary, tags, use_cases, indexing_status)
        VALUES (?, '', '', '', ?)
        ON CONFLICT (dataset_id) DO NOTHING
        """,
        [dataset_id, DEFAULT_INDEXING_STATUS],
    )


def mark_dataset_indexed(
    dataset_id: str,
    *,
    summary: str = "",
    tags: str = "",
    use_cases: str = "",
    config: CatalogConfig | None = None,
) -> None:
    """Mark a dataset as indexed — hook for a future LLM/indexer implementation."""
    cfg = config or load_catalog_config()
    connection = connect_catalog(cfg)
    try:
        upgrade_ai_metadata_schema(connection)
        connection.execute(
            """
            INSERT INTO ai_metadata (dataset_id, summary, tags, use_cases, indexing_status)
            VALUES (?, ?, ?, ?, 'indexed')
            ON CONFLICT (dataset_id) DO UPDATE SET
                summary = excluded.summary,
                tags = excluded.tags,
                use_cases = excluded.use_cases,
                indexing_status = 'indexed'
            """,
            [dataset_id, summary, tags, use_cases],
        )
        ai_last_index_run_timestamp_seconds.set(time.time())
    finally:
        connection.close()


def mark_dataset_index_failed(dataset_id: str, *, config: CatalogConfig | None = None) -> None:
    cfg = config or load_catalog_config()
    connection = connect_catalog(cfg)
    try:
        upgrade_ai_metadata_schema(connection)
        connection.execute(
            """
            UPDATE ai_metadata SET indexing_status = 'failed' WHERE dataset_id = ?
            """,
            [dataset_id],
        )
        ai_last_index_run_timestamp_seconds.set(time.time())
    finally:
        connection.close()


_seen_ai_status: set[tuple[str, str]] = set()


def publish_ai_metrics(*, config: CatalogConfig | None = None) -> None:
    """Read ai_metadata from DuckDB and publish Prometheus AI layer metrics."""
    global _seen_ai_status

    cfg = config or load_catalog_config()
    connection = connect_catalog(cfg)
    try:
        upgrade_ai_metadata_schema(connection)
        rows = connection.execute(
            """
            SELECT dataset_id, indexing_status
            FROM ai_metadata
            ORDER BY dataset_id
            """
        ).fetchall()
    finally:
        connection.close()

    current_status: set[tuple[str, str]] = set()
    indexed_count = 0

    for dataset_id, raw_status in rows:
        status = str(raw_status or DEFAULT_INDEXING_STATUS)
        if status not in INDEXING_STATUSES:
            status = DEFAULT_INDEXING_STATUS
        if status == "indexed":
            indexed_count += 1
        current_status.add((dataset_id, status))
        ai_indexing_status.labels(dataset_id=dataset_id, status=status).set(1)

    for dataset_id, status in _seen_ai_status - current_status:
        ai_indexing_status.labels(dataset_id=dataset_id, status=status).set(0)

    _seen_ai_status = current_status
    ai_datasets_total.set(len(rows))
    ai_datasets_indexed.set(indexed_count)

    logger.info(
        "AI metadata metrics published: total=%s indexed=%s pending=%s",
        len(rows),
        indexed_count,
        len(rows) - indexed_count,
    )
