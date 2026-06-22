#!/usr/bin/env python3
"""Sync parquet datasets into catalog.duckdb (Phase 3, Section 6)."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from catalog.ai_layer import ensure_dataset_ai_metadata, publish_ai_metrics
from catalog.db import CatalogConfig, connect_catalog, load_catalog_config
from catalog.schema import initialize_schema
from datasets import (
    dataset_id as build_dataset_id,
    dataset_relative_path,
    discover_symbol_dirs,
    symbol_parquet_root,
)
from metrics.catalog_registry import (
    catalog_dataset_files,
    catalog_dataset_rows,
    catalog_schema_changes_total,
    catalog_schema_columns,
)
from metrics.pipeline_registry import (
    catalog_datasets_discovered,
    catalog_last_sync_timestamp,
    catalog_metadata_extraction_failures_total,
)

logger = logging.getLogger(__name__)

DEFAULT_SYNC_INTERVAL_SEC = 300.0


@dataclass(frozen=True)
class DatasetKey:
    exchange: str
    category: str
    symbol: str

    @property
    def dataset_id(self) -> str:
        return build_dataset_id(self.exchange, self.category, self.symbol)

    @property
    def relative_path(self) -> str:
        return dataset_relative_path(self.exchange, self.category, self.symbol)


def _discover_parquet_files(data_root: Path) -> dict[DatasetKey, list[Path]]:
    grouped: dict[DatasetKey, list[Path]] = {}
    for exchange, category, symbol in discover_symbol_dirs(data_root):
        symbol_root = symbol_parquet_root(data_root, exchange, category, symbol)
        files = sorted(symbol_root.glob("*.parquet"))
        if not files:
            continue
        key = DatasetKey(exchange=exchange, category=category, symbol=symbol)
        grouped[key] = files
    return grouped


def _file_stats(parquet_path: Path) -> tuple[int, int, int, int] | None:
    try:
        frame = pd.read_parquet(parquet_path, columns=["timestamp"])
    except Exception:
        logger.exception("Failed to read timestamps from %s", parquet_path)
        return None

    if frame.empty:
        return 0, parquet_path.stat().st_size, 0, 0

    timestamps = frame["timestamp"]
    return (
        len(frame),
        parquet_path.stat().st_size,
        int(timestamps.min()),
        int(timestamps.max()),
    )


def _schema_rows(dataset_id: str, parquet_path: Path, schema_version: int) -> list[tuple]:
    try:
        arrow_schema = pq.read_schema(parquet_path)
    except Exception:
        logger.exception("Failed to read schema from %s", parquet_path)
        return []

    return [
        (dataset_id, schema_version, field.name, str(field.type))
        for field in arrow_schema
    ]


def _publish_catalog_metrics(
    *,
    grouped: dict[DatasetKey, list[Path]],
    dataset_totals: dict[str, tuple[int, int]],
    schema_by_dataset: dict[str, list[tuple[str, str]]],
) -> None:
    global _seen_datasets, _seen_schema_columns

    current_datasets: set[tuple[str, str, str]] = set()
    current_schema: set[tuple[str, str, str]] = set()

    for key, parquet_files in grouped.items():
        file_count, total_rows = dataset_totals.get(key.dataset_id, (len(parquet_files), 0))
        current_datasets.add((key.dataset_id, key.category, key.symbol))
        catalog_dataset_files.labels(
            dataset_id=key.dataset_id,
            category=key.category,
            symbol=key.symbol,
        ).set(file_count)
        catalog_dataset_rows.labels(
            dataset_id=key.dataset_id,
            category=key.category,
            symbol=key.symbol,
        ).set(total_rows)

        for column_name, data_type in schema_by_dataset.get(key.dataset_id, []):
            current_schema.add((key.dataset_id, column_name, data_type))
            catalog_schema_columns.labels(
                dataset_id=key.dataset_id,
                column_name=column_name,
                data_type=data_type,
            ).set(1)

    for dataset_id, category, symbol in _seen_datasets - current_datasets:
        catalog_dataset_files.labels(
            dataset_id=dataset_id,
            category=category,
            symbol=symbol,
        ).set(0)
        catalog_dataset_rows.labels(
            dataset_id=dataset_id,
            category=category,
            symbol=symbol,
        ).set(0)

    for dataset_id, column_name, data_type in _seen_schema_columns - current_schema:
        catalog_schema_columns.labels(
            dataset_id=dataset_id,
            column_name=column_name,
            data_type=data_type,
        ).set(0)

    _seen_datasets = current_datasets
    _seen_schema_columns = current_schema


_seen_datasets: set[tuple[str, str, str]] = set()
_seen_schema_columns: set[tuple[str, str, str]] = set()


def _delete_orphaned_datasets(connection, synced_ids: set[str]) -> None:
    """Remove catalog rows for datasets no longer present on disk."""
    if synced_ids:
        placeholders = ", ".join("?" for _ in synced_ids)
        orphan_filter = f"dataset_id NOT IN ({placeholders})"
        params = list(synced_ids)
    else:
        orphan_filter = "1=1"
        params = []

    for table in ("ai_metadata", "file_stats", "schema_registry"):
        connection.execute(f"DELETE FROM {table} WHERE {orphan_filter}", params)

    if synced_ids:
        connection.execute(
            f"DELETE FROM datasets WHERE dataset_id NOT IN ({placeholders})",
            list(synced_ids),
        )
    else:
        connection.execute("DELETE FROM datasets")


def sync_catalog(*, config: CatalogConfig | None = None) -> int:
    """Scan parquet datasets and upsert catalog tables. Returns datasets synced."""
    cfg = config or load_catalog_config()
    grouped = _discover_parquet_files(cfg.data_root)
    schema_by_dataset: dict[str, list[tuple[str, str]]] = {}
    dataset_totals: dict[str, tuple[int, int]] = {}

    connection = connect_catalog(cfg)
    try:
        initialize_schema(connection)
        synced_ids: set[str] = set()

        for key, parquet_files in grouped.items():
            total_rows = 0
            successful_files = 0
            synced_ids.add(key.dataset_id)
            connection.execute(
                """
                INSERT OR REPLACE INTO datasets
                    (dataset_id, dataset_type, exchange, symbol, path)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    key.dataset_id,
                    key.category,
                    key.exchange,
                    key.symbol,
                    key.relative_path,
                ],
            )

            connection.execute(
                "DELETE FROM file_stats WHERE dataset_id = ?",
                [key.dataset_id],
            )
            old_schema_rows = connection.execute(
                """
                SELECT column_name, data_type FROM schema_registry
                WHERE dataset_id = ? AND schema_version = ?
                """,
                [key.dataset_id, cfg.schema_version],
            ).fetchall()
            old_schema = frozenset(old_schema_rows)

            connection.execute(
                "DELETE FROM schema_registry WHERE dataset_id = ? AND schema_version = ?",
                [key.dataset_id, cfg.schema_version],
            )

            schema_rows: list[tuple] = []
            for parquet_path in parquet_files:
                stats = _file_stats(parquet_path)
                if stats is None:
                    catalog_metadata_extraction_failures_total.inc()
                    continue

                rows, file_size, start_ts, end_ts = stats
                total_rows += rows
                successful_files += 1
                rel_file = parquet_path.relative_to(cfg.project_root).as_posix()
                connection.execute(
                    """
                    INSERT OR REPLACE INTO file_stats
                        (dataset_id, file_path, rows, file_size, start_timestamp, end_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [key.dataset_id, rel_file, rows, file_size, start_ts, end_ts],
                )

                if not schema_rows:
                    schema_rows = _schema_rows(
                        key.dataset_id,
                        parquet_path,
                        cfg.schema_version,
                    )

            for row in schema_rows:
                connection.execute(
                    """
                    INSERT OR REPLACE INTO schema_registry
                        (dataset_id, schema_version, column_name, data_type)
                    VALUES (?, ?, ?, ?)
                    """,
                    list(row),
                )
            schema_by_dataset[key.dataset_id] = [
                (column_name, data_type) for _, _, column_name, data_type in schema_rows
            ]
            new_schema = frozenset(schema_by_dataset[key.dataset_id])
            if old_schema and new_schema != old_schema:
                catalog_schema_changes_total.labels(dataset_id=key.dataset_id).inc()
                logger.warning("Schema change detected for dataset %s", key.dataset_id)
            dataset_totals[key.dataset_id] = (successful_files, total_rows)

            ensure_dataset_ai_metadata(connection, key.dataset_id)

        if synced_ids:
            _delete_orphaned_datasets(connection, synced_ids)
        else:
            _delete_orphaned_datasets(connection, set())

        dataset_count = connection.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
    finally:
        connection.close()

    _publish_catalog_metrics(
        grouped=grouped,
        dataset_totals=dataset_totals,
        schema_by_dataset=schema_by_dataset,
    )
    publish_ai_metrics(config=cfg)
    catalog_last_sync_timestamp.set(time.time())
    catalog_datasets_discovered.set(int(dataset_count))
    logger.info("Catalog sync complete: datasets=%s", dataset_count)
    return int(dataset_count)


class CatalogSyncPoller:
    def __init__(
        self,
        *,
        config: CatalogConfig | None = None,
        interval_sec: float = DEFAULT_SYNC_INTERVAL_SEC,
    ) -> None:
        self.config = config
        self.interval_sec = interval_sec
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="catalog-sync-poller",
            daemon=True,
        )
        self._thread.start()
        logger.info("Catalog sync poller started (interval=%ss)", self.interval_sec)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.interval_sec + 1)
        logger.info("Catalog sync poller stopped")

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                sync_catalog(config=self.config)
            except Exception:
                logger.exception("Catalog sync failed")
            self._stop_event.wait(self.interval_sec)


def start_catalog_poller(
    *,
    config: CatalogConfig | None = None,
    interval_sec: float = DEFAULT_SYNC_INTERVAL_SEC,
) -> CatalogSyncPoller:
    poller = CatalogSyncPoller(config=config, interval_sec=interval_sec)
    poller.start()
    return poller
