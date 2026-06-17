#!/usr/bin/env python3
"""Scan raw/ and data/ storage and publish Prometheus metrics (Phase 3, Section 3)."""

from __future__ import annotations

import logging
import shutil
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from metrics.storage_registry import (
    storage_daily_growth_bytes,
    storage_free_bytes,
    storage_largest_dataset_bytes,
    storage_parquet_file_count,
    storage_raw_file_count,
    storage_total_bytes,
    storage_used_percent,
)

logger = logging.getLogger(__name__)

DEFAULT_SCAN_INTERVAL_SEC = 300.0
GROWTH_WINDOW = timedelta(hours=24)


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total


def _count_files(path: Path, suffix: str) -> int:
    if not path.exists():
        return 0
    return sum(1 for item in path.rglob(f"*{suffix}") if item.is_file())


def _bytes_added_since(path: Path, since: datetime) -> int:
    if not path.exists():
        return 0

    since_ts = since.timestamp()
    total = 0
    for item in path.rglob("*"):
        if item.is_file() and item.stat().st_mtime >= since_ts:
            total += item.stat().st_size
    return total


def _largest_symbol_dirs(data_root: Path, category: str) -> list[tuple[str, int]]:
    category_root = data_root / category / "binance_futures"
    if not category_root.exists():
        return []

    sizes: list[tuple[str, int]] = []
    for symbol_dir in category_root.iterdir():
        if not symbol_dir.is_dir():
            continue
        sizes.append((symbol_dir.name, _dir_size(symbol_dir)))

    sizes.sort(key=lambda item: item[1], reverse=True)
    return sizes


def collect_storage_metrics(
    *,
    project_root: Path,
    raw_root: Path,
    data_root: Path,
) -> None:
    """Scan storage paths once and update Prometheus gauges."""
    raw_bytes = _dir_size(raw_root)
    data_bytes = _dir_size(data_root)

    storage_total_bytes.labels(path="raw").set(raw_bytes)
    storage_total_bytes.labels(path="data").set(data_bytes)

    storage_raw_file_count.set(_count_files(raw_root, ".jsonl"))
    storage_parquet_file_count.set(_count_files(data_root, ".parquet"))

    since = datetime.now(UTC) - GROWTH_WINDOW
    storage_daily_growth_bytes.labels(path="raw").set(
        _bytes_added_since(raw_root, since)
    )
    storage_daily_growth_bytes.labels(path="data").set(
        _bytes_added_since(data_root, since)
    )

    usage = shutil.disk_usage(project_root)
    storage_free_bytes.set(usage.free)
    if usage.total > 0:
        storage_used_percent.set((usage.used / usage.total) * 100.0)

    _update_largest_datasets(data_root)


_seen_largest: set[tuple[str, str]] = set()


def _update_largest_datasets(data_root: Path) -> None:
    global _seen_largest

    current: set[tuple[str, str]] = set()
    for category in ("trades", "liquidations"):
        for symbol, size in _largest_symbol_dirs(data_root, category)[:5]:
            current.add((category, symbol))
            storage_largest_dataset_bytes.labels(
                category=category,
                symbol=symbol,
            ).set(size)

    for category, symbol in _seen_largest - current:
        storage_largest_dataset_bytes.labels(
            category=category,
            symbol=symbol,
        ).set(0)

    _seen_largest = current


class StorageMetricsPoller:
    def __init__(
        self,
        *,
        project_root: Path,
        raw_root: Path,
        data_root: Path,
        interval_sec: float = DEFAULT_SCAN_INTERVAL_SEC,
    ) -> None:
        self.project_root = project_root
        self.raw_root = raw_root
        self.data_root = data_root
        self.interval_sec = interval_sec
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="storage-metrics-poller",
            daemon=True,
        )
        self._thread.start()
        logger.info("Storage metrics poller started (interval=%ss)", self.interval_sec)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.interval_sec + 1)
        logger.info("Storage metrics poller stopped")

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                collect_storage_metrics(
                    project_root=self.project_root,
                    raw_root=self.raw_root,
                    data_root=self.data_root,
                )
            except Exception:
                logger.exception("Storage metrics scan failed")
            self._stop_event.wait(self.interval_sec)


def start_storage_poller(
    *,
    project_root: Path,
    raw_root: Path,
    data_root: Path,
    interval_sec: float = DEFAULT_SCAN_INTERVAL_SEC,
) -> StorageMetricsPoller:
    poller = StorageMetricsPoller(
        project_root=project_root,
        raw_root=raw_root,
        data_root=data_root,
        interval_sec=interval_sec,
    )
    poller.start()
    return poller
