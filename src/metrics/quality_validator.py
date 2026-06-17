#!/usr/bin/env python3
"""Validate parquet datasets and publish data quality metrics (Phase 3, Section 5)."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from metrics.quality_registry import (
    quality_rows_validated,
    quality_score,
    quality_validation_failures,
)

logger = logging.getLogger(__name__)

DEFAULT_SCAN_INTERVAL_SEC = 300.0
CATEGORIES = ("trades", "liquidations")

TRADE_REQUIRED_COLUMNS = [
    "timestamp",
    "symbol",
    "agg_trade_id",
    "price",
    "amount",
    "side",
    "exchange",
]
LIQUIDATION_REQUIRED_COLUMNS = [
    "timestamp",
    "symbol",
    "side",
    "price",
    "avg_price",
    "quantity",
    "filled_qty",
    "exchange",
]

ValidatorFunc = Callable[[pd.DataFrame], int]


@dataclass(frozen=True)
class RegisteredValidator:
    check_name: str
    validate: ValidatorFunc


def _count_monotonic_timestamp_violations(frame: pd.DataFrame) -> int:
    if len(frame) < 2:
        return 0
    return int((frame["timestamp"].diff() < 0).sum())


def _count_duplicate_trade_ids(frame: pd.DataFrame) -> int:
    if frame.empty or "agg_trade_id" not in frame.columns:
        return 0
    duplicated = frame["agg_trade_id"].duplicated(keep=False)
    return int(duplicated.sum())


def _count_null_field_rows(frame: pd.DataFrame, columns: list[str]) -> int:
    if frame.empty:
        return 0
    present = [column for column in columns if column in frame.columns]
    if not present:
        return 0
    return int(frame[present].isnull().any(axis=1).sum())


def _count_non_positive_quantity(frame: pd.DataFrame) -> int:
    if frame.empty or "quantity" not in frame.columns:
        return 0
    return int((frame["quantity"] <= 0).sum())


def _count_non_positive_price(frame: pd.DataFrame) -> int:
    if frame.empty or "price" not in frame.columns:
        return 0
    return int((frame["price"] <= 0).sum())


def _trade_null_fields(frame: pd.DataFrame) -> int:
    return _count_null_field_rows(frame, TRADE_REQUIRED_COLUMNS)


def _liquidation_null_fields(frame: pd.DataFrame) -> int:
    return _count_null_field_rows(frame, LIQUIDATION_REQUIRED_COLUMNS)


_BUILTIN_VALIDATORS: dict[str, list[RegisteredValidator]] = {
    "trades": [
        RegisteredValidator("monotonic_timestamps", _count_monotonic_timestamp_violations),
        RegisteredValidator("duplicates", _count_duplicate_trade_ids),
        RegisteredValidator("null_fields", _trade_null_fields),
    ],
    "liquidations": [
        RegisteredValidator("positive_quantity", _count_non_positive_quantity),
        RegisteredValidator("positive_price", _count_non_positive_price),
        RegisteredValidator("null_fields", _liquidation_null_fields),
    ],
}

_custom_validators: dict[str, list[RegisteredValidator]] = {}


def register_validator(
    category: str,
    check_name: str,
    validator: ValidatorFunc,
) -> None:
    """Register a custom validator for a dataset category."""
    _custom_validators.setdefault(category, []).append(
        RegisteredValidator(check_name=check_name, validate=validator)
    )
    logger.info("Registered quality validator: category=%s check=%s", category, check_name)


def validators_for_category(category: str) -> list[RegisteredValidator]:
    return [
        *_BUILTIN_VALIDATORS.get(category, []),
        *_custom_validators.get(category, []),
    ]


def _discover_datasets(data_root: Path) -> list[tuple[str, str]]:
    datasets: list[tuple[str, str]] = []
    for category in CATEGORIES:
        category_root = data_root / category / "binance_futures"
        if not category_root.exists():
            continue
        for symbol_dir in sorted(category_root.iterdir()):
            if symbol_dir.is_dir():
                datasets.append((category, symbol_dir.name))
    return datasets


def _read_parquet(path: Path) -> pd.DataFrame | None:
    try:
        return pd.read_parquet(path)
    except Exception:
        logger.exception("Failed to read parquet for quality validation: %s", path)
        return None


def _validate_dataset(
    data_root: Path,
    category: str,
    symbol: str,
) -> tuple[int, dict[str, int]]:
    symbol_root = data_root / category / "binance_futures" / symbol
    if not symbol_root.exists():
        return 0, {}

    checks = validators_for_category(category)
    failure_counts = {check.check_name: 0 for check in checks}
    total_rows = 0

    for parquet_path in sorted(symbol_root.glob("*.parquet")):
        frame = _read_parquet(parquet_path)
        if frame is None or frame.empty:
            continue

        total_rows += len(frame)
        for check in checks:
            failure_counts[check.check_name] += check.validate(frame)

    return total_rows, failure_counts


def _compute_quality_score(total_rows: int, failure_counts: dict[str, int]) -> float:
    if total_rows <= 0:
        return 100.0

    total_failures = sum(failure_counts.values())
    score = 100.0 * max(0.0, 1.0 - (total_failures / total_rows))
    return min(100.0, score)


def _zero_dataset_metrics(category: str, symbol: str) -> None:
    quality_score.labels(category=category, symbol=symbol).set(100.0)
    quality_rows_validated.labels(category=category, symbol=symbol).set(0)


_seen_datasets: set[tuple[str, str]] = set()
_seen_checks: set[tuple[str, str, str]] = set()


def collect_quality_metrics(*, data_root: Path) -> None:
    """Scan parquet datasets once and update Prometheus quality gauges."""
    global _seen_datasets, _seen_checks

    current_datasets: set[tuple[str, str]] = set()
    current_checks: set[tuple[str, str, str]] = set()

    for category, symbol in _discover_datasets(data_root):
        current_datasets.add((category, symbol))
        total_rows, failure_counts = _validate_dataset(data_root, category, symbol)

        if total_rows == 0:
            _zero_dataset_metrics(category, symbol)
            continue

        for check_name, count in failure_counts.items():
            current_checks.add((category, symbol, check_name))
            quality_validation_failures.labels(
                category=category,
                symbol=symbol,
                check=check_name,
            ).set(count)

        quality_rows_validated.labels(category=category, symbol=symbol).set(total_rows)
        quality_score.labels(category=category, symbol=symbol).set(
            _compute_quality_score(total_rows, failure_counts)
        )

    _clear_stale_labels(
        current_datasets=current_datasets,
        current_checks=current_checks,
    )


def _clear_stale_labels(
    *,
    current_datasets: set[tuple[str, str]],
    current_checks: set[tuple[str, str, str]],
) -> None:
    global _seen_datasets, _seen_checks

    for category, symbol in _seen_datasets - current_datasets:
        _zero_dataset_metrics(category, symbol)

    for category, symbol, check in _seen_checks - current_checks:
        quality_validation_failures.labels(
            category=category,
            symbol=symbol,
            check=check,
        ).set(0)

    _seen_datasets = current_datasets
    _seen_checks = current_checks


class QualityMetricsPoller:
    def __init__(
        self,
        *,
        data_root: Path,
        interval_sec: float = DEFAULT_SCAN_INTERVAL_SEC,
    ) -> None:
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
            name="quality-metrics-poller",
            daemon=True,
        )
        self._thread.start()
        logger.info("Quality metrics poller started (interval=%ss)", self.interval_sec)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.interval_sec + 1)
        logger.info("Quality metrics poller stopped")

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                collect_quality_metrics(data_root=self.data_root)
            except Exception:
                logger.exception("Quality metrics scan failed")
            self._stop_event.wait(self.interval_sec)


def start_quality_poller(
    *,
    data_root: Path,
    interval_sec: float = DEFAULT_SCAN_INTERVAL_SEC,
) -> QualityMetricsPoller:
    poller = QualityMetricsPoller(
        data_root=data_root,
        interval_sec=interval_sec,
    )
    poller.start()
    return poller
