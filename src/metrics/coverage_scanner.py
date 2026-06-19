#!/usr/bin/env python3
"""Scan parquet datasets and publish data coverage metrics (Phase 3, Section 4)."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

from datasets import discover_metric_datasets, symbol_parquet_root
from metrics.coverage_registry import (
    coverage_day_present,
    coverage_first_timestamp_seconds,
    coverage_hour_present,
    coverage_last_timestamp_seconds,
    coverage_missing_days_total,
    coverage_missing_hours_total,
    coverage_percent,
)

logger = logging.getLogger(__name__)

DEFAULT_SCAN_INTERVAL_SEC = 300.0
HOUR_TRACKING_LOOKBACK_DAYS = 14


@dataclass
class DayCoverage:
    day: str
    hours_present: set[int] = field(default_factory=set)
    has_file: bool = False


def _parse_day_from_filename(path: Path) -> str | None:
    stem = path.stem
    try:
        datetime.strptime(stem, "%Y-%m-%d")
    except ValueError:
        return None
    return stem


def _hours_from_timestamps_ms(timestamps_ms: pd.Series) -> set[int]:
    if timestamps_ms.empty:
        return set()
    hours = pd.to_datetime(timestamps_ms, unit="ms", utc=True).dt.hour
    return {int(hour) for hour in hours.unique()}


def _read_parquet_coverage(parquet_path: Path) -> tuple[set[int], int, int] | None:
    try:
        frame = pd.read_parquet(parquet_path, columns=["timestamp"])
    except Exception:
        logger.exception("Failed to read coverage from parquet: %s", parquet_path)
        return None

    if frame.empty:
        return set(), 0, 0

    timestamps = frame["timestamp"]
    return (
        _hours_from_timestamps_ms(timestamps),
        int(timestamps.min()),
        int(timestamps.max()),
    )


def _discover_datasets(data_root: Path) -> list[tuple[str, str, str]]:
    return discover_metric_datasets(data_root)


def _day_range(start_day: str, end_day: str) -> list[str]:
    start = datetime.strptime(start_day, "%Y-%m-%d").replace(tzinfo=UTC)
    end = datetime.strptime(end_day, "%Y-%m-%d").replace(tzinfo=UTC)
    days: list[str] = []
    current = start
    while current <= end:
        days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return days


def _expected_hours_for_day(
    day: str,
    *,
    first_ts: datetime,
    last_ts: datetime,
) -> set[int]:
    day_start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=UTC)
    day_end = day_start + timedelta(days=1)

    range_start = max(first_ts, day_start)
    range_end = min(last_ts, day_end - timedelta(microseconds=1))

    if range_start > range_end:
        return set()

    if range_start.date() == range_end.date():
        return set(range(range_start.hour, range_end.hour + 1))

    return set(range(range_start.hour, 24)) | set(range(0, range_end.hour + 1))


def _scan_dataset(
    data_root: Path,
    exchange: str,
    category: str,
    symbol: str,
) -> tuple[datetime | None, datetime | None, dict[str, DayCoverage]]:
    symbol_root = symbol_parquet_root(data_root, exchange, category, symbol)
    if not symbol_root.exists():
        return None, None, {}

    day_map: dict[str, DayCoverage] = {}
    first_ts: datetime | None = None
    last_ts: datetime | None = None

    for parquet_path in sorted(symbol_root.glob("*.parquet")):
        day = _parse_day_from_filename(parquet_path)
        if day is None:
            continue

        parsed = _read_parquet_coverage(parquet_path)
        if parsed is None:
            continue

        hours_present, min_ms, max_ms = parsed
        if min_ms == 0 and max_ms == 0:
            day_map[day] = DayCoverage(day=day, hours_present=set(), has_file=True)
            continue

        file_first = datetime.fromtimestamp(min_ms / 1000, tz=UTC)
        file_last = datetime.fromtimestamp(max_ms / 1000, tz=UTC)
        first_ts = file_first if first_ts is None else min(first_ts, file_first)
        last_ts = file_last if last_ts is None else max(last_ts, file_last)

        day_map[day] = DayCoverage(
            day=day,
            hours_present=hours_present,
            has_file=True,
        )

    return first_ts, last_ts, day_map


def _compute_coverage(
    *,
    category: str,
    symbol: str,
    first_ts: datetime,
    last_ts: datetime,
    day_map: dict[str, DayCoverage],
    hour_tracking_cutoff: str,
) -> tuple[int, int, float, set[tuple[str, int]], set[str]]:
    first_day = first_ts.strftime("%Y-%m-%d")
    last_day = last_ts.strftime("%Y-%m-%d")
    all_days = _day_range(first_day, last_day)

    missing_days = 0
    missing_hours = 0
    expected_hours = 0
    hour_labels: set[tuple[str, int]] = set()
    day_labels: set[str] = set()

    for day in all_days:
        expected_for_day = _expected_hours_for_day(
            day,
            first_ts=first_ts,
            last_ts=last_ts,
        )
        if not expected_for_day:
            continue

        day_labels.add(day)
        coverage = day_map.get(day)
        present_hours = coverage.hours_present if coverage else set()
        day_missing_hours = expected_for_day - present_hours
        missing_hours += len(day_missing_hours)

        has_file = coverage is not None and coverage.has_file
        if not has_file:
            missing_days += 1
            day_value = 0
        else:
            day_value = 1 if not day_missing_hours else 0

        coverage_day_present.labels(
            category=category,
            symbol=symbol,
            day=day,
        ).set(day_value)

        if day >= hour_tracking_cutoff:
            for hour in sorted(expected_for_day):
                hour_labels.add((day, hour))
                is_present = 1 if hour in present_hours else 0
                coverage_hour_present.labels(
                    category=category,
                    symbol=symbol,
                    day=day,
                    hour=str(hour),
                ).set(is_present)

        expected_hours += len(expected_for_day)

    percent = 100.0
    if expected_hours > 0:
        percent = ((expected_hours - missing_hours) / expected_hours) * 100.0

    return missing_days, missing_hours, percent, hour_labels, day_labels


def _zero_dataset_metrics(category: str, symbol: str) -> None:
    coverage_first_timestamp_seconds.labels(category=category, symbol=symbol).set(0)
    coverage_last_timestamp_seconds.labels(category=category, symbol=symbol).set(0)
    coverage_missing_days_total.labels(category=category, symbol=symbol).set(0)
    coverage_missing_hours_total.labels(category=category, symbol=symbol).set(0)
    coverage_percent.labels(category=category, symbol=symbol).set(0)


def collect_coverage_metrics(*, data_root: Path) -> None:
    """Scan parquet datasets once and update Prometheus coverage gauges."""
    global _seen_days, _seen_hours, _seen_datasets

    current_datasets: set[tuple[str, str]] = set()
    current_days: set[tuple[str, str, str]] = set()
    current_hours: set[tuple[str, str, str, str]] = set()
    hour_tracking_cutoff = (
        datetime.now(UTC) - timedelta(days=HOUR_TRACKING_LOOKBACK_DAYS)
    ).strftime("%Y-%m-%d")

    for exchange, category, symbol in _discover_datasets(data_root):
        current_datasets.add((category, symbol))
        first_ts, last_ts, day_map = _scan_dataset(data_root, exchange, category, symbol)

        if first_ts is None or last_ts is None:
            _zero_dataset_metrics(category, symbol)
            continue

        coverage_first_timestamp_seconds.labels(
            category=category,
            symbol=symbol,
        ).set(first_ts.timestamp())
        coverage_last_timestamp_seconds.labels(
            category=category,
            symbol=symbol,
        ).set(last_ts.timestamp())

        missing_days, missing_hours, percent, hour_labels, day_labels = _compute_coverage(
            category=category,
            symbol=symbol,
            first_ts=first_ts,
            last_ts=last_ts,
            day_map=day_map,
            hour_tracking_cutoff=hour_tracking_cutoff,
        )

        coverage_missing_days_total.labels(category=category, symbol=symbol).set(
            missing_days
        )
        coverage_missing_hours_total.labels(category=category, symbol=symbol).set(
            missing_hours
        )
        coverage_percent.labels(category=category, symbol=symbol).set(percent)

        for day in day_labels:
            current_days.add((category, symbol, day))
        for day, hour in hour_labels:
            current_hours.add((category, symbol, day, str(hour)))

    _clear_stale_labels(
        current_datasets=current_datasets,
        current_days=current_days,
        current_hours=current_hours,
    )


_seen_datasets: set[tuple[str, str]] = set()
_seen_days: set[tuple[str, str, str]] = set()
_seen_hours: set[tuple[str, str, str, str]] = set()


def _clear_stale_labels(
    *,
    current_datasets: set[tuple[str, str]],
    current_days: set[tuple[str, str, str]],
    current_hours: set[tuple[str, str, str, str]],
) -> None:
    global _seen_datasets, _seen_days, _seen_hours

    for category, symbol in _seen_datasets - current_datasets:
        _zero_dataset_metrics(category, symbol)

    for category, symbol, day in _seen_days - current_days:
        coverage_day_present.labels(category=category, symbol=symbol, day=day).set(0)

    for category, symbol, day, hour in _seen_hours - current_hours:
        coverage_hour_present.labels(
            category=category,
            symbol=symbol,
            day=day,
            hour=hour,
        ).set(0)

    _seen_datasets = current_datasets
    _seen_days = current_days
    _seen_hours = current_hours


class CoverageMetricsPoller:
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
            name="coverage-metrics-poller",
            daemon=True,
        )
        self._thread.start()
        logger.info("Coverage metrics poller started (interval=%ss)", self.interval_sec)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.interval_sec + 1)
        logger.info("Coverage metrics poller stopped")

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                collect_coverage_metrics(data_root=self.data_root)
            except Exception:
                logger.exception("Coverage metrics scan failed")
            self._stop_event.wait(self.interval_sec)


def start_coverage_poller(
    *,
    data_root: Path,
    interval_sec: float = DEFAULT_SCAN_INTERVAL_SEC,
) -> CoverageMetricsPoller:
    poller = CoverageMetricsPoller(
        data_root=data_root,
        interval_sec=interval_sec,
    )
    poller.start()
    return poller
