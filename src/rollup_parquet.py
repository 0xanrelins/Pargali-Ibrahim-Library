#!/usr/bin/env python3
"""Build the derived rollup parquet layer (hourly + daily) from immutable batches.

``data/`` is the source of truth and is only ever read here: one immutable
parquet batch per raw JSONL file. That layout makes a day cost ~1.4k files, so a
plain ``count(*)`` over one day has to open ~1.4k parquet footers. This module
adds a derived read layer on top, and nothing else:

    data_rollup/hourly/{category}/{exchange}/{symbol}/{YYYY-MM-DD}/{HH}.parquet
    data_rollup/daily/{category}/{exchange}/{symbol}/{YYYY-MM-DD}.parquet

* **hourly** — every row whose event timestamp falls in one closed UTC hour,
  merged into one file. The hour comes from the parquet timestamp column, never
  from the file name, so a merged batch (``first--last.parquet``) that spans
  several hours contributes only its own rows to each hour. Kept only for the
  last ``--hourly-days`` days (default 7); older hourly rollups are pruned once
  the day's daily file exists.
* **daily** — the same day's hourly rollups merged. Kept for all history, and
  written only after the day has closed.

A source file is never concatenated whole: each hourly rollup selects rows by
its timestamp column (``timestamp`` in ``normalizer/schemas.py``, with
``event_time`` as a fallback for future categories). The **day** partition still
comes from the source directory (``data/.../{YYYY-MM-DD}/``), so a day's rollup
contains exactly the rows the normalizer filed under that date — including the
few rows whose timestamp falls just before/after midnight — and the daily rollup
is always the union of that day's hourly files.

Discovery is bounded: a pass reads parquet footers only for day directories
inside the hourly window and for days whose daily file is missing or older than
the day directory. Everything else in ``data/`` is not opened, because the
daily layer already covers it.

A source batch is either **empty** (zero rows — normal for a quiet minute, only
counted) or **rejected** (non-empty but impossible to bucket: unreadable, no
integer timestamp column, implausible epoch, span over two days). A rejected
batch fails the pass (non-zero exit) and blocks **every** rollup of its day, so
a daily file is never published with rows silently missing. The previous files
of a blocked day are left untouched.

An hourly file whose hour is no longer produced by the day's sources is
removed, and a planned write that yields zero rows removes its old target, so a
re-normalized batch cannot leave a stale hour behind for the daily to pick up.
The daily is merged from the hour buckets of the current pass, never from a
directory scan.

Every output is derived: delete ``data_rollup/`` and rebuild it with
``--backfill`` (or let ``--run`` rebuild it incrementally). Losing it is not
data loss.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import time
from collections import Counter
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time as day_time, timedelta, timezone
from pathlib import Path

import duckdb
import pyarrow.compute as pc
import pyarrow.parquet as pq

from normalizer.paths import project_paths
from normalizer.schemas import CATEGORY_SPECS, category_column_specs, category_columns

logger = logging.getLogger(__name__)

ROLLUP_DIR_NAME = "data_rollup"
HOURLY_LAYER = "hourly"
DAILY_LAYER = "daily"
DEFAULT_HOURLY_DAYS = 7
DEFAULT_CLOSED_MARGIN_MINUTES = 5
DEFAULT_THREADS = 2
ROLLUP_MEMORY_LIMIT = "256MB"
PARQUET_COMPRESSION = "ZSTD"
TEMP_DIR_NAME = "_tmp"
TEMP_FILE_SUFFIX = ".tmp"
STALE_TEMP_MAX_AGE_SEC = 3600
PROGRESS_EVERY_FILES = 200

UTC = timezone.utc
HOUR_MS = 3_600_000
DAY_MS = 86_400_000
# Timestamps are BIGINT epoch milliseconds (``normalizer/schemas.py``). Anything
# outside 2000-01-01 .. 2100-01-01, or a span longer than two days, is treated
# as a broken batch instead of silently landing in year 1970's hour bucket.
PLAUSIBLE_EPOCH_MS_MIN = 946_684_800_000
PLAUSIBLE_EPOCH_MS_MAX = 4_102_444_800_000
MAX_SOURCE_SPAN_DAYS = 2
MAX_SOURCE_SPAN_MS = MAX_SOURCE_SPAN_DAYS * DAY_MS

# Why a source batch could not be bucketed. ``SKIP_EMPTY`` is not a failure:
# a zero-row batch is a normal product of a quiet flush interval.
SKIP_EMPTY = "empty"
SKIP_UNREADABLE = "unreadable"
SKIP_NO_TIME_COLUMN = "missing-time-column"
SKIP_NON_INTEGER_TIME = "non-integer-time"
SKIP_INVERTED_SPAN = "inverted-span"
SKIP_IMPLAUSIBLE_SPAN = "implausible-span"
SKIP_SPAN_TOO_LONG = "span-too-long"
SKIP_ALL_NULL_TIME = "all-null-time"
# Preferred time columns, in order, mirroring ``normalizer/schemas.py``. Every
# current category starts with ``timestamp``; ``event_time`` is only a fallback
# for a future category that does not have ``timestamp``.
TIME_COLUMN_CANDIDATES: tuple[str, ...] = ("timestamp", "event_time")
_TIME_COLUMNS: dict[str, str | None] = {}


def category_time_column(category: str) -> str | None:
    """The column that carries event time for ``category``, or ``None``.

    A category without a usable time column cannot be bucketed by timestamp, so
    the rollup skips it with a warning instead of guessing from file names.
    """
    if category in _TIME_COLUMNS:
        return _TIME_COLUMNS[category]
    columns = category_columns(category)
    chosen = next((name for name in TIME_COLUMN_CANDIDATES if name in columns), None)
    _TIME_COLUMNS[category] = chosen
    return chosen


@dataclass(frozen=True)
class SourceBatch:
    """One immutable parquet batch file from ``data/`` with its timestamp span."""

    path: Path
    mtime: float
    start_ms: int
    end_ms: int

    def hours(self) -> tuple[int, ...]:
        return hours_covered(self.start_ms, self.end_ms)


@dataclass(frozen=True)
class SourceRead:
    """Outcome of reading one parquet footer.

    ``span`` is ``(start_ms, end_ms, rows_without_time)`` when the batch can be
    bucketed. ``reason`` names why it cannot; ``SKIP_EMPTY`` is the benign case.
    """

    span: tuple[int, int, int] | None = None
    reason: str | None = None

    @property
    def usable(self) -> bool:
        return self.span is not None

    @property
    def empty(self) -> bool:
        return self.reason == SKIP_EMPTY


@dataclass(frozen=True)
class SkippedSource:
    """A non-empty source batch that could not be bucketed by timestamp."""

    path: Path
    reason: str


def hours_covered(start_ms: int, end_ms: int) -> tuple[int, ...]:
    """UTC hour-of-day values a ``[start_ms, end_ms]`` span can fall into."""
    first = start_ms // HOUR_MS
    last = end_ms // HOUR_MS
    return tuple(sorted({hour % 24 for hour in range(first, last + 1)}))


@dataclass(frozen=True)
class DayGroup:
    """All batches of one dataset for one day directory, grouped per hour."""

    category: str
    exchange: str
    symbol: str
    day: date
    sources: tuple[SourceBatch, ...]
    hours: Mapping[int, tuple[SourceBatch, ...]]
    rejected: tuple[SkippedSource, ...] = ()

    def newest_mtime(self) -> float:
        return max(batch.mtime for batch in self.sources)


@dataclass(frozen=True)
class HourlyTask:
    """One hourly rollup to write."""

    category: str
    exchange: str
    symbol: str
    day: date
    hour: int
    target: Path
    sources: tuple[Path, ...]


@dataclass(frozen=True)
class DailyTask:
    """One daily rollup to write, merged from the day's live hourly files.

    ``hours`` is the set of hour buckets the day's current sources produce;
    only those files are merged, so a stale ``HH.parquet`` left in the
    directory can never leak into the daily. ``force`` means the daily is
    missing or older than its newest source and must be written even if this
    pass did not change any hourly file. Without it, the daily is only rebuilt
    when this pass changed the day's hourly layer (wrote or removed a file).
    """

    category: str
    exchange: str
    symbol: str
    day: date
    target: Path
    hourly_dir: Path
    hours: tuple[int, ...] = ()
    force: bool = False


@dataclass(frozen=True)
class StaleHourly:
    """An hourly rollup whose hour no longer has any source row."""

    category: str
    exchange: str
    symbol: str
    day: date
    hour: int
    target: Path


@dataclass
class RollupPlan:
    """What one pass would do (dry-run) or is about to do."""

    hourly_tasks: list[HourlyTask] = field(default_factory=list)
    daily_tasks: list[DailyTask] = field(default_factory=list)
    stale_hourly: list[StaleHourly] = field(default_factory=list)
    prune_dirs: list[Path] = field(default_factory=list)
    prune_before: date | None = None
    hours_skipped_fresh: int = 0
    hours_skipped_open: int = 0
    days_skipped_fresh: int = 0
    days_skipped_open: int = 0
    days_blocked: int = 0

    def summary(self) -> dict[str, int]:
        return {
            "hourly_to_write": len(self.hourly_tasks),
            "daily_to_write": len(self.daily_tasks),
            "hourly_stale_to_delete": len(self.stale_hourly),
            # Plan-time preview: a pass can add transient day dirs it prunes again.
            "hourly_dirs_pending_prune": len(self.prune_dirs),
            "hours_skipped_fresh": self.hours_skipped_fresh,
            "hours_skipped_open": self.hours_skipped_open,
            "days_skipped_fresh": self.days_skipped_fresh,
            "days_skipped_open": self.days_skipped_open,
            "days_blocked": self.days_blocked,
        }


@dataclass
class RollupStats:
    """Counters for one pass."""

    hourly_written: int = 0
    hourly_empty_skipped: int = 0
    hourly_stale_deleted: int = 0
    hourly_empty_targets_deleted: int = 0
    hourly_failed: int = 0
    hourly_rows: int = 0
    hourly_bytes: int = 0
    daily_written: int = 0
    daily_failed: int = 0
    daily_rows: int = 0
    daily_bytes: int = 0
    daily_skipped_empty: int = 0
    daily_skipped_unchanged: int = 0
    daily_empty_targets_deleted: int = 0
    sources_empty: int = 0
    sources_skipped: int = 0
    sources_scanned: int = 0
    days_scanned: int = 0
    days_gated_out: int = 0
    days_blocked: int = 0
    hourly_dirs_pruned: int = 0
    temp_files_cleaned: int = 0
    hours_skipped_fresh: int = 0
    hours_skipped_open: int = 0
    days_skipped_fresh: int = 0
    days_skipped_open: int = 0
    duration_seconds: float = 0.0

    def as_dict(self) -> dict[str, float]:
        return {
            "hourly_written": self.hourly_written,
            "hourly_empty_skipped": self.hourly_empty_skipped,
            "hourly_stale_deleted": self.hourly_stale_deleted,
            "hourly_empty_targets_deleted": self.hourly_empty_targets_deleted,
            "hourly_failed": self.hourly_failed,
            "hourly_rows": self.hourly_rows,
            "hourly_bytes": self.hourly_bytes,
            "daily_written": self.daily_written,
            "daily_failed": self.daily_failed,
            "daily_rows": self.daily_rows,
            "daily_bytes": self.daily_bytes,
            "daily_skipped_empty": self.daily_skipped_empty,
            "daily_skipped_unchanged": self.daily_skipped_unchanged,
            "daily_empty_targets_deleted": self.daily_empty_targets_deleted,
            "sources_empty": self.sources_empty,
            "sources_skipped": self.sources_skipped,
            "sources_scanned": self.sources_scanned,
            "days_scanned": self.days_scanned,
            "days_gated_out": self.days_gated_out,
            "days_blocked": self.days_blocked,
            "hourly_dirs_pruned": self.hourly_dirs_pruned,
            "temp_files_cleaned": self.temp_files_cleaned,
            "hours_skipped_fresh": self.hours_skipped_fresh,
            "hours_skipped_open": self.hours_skipped_open,
            "days_skipped_fresh": self.days_skipped_fresh,
            "days_skipped_open": self.days_skipped_open,
            "duration_seconds": round(self.duration_seconds, 3),
        }

    @property
    def failed(self) -> int:
        """A pass fails when a write fails or a non-empty source was rejected."""
        return self.hourly_failed + self.daily_failed + self.sources_skipped


# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #


def default_rollup_root(project_root: Path) -> Path:
    return project_root / ROLLUP_DIR_NAME


def hourly_rollup_path(
    rollup_root: Path,
    category: str,
    exchange: str,
    symbol: str,
    day: date,
    hour: int,
) -> Path:
    return (
        rollup_root
        / HOURLY_LAYER
        / category
        / exchange
        / symbol
        / day.isoformat()
        / f"{hour:02d}.parquet"
    )


def daily_rollup_path(
    rollup_root: Path,
    category: str,
    exchange: str,
    symbol: str,
    day: date,
) -> Path:
    return (
        rollup_root
        / DAILY_LAYER
        / category
        / exchange
        / symbol
        / f"{day.isoformat()}.parquet"
    )


def hourly_day_dir(
    rollup_root: Path,
    category: str,
    exchange: str,
    symbol: str,
    day: date,
) -> Path:
    """The ``hourly/.../{day}/`` directory that holds the day's ``HH.parquet``."""
    return hourly_rollup_path(
        rollup_root, category, exchange, symbol, day, 0
    ).parent


def _iter_subdirs(path: Path) -> list[Path]:
    if not path.is_dir():
        return []
    return sorted((entry for entry in path.iterdir() if entry.is_dir()), key=lambda p: p.name)


def _parse_day(name: str) -> date | None:
    try:
        return date.fromisoformat(name)
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Source discovery
# --------------------------------------------------------------------------- #


def read_source_span(path: Path, *, time_column: str) -> SourceRead:
    """Read one parquet batch footer and decide whether it can be bucketed.

    Reads only the footer: row-group statistics give min/max without touching
    the data pages. If a row group does not carry statistics, the timestamp
    column alone is read as a fallback. An empty batch is a normal product of a
    quiet flush interval and is reported as such, not as a failure; every other
    unusable batch carries a reason so the pass can fail loudly.
    """
    try:
        metadata = pq.ParquetFile(path).metadata
    except Exception:
        logger.warning("Skipping unreadable parquet batch: %s", path, exc_info=True)
        return SourceRead(reason=SKIP_UNREADABLE)
    if metadata is None or metadata.num_rows == 0:
        logger.debug("Ignoring empty parquet batch: %s", path)
        return SourceRead(reason=SKIP_EMPTY)
    try:
        column_index = metadata.schema.names.index(time_column)
    except ValueError:
        logger.warning(
            "Skipping parquet batch without a %r column: %s", time_column, path
        )
        return SourceRead(reason=SKIP_NO_TIME_COLUMN)
    physical_type = metadata.schema.column(column_index).physical_type
    if physical_type not in {"INT64", "INT32"}:
        logger.warning(
            "Skipping parquet batch whose %r column is %s, not an integer: %s",
            time_column,
            physical_type,
            path,
        )
        return SourceRead(reason=SKIP_NON_INTEGER_TIME)

    lowest: int | float | None = None
    highest: int | float | None = None
    null_rows = 0
    for group in range(metadata.num_row_groups):
        statistics = metadata.row_group(group).column(column_index).statistics
        if statistics is None or not statistics.has_min_max:
            fallback = _read_span_from_column(path, time_column)
            if not fallback.usable:
                return fallback
            assert fallback.span is not None
            lowest, highest, null_rows = fallback.span
            break
        null_rows += statistics.null_count or 0
        lowest = statistics.min if lowest is None else min(lowest, statistics.min)
        highest = statistics.max if highest is None else max(highest, statistics.max)

    if lowest is None or highest is None:
        fallback = _read_span_from_column(path, time_column)
        if not fallback.usable:
            return fallback
        assert fallback.span is not None
        lowest, highest, null_rows = fallback.span

    try:
        start_ms = int(lowest)
        end_ms = int(highest)
    except (TypeError, ValueError):
        logger.warning(
            "Skipping parquet batch with a non-integer %r value: %s", time_column, path
        )
        return SourceRead(reason=SKIP_NON_INTEGER_TIME)
    if start_ms > end_ms:
        logger.warning(
            "Skipping parquet batch with an inverted %r span (%s..%s): %s",
            time_column,
            start_ms,
            end_ms,
            path,
        )
        return SourceRead(reason=SKIP_INVERTED_SPAN)
    if not (
        PLAUSIBLE_EPOCH_MS_MIN <= start_ms <= PLAUSIBLE_EPOCH_MS_MAX
        and PLAUSIBLE_EPOCH_MS_MIN <= end_ms <= PLAUSIBLE_EPOCH_MS_MAX
    ):
        logger.warning(
            "Skipping parquet batch with an implausible %r span (%s..%s): %s",
            time_column,
            start_ms,
            end_ms,
            path,
        )
        return SourceRead(reason=SKIP_IMPLAUSIBLE_SPAN)
    if end_ms - start_ms > MAX_SOURCE_SPAN_MS:
        logger.warning(
            "Skipping parquet batch whose %r span exceeds %d days: %s",
            time_column,
            MAX_SOURCE_SPAN_DAYS,
            path,
        )
        return SourceRead(reason=SKIP_SPAN_TOO_LONG)
    if null_rows:
        logger.warning(
            "Parquet batch has %d row(s) without %r; they are excluded from rollups: %s",
            null_rows,
            time_column,
            path,
        )
    return SourceRead(span=(start_ms, end_ms, null_rows))


def _read_span_from_column(path: Path, time_column: str) -> SourceRead:
    """Fallback span read when parquet statistics are missing."""
    try:
        column = pq.read_table(path, columns=[time_column]).column(time_column)
    except Exception:
        logger.warning(
            "Could not read %r from parquet batch: %s", time_column, path, exc_info=True
        )
        return SourceRead(reason=SKIP_UNREADABLE)
    null_rows = column.null_count
    values = pc.drop_null(column)
    if len(values) == 0:
        logger.warning(
            "Skipping parquet batch whose %r column is entirely null: %s",
            time_column,
            path,
        )
        return SourceRead(reason=SKIP_ALL_NULL_TIME)
    bounds = pc.min_max(values).as_py()
    return SourceRead(span=(int(bounds["min"]), int(bounds["max"]), null_rows))


def group_sources_by_hour(
    sources: Sequence[SourceBatch],
) -> dict[int, tuple[SourceBatch, ...]]:
    """Map each UTC hour-of-day to the batches whose span can contribute rows."""
    buckets: dict[int, list[SourceBatch]] = {}
    for batch in sources:
        for hour in batch.hours():
            buckets.setdefault(hour, []).append(batch)
    return {
        hour: tuple(sorted(batches, key=lambda batch: batch.path.name))
        for hour, batches in sorted(buckets.items())
    }


def scan_day_batches(
    day_dir: Path,
    category: str,
    *,
    skipped: list[SkippedSource] | None = None,
    empty: list[Path] | None = None,
    counters: DiscoveryCounters | None = None,
) -> tuple[SourceBatch, ...]:
    """Read one ``data/{category}/{exchange}/{symbol}/{date}/`` dir.

    Every ``.parquet`` file is a source regardless of its name: the batch stamp
    (``HH-MM-SS``) and the merged name (``first--last``) are both accepted, and
    an unexpected name is still read as long as the timestamp column is usable.
    The UTC hour buckets come from the file's timestamps, not from its name.

    A zero-row file is appended to ``empty`` and is not a failure. Any other
    file that cannot be bucketed (unreadable, no time column, implausible
    timestamps, span over two days) is appended to ``skipped`` with its reason
    so the caller can fail the pass; it is never silently dropped.
    """
    time_column = category_time_column(category)
    if time_column is None:
        logger.warning(
            "Category %r has no timestamp column (%s); skipping %s",
            category,
            ", ".join(TIME_COLUMN_CANDIDATES),
            day_dir,
        )
        return ()

    batches: list[SourceBatch] = []
    with os.scandir(day_dir) as entries:
        for entry in entries:
            if not entry.name.endswith(".parquet") or not entry.is_file():
                continue
            if counters is not None:
                counters.sources_scanned += 1
            path = Path(entry.path)
            read = read_source_span(path, time_column=time_column)
            if not read.usable:
                if read.empty:
                    if empty is not None:
                        empty.append(path)
                elif skipped is not None:
                    skipped.append(
                        SkippedSource(path=path, reason=read.reason or "unknown")
                    )
                continue
            assert read.span is not None
            start_ms, end_ms, _null_rows = read.span
            batches.append(
                SourceBatch(
                    path=path,
                    mtime=entry.stat().st_mtime,
                    start_ms=start_ms,
                    end_ms=end_ms,
                )
            )
    return tuple(sorted(batches, key=lambda batch: batch.path.name))


@dataclass
class DiscoveryCounters:
    """How much of ``data/`` one discovery pass actually opened."""

    days_scanned: int = 0
    days_gated_out: int = 0
    sources_scanned: int = 0


@dataclass(frozen=True)
class DiscoveryPolicy:
    """Bounds which day directories discovery is allowed to open.

    ``data/`` holds ~160k batch files; reading every footer on every pass costs
    too much for a 10-minute timer. A day directory is scanned only when one of
    these is true:

    * the day is inside the hourly window (``day >= window_start``);
    * its daily rollup is missing;
    * its daily rollup is older than the day directory, i.e. a batch was added
      or replaced after the daily was written.

    The normalizer publishes batches with an atomic rename into the day
    directory, so every new or replaced batch bumps the directory mtime. That
    keeps the staleness check to one ``stat`` per day without opening a footer.
    ``backfill`` scans everything because it rebuilds everything.
    """

    rollup_root: Path
    window_start: date
    backfill: bool = False

    def should_scan(
        self,
        *,
        category: str,
        exchange: str,
        symbol: str,
        day: date,
        day_dir: Path,
    ) -> bool:
        if self.backfill or day >= self.window_start:
            return True
        daily = daily_rollup_path(
            self.rollup_root, category, exchange, symbol, day
        )
        try:
            daily_mtime = daily.stat().st_mtime
        except FileNotFoundError:
            return True
        try:
            return day_dir.stat().st_mtime > daily_mtime
        except FileNotFoundError:
            return True


def discover_day_groups(
    data_root: Path,
    *,
    policy: DiscoveryPolicy | None = None,
    skipped: list[SkippedSource] | None = None,
    empty: list[Path] | None = None,
    counters: DiscoveryCounters | None = None,
) -> list[DayGroup]:
    """Every (category, exchange, symbol, day) that has at least one usable batch.

    A day whose only non-empty batches were rejected is included with zero
    usable sources, so it still counts as blocked. With a ``policy``, only day
    directories the policy allows are opened; the rest are skipped outright
    (``counters.days_gated_out``), so an unchanged history costs one ``stat``
    per day instead of one footer read per file.
    """
    groups: list[DayGroup] = []
    for category in sorted(CATEGORY_SPECS):
        if category_time_column(category) is None:
            logger.warning(
                "Category %r has no timestamp column; its batches are not rolled up",
                category,
            )
            continue
        category_dir = data_root / category
        for exchange_dir in _iter_subdirs(category_dir):
            for symbol_dir in _iter_subdirs(exchange_dir):
                for day_dir in _iter_subdirs(symbol_dir):
                    day = _parse_day(day_dir.name)
                    if day is None:
                        logger.debug("Ignoring directory that is not a date: %s", day_dir)
                        continue
                    if policy is not None and not policy.should_scan(
                        category=category,
                        exchange=exchange_dir.name,
                        symbol=symbol_dir.name,
                        day=day,
                        day_dir=day_dir,
                    ):
                        if counters is not None:
                            counters.days_gated_out += 1
                        continue
                    if counters is not None:
                        counters.days_scanned += 1
                    rejected: list[SkippedSource] = []
                    sources = scan_day_batches(
                        day_dir,
                        category,
                        skipped=rejected,
                        empty=empty,
                        counters=counters,
                    )
                    if skipped is not None:
                        skipped.extend(rejected)
                    if not sources and not rejected:
                        # Every batch was empty: nothing to roll up. A day whose
                        # only non-empty batches were rejected still becomes a
                        # group so it counts as blocked even without a usable
                        # source.
                        continue
                    groups.append(
                        DayGroup(
                            category=category,
                            exchange=exchange_dir.name,
                            symbol=symbol_dir.name,
                            day=day,
                            sources=sources,
                            hours=group_sources_by_hour(sources),
                            rejected=tuple(rejected),
                        )
                    )
    return groups


_HOURLY_FILE_NAME = re.compile(r"^(\d{2})\.parquet$")


def stale_hourly_targets(
    hourly_dir: Path, *, live_hours: Collection[int]
) -> list[tuple[int, Path]]:
    """``HH.parquet`` files whose hour is no longer produced by the sources.

    Only names matching the rollup contract (two digits + ``.parquet``, hour
    0..23) are considered, so a foreign file in the directory is never touched.
    The caller decides whether the hour may be removed; an hour that is still
    open keeps its file.
    """
    if not hourly_dir.is_dir():
        return []
    stale: list[tuple[int, Path]] = []
    for entry in sorted(hourly_dir.iterdir(), key=lambda item: item.name):
        if not entry.is_file():
            continue
        match = _HOURLY_FILE_NAME.match(entry.name)
        if match is None:
            continue
        hour = int(match.group(1))
        if hour > 23 or hour in live_hours:
            continue
        stale.append((hour, entry))
    return stale


# --------------------------------------------------------------------------- #
# Freshness and closure rules
# --------------------------------------------------------------------------- #


def _as_utc(moment: datetime) -> datetime:
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def _day_start(day: date) -> datetime:
    return datetime.combine(day, day_time(0), tzinfo=UTC)


def hour_end(day: date, hour: int) -> datetime:
    return _day_start(day) + timedelta(hours=hour + 1)


def day_end(day: date) -> datetime:
    return _day_start(day + timedelta(days=1))


def hour_is_closed(
    day: date,
    hour: int,
    *,
    now: datetime,
    margin_minutes: float = DEFAULT_CLOSED_MARGIN_MINUTES,
) -> bool:
    return hour_end(day, hour) + timedelta(minutes=margin_minutes) <= now


def day_is_closed(
    day: date,
    *,
    now: datetime,
    margin_minutes: float = DEFAULT_CLOSED_MARGIN_MINUTES,
) -> bool:
    return day_end(day) + timedelta(minutes=margin_minutes) <= now


def _target_is_fresh(target: Path, newest_source_mtime: float) -> bool:
    """Fresh when it exists and is at least as new as its newest source."""
    try:
        return target.stat().st_mtime >= newest_source_mtime
    except FileNotFoundError:
        return False


def _reason_summary(skipped: Sequence[SkippedSource]) -> str:
    """``reason=count, ...`` summary for logs and the final outcome line."""
    reasons = Counter(item.reason for item in skipped)
    return ", ".join(
        f"{reason}={count}" for reason, count in sorted(reasons.items())
    )


# --------------------------------------------------------------------------- #
# Planning
# --------------------------------------------------------------------------- #


def build_plan(
    groups: Sequence[DayGroup],
    *,
    rollup_root: Path,
    now: datetime,
    hourly_days: int = DEFAULT_HOURLY_DAYS,
    margin_minutes: float = DEFAULT_CLOSED_MARGIN_MINUTES,
    backfill: bool = False,
) -> RollupPlan:
    """Decide which hourly and daily files this pass has to write.

    Hourly work is limited to the last ``hourly_days`` days. A day outside that
    window still gets its hourly files rebuilt when its daily file is missing or
    stale — the daily layer is defined as the merge of that day's hourly layer —
    and those hourly files are pruned again at the end of the pass.
    """
    if hourly_days < 1:
        raise ValueError(f"hourly_days must be >= 1, got {hourly_days}")

    now = _as_utc(now)
    window_start = now.date() - timedelta(days=hourly_days - 1)
    plan = RollupPlan()

    for group in groups:
        _plan_group(
            group,
            plan=plan,
            rollup_root=rollup_root,
            now=now,
            window_start=window_start,
            margin_minutes=margin_minutes,
            backfill=backfill,
        )

    plan.prune_before = window_start
    plan.prune_dirs = prune_hourly(rollup_root, before=window_start, dry_run=True)
    return plan


def _plan_group(
    group: DayGroup,
    *,
    plan: RollupPlan,
    rollup_root: Path,
    now: datetime,
    window_start: date,
    margin_minutes: float,
    backfill: bool,
) -> None:
    if group.rejected:
        # A daily rollup is defined as "every row of the day". If any source of
        # the day was rejected, writing the day would publish an incomplete
        # file and silently break that contract, so block the whole day: its
        # existing hourly and daily files stay exactly as they are, and the
        # rejected source fails the pass.
        plan.days_blocked += 1
        logger.error(
            "Not writing rollups for %s/%s/%s %s: %d source batch(es) rejected (%s)",
            group.category,
            group.exchange,
            group.symbol,
            group.day.isoformat(),
            len(group.rejected),
            _reason_summary(group.rejected),
        )
        return

    in_window = group.day >= window_start
    closed = day_is_closed(group.day, now=now, margin_minutes=margin_minutes)
    daily_target = daily_rollup_path(
        rollup_root, group.category, group.exchange, group.symbol, group.day
    )
    hourly_dir = hourly_day_dir(
        rollup_root, group.category, group.exchange, group.symbol, group.day
    )

    # A re-normalized batch can move every row of an hour somewhere else. That
    # hour then disappears from ``group.hours`` and no task is produced for it,
    # so an old ``HH.parquet`` would stay on disk and leak into the daily.
    # Plan its removal explicitly; only closed hours are touched so an hour
    # that can still gain rows is left alone.
    live_hours = set(group.hours)
    stale_here: list[StaleHourly] = []
    for hour, target in stale_hourly_targets(hourly_dir, live_hours=live_hours):
        if not hour_is_closed(group.day, hour, now=now, margin_minutes=margin_minutes):
            continue
        stale_here.append(
            StaleHourly(
                category=group.category,
                exchange=group.exchange,
                symbol=group.symbol,
                day=group.day,
                hour=hour,
                target=target,
            )
        )
    plan.stale_hourly.extend(stale_here)

    newest_source = group.newest_mtime()
    daily_stale = backfill or not _target_is_fresh(daily_target, newest_source)
    hourly_tasks = 0

    for hour in sorted(group.hours):
        batches = group.hours[hour]
        if not hour_is_closed(group.day, hour, now=now, margin_minutes=margin_minutes):
            plan.hours_skipped_open += 1
            continue

        target = hourly_rollup_path(
            rollup_root, group.category, group.exchange, group.symbol, group.day, hour
        )
        newest = max(batch.mtime for batch in batches)
        if in_window:
            write = backfill or not _target_is_fresh(target, newest)
        else:
            # Outside the hourly window the layer is only an intermediate: build
            # what the daily rebuild needs, reuse what is still fresh.
            write = daily_stale and not _target_is_fresh(target, newest)

        if not write:
            plan.hours_skipped_fresh += 1
            continue

        plan.hourly_tasks.append(
            HourlyTask(
                category=group.category,
                exchange=group.exchange,
                symbol=group.symbol,
                day=group.day,
                hour=hour,
                target=target,
                sources=tuple(batch.path for batch in batches),
            )
        )
        hourly_tasks += 1

    if not closed:
        plan.days_skipped_open += 1
        return
    if not (daily_stale or hourly_tasks or stale_here):
        plan.days_skipped_fresh += 1
        return

    # The daily layer is the merge of the day's hourly files, so it must be
    # rewritten whenever this pass wrote, removed, or emptied an hourly file.
    # Only the hours of the current source set are merged, never every parquet
    # file lying in the directory.
    plan.daily_tasks.append(
        DailyTask(
            category=group.category,
            exchange=group.exchange,
            symbol=group.symbol,
            day=group.day,
            target=daily_target,
            hourly_dir=hourly_dir,
            hours=tuple(sorted(group.hours)),
            force=daily_stale,
        )
    )


def cleanup_stale_temp_files(
    rollup_root: Path,
    *,
    max_age_seconds: float = STALE_TEMP_MAX_AGE_SEC,
    now: float | None = None,
) -> int:
    """Remove ``.<name>.<pid>.tmp`` files left behind by a killed pass.

    Only hidden ``.tmp`` files are considered (DuckDB's own scratch files under
    ``_tmp/`` are not hidden) and only ones older than ``max_age_seconds``, so a
    concurrent pass keeps its in-flight temp file.
    """
    if not rollup_root.is_dir():
        return 0
    cutoff = (now if now is not None else time.time()) - max_age_seconds
    removed = 0
    for path in rollup_root.rglob(f".*{TEMP_FILE_SUFFIX}"):
        try:
            if not path.is_file() or path.stat().st_mtime >= cutoff:
                continue
            path.unlink()
        except FileNotFoundError:
            continue
        removed += 1
    return removed


def prune_hourly(
    rollup_root: Path,
    *,
    before: date,
    dry_run: bool = False,
) -> list[Path]:
    """Remove hourly day dirs older than ``before`` that already have a daily file.

    A day whose daily file is missing keeps its hourly files: the daily layer is
    the long-lived output, so pruning is delayed until the day is really rolled
    up. Never touches anything outside ``data_rollup/hourly/``.
    """
    hourly_root = rollup_root / HOURLY_LAYER
    removed: list[Path] = []
    for category_dir in _iter_subdirs(hourly_root):
        for exchange_dir in _iter_subdirs(category_dir):
            for symbol_dir in _iter_subdirs(exchange_dir):
                for day_dir in _iter_subdirs(symbol_dir):
                    day = _parse_day(day_dir.name)
                    if day is None or day >= before:
                        continue
                    daily = daily_rollup_path(
                        rollup_root,
                        category_dir.name,
                        exchange_dir.name,
                        symbol_dir.name,
                        day,
                    )
                    if not daily.exists():
                        logger.debug("Keeping hourly rollups without a daily file: %s", day_dir)
                        continue
                    removed.append(day_dir)
                    if not dry_run:
                        shutil.rmtree(day_dir, ignore_errors=True)
    return removed


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def source_select_sql(
    category: str,
    sources: Sequence[Path],
    *,
    hour: int | None = None,
) -> str:
    """``SELECT`` projecting the category contract from one or more parquet files.

    Columns are cast to the contract types in contract order, so a rollup has
    exactly the schema of its ``data/`` sources and stays stable even if a
    source file was written by an older normalizer.

    ``hour`` restricts the read to rows whose UTC hour-of-day matches, using the
    category's timestamp column. A file that spans several hours therefore
    contributes only the rows of the bucket being written, never its whole
    content. The hour is derived from the row timestamp, so merged batches
    (``first--last.parquet``) and per-file batches (``HH-MM-SS.parquet``) are
    treated identically; the file name is irrelevant.

    Rows are not re-sorted: batches are near time-ordered and the hourly filter
    keeps that order, so the daily layer inherits it. Sorting instead cost 2.4x
    more CPU on the VPS, which is the scarce resource there; parquet file and
    row-group statistics still prune time ranges either way.
    """
    if not sources:
        raise ValueError("Cannot build a rollup from zero source files")
    specs = category_column_specs(category)
    columns = ", ".join(
        f'CAST("{name}" AS {column_type}) AS "{name}"' for name, column_type in specs
    )
    paths = ", ".join(_sql_string(str(Path(source))) for source in sources)
    where_sql = ""
    if hour is not None:
        if not 0 <= hour <= 23:
            raise ValueError(f"hour must be in 0..23, got {hour}")
        time_column = category_time_column(category)
        if time_column is None:
            raise ValueError(f"Category {category!r} has no timestamp column to bucket by")
        where_sql = (
            f' WHERE hour(epoch_ms(CAST("{time_column}" AS BIGINT))) = {int(hour)}'
        )
    return f"SELECT {columns} FROM read_parquet([{paths}]){where_sql}"


@dataclass(frozen=True)
class WriteResult:
    """One COPY attempt: rows produced, whether it was published, target removed."""

    rows: int
    size_bytes: int
    written: bool
    removed: bool = False


def _remove_target(path: Path) -> bool:
    """Remove one rollup file atomically; never touches a foreign name.

    The file is renamed to a hidden ``.tmp`` sibling first, so a reader never
    observes a half-removed target and an interrupted pass leaves a stale temp
    file that ``cleanup_stale_temp_files`` collects later.
    """
    if not path.is_file():
        return False
    tmp_path = path.with_name(f".{path.stem}.{os.getpid()}.deleted.tmp")
    try:
        os.replace(path, tmp_path)
    except FileNotFoundError:
        return False
    tmp_path.unlink(missing_ok=True)
    return True


def copy_to_parquet(
    con: duckdb.DuckDBPyConnection,
    *,
    select_sql: str,
    target: Path,
    skip_empty: bool = False,
    drop_empty_target: bool = False,
) -> WriteResult:
    """``COPY (select_sql)`` into a sibling tmp file, then publish it atomically.

    The tmp file never ends in ``.parquet``, so a crash mid-write leaves the
    previous target untouched and never a half-written parquet a reader would
    pick up. With ``skip_empty`` a zero-row result is discarded along with its
    tmp file instead of publishing an empty parquet: a wide source span can
    cover an hour that simply has no rows, and that hour must not appear as an
    output file. With ``drop_empty_target`` the previous target, if any, is
    removed too: a file left from an earlier pass would otherwise keep rows
    this pass proves are gone.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_name(f".{target.stem}.{os.getpid()}.tmp")
    tmp_path.unlink(missing_ok=True)
    try:
        result = con.execute(
            f"COPY ({select_sql}) TO {_sql_string(str(tmp_path))} "
            f"(FORMAT PARQUET, COMPRESSION {PARQUET_COMPRESSION})"
        ).fetchone()
        rows = int(result[0]) if result else 0
        if skip_empty and rows == 0:
            tmp_path.unlink(missing_ok=True)
            removed = drop_empty_target and _remove_target(target)
            return WriteResult(
                rows=0, size_bytes=0, written=False, removed=removed
            )
        size = tmp_path.stat().st_size
        os.replace(tmp_path, target)
        return WriteResult(rows=rows, size_bytes=size, written=True)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


class RollupBuilder:
    """Reusable in-memory DuckDB session for one rollup pass.

    One connection serves every file: opening DuckDB costs ~0.1 s, and a pass
    writes hundreds of files. Memory stays bounded (``memory_limit``) and
    DuckDB spills to a scratch dir inside ``data_rollup/`` instead of growing.
    """

    def __init__(
        self,
        *,
        rollup_root: Path,
        threads: int = DEFAULT_THREADS,
        memory_limit: str = ROLLUP_MEMORY_LIMIT,
    ) -> None:
        self._rollup_root = rollup_root
        self._threads = threads
        self._memory_limit = memory_limit
        self._con: duckdb.DuckDBPyConnection | None = None
        self._temp_dir: Path | None = None

    def build_hourly(self, task: HourlyTask) -> WriteResult:
        logger.debug(
            "hourly %s/%s/%s %s %02d: %d batch(es)",
            task.category,
            task.exchange,
            task.symbol,
            task.day.isoformat(),
            task.hour,
            len(task.sources),
        )
        return copy_to_parquet(
            self._connection(),
            select_sql=source_select_sql(task.category, task.sources, hour=task.hour),
            target=task.target,
            skip_empty=True,
            drop_empty_target=True,
        )

    def build_daily(self, task: DailyTask) -> WriteResult | None:
        # Merge exactly the hours this pass says the day produces. A directory
        # scan would also pick up a stale or foreign parquet, reintroducing the
        # rows the plan just removed.
        sources = [
            path
            for hour in task.hours
            if (path := task.hourly_dir / f"{hour:02d}.parquet").is_file()
        ]
        if not sources:
            logger.warning(
                "Skipping daily rollup without hourly sources: %s", task.target
            )
            return None
        logger.debug(
            "daily %s/%s/%s %s: %d hourly file(s)",
            task.category,
            task.exchange,
            task.symbol,
            task.day.isoformat(),
            len(sources),
        )
        return copy_to_parquet(
            self._connection(),
            select_sql=source_select_sql(task.category, sources),
            target=task.target,
            skip_empty=True,
            drop_empty_target=True,
        )

    def close(self) -> None:
        con, self._con = self._con, None
        if con is not None:
            con.close()
        temp_dir, self._temp_dir = self._temp_dir, None
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def __enter__(self) -> RollupBuilder:
        self._connection()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def _connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            temp_dir = self._rollup_root / TEMP_DIR_NAME
            temp_dir.mkdir(parents=True, exist_ok=True)
            con = duckdb.connect(":memory:")
            con.execute(f"SET memory_limit={_sql_string(self._memory_limit)}")
            con.execute(f"SET threads={int(self._threads)}")
            con.execute(f"SET temp_directory={_sql_string(str(temp_dir))}")
            self._con = con
            self._temp_dir = temp_dir
        return self._con


# --------------------------------------------------------------------------- #
# Pass orchestration
# --------------------------------------------------------------------------- #


def run_rollup(
    *,
    data_root: Path | None = None,
    rollup_root: Path | None = None,
    hourly_days: int = DEFAULT_HOURLY_DAYS,
    backfill: bool = False,
    dry_run: bool = False,
    keep_hourly: bool = False,
    margin_minutes: float = DEFAULT_CLOSED_MARGIN_MINUTES,
    now: datetime | None = None,
    threads: int = DEFAULT_THREADS,
) -> RollupStats:
    """Run one rollup pass over ``data_root`` and return its counters."""
    started = time.monotonic()
    if hourly_days < 1:
        raise ValueError(f"hourly_days must be >= 1, got {hourly_days}")

    now_utc = _as_utc(now if now is not None else datetime.now(UTC))
    if data_root is None or rollup_root is None:
        project_root, project_data_root, _, _ = project_paths()
        data_root = data_root if data_root is not None else project_data_root
        rollup_root = rollup_root if rollup_root is not None else default_rollup_root(project_root)

    skipped_sources: list[SkippedSource] = []
    empty_sources: list[Path] = []
    discovery = DiscoveryCounters()
    source_root = Path(data_root)
    output_root = Path(rollup_root)
    window_start = now_utc.date() - timedelta(days=hourly_days - 1)
    groups = discover_day_groups(
        source_root,
        policy=DiscoveryPolicy(
            rollup_root=output_root,
            window_start=window_start,
            backfill=backfill,
        ),
        skipped=skipped_sources,
        empty=empty_sources,
        counters=discovery,
    )
    if empty_sources:
        logger.debug(
            "Ignored %d empty parquet batch(es); an empty batch is not a skipped source",
            len(empty_sources),
        )
    if skipped_sources:
        logger.warning(
            "Skipped %d non-empty source batch(es) that could not be bucketed (%s)",
            len(skipped_sources),
            _reason_summary(skipped_sources),
        )
    logger.info(
        "Discovery scanned %d day(s) / %d batch(es); %d day(s) outside the hourly "
        "window had a fresh daily and were not opened",
        discovery.days_scanned,
        discovery.sources_scanned,
        discovery.days_gated_out,
    )
    plan = build_plan(
        groups,
        rollup_root=output_root,
        now=now_utc,
        hourly_days=hourly_days,
        margin_minutes=margin_minutes,
        backfill=backfill,
    )

    stats = RollupStats(
        sources_empty=len(empty_sources),
        sources_skipped=len(skipped_sources),
        sources_scanned=discovery.sources_scanned,
        days_scanned=discovery.days_scanned,
        days_gated_out=discovery.days_gated_out,
        days_blocked=plan.days_blocked,
        hours_skipped_fresh=plan.hours_skipped_fresh,
        hours_skipped_open=plan.hours_skipped_open,
        days_skipped_fresh=plan.days_skipped_fresh,
        days_skipped_open=plan.days_skipped_open,
    )
    summary = plan.summary()
    logger.info(
        "Rollup plan (now=%s, hourly_days=%s, backfill=%s, dry_run=%s): %s",
        now_utc.isoformat(timespec="seconds"),
        hourly_days,
        backfill,
        dry_run,
        ", ".join(f"{key}={value}" for key, value in summary.items()),
    )

    if dry_run:
        for stale in plan.stale_hourly:
            logger.debug(
                "would remove stale hourly %s (hour %02d has no source rows)",
                stale.target,
                stale.hour,
            )
        for task in plan.hourly_tasks:
            logger.debug("would write %s (%d source file(s))", task.target, len(task.sources))
        for task in plan.daily_tasks:
            logger.debug("would write %s", task.target)
        for directory in plan.prune_dirs:
            logger.debug("would prune %s", directory)
        stats.duration_seconds = round(time.monotonic() - started, 3)
        _log_outcome(stats=stats, skipped=skipped_sources)
        return stats

    stats.temp_files_cleaned = cleanup_stale_temp_files(output_root)
    if stats.temp_files_cleaned:
        logger.info(
            "Removed %d stale temp file(s) from an interrupted pass",
            stats.temp_files_cleaned,
        )

    builder = RollupBuilder(rollup_root=output_root, threads=threads)
    hourly_changed_days: set[tuple[str, str, str, date]] = set()
    try:
        # Remove stale hourly files before writing anything: the daily merge
        # must never see a file the current sources no longer produce.
        for stale in plan.stale_hourly:
            key = (stale.category, stale.exchange, stale.symbol, stale.day)
            try:
                removed = _remove_target(stale.target)
            except OSError:
                stats.hourly_failed += 1
                logger.exception(
                    "Failed to remove stale hourly rollup: %s", stale.target
                )
                continue
            if not removed:
                continue
            stats.hourly_stale_deleted += 1
            hourly_changed_days.add(key)
            logger.info(
                "Removed stale hourly rollup: %s (hour %02d has no source rows)",
                stale.target,
                stale.hour,
            )
        for task in plan.hourly_tasks:
            try:
                result = builder.build_hourly(task)
            except Exception:
                stats.hourly_failed += 1
                logger.exception("Failed to write hourly rollup: %s", task.target)
                continue
            if not result.written:
                stats.hourly_empty_skipped += 1
                if result.removed:
                    stats.hourly_empty_targets_deleted += 1
                    hourly_changed_days.add(
                        (task.category, task.exchange, task.symbol, task.day)
                    )
                    logger.info(
                        "Removed hourly rollup with no source rows: %s", task.target
                    )
                else:
                    logger.debug(
                        "hourly %s has no rows in hour %02d; no file written",
                        task.day.isoformat(),
                        task.hour,
                    )
                continue
            hourly_changed_days.add(
                (task.category, task.exchange, task.symbol, task.day)
            )
            stats.hourly_written += 1
            stats.hourly_rows += result.rows
            stats.hourly_bytes += result.size_bytes
            if stats.hourly_written % PROGRESS_EVERY_FILES == 0:
                logger.info(
                    "hourly progress: %d/%d written, %d failed, %.1f MB",
                    stats.hourly_written,
                    len(plan.hourly_tasks),
                    stats.hourly_failed,
                    stats.hourly_bytes / 1_000_000,
                )
        for task in plan.daily_tasks:
            key = (task.category, task.exchange, task.symbol, task.day)
            if not task.force and key not in hourly_changed_days:
                # Planned only because an hourly file was about to be rewritten;
                # every such write turned out empty, so the daily is unchanged.
                stats.daily_skipped_unchanged += 1
                continue
            try:
                result = builder.build_daily(task)
            except Exception:
                stats.daily_failed += 1
                logger.exception("Failed to write daily rollup: %s", task.target)
                continue
            if result is None or not result.written:
                stats.daily_skipped_empty += 1
                if result is not None and result.removed:
                    stats.daily_empty_targets_deleted += 1
                    logger.info(
                        "Removed daily rollup with no source rows: %s", task.target
                    )
                continue
            stats.daily_written += 1
            stats.daily_rows += result.rows
            stats.daily_bytes += result.size_bytes
            logger.info(
                "daily written: %s (%d rows, %.1f MB)",
                task.target,
                result.rows,
                result.size_bytes / 1_000_000,
            )
    finally:
        builder.close()

    prune_before = plan.prune_before if plan.prune_before is not None else now_utc.date()
    if keep_hourly:
        logger.info(
            "Keeping hourly day dirs older than %s (--keep-old-hourly)",
            prune_before.isoformat(),
        )
    else:
        # Re-scan after the writes: this pass may have created hourly day dirs
        # outside the window purely to rebuild those days' daily files.
        stats.hourly_dirs_pruned = len(prune_hourly(output_root, before=prune_before))

    _log_outcome(stats=stats, skipped=skipped_sources)
    stats.duration_seconds = round(time.monotonic() - started, 3)
    return stats


def _log_outcome(*, stats: RollupStats, skipped: Sequence[SkippedSource]) -> None:
    """One final line: what was really skipped, why, and whether the day survived."""
    if stats.sources_skipped:
        logger.error(
            "Rollup rejected %d non-empty source batch(es): %s; %d day(s) blocked, "
            "their hourly/daily files are left untouched",
            stats.sources_skipped,
            _reason_summary(skipped),
            stats.days_blocked,
        )
    else:
        logger.info(
            "No source batch rejected; %d empty parquet batch(es) ignored",
            stats.sources_empty,
        )


def log_stats(stats: Mapping[str, float], *, label: str) -> None:
    logger.info(label)
    for key, value in stats.items():
        logger.info("%s: %s", key, value)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the derived rollup parquet layer (hourly + daily).",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--run",
        action="store_true",
        help="Incremental pass (what the systemd timer calls).",
    )
    mode.add_argument(
        "--backfill",
        action="store_true",
        help="Rebuild the whole derived layer (every closed day, ignoring freshness).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report the planned writes and prunes; write nothing.",
    )
    parser.add_argument(
        "--hourly-days",
        type=int,
        default=DEFAULT_HOURLY_DAYS,
        help=(
            "Hourly window: days of hourly rollups to keep, newest first "
            f"(default: {DEFAULT_HOURLY_DAYS}). Only days in this window, plus "
            "days whose daily rollup is missing or stale, are scanned."
        ),
    )
    parser.add_argument(
        "--keep-old-hourly",
        action="store_true",
        help="Do not prune hourly rollups older than --hourly-days.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=DEFAULT_THREADS,
        help=f"DuckDB threads for one pass (default: {DEFAULT_THREADS}).",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="Source parquet root (default: <repo>/data).",
    )
    parser.add_argument(
        "--rollup-root",
        type=Path,
        default=None,
        help=f"Output root (default: <repo>/{ROLLUP_DIR_NAME}).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.hourly_days < 1:
        parser.error("--hourly-days must be >= 1")
    if args.threads < 1:
        parser.error("--threads must be >= 1")

    stats = run_rollup(
        data_root=args.data_root,
        rollup_root=args.rollup_root,
        hourly_days=args.hourly_days,
        backfill=args.backfill,
        dry_run=args.dry_run,
        keep_hourly=args.keep_old_hourly,
        threads=args.threads,
    )
    log_stats(stats.as_dict(), label="Rollup complete")
    return 1 if stats.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
