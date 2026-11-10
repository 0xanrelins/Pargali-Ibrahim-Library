#!/usr/bin/env python3
"""Build the derived rollup parquet layer (hourly + daily) from immutable batches.

``data/`` is the source of truth and is only ever read here: one immutable
parquet batch per raw JSONL file. That layout makes a day cost ~1.4k files, so a
plain ``count(*)`` over one day has to open ~1.4k parquet footers. This module
adds a derived read layer on top, and nothing else:

    data_rollup/hourly/{category}/{exchange}/{symbol}/{YYYY-MM-DD}/{HH}.parquet
    data_rollup/daily/{category}/{exchange}/{symbol}/{YYYY-MM-DD}.parquet

* **hourly** — every batch of one closed hour merged into one file. Kept only
  for the last ``--hourly-days`` days (default 7); older hourly rollups are
  pruned once the day's daily file exists.
* **daily** — the same day's hourly rollups merged. Kept for all history, and
  written only after the day has closed.

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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time as day_time, timedelta, timezone
from pathlib import Path

import duckdb

from normalizer.paths import project_paths
from normalizer.schemas import CATEGORY_SPECS, category_column_specs

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
BATCH_STAMP_PATTERN = re.compile(r"^(?P<hour>\d{2})-(?P<minute>\d{2})-(?P<second>\d{2})$")


@dataclass(frozen=True)
class SourceBatch:
    """One immutable parquet batch file from ``data/``."""

    path: Path
    hour: int
    mtime: float


@dataclass(frozen=True)
class DayGroup:
    """All batches of one dataset for one day, grouped per hour."""

    category: str
    exchange: str
    symbol: str
    day: date
    hours: Mapping[int, tuple[SourceBatch, ...]]

    def newest_mtime(self) -> float:
        return max(batch.mtime for batches in self.hours.values() for batch in batches)


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
    """One daily rollup to write; its hourly sources are read from disk."""

    category: str
    exchange: str
    symbol: str
    day: date
    target: Path
    hourly_dir: Path


@dataclass
class RollupPlan:
    """What one pass would do (dry-run) or is about to do."""

    hourly_tasks: list[HourlyTask] = field(default_factory=list)
    daily_tasks: list[DailyTask] = field(default_factory=list)
    prune_dirs: list[Path] = field(default_factory=list)
    prune_before: date | None = None
    hours_skipped_fresh: int = 0
    hours_skipped_open: int = 0
    days_skipped_fresh: int = 0
    days_skipped_open: int = 0

    def summary(self) -> dict[str, int]:
        return {
            "hourly_to_write": len(self.hourly_tasks),
            "daily_to_write": len(self.daily_tasks),
            # Plan-time preview: a pass can add transient day dirs it prunes again.
            "hourly_dirs_pending_prune": len(self.prune_dirs),
            "hours_skipped_fresh": self.hours_skipped_fresh,
            "hours_skipped_open": self.hours_skipped_open,
            "days_skipped_fresh": self.days_skipped_fresh,
            "days_skipped_open": self.days_skipped_open,
        }


@dataclass
class RollupStats:
    """Counters for one pass."""

    hourly_written: int = 0
    hourly_failed: int = 0
    hourly_rows: int = 0
    hourly_bytes: int = 0
    daily_written: int = 0
    daily_failed: int = 0
    daily_rows: int = 0
    daily_bytes: int = 0
    daily_skipped_empty: int = 0
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
            "hourly_failed": self.hourly_failed,
            "hourly_rows": self.hourly_rows,
            "hourly_bytes": self.hourly_bytes,
            "daily_written": self.daily_written,
            "daily_failed": self.daily_failed,
            "daily_rows": self.daily_rows,
            "daily_bytes": self.daily_bytes,
            "daily_skipped_empty": self.daily_skipped_empty,
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
        return self.hourly_failed + self.daily_failed


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


def scan_day_batches(day_dir: Path) -> dict[int, tuple[SourceBatch, ...]]:
    """Group one ``data/{category}/{exchange}/{symbol}/{date}/`` dir into hours.

    Only names shaped like the normalizer's ``HH-MM-SS.parquet`` batch stamp are
    considered; anything else (legacy ``{day}.parquet`` files live one level up,
    partial ``.tmp`` files) is ignored.
    """
    buckets: dict[int, list[SourceBatch]] = {}
    with os.scandir(day_dir) as entries:
        for entry in entries:
            if not entry.name.endswith(".parquet"):
                continue
            match = BATCH_STAMP_PATTERN.match(entry.name[: -len(".parquet")])
            if match is None:
                logger.debug("Ignoring parquet without a batch stamp: %s", entry.path)
                continue
            hour = int(match.group("hour"))
            if hour > 23:
                logger.warning("Ignoring parquet with an invalid hour stamp: %s", entry.path)
                continue
            buckets.setdefault(hour, []).append(
                SourceBatch(path=Path(entry.path), hour=hour, mtime=entry.stat().st_mtime)
            )
    return {
        hour: tuple(sorted(batches, key=lambda batch: batch.path.name))
        for hour, batches in buckets.items()
    }


def discover_day_groups(data_root: Path) -> list[DayGroup]:
    """Every (category, exchange, symbol, day) that has at least one batch."""
    groups: list[DayGroup] = []
    for category in sorted(CATEGORY_SPECS):
        category_dir = data_root / category
        for exchange_dir in _iter_subdirs(category_dir):
            for symbol_dir in _iter_subdirs(exchange_dir):
                for day_dir in _iter_subdirs(symbol_dir):
                    day = _parse_day(day_dir.name)
                    if day is None:
                        logger.debug("Ignoring directory that is not a date: %s", day_dir)
                        continue
                    hours = scan_day_batches(day_dir)
                    if not hours:
                        continue
                    groups.append(
                        DayGroup(
                            category=category,
                            exchange=exchange_dir.name,
                            symbol=symbol_dir.name,
                            day=day,
                            hours=hours,
                        )
                    )
    return groups


def _hourly_sources_for_day(day_dir: Path) -> list[Path]:
    """The hourly rollup files of one day, in hour order (temp files excluded)."""
    if not day_dir.is_dir():
        return []
    return sorted(
        (
            entry
            for entry in day_dir.iterdir()
            if entry.is_file() and entry.suffix == ".parquet"
        ),
        key=lambda entry: entry.name,
    )


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
    in_window = group.day >= window_start
    closed = day_is_closed(group.day, now=now, margin_minutes=margin_minutes)
    daily_target = daily_rollup_path(
        rollup_root, group.category, group.exchange, group.symbol, group.day
    )

    need_daily = False
    if not closed:
        plan.days_skipped_open += 1
    elif backfill or not _target_is_fresh(daily_target, group.newest_mtime()):
        need_daily = True
    else:
        plan.days_skipped_fresh += 1

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
            write = need_daily and not _target_is_fresh(target, newest)

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

    if need_daily:
        plan.daily_tasks.append(
            DailyTask(
                category=group.category,
                exchange=group.exchange,
                symbol=group.symbol,
                day=group.day,
                target=daily_target,
                hourly_dir=hourly_rollup_path(
                    rollup_root, group.category, group.exchange, group.symbol, group.day, 0
                ).parent,
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


def source_select_sql(category: str, sources: Sequence[Path]) -> str:
    """``SELECT`` projecting the category contract from one or more parquet files.

    Columns are cast to the contract types in contract order, so a rollup has
    exactly the schema of its ``data/`` sources and stays stable even if a
    source file was written by an older normalizer.

    Rows are not re-sorted: batches are one-minute files written in event order,
    so reading them by name already produces a near time-ordered file (and the
    daily layer inherits that from its hourly inputs). Sorting instead cost 2.4x
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
    return f"SELECT {columns} FROM read_parquet([{paths}])"


def copy_to_parquet(
    con: duckdb.DuckDBPyConnection,
    *,
    select_sql: str,
    target: Path,
) -> tuple[int, int]:
    """``COPY (select_sql)`` into a sibling tmp file, then publish it atomically.

    The tmp file never ends in ``.parquet``, so a crash mid-write leaves the
    previous target untouched and never a half-written parquet a reader would
    pick up. Returns ``(rows, bytes)``.
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
        size = tmp_path.stat().st_size
        os.replace(tmp_path, target)
        return rows, size
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

    def build_hourly(self, task: HourlyTask) -> tuple[int, int]:
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
            select_sql=source_select_sql(task.category, task.sources),
            target=task.target,
        )

    def build_daily(self, task: DailyTask) -> tuple[int, int] | None:
        sources = _hourly_sources_for_day(task.hourly_dir)
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

    groups = discover_day_groups(Path(data_root))
    plan = build_plan(
        groups,
        rollup_root=Path(rollup_root),
        now=now_utc,
        hourly_days=hourly_days,
        margin_minutes=margin_minutes,
        backfill=backfill,
    )

    stats = RollupStats(
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
        for task in plan.hourly_tasks:
            logger.debug("would write %s (%d source file(s))", task.target, len(task.sources))
        for task in plan.daily_tasks:
            logger.debug("would write %s", task.target)
        for directory in plan.prune_dirs:
            logger.debug("would prune %s", directory)
        stats.duration_seconds = round(time.monotonic() - started, 3)
        return stats

    stats.temp_files_cleaned = cleanup_stale_temp_files(Path(rollup_root))
    if stats.temp_files_cleaned:
        logger.info(
            "Removed %d stale temp file(s) from an interrupted pass",
            stats.temp_files_cleaned,
        )

    builder = RollupBuilder(rollup_root=Path(rollup_root), threads=threads)
    try:
        for task in plan.hourly_tasks:
            try:
                rows, size = builder.build_hourly(task)
            except Exception:
                stats.hourly_failed += 1
                logger.exception("Failed to write hourly rollup: %s", task.target)
                continue
            stats.hourly_written += 1
            stats.hourly_rows += rows
            stats.hourly_bytes += size
            if stats.hourly_written % PROGRESS_EVERY_FILES == 0:
                logger.info(
                    "hourly progress: %d/%d written, %d failed, %.1f MB",
                    stats.hourly_written,
                    len(plan.hourly_tasks),
                    stats.hourly_failed,
                    stats.hourly_bytes / 1_000_000,
                )
        for task in plan.daily_tasks:
            try:
                result = builder.build_daily(task)
            except Exception:
                stats.daily_failed += 1
                logger.exception("Failed to write daily rollup: %s", task.target)
                continue
            if result is None:
                stats.daily_skipped_empty += 1
                continue
            rows, size = result
            stats.daily_written += 1
            stats.daily_rows += rows
            stats.daily_bytes += size
            logger.info(
                "daily written: %s (%d rows, %.1f MB)",
                task.target,
                rows,
                size / 1_000_000,
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
        stats.hourly_dirs_pruned = len(
            prune_hourly(Path(rollup_root), before=prune_before)
        )

    stats.duration_seconds = round(time.monotonic() - started, 3)
    return stats


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
        help=f"Days of hourly rollups to keep, newest first (default: {DEFAULT_HOURLY_DAYS}).",
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
