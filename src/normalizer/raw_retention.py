"""Conservative daily raw JSONL retention after immutable parquet batches exist."""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import DefaultDict

from datasets import POLYMARKET, REGISTERED_SOURCES, all_categories, iter_jsonl_paths, raw_root
from metrics.pipeline_registry import (
    raw_retention_backlog_blocked_files,
    raw_retention_bytes_freed_total,
    raw_retention_files_deleted_total,
    raw_retention_last_run_timestamp,
    raw_retention_run_skipped,
    raw_retention_skipped_files_last_run,
    raw_retention_would_delete_bytes,
    raw_retention_would_delete_files,
)
from normalizer.paths import batch_parquet_path_from_jsonl, project_paths, relative_to_project
from normalizer.runner import collect_backlog_metrics

logger = logging.getLogger(__name__)

DEFAULT_RETENTION_INTERVAL_SEC = 86_400.0
DEFAULT_GRACE_HOURS = 24.0

SKIP_REASON_NO_PARQUET = "no_parquet"
SKIP_REASON_GRACE = "grace"
SKIP_REASON_POLYMARKET = "polymarket_excluded"
SKIP_REASON_BACKLOG = "backlog"

SkipKey = tuple[str, str, str]


@dataclass(frozen=True)
class RawRetentionConfig:
    enabled: bool = False
    dry_run: bool = True
    grace_hours: float = DEFAULT_GRACE_HOURS
    interval_sec: float = DEFAULT_RETENTION_INTERVAL_SEC


@dataclass
class RawRetentionStats:
    run_skipped_backlog: int = 0
    would_delete: int = 0
    deleted: int = 0
    bytes_would_free: int = 0
    bytes_freed: int = 0
    skipped_no_parquet: int = 0
    skipped_grace: int = 0
    skipped_polymarket: int = 0
    errors: int = 0
    backlog_by_category: dict[str, int] = field(default_factory=dict)
    skip_by_source: DefaultDict[SkipKey, int] = field(default_factory=lambda: defaultdict(int))

    def as_dict(self) -> dict[str, int]:
        return {
            "run_skipped_backlog": self.run_skipped_backlog,
            "would_delete": self.would_delete,
            "deleted": self.deleted,
            "bytes_would_free": self.bytes_would_free,
            "bytes_freed": self.bytes_freed,
            "skipped_no_parquet": self.skipped_no_parquet,
            "skipped_grace": self.skipped_grace,
            "skipped_polymarket": self.skipped_polymarket,
            "errors": self.errors,
        }


def _remove_empty_dirs(root: Path) -> None:
    if not root.exists():
        return

    for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()


def _iter_retention_skip_label_sets() -> list[SkipKey]:
    labels: list[SkipKey] = []
    for source in REGISTERED_SOURCES.values():
        if not source.has_parquet_pipeline:
            continue
        for category in source.categories:
            labels.append((SKIP_REASON_NO_PARQUET, source.exchange, category))
            labels.append((SKIP_REASON_GRACE, source.exchange, category))
            labels.append((SKIP_REASON_POLYMARKET, source.exchange, category))
    return labels


def _reset_skip_gauges() -> None:
    for reason, exchange, category in _iter_retention_skip_label_sets():
        raw_retention_skipped_files_last_run.labels(
            reason=reason,
            exchange=exchange,
            category=category,
        ).set(0.0)
    raw_retention_run_skipped.labels(reason=SKIP_REASON_BACKLOG).set(0.0)
    for category in all_categories():
        raw_retention_backlog_blocked_files.labels(category=category).set(0.0)


def publish_retention_metrics(stats: RawRetentionStats, *, dry_run: bool) -> None:
    raw_retention_last_run_timestamp.set(time.time())
    _reset_skip_gauges()

    if stats.run_skipped_backlog:
        raw_retention_run_skipped.labels(reason=SKIP_REASON_BACKLOG).set(1.0)
        for category, count in stats.backlog_by_category.items():
            if count > 0:
                raw_retention_backlog_blocked_files.labels(category=category).set(float(count))
        raw_retention_would_delete_files.set(0.0)
        raw_retention_would_delete_bytes.set(0.0)
        return

    for (reason, exchange, category), count in stats.skip_by_source.items():
        if count > 0:
            raw_retention_skipped_files_last_run.labels(
                reason=reason,
                exchange=exchange,
                category=category,
            ).set(float(count))

    if dry_run:
        raw_retention_would_delete_files.set(float(stats.would_delete))
        raw_retention_would_delete_bytes.set(float(stats.bytes_would_free))
        return

    raw_retention_would_delete_files.set(0.0)
    raw_retention_would_delete_bytes.set(0.0)


def _record_skip(
    stats: RawRetentionStats,
    *,
    reason: str,
    exchange: str,
    category: str,
) -> None:
    stats.skip_by_source[(reason, exchange, category)] += 1
    if reason == SKIP_REASON_NO_PARQUET:
        stats.skipped_no_parquet += 1
    elif reason == SKIP_REASON_GRACE:
        stats.skipped_grace += 1
    elif reason == SKIP_REASON_POLYMARKET:
        stats.skipped_polymarket += 1


def _log_skip_summary(stats: RawRetentionStats) -> None:
    if not stats.skip_by_source:
        return
    breakdown = {
        f"{exchange}/{category}/{reason}": count
        for (reason, exchange, category), count in sorted(stats.skip_by_source.items())
        if count > 0
    }
    logger.info("Raw retention skip breakdown: %s", breakdown)


def run_raw_retention(
    *,
    project_root: Path,
    data_root: Path,
    state_path: Path,
    grace_hours: float = DEFAULT_GRACE_HOURS,
    dry_run: bool = True,
) -> dict[str, int]:
    """Delete eligible raw JSONL batches or report dry-run candidates."""
    stats = RawRetentionStats()

    backlog_by_category = collect_backlog_metrics(
        project_root=project_root,
        state_path=state_path,
    )
    backlog_total = sum(backlog_by_category.values())
    if backlog_total > 0:
        stats.run_skipped_backlog = 1
        stats.backlog_by_category = dict(backlog_by_category)
        logger.info(
            "Raw retention skipped — backlog_total=%s backlog_by_category=%s",
            backlog_total,
            backlog_by_category,
        )
        publish_retention_metrics(stats, dry_run=dry_run)
        return stats.as_dict()

    cutoff_ts = time.time() - (grace_hours * 3600)

    for source in REGISTERED_SOURCES.values():
        if not source.has_parquet_pipeline:
            continue

        exchange_raw_root = raw_root(project_root, source.exchange)
        if source.exchange == POLYMARKET:
            for category in source.categories:
                pm_count = len(iter_jsonl_paths(exchange_raw_root, category))
                stats.skipped_polymarket += pm_count
                if pm_count:
                    stats.skip_by_source[(SKIP_REASON_POLYMARKET, source.exchange, category)] = pm_count
                    logger.info(
                        "Raw retention skipped Polymarket raw — exchange=%s category=%s files=%s",
                        source.exchange,
                        category,
                        pm_count,
                    )
            continue

        for category in source.categories:
            for jsonl_path in iter_jsonl_paths(exchange_raw_root, category):
                parquet_path = batch_parquet_path_from_jsonl(
                    jsonl_path,
                    project_root=project_root,
                    data_root=data_root,
                )
                if not parquet_path.exists():
                    _record_skip(
                        stats,
                        reason=SKIP_REASON_NO_PARQUET,
                        exchange=source.exchange,
                        category=category,
                    )
                    continue

                try:
                    file_stat = jsonl_path.stat()
                except FileNotFoundError:
                    continue

                if file_stat.st_mtime > cutoff_ts:
                    _record_skip(
                        stats,
                        reason=SKIP_REASON_GRACE,
                        exchange=source.exchange,
                        category=category,
                    )
                    continue

                rel_path = relative_to_project(jsonl_path, project_root)
                file_size = file_stat.st_size

                if dry_run:
                    stats.would_delete += 1
                    stats.bytes_would_free += file_size
                    logger.info(
                        "Dry-run: would delete %s (%s bytes) exchange=%s category=%s",
                        rel_path,
                        file_size,
                        source.exchange,
                        category,
                    )
                    continue

                try:
                    jsonl_path.unlink()
                    stats.deleted += 1
                    stats.bytes_freed += file_size
                    raw_retention_files_deleted_total.inc()
                    raw_retention_bytes_freed_total.inc(file_size)
                    logger.info(
                        "Deleted processed raw file %s (%s bytes) exchange=%s category=%s",
                        rel_path,
                        file_size,
                        source.exchange,
                        category,
                    )
                except OSError:
                    stats.errors += 1
                    logger.exception(
                        "Failed to delete raw file: %s exchange=%s category=%s",
                        jsonl_path,
                        source.exchange,
                        category,
                    )

    if not dry_run and stats.deleted > 0:
        _remove_empty_dirs(project_root / "raw")

    _log_skip_summary(stats)
    publish_retention_metrics(stats, dry_run=dry_run)
    return stats.as_dict()


class RawRetentionPoller:
    def __init__(
        self,
        *,
        project_root: Path,
        data_root: Path,
        state_path: Path,
        config: RawRetentionConfig,
    ) -> None:
        self.project_root = project_root
        self.data_root = data_root
        self.state_path = state_path
        self.config = config
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="raw-retention-poller",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Raw retention poller started (interval=%ss, grace=%sh, dry_run=%s, polymarket=excluded)",
            self.config.interval_sec,
            self.config.grace_hours,
            self.config.dry_run,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.config.interval_sec + 1)
        logger.info("Raw retention poller stopped")

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                stats = run_raw_retention(
                    project_root=self.project_root,
                    data_root=self.data_root,
                    state_path=self.state_path,
                    grace_hours=self.config.grace_hours,
                    dry_run=self.config.dry_run,
                )
                if stats.get("run_skipped_backlog"):
                    logger.info("Raw retention complete — skipped (backlog)")
                elif self.config.dry_run:
                    logger.info(
                        "Raw retention dry-run complete — would_delete: %s, bytes: %s, "
                        "skipped_no_parquet: %s, skipped_grace: %s, skipped_polymarket: %s",
                        stats["would_delete"],
                        stats["bytes_would_free"],
                        stats["skipped_no_parquet"],
                        stats["skipped_grace"],
                        stats["skipped_polymarket"],
                    )
                else:
                    logger.info(
                        "Raw retention complete — deleted: %s, bytes_freed: %s, errors: %s",
                        stats["deleted"],
                        stats["bytes_freed"],
                        stats["errors"],
                    )
            except Exception:
                logger.exception("Raw retention failed")
            self._stop_event.wait(self.config.interval_sec)


def start_raw_retention_poller(*, config: RawRetentionConfig) -> RawRetentionPoller | None:
    if not config.enabled:
        return None

    project_root, data_root, state_path, _ = project_paths()
    poller = RawRetentionPoller(
        project_root=project_root,
        data_root=data_root,
        state_path=state_path,
        config=config,
    )
    poller.start()
    return poller
