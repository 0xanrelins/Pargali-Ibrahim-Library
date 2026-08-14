"""Conservative daily raw JSONL retention after immutable parquet batches exist."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from datasets import POLYMARKET, REGISTERED_SOURCES, iter_jsonl_paths, raw_root
from metrics.pipeline_registry import (
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


@dataclass(frozen=True)
class RawRetentionConfig:
    enabled: bool = False
    dry_run: bool = True
    grace_hours: float = DEFAULT_GRACE_HOURS
    interval_sec: float = DEFAULT_RETENTION_INTERVAL_SEC
    exclude_polymarket: bool = True


def _remove_empty_dirs(root: Path) -> None:
    if not root.exists():
        return

    for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()


def _global_backlog_total(*, project_root: Path, state_path: Path) -> int:
    backlog = collect_backlog_metrics(project_root=project_root, state_path=state_path)
    return sum(backlog.values())


def _reset_skip_gauges() -> None:
    for reason in (
        SKIP_REASON_NO_PARQUET,
        SKIP_REASON_GRACE,
        SKIP_REASON_POLYMARKET,
    ):
        raw_retention_skipped_files_last_run.labels(reason=reason).set(0.0)
    raw_retention_run_skipped.labels(reason=SKIP_REASON_BACKLOG).set(0.0)


def publish_retention_metrics(stats: dict[str, int], *, dry_run: bool) -> None:
    raw_retention_last_run_timestamp.set(time.time())
    _reset_skip_gauges()

    if stats.get("run_skipped_backlog", 0):
        raw_retention_run_skipped.labels(reason=SKIP_REASON_BACKLOG).set(1.0)
        raw_retention_would_delete_files.set(0.0)
        raw_retention_would_delete_bytes.set(0.0)
        return

    raw_retention_skipped_files_last_run.labels(reason=SKIP_REASON_NO_PARQUET).set(
        float(stats.get("skipped_no_parquet", 0))
    )
    raw_retention_skipped_files_last_run.labels(reason=SKIP_REASON_GRACE).set(
        float(stats.get("skipped_grace", 0))
    )
    raw_retention_skipped_files_last_run.labels(reason=SKIP_REASON_POLYMARKET).set(
        float(stats.get("skipped_polymarket", 0))
    )

    if dry_run:
        raw_retention_would_delete_files.set(float(stats.get("would_delete", 0)))
        raw_retention_would_delete_bytes.set(float(stats.get("bytes_would_free", 0)))
        return

    raw_retention_would_delete_files.set(0.0)
    raw_retention_would_delete_bytes.set(0.0)


def run_raw_retention(
    *,
    project_root: Path,
    data_root: Path,
    state_path: Path,
    grace_hours: float = DEFAULT_GRACE_HOURS,
    dry_run: bool = True,
    exclude_polymarket: bool = True,
) -> dict[str, int]:
    """Delete eligible raw JSONL batches or report dry-run candidates."""
    stats = {
        "run_skipped_backlog": 0,
        "would_delete": 0,
        "deleted": 0,
        "bytes_would_free": 0,
        "bytes_freed": 0,
        "skipped_no_parquet": 0,
        "skipped_grace": 0,
        "skipped_polymarket": 0,
        "errors": 0,
    }

    backlog_total = _global_backlog_total(project_root=project_root, state_path=state_path)
    if backlog_total > 0:
        stats["run_skipped_backlog"] = 1
        logger.info(
            "Raw retention skipped — backlog=%s unprocessed raw files",
            backlog_total,
        )
        publish_retention_metrics(stats, dry_run=dry_run)
        return stats

    cutoff_ts = time.time() - (grace_hours * 3600)

    for source in REGISTERED_SOURCES.values():
        if not source.has_parquet_pipeline:
            continue

        if exclude_polymarket and source.exchange == POLYMARKET:
            exchange_raw_root = raw_root(project_root, source.exchange)
            for category in source.categories:
                stats["skipped_polymarket"] += len(iter_jsonl_paths(exchange_raw_root, category))
            continue

        exchange_raw_root = raw_root(project_root, source.exchange)
        for category in source.categories:
            for jsonl_path in iter_jsonl_paths(exchange_raw_root, category):
                parquet_path = batch_parquet_path_from_jsonl(
                    jsonl_path,
                    project_root=project_root,
                    data_root=data_root,
                )
                if not parquet_path.exists():
                    stats["skipped_no_parquet"] += 1
                    continue

                try:
                    file_stat = jsonl_path.stat()
                except FileNotFoundError:
                    continue

                if file_stat.st_mtime > cutoff_ts:
                    stats["skipped_grace"] += 1
                    continue

                rel_path = relative_to_project(jsonl_path, project_root)
                file_size = file_stat.st_size

                if dry_run:
                    stats["would_delete"] += 1
                    stats["bytes_would_free"] += file_size
                    logger.info(
                        "Dry-run: would delete %s (%s bytes)",
                        rel_path,
                        file_size,
                    )
                    continue

                try:
                    jsonl_path.unlink()
                    stats["deleted"] += 1
                    stats["bytes_freed"] += file_size
                    raw_retention_files_deleted_total.inc()
                    raw_retention_bytes_freed_total.inc(file_size)
                    logger.info("Deleted processed raw file %s (%s bytes)", rel_path, file_size)
                except OSError:
                    stats["errors"] += 1
                    logger.exception("Failed to delete raw file: %s", jsonl_path)

    if not dry_run and stats["deleted"] > 0:
        _remove_empty_dirs(project_root / "raw")

    publish_retention_metrics(stats, dry_run=dry_run)
    return stats


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
            "Raw retention poller started (interval=%ss, grace=%sh, dry_run=%s, exclude_polymarket=%s)",
            self.config.interval_sec,
            self.config.grace_hours,
            self.config.dry_run,
            self.config.exclude_polymarket,
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
                    exclude_polymarket=self.config.exclude_polymarket,
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
