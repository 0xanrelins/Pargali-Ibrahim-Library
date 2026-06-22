#!/usr/bin/env python3
"""Delete processed raw JSONL files after a grace period (Phase 2 retention)."""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

from metrics.pipeline_registry import (
    raw_cleanup_bytes_freed_total,
    raw_cleanup_files_deleted_total,
    raw_last_cleanup_timestamp,
)

logger = logging.getLogger(__name__)

DEFAULT_CLEANUP_INTERVAL_SEC = 86_400.0
DEFAULT_GRACE_HOURS = 24.0


def _relative_path(path: Path, project_root: Path) -> str:
    return path.relative_to(project_root).as_posix()


def load_processed_files(state_path: Path) -> set[str]:
    if not state_path.exists():
        return set()

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    files = payload.get("processed_files", [])
    if not isinstance(files, list):
        raise ValueError(f"Invalid processed state file: {state_path}")
    return {str(item) for item in files}


def _remove_empty_dirs(root: Path) -> None:
    if not root.exists():
        return

    for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()


def count_raw_cleanup_states(
    *,
    project_root: Path,
    raw_root: Path,
    state_path: Path,
    grace_hours: float = DEFAULT_GRACE_HOURS,
) -> tuple[int, int]:
    """Return (awaiting_cleanup, pending_grace) counts for processed raw files still on disk."""
    processed_files = load_processed_files(state_path)
    cutoff_ts = time.time() - (grace_hours * 3600)
    awaiting_cleanup = 0
    pending_grace = 0

    if not raw_root.exists():
        return awaiting_cleanup, pending_grace

    for jsonl_path in raw_root.rglob("*.jsonl"):
        rel_path = _relative_path(jsonl_path, project_root)
        if rel_path not in processed_files:
            continue
        try:
            file_mtime = jsonl_path.stat().st_mtime
        except FileNotFoundError:
            continue
        if file_mtime <= cutoff_ts:
            awaiting_cleanup += 1
        else:
            pending_grace += 1

    return awaiting_cleanup, pending_grace


def cleanup_processed_raw(
    *,
    project_root: Path,
    raw_root: Path,
    state_path: Path,
    grace_hours: float = DEFAULT_GRACE_HOURS,
    dry_run: bool = False,
) -> dict[str, int]:
    """Delete processed raw JSONL files older than the grace period."""
    processed_files = load_processed_files(state_path)
    cutoff_ts = time.time() - (grace_hours * 3600)

    stats = {
        "candidates": 0,
        "deleted": 0,
        "bytes_freed": 0,
        "skipped_unprocessed": 0,
        "skipped_too_new": 0,
        "errors": 0,
    }

    if not raw_root.exists():
        raw_last_cleanup_timestamp.set(time.time())
        return stats

    for jsonl_path in sorted(raw_root.rglob("*.jsonl")):
        rel_path = _relative_path(jsonl_path, project_root)
        if rel_path not in processed_files:
            stats["skipped_unprocessed"] += 1
            continue

        try:
            file_stat = jsonl_path.stat()
        except FileNotFoundError:
            continue

        if file_stat.st_mtime > cutoff_ts:
            stats["skipped_too_new"] += 1
            continue

        stats["candidates"] += 1
        file_size = file_stat.st_size

        if dry_run:
            logger.info("Dry-run: would delete %s (%s bytes)", rel_path, file_size)
            stats["deleted"] += 1
            stats["bytes_freed"] += file_size
            continue

        try:
            jsonl_path.unlink()
            stats["deleted"] += 1
            stats["bytes_freed"] += file_size
            raw_cleanup_files_deleted_total.inc()
            raw_cleanup_bytes_freed_total.inc(file_size)
            logger.info("Deleted processed raw file %s (%s bytes)", rel_path, file_size)
        except OSError:
            stats["errors"] += 1
            logger.exception("Failed to delete raw file: %s", jsonl_path)

    if not dry_run and stats["deleted"] > 0:
        _remove_empty_dirs(raw_root)

    raw_last_cleanup_timestamp.set(time.time())
    return stats


class RawCleanupPoller:
    def __init__(
        self,
        *,
        project_root: Path,
        raw_root: Path,
        state_path: Path,
        grace_hours: float = DEFAULT_GRACE_HOURS,
        interval_sec: float = DEFAULT_CLEANUP_INTERVAL_SEC,
        dry_run: bool = False,
    ) -> None:
        self.project_root = project_root
        self.raw_root = raw_root
        self.state_path = state_path
        self.grace_hours = grace_hours
        self.interval_sec = interval_sec
        self.dry_run = dry_run
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="raw-cleanup-poller",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Raw cleanup poller started (interval=%ss, grace=%sh, dry_run=%s)",
            self.interval_sec,
            self.grace_hours,
            self.dry_run,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.interval_sec + 1)
        logger.info("Raw cleanup poller stopped")

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                stats = cleanup_processed_raw(
                    project_root=self.project_root,
                    raw_root=self.raw_root,
                    state_path=self.state_path,
                    grace_hours=self.grace_hours,
                    dry_run=self.dry_run,
                )
                logger.info(
                    "Raw cleanup complete — deleted: %s, bytes_freed: %s, "
                    "skipped_unprocessed: %s, skipped_too_new: %s, errors: %s",
                    stats["deleted"],
                    stats["bytes_freed"],
                    stats["skipped_unprocessed"],
                    stats["skipped_too_new"],
                    stats["errors"],
                )
            except Exception:
                logger.exception("Raw cleanup failed")
            self._stop_event.wait(self.interval_sec)


def start_raw_cleanup_poller(
    *,
    project_root: Path,
    raw_root: Path,
    state_path: Path,
    grace_hours: float = DEFAULT_GRACE_HOURS,
    interval_sec: float = DEFAULT_CLEANUP_INTERVAL_SEC,
    dry_run: bool = False,
) -> RawCleanupPoller:
    poller = RawCleanupPoller(
        project_root=project_root,
        raw_root=raw_root,
        state_path=state_path,
        grace_hours=grace_hours,
        interval_sec=interval_sec,
        dry_run=dry_run,
    )
    poller.start()
    return poller
