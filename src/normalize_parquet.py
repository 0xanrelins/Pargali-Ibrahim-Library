#!/usr/bin/env python3
"""Normalize raw JSONL batches into immutable parquet files (DuckDB writer)."""

from __future__ import annotations

import argparse
import logging
import signal
import time

from metrics.exporter import DEFAULT_PIPELINE_METRICS_PORT, start_exporter
from metrics.storage_scanner import collect_storage_metrics, start_storage_poller
from normalizer.paths import project_paths
from normalizer.runner import NORMALIZE_INTERVAL_SEC, normalize_once

logger = logging.getLogger(__name__)

stop_requested = False


def request_stop(signum: int, _frame: object) -> None:
    global stop_requested
    logger.info("Shutdown requested (signal %s)", signum)
    stop_requested = True


def log_stats(stats: dict[str, int], *, label: str) -> None:
    logger.info(label)
    for key, value in stats.items():
        logger.info("%s: %s", key, value)


def sleep_until_stop(seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while not stop_requested and time.monotonic() < deadline:
        time.sleep(min(1.0, deadline - time.monotonic()))


def run_loop() -> None:
    global stop_requested

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    project_root, data_root, _, _ = project_paths()
    raw_root = project_root / "raw"

    start_exporter(port=DEFAULT_PIPELINE_METRICS_PORT)
    storage_poller = start_storage_poller(
        project_root=project_root,
        raw_root=raw_root,
        data_root=data_root,
    )
    logger.info(
        "Prometheus normalizer metrics: http://127.0.0.1:%s/metrics",
        DEFAULT_PIPELINE_METRICS_PORT,
    )
    logger.info("Starting normalizer loop (interval=%ss)", NORMALIZE_INTERVAL_SEC)

    try:
        while not stop_requested:
            stats = normalize_once()
            log_stats(stats, label="Normalize batch complete")

            if stop_requested:
                break

            logger.info("Next normalize batch in %s seconds", NORMALIZE_INTERVAL_SEC)
            sleep_until_stop(NORMALIZE_INTERVAL_SEC)
    finally:
        storage_poller.stop()

    logger.info("Normalizer loop stopped")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normalize raw JSONL batches into immutable parquet files.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--once",
        action="store_true",
        help="Process all unprocessed raw JSONL files once and exit.",
    )
    mode.add_argument(
        "--loop",
        action="store_true",
        help="Run normalization continuously until interrupted.",
    )
    return parser


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = build_parser()
    args = parser.parse_args()

    if args.loop:
        run_loop()
        return 0

    start_exporter(port=DEFAULT_PIPELINE_METRICS_PORT)
    project_root, data_root, _, _ = project_paths()
    collect_storage_metrics(
        project_root=project_root,
        raw_root=project_root / "raw",
        data_root=data_root,
    )
    stats = normalize_once()
    log_stats(stats, label="Normalize complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
