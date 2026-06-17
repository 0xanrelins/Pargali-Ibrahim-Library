"""Convert raw JSONL files to daily Parquet archives (Plan 2)."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict

import pandas as pd

from catalog.sync import start_catalog_poller, sync_catalog
from metrics.exporter import DEFAULT_PIPELINE_METRICS_PORT, start_exporter
from metrics.pipeline_registry import (
    parquet_failed_conversions_total,
    parquet_files_processed_total,
    parquet_last_batch_duration_seconds,
    parquet_last_batch_files_processed,
    parquet_last_batch_timestamp,
)
from metrics.coverage_scanner import collect_coverage_metrics, start_coverage_poller
from metrics.quality_validator import collect_quality_metrics, start_quality_poller
from metrics.storage_scanner import collect_storage_metrics, start_storage_poller
from parse_raw import EventCategory, ParsedRow, iter_jsonl_paths, parse_raw_line

logger = logging.getLogger(__name__)

CONVERT_INTERVAL_SEC = 3600
stop_requested = False

TRADE_COLUMNS = [
    "timestamp",
    "symbol",
    "agg_trade_id",
    "price",
    "amount",
    "side",
    "exchange",
]
LIQUIDATION_COLUMNS = [
    "timestamp",
    "symbol",
    "side",
    "price",
    "avg_price",
    "quantity",
    "filled_qty",
    "exchange",
]

CATEGORY_COLUMNS: dict[EventCategory, list[str]] = {
    "trades": TRADE_COLUMNS,
    "liquidations": LIQUIDATION_COLUMNS,
}

ParquetKey = tuple[EventCategory, str, str]


class ProcessedState(TypedDict):
    processed_files: list[str]


def project_paths() -> tuple[Path, Path, Path, Path]:
    project_root = Path(__file__).resolve().parent.parent
    return (
        project_root,
        project_root / "raw" / "binance_futures",
        project_root / "data",
        project_root / "state" / "processed.json",
    )


def relative_path(path: Path, project_root: Path) -> str:
    return path.relative_to(project_root).as_posix()


def load_processed_state(state_path: Path) -> set[str]:
    if not state_path.exists():
        return set()

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    files = payload.get("processed_files", [])
    if not isinstance(files, list):
        raise ValueError(f"Invalid processed state file: {state_path}")
    return {str(item) for item in files}


def save_processed_state(state_path: Path, processed_files: set[str]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload: ProcessedState = {
        "processed_files": sorted(processed_files),
    }
    state_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def date_from_timestamp_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).strftime("%Y-%m-%d")


def parquet_path(data_root: Path, category: EventCategory, symbol: str, day: str) -> Path:
    return data_root / category / "binance_futures" / symbol / f"{day}.parquet"


def rows_to_dataframe(rows: list[ParsedRow], category: EventCategory) -> pd.DataFrame:
    columns = CATEGORY_COLUMNS[category]
    frame = pd.DataFrame(rows, columns=columns)

    if category == "trades":
        frame = frame.astype(
            {
                "timestamp": "int64",
                "symbol": "string",
                "agg_trade_id": "int64",
                "price": "float64",
                "amount": "float64",
                "side": "string",
                "exchange": "string",
            }
        )
    else:
        frame = frame.astype(
            {
                "timestamp": "int64",
                "symbol": "string",
                "side": "string",
                "price": "float64",
                "avg_price": "float64",
                "quantity": "float64",
                "filled_qty": "float64",
                "exchange": "string",
            }
        )

    return frame


def append_parquet(out_path: Path, frame: pd.DataFrame) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        existing = pd.read_parquet(out_path)
        combined = pd.concat([existing, frame], ignore_index=True)
    else:
        combined = frame

    combined.to_parquet(out_path, index=False)
    return len(frame)


def collect_rows_from_file(jsonl_path: Path) -> dict[ParquetKey, list[ParsedRow]]:
    grouped: dict[ParquetKey, list[ParsedRow]] = defaultdict(list)

    for raw_line in jsonl_path.read_text(encoding="utf-8").splitlines():
        parsed = parse_raw_line(raw_line)
        if parsed is None:
            continue

        category, row = parsed
        day = date_from_timestamp_ms(int(row["timestamp"]))
        symbol = str(row["symbol"])
        grouped[(category, symbol, day)].append(row)

    return grouped


def convert_once() -> dict[str, int]:
    batch_started = time.monotonic()
    project_root, raw_root, data_root, state_path = project_paths()
    processed_files = load_processed_state(state_path)

    stats = {
        "raw_files_seen": 0,
        "raw_files_processed": 0,
        "raw_files_skipped": 0,
        "rows_written": 0,
        "parquet_files_updated": 0,
        "raw_files_failed": 0,
    }
    updated_parquet_paths: set[Path] = set()

    pending_groups: dict[ParquetKey, list[ParsedRow]] = defaultdict(list)
    newly_processed: list[str] = []

    for category in ("trades", "liquidations"):
        for jsonl_path in iter_jsonl_paths(raw_root, category):
            stats["raw_files_seen"] += 1
            rel_path = relative_path(jsonl_path, project_root)
            if rel_path in processed_files:
                stats["raw_files_skipped"] += 1
                continue

            try:
                grouped = collect_rows_from_file(jsonl_path)
            except Exception:
                stats["raw_files_failed"] += 1
                parquet_failed_conversions_total.labels(reason="raw_read").inc()
                logger.exception("Failed to read raw file: %s", jsonl_path)
                continue

            for key, rows in grouped.items():
                pending_groups[key].extend(rows)

            newly_processed.append(rel_path)
            stats["raw_files_processed"] += 1
            parquet_files_processed_total.inc()

    for (category, symbol, day), rows in sorted(pending_groups.items()):
        if not rows:
            continue

        out_path = parquet_path(data_root, category, symbol, day)
        try:
            frame = rows_to_dataframe(rows, category)
            rows_written = append_parquet(out_path, frame)
        except Exception:
            parquet_failed_conversions_total.labels(reason="parquet_write").inc()
            logger.exception(
                "Failed to write parquet: %s (%s %s %s)",
                out_path,
                category,
                symbol,
                day,
            )
            continue

        stats["rows_written"] += rows_written
        updated_parquet_paths.add(out_path)

    stats["parquet_files_updated"] = len(updated_parquet_paths)

    if newly_processed:
        processed_files.update(newly_processed)
        save_processed_state(state_path, processed_files)

    parquet_last_batch_timestamp.set(time.time())
    parquet_last_batch_duration_seconds.set(time.monotonic() - batch_started)
    parquet_last_batch_files_processed.set(stats["raw_files_processed"])

    return stats


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

    project_root, raw_root, data_root, _state_path = project_paths()
    storage_poller = None
    coverage_poller = None
    quality_poller = None
    catalog_poller = None

    try:
        start_exporter(port=DEFAULT_PIPELINE_METRICS_PORT)
        logger.info(
            "Prometheus pipeline metrics: http://127.0.0.1:%s/metrics",
            DEFAULT_PIPELINE_METRICS_PORT,
        )
        collect_storage_metrics(
            project_root=project_root,
            raw_root=raw_root,
            data_root=data_root,
        )
        collect_coverage_metrics(data_root=data_root)
        collect_quality_metrics(data_root=data_root)
        sync_catalog()
        storage_poller = start_storage_poller(
            project_root=project_root,
            raw_root=raw_root,
            data_root=data_root,
        )
        coverage_poller = start_coverage_poller(data_root=data_root)
        quality_poller = start_quality_poller(data_root=data_root)
        catalog_poller = start_catalog_poller()
        logger.info("Starting hourly conversion loop (interval=%ss)", CONVERT_INTERVAL_SEC)

        while not stop_requested:
            stats = convert_once()
            log_stats(stats, label="Conversion batch complete")

            if stop_requested:
                break

            logger.info("Next conversion in %s seconds", CONVERT_INTERVAL_SEC)
            sleep_until_stop(CONVERT_INTERVAL_SEC)
    finally:
        if catalog_poller is not None:
            catalog_poller.stop()
        if quality_poller is not None:
            quality_poller.stop()
        if coverage_poller is not None:
            coverage_poller.stop()
        if storage_poller is not None:
            storage_poller.stop()

    logger.info("Conversion loop stopped")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert raw JSONL to daily Parquet files.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--once",
        action="store_true",
        help="Process all unprocessed raw JSONL files once and exit.",
    )
    mode.add_argument(
        "--loop",
        action="store_true",
        help="Run conversion every hour until interrupted.",
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
    project_root, raw_root, data_root, _state_path = project_paths()
    collect_storage_metrics(
        project_root=project_root,
        raw_root=raw_root,
        data_root=data_root,
    )
    collect_coverage_metrics(data_root=data_root)
    collect_quality_metrics(data_root=data_root)
    sync_catalog()
    stats = convert_once()
    log_stats(stats, label="Conversion complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
