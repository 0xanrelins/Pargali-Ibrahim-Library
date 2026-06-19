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
    parquet_raw_backlog_files,
)
from metrics.coverage_scanner import collect_coverage_metrics, start_coverage_poller
from metrics.quality_validator import collect_quality_metrics, start_quality_poller
from derive_polymarket import derive_polymarket_once
from knowledge.btc_suite import compute_knowledge_once
from metrics.raw_cleanup import start_raw_cleanup_poller
from metrics.storage_scanner import collect_storage_metrics, start_storage_poller
from datasets import (
    BINANCE_FUTURES,
    ORDERBOOK_CATEGORIES,
    POLYMARKET,
    POLYMARKET_SYMBOL,
    PREDICTION_ORDERBOOK_CATEGORIES,
    REGISTERED_SOURCES,
    all_categories,
    parquet_path as dataset_parquet_path,
    raw_root as dataset_raw_root,
    symbol_parquet_root,
)
from parse_polymarket import (
    OutcomeTokenRegistry,
    PolymarketCategory,
    parse_polymarket_lines,
    seed_token_registry_from_raw,
)
from parse_raw import EventCategory as BinanceEventCategory, ParsedRow, parse_raw_lines

logger = logging.getLogger(__name__)

CONVERT_INTERVAL_SEC = 1800
ORDERBOOK_BATCH_LIMIT = 80
stop_requested = False

ALL_CATEGORIES = all_categories()

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
ORDERBOOK_DIFF_COLUMNS = [
    "timestamp",
    "event_time",
    "transaction_time",
    "symbol",
    "first_update_id",
    "final_update_id",
    "previous_final_update_id",
    "side",
    "price",
    "quantity",
    "exchange",
]
ORDERBOOK_SNAPSHOT_COLUMNS = [
    "timestamp",
    "last_update_id",
    "symbol",
    "side",
    "level_index",
    "price",
    "quantity",
    "exchange",
]

PREDICTION_METADATA_COLUMNS = [
    "timestamp",
    "symbol",
    "event_type",
    "slug",
    "condition_id",
    "question",
    "end_date",
    "yes_token_id",
    "no_token_id",
    "exchange",
]
PREDICTION_PRICE_COLUMNS = [
    "timestamp",
    "symbol",
    "event_type",
    "asset_id",
    "outcome",
    "best_bid",
    "best_ask",
    "spread",
    "price",
    "side",
    "exchange",
]
PREDICTION_TRADE_COLUMNS = [
    "timestamp",
    "symbol",
    "asset_id",
    "outcome",
    "price",
    "side",
    "size",
    "exchange",
]
PREDICTION_ORDERBOOK_COLUMNS = [
    "timestamp",
    "symbol",
    "asset_id",
    "outcome",
    "side",
    "level_index",
    "price",
    "quantity",
    "exchange",
]

EventCategory = BinanceEventCategory | PolymarketCategory

CATEGORY_COLUMNS: dict[str, list[str]] = {
    "trades": TRADE_COLUMNS,
    "liquidations": LIQUIDATION_COLUMNS,
    "orderbook_diff": ORDERBOOK_DIFF_COLUMNS,
    "orderbook_snapshots": ORDERBOOK_SNAPSHOT_COLUMNS,
    "prediction_metadata": PREDICTION_METADATA_COLUMNS,
    "prediction_price": PREDICTION_PRICE_COLUMNS,
    "prediction_trades": PREDICTION_TRADE_COLUMNS,
    "prediction_orderbook": PREDICTION_ORDERBOOK_COLUMNS,
}

ParquetKey = tuple[str, str, str, str]


class ProcessedState(TypedDict):
    processed_files: list[str]


def project_paths() -> tuple[Path, Path, Path]:
    project_root = Path(__file__).resolve().parent.parent
    return (
        project_root,
        project_root / "data",
        project_root / "state" / "processed.json",
    )


def iter_jsonl_paths(raw_root: Path, category: str) -> list[Path]:
    category_dir = raw_root / category
    if not category_dir.exists():
        return []
    return sorted(category_dir.glob("*/*.jsonl"))


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


def parquet_path(
    data_root: Path,
    exchange: str,
    category: str,
    symbol: str,
    day: str,
) -> Path:
    return dataset_parquet_path(data_root, exchange, category, symbol, day)


def rows_to_dataframe(rows: list[ParsedRow], category: str) -> pd.DataFrame:
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
    elif category == "liquidations":
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
    elif category == "orderbook_diff":
        frame = frame.astype(
            {
                "timestamp": "int64",
                "event_time": "int64",
                "transaction_time": "int64",
                "symbol": "string",
                "first_update_id": "int64",
                "final_update_id": "int64",
                "previous_final_update_id": "Int64",
                "side": "string",
                "price": "float64",
                "quantity": "float64",
                "exchange": "string",
            }
        )
    elif category == "prediction_metadata":
        frame = frame.astype(
            {
                "timestamp": "int64",
                "symbol": "string",
                "event_type": "string",
                "slug": "string",
                "condition_id": "string",
                "question": "string",
                "end_date": "string",
                "yes_token_id": "string",
                "no_token_id": "string",
                "exchange": "string",
            }
        )
    elif category == "prediction_price":
        frame = frame.astype(
            {
                "timestamp": "int64",
                "symbol": "string",
                "event_type": "string",
                "asset_id": "string",
                "outcome": "string",
                "best_bid": "Float64",
                "best_ask": "Float64",
                "spread": "Float64",
                "price": "Float64",
                "side": "string",
                "exchange": "string",
            }
        )
    elif category == "prediction_trades":
        frame = frame.astype(
            {
                "timestamp": "int64",
                "symbol": "string",
                "asset_id": "string",
                "outcome": "string",
                "price": "float64",
                "side": "string",
                "size": "float64",
                "exchange": "string",
            }
        )
    elif category == "prediction_orderbook":
        frame = frame.astype(
            {
                "timestamp": "int64",
                "symbol": "string",
                "asset_id": "string",
                "outcome": "string",
                "side": "string",
                "level_index": "int64",
                "price": "float64",
                "quantity": "float64",
                "exchange": "string",
            }
        )
    else:
        frame = frame.astype(
            {
                "timestamp": "int64",
                "last_update_id": "int64",
                "symbol": "string",
                "side": "string",
                "level_index": "int64",
                "price": "float64",
                "quantity": "float64",
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


def enrich_token_registry_from_parquet(
    registry: OutcomeTokenRegistry,
    data_root: Path,
) -> None:
    metadata_dir = symbol_parquet_root(
        data_root,
        POLYMARKET,
        "prediction_metadata",
        POLYMARKET_SYMBOL,
    )
    if not metadata_dir.exists():
        return

    for parquet_file in sorted(metadata_dir.glob("*.parquet")):
        frame = pd.read_parquet(parquet_file)
        for row in frame.to_dict(orient="records"):
            registry.update_from_metadata(row)


def collect_rows_from_file(
    jsonl_path: Path,
    *,
    exchange: str,
    token_registry: OutcomeTokenRegistry | None = None,
) -> dict[ParquetKey, list[ParsedRow]]:
    grouped: dict[ParquetKey, list[ParsedRow]] = defaultdict(list)
    parser = parse_polymarket_lines if exchange == POLYMARKET else parse_raw_lines

    for raw_line in jsonl_path.read_text(encoding="utf-8").splitlines():
        if exchange == POLYMARKET:
            parsed = parser(raw_line, token_registry=token_registry)
        else:
            parsed = parser(raw_line)
        for category, row in parsed:
            day = date_from_timestamp_ms(int(row["timestamp"]))
            symbol = str(row["symbol"])
            grouped[(exchange, category, symbol, day)].append(row)

    return grouped


def write_grouped_rows(
    grouped: dict[ParquetKey, list[ParsedRow]],
    data_root: Path,
) -> tuple[int, set[Path], bool]:
    rows_written = 0
    updated_paths: set[Path] = set()

    for (exchange, category, symbol, day), rows in sorted(grouped.items()):
        if not rows:
            continue

        out_path = parquet_path(data_root, exchange, category, symbol, day)
        try:
            frame = rows_to_dataframe(rows, category)
            written = append_parquet(out_path, frame)
        except Exception:
            parquet_failed_conversions_total.labels(reason="parquet_write").inc()
            logger.exception(
                "Failed to write parquet: %s (%s %s %s)",
                out_path,
                category,
                symbol,
                day,
            )
            return rows_written, updated_paths, False

        rows_written += written
        updated_paths.add(out_path)

    return rows_written, updated_paths, True


def collect_parquet_backlog_metrics(
    *,
    project_root: Path,
    state_path: Path,
) -> dict[str, int]:
    processed_files = load_processed_state(state_path)
    backlog = {category: 0 for category in ALL_CATEGORIES}

    for source in REGISTERED_SOURCES.values():
        if not source.has_parquet_pipeline:
            continue
        raw_root = dataset_raw_root(project_root, source.exchange)
        for category in source.categories:
            for jsonl_path in iter_jsonl_paths(raw_root, category):
                rel_path = relative_path(jsonl_path, project_root)
                if rel_path not in processed_files:
                    backlog[category] += 1
            parquet_raw_backlog_files.labels(category=category).set(backlog[category])

    return backlog


def convert_once() -> dict[str, int]:
    batch_started = time.monotonic()
    project_root, data_root, state_path = project_paths()
    processed_files = load_processed_state(state_path)

    stats = {
        "raw_files_seen": 0,
        "raw_files_processed": 0,
        "raw_files_skipped": 0,
        "raw_files_deferred": 0,
        "rows_written": 0,
        "parquet_files_updated": 0,
        "raw_files_failed": 0,
    }
    updated_parquet_paths: set[Path] = set()
    orderbook_batch_count: dict[str, int] = defaultdict(int)

    collect_parquet_backlog_metrics(
        project_root=project_root,
        state_path=state_path,
    )

    for source in REGISTERED_SOURCES.values():
        if not source.has_parquet_pipeline:
            continue
        raw_root = dataset_raw_root(project_root, source.exchange)
        polymarket_token_registry: OutcomeTokenRegistry | None = None
        if source.exchange == POLYMARKET:
            polymarket_token_registry = seed_token_registry_from_raw(raw_root)
            enrich_token_registry_from_parquet(polymarket_token_registry, data_root)

        for category in source.categories:
            for jsonl_path in iter_jsonl_paths(raw_root, category):
                stats["raw_files_seen"] += 1
                rel_path = relative_path(jsonl_path, project_root)
                if rel_path in processed_files:
                    stats["raw_files_skipped"] += 1
                    continue

                if category in ORDERBOOK_CATEGORIES or category in PREDICTION_ORDERBOOK_CATEGORIES:
                    if orderbook_batch_count[category] >= ORDERBOOK_BATCH_LIMIT:
                        stats["raw_files_deferred"] += 1
                        continue

                try:
                    grouped = collect_rows_from_file(
                        jsonl_path,
                        exchange=source.exchange,
                        token_registry=polymarket_token_registry,
                    )
                except Exception:
                    stats["raw_files_failed"] += 1
                    parquet_failed_conversions_total.labels(reason="raw_read").inc()
                    logger.exception("Failed to read raw file: %s", jsonl_path)
                    continue

                rows_written, paths, ok = write_grouped_rows(grouped, data_root)
                if not ok:
                    stats["raw_files_failed"] += 1
                    continue

                processed_files.add(rel_path)
                save_processed_state(state_path, processed_files)

                if category in ORDERBOOK_CATEGORIES or category in PREDICTION_ORDERBOOK_CATEGORIES:
                    orderbook_batch_count[category] += 1

                stats["raw_files_processed"] += 1
                stats["rows_written"] += rows_written
                updated_parquet_paths.update(paths)
                parquet_files_processed_total.inc()

    stats["parquet_files_updated"] = len(updated_parquet_paths)

    collect_parquet_backlog_metrics(
        project_root=project_root,
        state_path=state_path,
    )

    parquet_last_batch_timestamp.set(time.time())
    parquet_last_batch_duration_seconds.set(time.monotonic() - batch_started)
    parquet_last_batch_files_processed.set(stats["raw_files_processed"])

    derived_stats = derive_polymarket_once(data_root)
    if derived_stats["rows_written"]:
        logger.info(
            "Polymarket derived metrics: days=%s rows=%s",
            derived_stats["days_processed"],
            derived_stats["rows_written"],
        )

    knowledge_stats = compute_knowledge_once(data_root)
    if knowledge_stats["rows_written"]:
        logger.info(
            "BTC knowledge objects: days=%s rows=%s",
            knowledge_stats["days_processed"],
            knowledge_stats["rows_written"],
        )

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

    project_root, data_root, state_path = project_paths()
    raw_root = dataset_raw_root(project_root, BINANCE_FUTURES)
    storage_poller = None
    coverage_poller = None
    quality_poller = None
    catalog_poller = None
    cleanup_poller = None

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
        cleanup_poller = start_raw_cleanup_poller(
            project_root=project_root,
            raw_root=raw_root,
            state_path=state_path,
        )
        logger.info("Starting hourly conversion loop (interval=%ss)", CONVERT_INTERVAL_SEC)

        while not stop_requested:
            stats = convert_once()
            log_stats(stats, label="Conversion batch complete")

            if stop_requested:
                break

            logger.info("Next conversion in %s seconds", CONVERT_INTERVAL_SEC)
            sleep_until_stop(CONVERT_INTERVAL_SEC)
    finally:
        if cleanup_poller is not None:
            cleanup_poller.stop()
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
    project_root, data_root, _state_path = project_paths()
    raw_root = dataset_raw_root(project_root, BINANCE_FUTURES)
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
