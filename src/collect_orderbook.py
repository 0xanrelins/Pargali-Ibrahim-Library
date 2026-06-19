#!/usr/bin/env python3
"""Binance USDT-M Futures orderbook raw collector (depth@100ms + snapshots)."""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from unicorn_binance_rest_api import BinanceRestApiManager
from unicorn_binance_websocket_api import BinanceWebSocketApiManager

from datasets import BINANCE_FUTURES, raw_root as dataset_raw_root
from metrics.pipeline_registry import (
    raw_bytes_written_total,
    raw_files_written_total,
    raw_last_flush_duration_seconds,
    raw_last_flush_timestamp,
)

EXCHANGE = "binance.com-futures"
SYMBOL = "BTCUSDT"
MARKET = "btcusdt"
POLL_INTERVAL_SEC = 0.01
FLUSH_INTERVAL_SEC = 60
REPORT_INTERVAL_SEC = 10
SNAPSHOT_INTERVAL_SEC = 900
STREAM_BUFFER_MAXLEN = 100_000
STARTUP_DIFF_WAIT_SEC = 5.0
SNAPSHOT_LIMIT = 1000

RAW_ROOT = dataset_raw_root(Path(__file__).resolve().parent.parent, BINANCE_FUTURES)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

stop_requested = False


def request_stop(signum: int, _frame: object) -> None:
    global stop_requested
    logger.info("Shutdown requested (signal %s)", signum)
    stop_requested = True


def classify_category(raw_line: str) -> str | None:
    if "depthUpdate" in raw_line:
        return "orderbook_diff"
    return None


class RawFlushBuffer:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.buffers: dict[str, list[str]] = {
            "orderbook_diff": [],
            "orderbook_snapshots": [],
        }
        self.last_flush = time.monotonic()

    def append(self, category: str, line: str) -> None:
        self.buffers[category].append(line)

    def should_flush(self) -> bool:
        return time.monotonic() - self.last_flush >= FLUSH_INTERVAL_SEC

    def flush(self, *, force: bool = False) -> list[tuple[str, Path, int]]:
        if not force and not self.should_flush():
            return []

        flush_started = time.monotonic()
        written: list[tuple[str, Path, int]] = []
        now = datetime.now(UTC)
        date_dir = now.strftime("%Y-%m-%d")
        file_stamp = now.strftime("%H-%M-%S")

        for category, lines in self.buffers.items():
            if not lines:
                continue

            out_dir = self.root / category / date_dir
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{file_stamp}.jsonl"
            content = "\n".join(lines) + "\n"
            out_path.write_text(content, encoding="utf-8")
            written.append((category, out_path, len(lines)))
            raw_files_written_total.labels(category=category).inc()
            raw_bytes_written_total.labels(category=category).inc(len(content.encode("utf-8")))
            lines.clear()

        if written:
            raw_last_flush_timestamp.set(time.time())
            raw_last_flush_duration_seconds.set(time.monotonic() - flush_started)

        self.last_flush = time.monotonic()
        return written


def setup_stream(ubwa: BinanceWebSocketApiManager) -> str:
    """Create depth@100ms stream. Returns stream_id."""
    stream_id = ubwa.create_stream(
        channels="depth@100ms",
        markets=[MARKET],
        stream_label="depth100ms-btcusdt",
        stream_buffer_maxlen=STREAM_BUFFER_MAXLEN,
    )
    logger.info("Stream created: depth@100ms stream_id=%s market=%s", stream_id, MARKET)
    return stream_id


def snapshot_to_line(result: dict[str, object], captured_at_ms: int) -> str:
    payload = {
        "captured_at_ms": captured_at_ms,
        "symbol": SYMBOL,
        "lastUpdateId": result.get("lastUpdateId"),
        "E": result.get("E"),
        "T": result.get("T"),
        "bids": result.get("bids", []),
        "asks": result.get("asks", []),
    }
    return json.dumps(payload, separators=(",", ":"))


def fetch_snapshot(ubra: BinanceRestApiManager) -> dict[str, object] | None:
    """Fetch a full order book snapshot via UBRA (Binance Futures REST)."""
    try:
        payload = ubra.futures_order_book(symbol=SYMBOL, limit=SNAPSHOT_LIMIT)
    except Exception as exc:
        logger.warning("Snapshot REST request failed: %s", exc)
        return None

    if not isinstance(payload, dict):
        logger.warning("Snapshot payload is not a dict: %r", type(payload))
        return None

    if "lastUpdateId" not in payload:
        logger.warning("Snapshot payload missing lastUpdateId: %s", payload)
        return None

    return payload


def wait_for_first_diff(ubwa: BinanceWebSocketApiManager, timeout_sec: float) -> bool:
    deadline = time.monotonic() + timeout_sec
    while not stop_requested and time.monotonic() < deadline:
        raw_line = ubwa.pop_stream_data_from_stream_buffer()
        if raw_line and classify_category(raw_line) == "orderbook_diff":
            return True
        time.sleep(POLL_INTERVAL_SEC)
    return False


def write_initial_snapshot(
    ubwa: BinanceWebSocketApiManager,
    ubra: BinanceRestApiManager,
    writer: RawFlushBuffer,
) -> bool:
    logger.info("Waiting up to %.0fs for first depthUpdate before snapshot", STARTUP_DIFF_WAIT_SEC)
    if not wait_for_first_diff(ubwa, STARTUP_DIFF_WAIT_SEC):
        logger.warning("No depthUpdate received before initial snapshot; continuing anyway")

    result = fetch_snapshot(ubra)
    if result is None:
        return False

    captured_at_ms = int(time.time() * 1000)
    writer.append("orderbook_snapshots", snapshot_to_line(result, captured_at_ms))
    logger.info(
        "Initial snapshot captured lastUpdateId=%s levels=%s/%s",
        result.get("lastUpdateId"),
        len(result.get("bids", [])),
        len(result.get("asks", [])),
    )
    return True


def run_collector_loop(
    ubwa: BinanceWebSocketApiManager,
    stream_id: str,
) -> dict[str, int]:
    """Poll UBWA buffer, periodic snapshots, and flush raw JSONL."""
    _ = stream_id
    counts = {"depthUpdate": 0, "snapshots": 0, "other": 0}
    last_report = time.monotonic()
    last_snapshot = time.monotonic()
    writer = RawFlushBuffer(RAW_ROOT)

    with BinanceRestApiManager(exchange=EXCHANGE) as ubra:
        write_initial_snapshot(ubwa, ubra, writer)
        last_snapshot = time.monotonic()
        counts["snapshots"] += 1

        while not stop_requested and not ubwa.is_manager_stopping():
            now = time.monotonic()

            if now - last_snapshot >= SNAPSHOT_INTERVAL_SEC:
                result = fetch_snapshot(ubra)
                if result is not None:
                    captured_at_ms = int(time.time() * 1000)
                    writer.append(
                        "orderbook_snapshots",
                        snapshot_to_line(result, captured_at_ms),
                    )
                    counts["snapshots"] += 1
                    logger.info(
                        "Periodic snapshot captured lastUpdateId=%s",
                        result.get("lastUpdateId"),
                    )
                last_snapshot = now

            raw_line = ubwa.pop_stream_data_from_stream_buffer()
            if not raw_line:
                if writer.should_flush():
                    for category, path, line_count in writer.flush():
                        logger.info(
                            "Flushed %s lines to %s (%s)",
                            line_count,
                            path,
                            category,
                        )
                time.sleep(POLL_INTERVAL_SEC)
                continue

            category = classify_category(raw_line)
            if category == "orderbook_diff":
                counts["depthUpdate"] += 1
                writer.append(category, raw_line)
            else:
                counts["other"] += 1

            if now - last_report >= REPORT_INTERVAL_SEC:
                logger.info(
                    "Messages received — depthUpdate: %s, snapshots: %s, other: %s",
                    counts["depthUpdate"],
                    counts["snapshots"],
                    counts["other"],
                )
                last_report = now

            if writer.should_flush():
                for category, path, line_count in writer.flush():
                    logger.info(
                        "Flushed %s lines to %s (%s)",
                        line_count,
                        path,
                        category,
                    )

        for category, path, line_count in writer.flush(force=True):
            logger.info(
                "Final flush: %s lines to %s (%s)",
                line_count,
                path,
                category,
            )

    logger.info(
        "Final counts — depthUpdate: %s, snapshots: %s, other: %s",
        counts["depthUpdate"],
        counts["snapshots"],
        counts["other"],
    )
    return counts


def main() -> int:
    global stop_requested

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    logger.info("Starting orderbook UBWA manager (%s)", EXCHANGE)
    logger.info("Raw output root: %s", RAW_ROOT)
    logger.info("Flush interval: %s seconds", FLUSH_INTERVAL_SEC)
    logger.info("Snapshot interval: %s seconds", SNAPSHOT_INTERVAL_SEC)

    with BinanceWebSocketApiManager(exchange=EXCHANGE) as ubwa:
        stream_id = setup_stream(ubwa)
        run_collector_loop(ubwa, stream_id)

    logger.info("Orderbook UBWA manager stopped cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
