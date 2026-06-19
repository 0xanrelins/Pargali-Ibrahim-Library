#!/usr/bin/env python3
"""Binance USDT-M Futures raw WebSocket collector (Plan 1)."""

import logging
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from unicorn_binance_websocket_api import BinanceWebSocketApiManager

from datasets import BINANCE_FUTURES, raw_root as dataset_raw_root
from metrics.pipeline_registry import (
    raw_bytes_written_total,
    raw_files_written_total,
    raw_last_flush_duration_seconds,
    raw_last_flush_timestamp,
)

EXCHANGE = "binance.com-futures"
TRADE_MARKETS = ["btcusdt"]
POLL_INTERVAL_SEC = 0.01
FLUSH_INTERVAL_SEC = 60
REPORT_INTERVAL_SEC = 10

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
    if "aggTrade" in raw_line:
        return "trades"
    if "forceOrder" in raw_line:
        return "liquidations"
    return None


class RawFlushBuffer:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.buffers: dict[str, list[str]] = {"trades": [], "liquidations": []}
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


def setup_streams(ubwa: BinanceWebSocketApiManager) -> tuple[str, str]:
    """Create aggTrade and liquidation streams. Returns (trade_stream_id, liquidation_stream_id)."""
    trade_stream_id = ubwa.create_stream(
        channels="aggTrade",
        markets=TRADE_MARKETS,
        stream_label="aggTrade-7-markets",
    )
    liquidation_stream_id = ubwa.create_stream(
        channels="!forceOrder",
        markets="arr",
        stream_label="forceOrder-all-markets",
    )
    logger.info(
        "Streams created: aggTrade=%s liquidations=%s",
        trade_stream_id,
        liquidation_stream_id,
    )
    logger.info(
        "Subscriptions: %s aggTrade + 1 !forceOrder = %s total",
        len(TRADE_MARKETS),
        len(TRADE_MARKETS) + 1,
    )
    return trade_stream_id, liquidation_stream_id


def run_collector_loop(ubwa: BinanceWebSocketApiManager) -> dict[str, int]:
    """Poll UBWA buffer and flush raw JSONL until stop is requested."""
    counts = {"aggTrade": 0, "forceOrder": 0, "other": 0}
    last_report = time.monotonic()
    writer = RawFlushBuffer(RAW_ROOT)

    while not stop_requested and not ubwa.is_manager_stopping():
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
        if category == "trades":
            counts["aggTrade"] += 1
            writer.append(category, raw_line)
        elif category == "liquidations":
            counts["forceOrder"] += 1
            writer.append(category, raw_line)
        else:
            counts["other"] += 1

        now = time.monotonic()
        if now - last_report >= REPORT_INTERVAL_SEC:
            logger.info(
                "Messages received — aggTrade: %s, forceOrder: %s, other: %s",
                counts["aggTrade"],
                counts["forceOrder"],
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
        "Final counts — aggTrade: %s, forceOrder: %s, other: %s",
        counts["aggTrade"],
        counts["forceOrder"],
        counts["other"],
    )
    return counts


def main() -> int:
    global stop_requested

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    logger.info("Starting UBWA manager (%s)", EXCHANGE)
    logger.info("Raw output root: %s", RAW_ROOT)
    logger.info("Flush interval: %s seconds", FLUSH_INTERVAL_SEC)

    with BinanceWebSocketApiManager(exchange=EXCHANGE) as ubwa:
        setup_streams(ubwa)
        run_collector_loop(ubwa)

    logger.info("UBWA manager stopped cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
