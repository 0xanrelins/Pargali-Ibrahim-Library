#!/usr/bin/env python3
"""Run raw collector with UBWA Prometheus metrics (Phase 3, Section 1)."""

from __future__ import annotations

import logging
import signal
import sys

from unicorn_binance_websocket_api import BinanceWebSocketApiManager

from collectors.binance import BinanceUbwaAdapter
from collect_raw import (
    EXCHANGE,
    RAW_ROOT,
    FLUSH_INTERVAL_SEC,
    TRADE_MARKETS,
    request_stop,
    run_collector_loop,
    setup_streams,
    stop_requested,
)
from metrics.exporter import DEFAULT_METRICS_PORT, start_exporter, stop_exporter
from metrics.ubwa_collector import start_ubwa_poller
from metrics.ubwa_signals import StreamSignalRecorder

logger = logging.getLogger(__name__)


def main() -> int:
    global stop_requested

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    signal_recorder = StreamSignalRecorder(keep_buffer=True)
    poller = None

    logger.info("Starting collector with metrics (exchange=%s)", EXCHANGE)
    logger.info("Raw output root: %s", RAW_ROOT)
    logger.info("Flush interval: %s seconds", FLUSH_INTERVAL_SEC)
    logger.info("Prometheus metrics: http://127.0.0.1:%s/metrics", DEFAULT_METRICS_PORT)

    start_exporter()

    try:
        with BinanceWebSocketApiManager(
            exchange=EXCHANGE,
            process_stream_signals=signal_recorder,
            enable_stream_signal_buffer=True,
        ) as ubwa:
            signal_recorder.bind(ubwa)
            adapter = BinanceUbwaAdapter(
                ubwa=ubwa,
                service_name="market-data-platform-collector",
                subscription_count=len(TRADE_MARKETS) + 1,
            )
            adapter.connect()
            setup_streams(ubwa)
            adapter.subscribe()
            poller = start_ubwa_poller(ubwa)
            run_collector_loop(ubwa)
            adapter.disconnect()
    finally:
        if poller is not None:
            poller.stop()
        stop_exporter()

    logger.info("Collector with metrics stopped cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
