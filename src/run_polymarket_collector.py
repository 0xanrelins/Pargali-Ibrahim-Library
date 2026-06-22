#!/usr/bin/env python3
"""Run Polymarket collector with Prometheus metrics on :9103."""

from __future__ import annotations

import logging
import queue
import signal
import sys
import threading

from collect_polymarket import (
    FLUSH_INTERVAL_SEC,
    RAW_ROOT,
    PolymarketWsClient,
    request_stop,
    run_collector_loop,
    stop_requested,
)
from polymarket.lifecycle import MarketLifecycleState
from collectors.polymarket import PolymarketAdapter
from metrics.exporter import (
    DEFAULT_POLYMARKET_METRICS_PORT,
    start_exporter,
    stop_exporter,
)

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

    logger.info("Starting Polymarket collector with metrics")
    logger.info("Raw output root: %s", RAW_ROOT)
    logger.info("Flush interval: %s seconds", FLUSH_INTERVAL_SEC)
    logger.info(
        "Prometheus metrics: http://127.0.0.1:%s/metrics",
        DEFAULT_POLYMARKET_METRICS_PORT,
    )

    start_exporter(port=DEFAULT_POLYMARKET_METRICS_PORT)

    adapter = PolymarketAdapter(service_name="market-data-platform-polymarket")
    lifecycle = MarketLifecycleState()
    state_lock = threading.Lock()
    outbound: queue.Queue[str] = queue.Queue()
    ws_client = PolymarketWsClient(
        adapter=adapter,
        outbound=outbound,
        lifecycle=lifecycle,
        state_lock=state_lock,
    )

    try:
        ws_client.start()
        run_collector_loop(
            adapter=adapter,
            ws_client=ws_client,
            lifecycle=lifecycle,
            state_lock=state_lock,
            inbound=outbound,
        )
    finally:
        ws_client.stop()
        adapter.disconnect()
        adapter.publish_prometheus()
        stop_exporter()

    logger.info("Polymarket collector stopped cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
