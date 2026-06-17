#!/usr/bin/env python3
"""Prometheus metrics exporter for the Market Data Platform (Phase 3, Section 1)."""

from __future__ import annotations

import logging
import threading

from prometheus_client import Gauge, start_http_server

logger = logging.getLogger(__name__)

DEFAULT_METRICS_PORT = 9100
DEFAULT_PIPELINE_METRICS_PORT = 9101

mdp_up = Gauge(
    "mdp_up",
    "Market Data Platform exporter is running (1=up, 0=down).",
)

_server_started = False
_server_lock = threading.Lock()


def start_exporter(port: int = DEFAULT_METRICS_PORT) -> None:
    """Start the Prometheus HTTP server once per process."""
    global _server_started

    with _server_lock:
        if _server_started:
            return
        start_http_server(port)
        mdp_up.set(1)
        _server_started = True
        logger.info("Prometheus exporter listening on :%s", port)


def stop_exporter() -> None:
    """Mark exporter metrics as down (HTTP server stops with the process)."""
    mdp_up.set(0)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    start_exporter()
    logger.info("Standalone exporter running — Ctrl+C to stop")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        stop_exporter()
        logger.info("Exporter stopped")
