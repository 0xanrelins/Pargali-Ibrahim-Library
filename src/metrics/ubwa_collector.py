#!/usr/bin/env python3
"""Poll UBWA monitoring APIs and update Prometheus metrics (Phase 3, Section 1)."""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import TYPE_CHECKING, Any

import psutil

from metrics.ubwa_registry import (
    active_streams,
    average_bytes_per_second,
    bytes_per_second,
    crashed_streams,
    endpoint_errors,
    endpoint_results,
    messages_per_second,
    messages_per_second_by_stream,
    process_cpu_percent,
    process_memory_bytes,
    process_threads,
    received_megabytes,
    receives_per_second,
    reconnects_total,
    restarting_streams,
    stopped_streams,
    stream_buffer_bytes,
    stream_buffer_length,
    stream_buffer_maxlen,
    stream_subscriptions,
    total_subscriptions,
    uptime_seconds,
)
from metrics.ubwa_signals import drain_stream_signal_buffer

if TYPE_CHECKING:
    from unicorn_binance_websocket_api import BinanceWebSocketApiManager

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SEC = 2.0


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def collect_ubwa_metrics(ubwa: BinanceWebSocketApiManager) -> None:
    """Read UBWA monitoring APIs once and publish Prometheus gauges."""
    status = ubwa.get_monitoring_status_plain(warn_on_update=False)

    active_streams.set(_as_int(status.get("active_streams")))
    crashed_streams.set(_as_int(status.get("crashed_streams")))
    restarting_streams.set(_as_int(status.get("restarting_streams")))
    stopped_streams.set(_as_int(status.get("stopped_streams")))
    reconnects_total.set(_as_int(status.get("reconnects")))
    received_megabytes.set(_as_float(status.get("total_received_mb")))
    receives_per_second.set(_as_float(status.get("average_receives_per_second")))
    average_bytes_per_second.set(
        _as_float(status.get("average_speed_per_second")) * 1024.0
    )

    start_time = ubwa.get_start_time()
    if start_time:
        uptime_seconds.set(max(0.0, time.time() - float(start_time)))

    stream_buffer_length.set(_as_int(ubwa.get_stream_buffer_length()))
    stream_buffer_bytes.set(_as_int(ubwa.get_stream_buffer_byte_size()))
    stream_buffer_maxlen.set(_as_int(ubwa.get_stream_buffer_maxlen()))

    bytes_per_second.set(_as_int(ubwa.get_current_receiving_speed_global()))
    messages_per_second.set(_as_int(ubwa.get_all_receives_last_second()))

    errors = ubwa.get_errors_from_endpoints()
    results = ubwa.get_results_from_endpoints()
    endpoint_errors.set(len(errors) if errors is not None else 0)
    endpoint_results.set(len(results) if results is not None else 0)

    cpu = ubwa.get_process_usage_cpu()
    if cpu is not None:
        process_cpu_percent.set(_as_float(cpu))

    process = psutil.Process(os.getpid())
    process_memory_bytes.set(process.memory_info().rss)
    process_threads.set(_as_int(ubwa.get_process_usage_threads()))

    current_streams: set[tuple[str, str]] = set()
    stream_ids = ubwa.get_stream_list() or []
    for stream_id in stream_ids:
        stream_label = ubwa.get_stream_label(stream_id=stream_id) or "unknown"
        current_streams.add((stream_id, stream_label))
        messages_per_second_by_stream.labels(
            stream_id=stream_id,
            stream_label=stream_label,
        ).set(_as_int(ubwa.get_stream_receives_last_second(stream_id)))
        stream_subscriptions.labels(
            stream_id=stream_id,
            stream_label=stream_label,
        ).set(_as_int(ubwa.get_number_of_subscriptions(stream_id)))

    total_subscriptions.set(_as_int(ubwa.get_number_of_all_subscriptions()))

    _clear_stale_stream_metrics(current_streams)
    drain_stream_signal_buffer(ubwa)


_seen_streams: set[tuple[str, str]] = set()


def _clear_stale_stream_metrics(current_streams: set[tuple[str, str]]) -> None:
    global _seen_streams

    for stream_id, stream_label in _seen_streams - current_streams:
        messages_per_second_by_stream.labels(
            stream_id=stream_id,
            stream_label=stream_label,
        ).set(0)
        stream_subscriptions.labels(
            stream_id=stream_id,
            stream_label=stream_label,
        ).set(0)

    _seen_streams = current_streams


class UbwaMetricsPoller:
    def __init__(
        self,
        ubwa: BinanceWebSocketApiManager,
        interval_sec: float = DEFAULT_POLL_INTERVAL_SEC,
    ) -> None:
        self.ubwa = ubwa
        self.interval_sec = interval_sec
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="ubwa-metrics-poller",
            daemon=True,
        )
        self._thread.start()
        logger.info("UBWA metrics poller started (interval=%ss)", self.interval_sec)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.interval_sec + 1)
        logger.info("UBWA metrics poller stopped")

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                collect_ubwa_metrics(self.ubwa)
            except Exception:
                logger.exception("UBWA metrics poll failed")
            self._stop_event.wait(self.interval_sec)


def start_ubwa_poller(
    ubwa: BinanceWebSocketApiManager,
    interval_sec: float = DEFAULT_POLL_INTERVAL_SEC,
) -> UbwaMetricsPoller:
    poller = UbwaMetricsPoller(ubwa, interval_sec=interval_sec)
    poller.start()
    return poller
