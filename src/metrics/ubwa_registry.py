#!/usr/bin/env python3
"""Prometheus metric definitions for UBWA collector health (Phase 3, Section 1)."""

from __future__ import annotations

from typing import Final

from prometheus_client import Counter, Gauge

STREAM_SIGNALS: Final[tuple[str, ...]] = (
    "CONNECT",
    "FIRST_RECEIVED_DATA",
    "DISCONNECT",
    "STOP",
    "STREAM_UNREPAIRABLE",
)

# --- Stream lifecycle (push via process_stream_signals) ---

stream_signal_total = Counter(
    "mdp_ubwa_stream_signal_total",
    "UBWA stream lifecycle signals.",
    ["signal", "stream_id", "stream_label"],
)

# --- Buffer ---

stream_buffer_length = Gauge(
    "mdp_ubwa_stream_buffer_length",
    "Messages waiting in the UBWA stream buffer.",
)

stream_buffer_bytes = Gauge(
    "mdp_ubwa_stream_buffer_bytes",
    "Estimated byte size of the UBWA stream buffer.",
)

stream_buffer_maxlen = Gauge(
    "mdp_ubwa_stream_buffer_maxlen",
    "Maximum UBWA stream buffer length before data is dropped.",
)

# --- System summary (get_monitoring_status_plain) ---

active_streams = Gauge(
    "mdp_ubwa_active_streams",
    "Number of active UBWA streams.",
)

crashed_streams = Gauge(
    "mdp_ubwa_crashed_streams",
    "Number of crashed UBWA streams.",
)

restarting_streams = Gauge(
    "mdp_ubwa_restarting_streams",
    "Number of restarting UBWA streams.",
)

stopped_streams = Gauge(
    "mdp_ubwa_stopped_streams",
    "Number of stopped UBWA streams.",
)

reconnects_total = Gauge(
    "mdp_ubwa_reconnects_total",
    "Total UBWA reconnect count reported by the manager.",
)

uptime_seconds = Gauge(
    "mdp_ubwa_uptime_seconds",
    "UBWA manager uptime in seconds.",
)

received_megabytes = Gauge(
    "mdp_ubwa_received_megabytes",
    "Total megabytes received by the UBWA manager.",
)

receives_per_second = Gauge(
    "mdp_ubwa_receives_per_second",
    "Average messages received per second (monitoring window).",
)

average_bytes_per_second = Gauge(
    "mdp_ubwa_average_bytes_per_second",
    "Average receive speed in bytes per second (monitoring window).",
)

# --- Throughput (instant) ---

bytes_per_second = Gauge(
    "mdp_ubwa_bytes_per_second",
    "Current receive speed in bytes per second.",
)

messages_per_second = Gauge(
    "mdp_ubwa_messages_per_second",
    "Current messages received per second (all streams).",
)

messages_per_second_by_stream = Gauge(
    "mdp_ubwa_messages_per_second_by_stream",
    "Current messages received per second for one stream.",
    ["stream_id", "stream_label"],
)

# --- Errors / endpoint ringbuffers ---

endpoint_errors = Gauge(
    "mdp_ubwa_endpoint_errors",
    "Number of error records in the UBWA endpoint ringbuffer.",
)

endpoint_results = Gauge(
    "mdp_ubwa_endpoint_results",
    "Number of result records in the UBWA endpoint ringbuffer.",
)

# --- Stream details ---

stream_subscriptions = Gauge(
    "mdp_ubwa_stream_subscriptions",
    "Active Binance subscriptions on a UBWA stream.",
    ["stream_id", "stream_label"],
)

total_subscriptions = Gauge(
    "mdp_ubwa_total_subscriptions",
    "Total subscriptions across all UBWA streams.",
)

# --- Process ---

process_cpu_percent = Gauge(
    "mdp_ubwa_process_cpu_percent",
    "UBWA manager process CPU usage percent.",
)

process_memory_bytes = Gauge(
    "mdp_ubwa_process_memory_bytes",
    "UBWA manager process memory usage in bytes.",
)

process_threads = Gauge(
    "mdp_ubwa_process_threads",
    "UBWA manager process thread count.",
)


def record_stream_signal(
    signal_type: str | None,
    stream_id: str | None = None,
    stream_label: str | None = None,
) -> None:
    """Increment stream lifecycle counter for a UBWA signal."""
    if not signal_type:
        return

    normalized = signal_type.upper()
    if normalized not in STREAM_SIGNALS:
        normalized = "UNKNOWN"

    stream_signal_total.labels(
        signal=normalized,
        stream_id=stream_id or "unknown",
        stream_label=stream_label or "unknown",
    ).inc()
