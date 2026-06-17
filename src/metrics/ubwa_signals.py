#!/usr/bin/env python3
"""UBWA stream lifecycle signals → Prometheus (Phase 3, Section 1)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable

from metrics.ubwa_registry import STREAM_SIGNALS, record_stream_signal

if TYPE_CHECKING:
    from unicorn_binance_websocket_api import BinanceWebSocketApiManager

logger = logging.getLogger(__name__)

StreamSignalHandler = Callable[
    [str | None, str | None, object | None, object | None],
    None,
]


class StreamSignalRecorder:
    """UBWA constructor callback that binds to the manager after context entry."""

    def __init__(self, *, keep_buffer: bool = True) -> None:
        self._keep_buffer = keep_buffer
        self._handler: StreamSignalHandler | None = None

    def bind(self, ubwa: BinanceWebSocketApiManager) -> None:
        self._handler = create_stream_signal_handler(ubwa, keep_buffer=self._keep_buffer)

    def __call__(
        self,
        signal_type: str | None = None,
        stream_id: str | None = None,
        data_record: object | None = None,
        error_msg: object | None = None,
    ) -> None:
        if self._handler is None:
            record_stream_signal(signal_type, stream_id, None)
            return
        self._handler(signal_type, stream_id, data_record, error_msg)


def _resolve_stream_label(
    ubwa: BinanceWebSocketApiManager,
    stream_id: str | None,
) -> str | None:
    if not stream_id:
        return None
    try:
        return ubwa.get_stream_label(stream_id=stream_id)
    except Exception:
        logger.debug("Could not resolve stream label for %s", stream_id, exc_info=True)
        return None


def create_stream_signal_handler(
    ubwa: BinanceWebSocketApiManager,
    *,
    keep_buffer: bool = True,
) -> StreamSignalHandler:
    """Return a UBWA `process_stream_signals` callback that records Prometheus counters."""

    def on_stream_signal(
        signal_type: str | None = None,
        stream_id: str | None = None,
        data_record: object | None = None,
        error_msg: object | None = None,
    ) -> None:
        stream_label = _resolve_stream_label(ubwa, stream_id)
        record_stream_signal(signal_type, stream_id, stream_label)

        normalized = (signal_type or "").upper()
        if normalized == "STREAM_UNREPAIRABLE":
            logger.error(
                "STREAM_UNREPAIRABLE stream_id=%s label=%s error=%s",
                stream_id,
                stream_label,
                error_msg,
            )
        elif normalized == "DISCONNECT":
            logger.warning(
                "DISCONNECT stream_id=%s label=%s",
                stream_id,
                stream_label,
            )
        elif normalized not in STREAM_SIGNALS and normalized:
            logger.warning("Unknown UBWA stream signal: %s", signal_type)

        if keep_buffer and ubwa.enable_stream_signal_buffer:
            ubwa.add_to_stream_signal_buffer(
                signal_type=signal_type,
                stream_id=stream_id,
                data_record=data_record,
                error_msg=error_msg,
            )

    return on_stream_signal


def drain_stream_signal_buffer(ubwa: BinanceWebSocketApiManager) -> int:
    """Drain UBWA signal buffer and record any pending signals. Returns count drained."""
    drained = 0

    while True:
        signal = ubwa.pop_stream_signal_from_stream_signal_buffer()
        if not signal:
            break

        stream_id = signal.get("stream_id")
        stream_label = _resolve_stream_label(ubwa, stream_id)
        record_stream_signal(signal.get("type"), stream_id, stream_label)
        drained += 1

    return drained
