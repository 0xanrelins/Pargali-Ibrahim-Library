"""Polymarket collector adapter with operational health tracking."""

from __future__ import annotations

import time
from typing import Any

from collectors.base import CollectorAdapter, CollectorMetrics, CollectorStatus
from datasets import POLYMARKET


class PolymarketAdapter(CollectorAdapter):
    """Wraps direct Polymarket CLOB WebSocket with normalized collector health."""

    def __init__(self, *, service_name: str) -> None:
        self._service_name = service_name
        self._connected = False
        self._reconnect_count = 0
        self._error_count = 0
        self._subscription_count = 0
        self._last_message_timestamp: float | None = None
        self._active_slug: str | None = None
        self._active_window_timestamp: int | None = None
        self._token_ids: tuple[str, ...] = ()

    @property
    def exchange(self) -> str:
        return POLYMARKET

    @property
    def service_name(self) -> str:
        return self._service_name

    def connect(self) -> None:
        self._connected = True
        if self._last_message_timestamp is None:
            self._last_message_timestamp = time.time()

    def disconnect(self) -> None:
        self._connected = False

    def subscribe(self) -> None:
        pass

    def set_market(
        self,
        *,
        slug: str,
        window_timestamp: int,
        token_ids: tuple[str, ...],
    ) -> None:
        self._active_slug = slug
        self._active_window_timestamp = window_timestamp
        self._token_ids = token_ids
        self._subscription_count = len(token_ids)

    def note_message(self) -> None:
        self._last_message_timestamp = time.time()

    def note_reconnect(self) -> None:
        self._reconnect_count += 1

    def note_error(self) -> None:
        self._error_count += 1

    def get_status(self) -> CollectorStatus:
        return CollectorStatus(
            connected=self._connected,
            last_message_timestamp=self._last_message_timestamp,
            reconnect_count=self._reconnect_count,
            active_subscriptions=self._subscription_count,
            error_count=self._error_count,
            details={
                "service": self._service_name,
                "exchange": self.exchange,
                "active_slug": self._active_slug,
                "active_window_timestamp": self._active_window_timestamp,
                "token_ids": list(self._token_ids),
            },
        )

    def get_metrics(self) -> CollectorMetrics:
        status = self.get_status()
        age: float | None = None
        if status.last_message_timestamp is not None:
            age = max(0.0, time.time() - status.last_message_timestamp)

        extra: dict[str, float] = {}
        if self._active_window_timestamp is not None:
            extra["active_market_window_timestamp"] = float(self._active_window_timestamp)

        return CollectorMetrics(
            up=True,
            connected=status.connected,
            last_message_age_seconds=age,
            reconnect_total=status.reconnect_count,
            active_subscriptions=status.active_subscriptions,
            error_total=status.error_count,
            extra=extra,
        )

    def publish_prometheus(self) -> None:
        from metrics.polymarket_registry import (
            polymarket_active_market_slug,
            polymarket_active_subscriptions,
            polymarket_connected,
            polymarket_last_message_age_seconds,
            polymarket_last_message_timestamp,
            polymarket_reconnect_total,
        )

        metrics = self.get_metrics()
        polymarket_connected.set(1.0 if metrics.connected else 0.0)
        polymarket_active_subscriptions.set(metrics.active_subscriptions)
        if self._last_message_timestamp is not None:
            polymarket_last_message_timestamp.set(self._last_message_timestamp)
        if metrics.last_message_age_seconds is not None:
            polymarket_last_message_age_seconds.set(metrics.last_message_age_seconds)
        if self._active_window_timestamp is not None:
            polymarket_active_market_slug.set(float(self._active_window_timestamp))

        # Counters are incremented at event sites; reconnect gauge mirrors adapter state.
        _ = polymarket_reconnect_total
