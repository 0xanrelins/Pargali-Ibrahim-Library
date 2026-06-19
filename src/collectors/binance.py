"""Binance USDT-M Futures collector adapters (UBWA-backed)."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from collectors.base import CollectorAdapter, CollectorMetrics, CollectorStatus
from datasets import BINANCE_FUTURES

if TYPE_CHECKING:
    from unicorn_binance_websocket_api import BinanceWebSocketApiManager


class BinanceUbwaAdapter(CollectorAdapter):
    """Wraps UBWA manager with the normalized collector interface."""

    def __init__(
        self,
        *,
        ubwa: BinanceWebSocketApiManager,
        service_name: str,
        subscription_count: int = 0,
    ) -> None:
        self._ubwa = ubwa
        self._service_name = service_name
        self._subscription_count = subscription_count
        self._reconnect_count = 0
        self._error_count = 0
        self._last_message_timestamp: float | None = None
        self._connected = False

    @property
    def exchange(self) -> str:
        return BINANCE_FUTURES

    @property
    def service_name(self) -> str:
        return self._service_name

    def connect(self) -> None:
        self._connected = not self._ubwa.is_manager_stopping()
        if self._connected:
            self._last_message_timestamp = time.time()

    def disconnect(self) -> None:
        self._connected = False

    def subscribe(self) -> None:
        # Stream setup is owned by collect_raw / collect_orderbook setup_* helpers.
        pass

    def note_message(self) -> None:
        self._last_message_timestamp = time.time()

    def note_reconnect(self) -> None:
        self._reconnect_count += 1

    def note_error(self) -> None:
        self._error_count += 1

    def set_subscription_count(self, count: int) -> None:
        self._subscription_count = count

    def get_status(self) -> CollectorStatus:
        manager_active = not self._ubwa.is_manager_stopping()
        return CollectorStatus(
            connected=self._connected and manager_active,
            last_message_timestamp=self._last_message_timestamp,
            reconnect_count=self._reconnect_count,
            active_subscriptions=self._subscription_count,
            error_count=self._error_count,
            details={
                "service": self._service_name,
                "exchange": self.exchange,
            },
        )

    def get_metrics(self) -> CollectorMetrics:
        status = self.get_status()
        last_age: float | None = None
        if status.last_message_timestamp is not None:
            last_age = max(0.0, time.time() - status.last_message_timestamp)

        extra: dict[str, float] = {}
        try:
            extra["active_streams"] = float(self._ubwa.get_active_stream_count())
        except (AttributeError, TypeError):
            pass

        return CollectorMetrics(
            up=status.connected,
            connected=status.connected,
            last_message_age_seconds=last_age,
            reconnect_total=status.reconnect_count,
            active_subscriptions=status.active_subscriptions,
            error_total=status.error_count,
            extra=extra,
        )

    def ubwa_snapshot(self) -> dict[str, Any]:
        """Optional UBWA-specific detail for debugging; not used by pipeline."""
        return {
            "active_streams": getattr(self._ubwa, "get_active_stream_count", lambda: None)(),
            "manager_stopping": self._ubwa.is_manager_stopping(),
        }
