"""Collector adapter interface — platform code depends on this, not vendor SDKs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class CollectorStatus:
    connected: bool
    last_message_timestamp: float | None
    reconnect_count: int
    active_subscriptions: int
    error_count: int = 0
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class CollectorMetrics:
    """Normalized operational metrics for Prometheus/Grafana."""

    up: bool
    connected: bool
    last_message_age_seconds: float | None
    reconnect_total: int
    active_subscriptions: int
    error_total: int
    extra: dict[str, float] = field(default_factory=dict)


class CollectorAdapter(ABC):
    """Vendor-neutral collector contract."""

    @property
    @abstractmethod
    def exchange(self) -> str:
        """Registered exchange id (e.g. binance_futures, polymarket)."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the upstream data source."""

    @abstractmethod
    def disconnect(self) -> None:
        """Tear down connection and release resources."""

    @abstractmethod
    def subscribe(self) -> None:
        """Subscribe to configured streams/markets."""

    @abstractmethod
    def get_status(self) -> CollectorStatus:
        """Current connection and subscription health."""

    @abstractmethod
    def get_metrics(self) -> CollectorMetrics:
        """Normalized metrics for monitoring exporters."""

    def run_until_stopped(self) -> None:
        """Optional main loop hook for blocking collectors."""
