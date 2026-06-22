"""BTC 15m market listen-window scheduling (pre-market → WS resolve grace)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from polymarket.discovery import WINDOW_SECONDS, Btc15mMarket, current_window_timestamp

PRE_WINDOW_SECONDS = 900
RESOLVE_GRACE_SECONDS = 300
LIFECYCLE_TICK_SECONDS = 1.0


@dataclass
class TrackedMarket:
    market: Btc15mMarket
    subscription_recorded: bool = False
    resolved: bool = False


@dataclass
class MarketLifecycleState:
    tracked: dict[int, TrackedMarket] = field(default_factory=dict)
    finished_windows: set[int] = field(default_factory=set)


def listen_start_unix(window_timestamp: int) -> float:
    return float(window_timestamp - PRE_WINDOW_SECONDS)


def window_end_unix(window_timestamp: int) -> float:
    return float(window_timestamp + WINDOW_SECONDS)


def listen_end_unix(window_timestamp: int) -> float:
    return float(window_timestamp + WINDOW_SECONDS + RESOLVE_GRACE_SECONDS)


def candidate_window_timestamps(now: float) -> list[int]:
    """Window starts that may still be within the listen envelope."""
    current = current_window_timestamp(now)
    offsets = (-WINDOW_SECONDS, 0, WINDOW_SECONDS)
    candidates: list[int] = []
    for offset in offsets:
        window_ts = current + offset
        if listen_start_unix(window_ts) <= now < listen_end_unix(window_ts):
            candidates.append(window_ts)
    return candidates


def collect_token_ids(state: MarketLifecycleState) -> tuple[str, ...]:
    token_ids: list[str] = []
    for tracked in sorted(state.tracked.values(), key=lambda item: item.market.window_timestamp):
        for token_id in tracked.market.token_ids:
            if token_id not in token_ids:
                token_ids.append(token_id)
    return tuple(token_ids)


def find_tracked_by_slug(state: MarketLifecycleState, slug: str) -> TrackedMarket | None:
    for tracked in state.tracked.values():
        if tracked.market.slug == slug:
            return tracked
    return None


def find_tracked_by_token(state: MarketLifecycleState, asset_id: str) -> TrackedMarket | None:
    if not asset_id:
        return None
    for tracked in state.tracked.values():
        if asset_id in tracked.market.token_ids:
            return tracked
    return None


def mark_resolved(state: MarketLifecycleState, *, slug: str) -> bool:
    tracked = find_tracked_by_slug(state, slug)
    if tracked is None:
        return False
    tracked.resolved = True
    state.finished_windows.add(tracked.market.window_timestamp)
    return True


def prune_resolved_or_expired(state: MarketLifecycleState, *, now: float) -> list[Btc15mMarket]:
    removed: list[Btc15mMarket] = []
    for window_ts, tracked in list(state.tracked.items()):
        expired = now >= listen_end_unix(window_ts)
        if not tracked.resolved and not expired:
            continue
        state.finished_windows.add(window_ts)
        removed.append(tracked.market)
        del state.tracked[window_ts]
    return removed


def pending_subscription_markets(state: MarketLifecycleState) -> list[Btc15mMarket]:
    pending: list[Btc15mMarket] = []
    for tracked in sorted(state.tracked.values(), key=lambda item: item.market.window_timestamp):
        if not tracked.subscription_recorded:
            tracked.subscription_recorded = True
            pending.append(tracked.market)
    return pending


def seconds_until_next_lifecycle_event(now: float | None = None) -> float:
    """Earliest time we may need to add/remove a market."""
    ts = now if now is not None else time.time()
    current = current_window_timestamp(ts)
    event_times: list[float] = []
    for offset in (-WINDOW_SECONDS, 0, WINDOW_SECONDS, WINDOW_SECONDS * 2):
        window_ts = current + offset
        event_times.append(listen_start_unix(window_ts))
        event_times.append(window_end_unix(window_ts))
        event_times.append(listen_end_unix(window_ts))
    future = [event for event in event_times if event > ts]
    if not future:
        return LIFECYCLE_TICK_SECONDS
    return max(0.1, min(LIFECYCLE_TICK_SECONDS, min(future) - ts))
