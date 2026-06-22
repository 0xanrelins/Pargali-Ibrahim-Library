#!/usr/bin/env python3
"""Polymarket BTC 15m raw WebSocket collector (direct CLOB WS)."""

from __future__ import annotations

import json
import logging
import queue
import signal
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import websocket

from collectors.polymarket import PolymarketAdapter
from datasets import POLYMARKET, POLYMARKET_CATEGORIES, raw_root as dataset_raw_root
from metrics.pipeline_registry import (
    raw_bytes_written_total,
    raw_files_written_total,
    raw_last_flush_duration_seconds,
    raw_last_flush_timestamp,
)
from metrics.polymarket_registry import (
    polymarket_errors_total,
    polymarket_market_rotations_total,
    polymarket_messages_total,
    polymarket_reconnect_total,
)
from polymarket.discovery import (
    Btc15mMarket,
    fetch_btc_15m_market,
    is_btc_15m_metadata,
    is_btc_15m_slug,
)
from polymarket.lifecycle import (
    LIFECYCLE_TICK_SECONDS,
    MarketLifecycleState,
    TrackedMarket,
    candidate_window_timestamps,
    collect_token_ids,
    find_tracked_by_slug,
    find_tracked_by_token,
    pending_subscription_markets,
    prune_resolved_or_expired,
)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
FLUSH_INTERVAL_SEC = 60
REPORT_INTERVAL_SEC = 10
METRICS_INTERVAL_SEC = 5
PING_INTERVAL_SEC = 10
STALL_WARN_SECONDS = 30

RAW_ROOT = dataset_raw_root(Path(__file__).resolve().parent.parent, POLYMARKET)

logger = logging.getLogger(__name__)
stop_requested = False


def request_stop(signum: int, _frame: object) -> None:
    global stop_requested
    logger.info("Shutdown requested (signal %s)", signum)
    stop_requested = True


def classify_category(event_type: str) -> str | None:
    if event_type in {"new_market", "market_resolved", "market_subscription"}:
        return "prediction_metadata"
    if event_type in {"price_change", "best_bid_ask", "tick_size_change"}:
        return "prediction_price"
    if event_type == "last_trade_price":
        return "prediction_trades"
    if event_type == "book":
        return "prediction_orderbook"
    return None


class RawFlushBuffer:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.buffers: dict[str, list[str]] = {category: [] for category in POLYMARKET_CATEGORIES}
        self.last_flush = time.monotonic()

    def append(self, category: str, line: str) -> None:
        self.buffers[category].append(line)

    def should_flush(self) -> bool:
        return time.monotonic() - self.last_flush >= FLUSH_INTERVAL_SEC

    def flush(self, *, force: bool = False) -> list[tuple[str, Path, int]]:
        if not force and not self.should_flush():
            return []

        flush_started = time.monotonic()
        written: list[tuple[str, Path, int]] = []
        now = datetime.now(UTC)
        date_dir = now.strftime("%Y-%m-%d")
        file_stamp = now.strftime("%H-%M-%S")

        for category, lines in self.buffers.items():
            if not lines:
                continue

            out_dir = self.root / category / date_dir
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{file_stamp}.jsonl"
            content = "\n".join(lines) + "\n"
            out_path.write_text(content, encoding="utf-8")
            written.append((category, out_path, len(lines)))
            raw_files_written_total.labels(category=category).inc()
            raw_bytes_written_total.labels(category=category).inc(len(content.encode("utf-8")))
            lines.clear()

        if written:
            raw_last_flush_timestamp.set(time.time())
            raw_last_flush_duration_seconds.set(time.monotonic() - flush_started)

        self.last_flush = time.monotonic()
        return written


class PolymarketWsClient:
    def __init__(
        self,
        *,
        adapter: PolymarketAdapter,
        outbound: queue.Queue[str],
        lifecycle: MarketLifecycleState,
        state_lock: threading.Lock,
    ) -> None:
        self._adapter = adapter
        self._outbound = outbound
        self._lifecycle = lifecycle
        self._state_lock = state_lock
        self._ws_app: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._ping_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="polymarket-ws", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._ws_app is not None:
            self._ws_app.close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._ping_thread is not None:
            self._ping_thread.join(timeout=2)

    def reconnect(self) -> None:
        if self._ws_app is not None:
            self._ws_app.close()

    def _run_loop(self) -> None:
        while not self._stop_event.is_set() and not stop_requested:
            self._connected_event.clear()
            self._adapter.disconnect()
            self._ws_app = websocket.WebSocketApp(
                WS_URL,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._ws_app.run_forever(ping_interval=0)
            if self._stop_event.is_set() or stop_requested:
                break
            self._adapter.note_reconnect()
            polymarket_reconnect_total.inc()
            logger.warning("Polymarket WebSocket disconnected; reconnecting in 3s")
            time.sleep(3)

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        self._adapter.connect()
        self._connected_event.set()
        self._send_subscription(ws)
        if self._ping_thread is None or not self._ping_thread.is_alive():
            self._ping_thread = threading.Thread(
                target=self._ping_loop,
                args=(ws,),
                name="polymarket-ws-ping",
                daemon=True,
            )
            self._ping_thread.start()

    def _send_subscription(self, ws: websocket.WebSocketApp) -> None:
        with self._state_lock:
            token_ids = collect_token_ids(self._lifecycle)
            markets = [tracked.market for tracked in self._lifecycle.tracked.values()]

        if not token_ids:
            return

        payload = {
            "assets_ids": list(token_ids),
            "type": "market",
            "custom_feature_enabled": True,
        }
        ws.send(json.dumps(payload))
        self._adapter.set_markets(markets)
        logger.info(
            "Subscribed to %s Polymarket market(s) tokens=%s slugs=%s",
            len(markets),
            token_ids,
            [market.slug for market in markets],
        )

    def _ping_loop(self, ws: websocket.WebSocketApp) -> None:
        while not self._stop_event.is_set() and not stop_requested:
            if self._connected_event.is_set():
                try:
                    ws.send("PING")
                except Exception:
                    return
            time.sleep(PING_INTERVAL_SEC)

    def _on_message(self, _ws: websocket.WebSocketApp, message: str) -> None:
        if message == "PONG":
            return

        self._adapter.note_message()
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            self._adapter.note_error()
            polymarket_errors_total.labels(reason="json_decode").inc()
            return

        if not isinstance(payload, dict):
            return

        event_type = str(payload.get("event_type", payload.get("type", "unknown")))
        polymarket_messages_total.labels(event_type=event_type).inc()
        self._outbound.put(message)

    def _on_error(self, _ws: websocket.WebSocketApp, error: object) -> None:
        self._adapter.note_error()
        polymarket_errors_total.labels(reason="websocket").inc()
        logger.warning("Polymarket WebSocket error: %s", error)

    def _on_close(
        self,
        _ws: websocket.WebSocketApp,
        status_code: int | None,
        msg: str | None,
    ) -> None:
        self._adapter.disconnect()
        self._connected_event.clear()
        logger.info("Polymarket WebSocket closed code=%s msg=%s", status_code, msg)


def market_subscription_record(market: Btc15mMarket) -> str:
    payload = {
        "event_type": "market_subscription",
        "timestamp": int(time.time() * 1000),
        "slug": market.slug,
        "condition_id": market.condition_id,
        "question": market.question,
        "end_date": market.end_date,
        "yes_token_id": market.yes_token_id,
        "no_token_id": market.no_token_id,
        "window_timestamp": market.window_timestamp,
    }
    return json.dumps(payload, separators=(",", ":"))


def _enrich_ws_resolved_payload(
    payload: dict[str, Any],
    tracked: TrackedMarket,
) -> dict[str, Any]:
    enriched = dict(payload)
    if not str(enriched.get("slug", "")):
        enriched["slug"] = tracked.market.slug
    if not str(enriched.get("condition_id", "")):
        enriched["condition_id"] = tracked.market.condition_id
    if not str(enriched.get("question", "")):
        enriched["question"] = tracked.market.question
    if not str(enriched.get("yes_token_id", "")):
        enriched["yes_token_id"] = tracked.market.yes_token_id
    if not str(enriched.get("no_token_id", "")):
        enriched["no_token_id"] = tracked.market.no_token_id
    if not str(enriched.get("resolved_source", "")):
        enriched["resolved_source"] = "ws"
    return enriched


def _tracked_market_for_resolved(
    lifecycle: MarketLifecycleState,
    payload: dict[str, Any],
) -> TrackedMarket | None:
    slug = str(payload.get("slug", ""))
    if is_btc_15m_slug(slug):
        tracked = find_tracked_by_slug(lifecycle, slug)
        if tracked is not None:
            return tracked
    winning_asset_id = str(payload.get("winning_asset_id", ""))
    return find_tracked_by_token(lifecycle, winning_asset_id)


def _should_store_metadata(payload: dict[str, Any], lifecycle: MarketLifecycleState) -> bool:
    event_type = str(payload.get("event_type", payload.get("type", "")))
    if event_type == "market_resolved" and _tracked_market_for_resolved(lifecycle, payload) is not None:
        return True
    return is_btc_15m_metadata(payload)


def sync_market_lifecycle(
    *,
    lifecycle: MarketLifecycleState,
    ws_client: PolymarketWsClient,
    writer: RawFlushBuffer,
    counts: dict[str, int],
) -> None:
    now = time.time()
    previous_tokens = collect_token_ids(lifecycle)

    for window_ts in candidate_window_timestamps(now):
        if window_ts in lifecycle.tracked:
            continue
        if window_ts in lifecycle.finished_windows:
            continue
        market = fetch_btc_15m_market(window_timestamp=window_ts)
        if market is None:
            logger.warning("No BTC 15m market available for window=%s", window_ts)
            continue
        lifecycle.tracked[window_ts] = TrackedMarket(market=market)
        logger.info("Tracking market slug=%s window=%s", market.slug, window_ts)

    removed = prune_resolved_or_expired(lifecycle, now=now)
    for market in removed:
        logger.info("Stopped tracking market slug=%s", market.slug)

    for market in pending_subscription_markets(lifecycle):
        line = market_subscription_record(market)
        writer.append("prediction_metadata", line)
        counts["prediction_metadata"] += 1

    current_tokens = collect_token_ids(lifecycle)
    if current_tokens != previous_tokens:
        if removed:
            polymarket_market_rotations_total.inc(len(removed))
        ws_client.reconnect()


def handle_ws_payload(
    payload: dict[str, Any],
    *,
    lifecycle: MarketLifecycleState,
    state_lock: threading.Lock,
    writer: RawFlushBuffer,
    counts: dict[str, int],
    ws_client: PolymarketWsClient,
) -> None:
    event_type = str(payload.get("event_type", payload.get("type", "")))
    stored_payload = payload

    if event_type == "market_resolved":
        tracked = _tracked_market_for_resolved(lifecycle, payload)
        if tracked is not None:
            stored_payload = _enrich_ws_resolved_payload(payload, tracked)
            tracked.resolved = True
            lifecycle.finished_windows.add(tracked.market.window_timestamp)
            logger.info("WS market_resolved slug=%s", tracked.market.slug)
            sync_market_lifecycle(
                lifecycle=lifecycle,
                ws_client=ws_client,
                writer=writer,
                counts=counts,
            )

    category = classify_category(event_type)
    if category is None:
        counts["other"] += 1
        return
    if category == "prediction_metadata" and not _should_store_metadata(stored_payload, lifecycle):
        counts["other"] += 1
        return

    writer.append(category, json.dumps(stored_payload, separators=(",", ":")))
    counts[category] += 1


def run_collector_loop(
    *,
    adapter: PolymarketAdapter,
    ws_client: PolymarketWsClient,
    lifecycle: MarketLifecycleState,
    state_lock: threading.Lock,
    inbound: queue.Queue[str],
) -> dict[str, int]:
    counts = {category: 0 for category in POLYMARKET_CATEGORIES}
    counts["other"] = 0
    writer = RawFlushBuffer(RAW_ROOT)

    last_report = time.monotonic()
    last_lifecycle = 0.0
    last_metrics = 0.0

    with state_lock:
        sync_market_lifecycle(
            lifecycle=lifecycle,
            ws_client=ws_client,
            writer=writer,
            counts=counts,
        )

    while not stop_requested:
        now = time.monotonic()

        if now - last_lifecycle >= LIFECYCLE_TICK_SECONDS:
            with state_lock:
                sync_market_lifecycle(
                    lifecycle=lifecycle,
                    ws_client=ws_client,
                    writer=writer,
                    counts=counts,
                )
            last_lifecycle = now

        try:
            message = inbound.get(timeout=0.2)
        except queue.Empty:
            message = None

        if message is not None:
            try:
                payload: dict[str, Any] = json.loads(message)
            except json.JSONDecodeError:
                counts["other"] += 1
                continue

            with state_lock:
                handle_ws_payload(
                    payload,
                    lifecycle=lifecycle,
                    state_lock=state_lock,
                    writer=writer,
                    counts=counts,
                    ws_client=ws_client,
                )

        if writer.should_flush():
            for category, path, line_count in writer.flush():
                logger.info("Flushed %s lines to %s (%s)", line_count, path, category)

        if now - last_report >= REPORT_INTERVAL_SEC:
            with state_lock:
                active_slugs = [tracked.market.slug for tracked in lifecycle.tracked.values()]
            logger.info("Polymarket messages — %s | active=%s", counts, active_slugs)
            status = adapter.get_status()
            if (
                status.last_message_timestamp is not None
                and time.time() - status.last_message_timestamp > STALL_WARN_SECONDS
            ):
                logger.warning(
                    "Polymarket data stall suspected (last message %.1fs ago)",
                    time.time() - status.last_message_timestamp,
                )
            last_report = now

        if now - last_metrics >= METRICS_INTERVAL_SEC:
            adapter.publish_prometheus()
            last_metrics = now

    for category, path, line_count in writer.flush(force=True):
        logger.info("Final flush: %s lines to %s (%s)", line_count, path, category)

    return counts


def main() -> int:
    global stop_requested

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    logger.info("Raw output root: %s", RAW_ROOT)
    logger.info("Flush interval: %s seconds", FLUSH_INTERVAL_SEC)

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

    ws_client.start()
    try:
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

    return 0


if __name__ == "__main__":
    sys.exit(main())
