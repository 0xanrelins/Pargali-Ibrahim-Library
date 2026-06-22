#!/usr/bin/env python3
"""One-off: does Polymarket WS send price data before the 15m window starts?"""

from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import websocket

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from polymarket.discovery import (  # noqa: E402
    WINDOW_SECONDS,
    btc_15m_slug,
    current_window_timestamp,
    fetch_btc_15m_market,
)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PRE_WINDOW_SEC = 900
POST_WINDOW_SEC = 120


@dataclass
class EventRecord:
    event_type: str
    recv_mono: float
    recv_unix: float
    payload: dict


@dataclass
class Capture:
    window_ts: int
    slug: str
    connect_unix: float
    events: list[EventRecord] = field(default_factory=list)


def _fmt_ts(unix: float) -> str:
    return datetime.fromtimestamp(unix, tz=timezone.utc).strftime("%H:%M:%S")


def _price_hint(payload: dict) -> str:
    for key in ("price", "best_bid", "best_ask"):
        if key in payload and payload[key] not in (None, ""):
            return f"{key}={payload[key]}"
    changes = payload.get("changes")
    if isinstance(changes, list) and changes:
        first = changes[0]
        if isinstance(first, dict) and "price" in first:
            return f"price={first['price']}"
    return ""


def run_capture(*, listen_sec: float, window_timestamp: int | None = None) -> Capture:
    now = time.time()
    target_window = (
        window_timestamp
        if window_timestamp is not None
        else current_window_timestamp(now) + WINDOW_SECONDS
    )
    market = fetch_btc_15m_market(window_timestamp=target_window)
    if market is None:
        market = fetch_btc_15m_market(window_timestamp=target_window, look_ahead_windows=1)
    if market is None:
        raise RuntimeError("Could not resolve target BTC 15m market from Gamma")

    cap = Capture(
        window_ts=market.window_timestamp,
        slug=market.slug,
        connect_unix=time.time(),
    )
    sec_to_window = market.window_timestamp - cap.connect_unix
    print(
        f"target={market.slug} connect={_fmt_ts(cap.connect_unix)} "
        f"window_start={_fmt_ts(market.window_timestamp)} "
        f"sec_to_window={sec_to_window:.0f}",
        flush=True,
    )

    done = time.time() + listen_sec
    ws_app: websocket.WebSocketApp | None = None

    def on_open(ws: websocket.WebSocketApp) -> None:
        payload = {
            "assets_ids": list(market.token_ids),
            "type": "market",
            "custom_feature_enabled": True,
        }
        ws.send(json.dumps(payload))
        print(f"subscribed tokens={market.token_ids}", flush=True)

    def on_message(_ws: websocket.WebSocketApp, message: str) -> None:
        if message == "PONG":
            return
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        event_type = str(payload.get("event_type", payload.get("type", "unknown")))
        cap.events.append(
            EventRecord(
                event_type=event_type,
                recv_mono=time.monotonic(),
                recv_unix=time.time(),
                payload=payload,
            )
        )

    def on_error(_ws: websocket.WebSocketApp, error: object) -> None:
        print(f"ws_error={error}", flush=True)

    ws_app = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
    )

    import threading

    thread = threading.Thread(
        target=lambda: ws_app.run_forever(ping_interval=0),
        daemon=True,
    )
    thread.start()

    while time.time() < done:
        time.sleep(0.2)
        if ws_app.sock and ws_app.sock.connected:
            try:
                ws_app.send("PING")
            except Exception:
                pass
        time.sleep(9.8)

    ws_app.close()
    thread.join(timeout=3)
    return cap


def summarize(cap: Capture) -> dict:
    window = cap.window_ts
    price_types = {"price_change", "best_bid_ask", "last_trade_price", "book"}
    price_events = [e for e in cap.events if e.event_type in price_types]

    pre = [e for e in price_events if e.recv_unix < window]
    post = [e for e in price_events if e.recv_unix >= window]

    first_pre = min(pre, key=lambda e: e.recv_unix) if pre else None
    first_post = min(post, key=lambda e: e.recv_unix) if post else None

    by_type_pre: dict[str, int] = {}
    by_type_post: dict[str, int] = {}
    for e in price_events:
        bucket = by_type_pre if e.recv_unix < window else by_type_post
        bucket[e.event_type] = bucket.get(e.event_type, 0) + 1

    samples: list[str] = []
    for e in pre[:5]:
        rel = e.recv_unix - window
        samples.append(f"{e.event_type} {rel:+.0f}s {_price_hint(e.payload)}")
    for e in post[:3]:
        rel = e.recv_unix - window
        samples.append(f"{e.event_type} {rel:+.0f}s {_price_hint(e.payload)}")

    return {
        "slug": cap.slug,
        "window_start_utc": _fmt_ts(window),
        "connected_sec_before_window": window - cap.connect_unix,
        "total_events": len(cap.events),
        "price_events_pre": len(pre),
        "price_events_post": len(post),
        "by_type_pre": by_type_pre,
        "by_type_post": by_type_post,
        "first_pre_sec": (first_pre.recv_unix - window) if first_pre else None,
        "first_post_sec": (first_post.recv_unix - window) if first_post else None,
        "first_pre_hint": _price_hint(first_pre.payload) if first_pre else "",
        "first_post_hint": _price_hint(first_post.payload) if first_post else "",
        "samples": samples,
        "pre_market_data": len(pre) > 0,
    }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--window-ts", type=int, default=None)
    parser.add_argument("--out", type=str, default="premarket_ws_test.json")
    args = parser.parse_args()

    now = time.time()
    target_window = (
        args.window_ts
        if args.window_ts is not None
        else current_window_timestamp(now) + WINDOW_SECONDS
    )
    sec_to_window = target_window - now

    if sec_to_window > PRE_WINDOW_SEC:
        wait = sec_to_window - PRE_WINDOW_SEC
        print(f"waiting {wait:.0f}s to connect 15m before window", flush=True)
        time.sleep(wait)

    listen_sec = max(60.0, (target_window - time.time()) + POST_WINDOW_SEC)
    cap = run_capture(listen_sec=listen_sec, window_timestamp=target_window)
    result = summarize(cap)

    print("\n=== RESULT ===", flush=True)
    for key, value in result.items():
        print(f"{key}: {value}", flush=True)

    out = ROOT / "state" / args.out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"saved {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
