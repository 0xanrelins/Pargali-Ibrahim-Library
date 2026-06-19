#!/usr/bin/env python3
"""Cross-venue derived metrics: Polymarket prediction vs Binance perp."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from datasets import BINANCE_FUTURES, symbol_parquet_root
from polymarket.discovery import WINDOW_SECONDS

logger = logging.getLogger(__name__)

BINANCE_SYMBOL = "BTCUSDT"
SLUG_PREFIX = "btc-updown-15m"
RETURN_BPS_FOR_FULL_CONVICTION = 200.0

CROSS_COLUMNS = (
    "btc_price",
    "window_open_price",
    "btc_return_bps",
    "perp_implied_up",
    "pm_perp_divergence",
)


def window_open_ms_from_slug(slug: str) -> int | None:
    if not slug.startswith(f"{SLUG_PREFIX}-"):
        return None
    try:
        return int(slug.rsplit("-", 1)[-1]) * 1000
    except ValueError:
        return None


def _empty_cross_columns(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy()
    for column in CROSS_COLUMNS:
        enriched[column] = pd.NA
    return enriched


def _window_open_prices(trades: pd.DataFrame, slugs: pd.Series) -> dict[str, float]:
    if trades.empty:
        return {}

    prices: dict[str, float] = {}
    ordered = trades.sort_values("timestamp")
    for slug in slugs.dropna().unique():
        slug_str = str(slug)
        window_open_ms = window_open_ms_from_slug(slug_str)
        if window_open_ms is None:
            continue
        window_trades = ordered[ordered["timestamp"] >= window_open_ms]
        if window_trades.empty:
            continue
        prices[slug_str] = float(window_trades.iloc[0]["price"])
    return prices


def _perp_implied_up(btc_return_bps: float, seconds_to_expiry: float | None) -> float:
    if seconds_to_expiry is None or pd.isna(seconds_to_expiry):
        time_factor = 1.0
    else:
        time_factor = max(0.0, min(1.0, 1.0 - float(seconds_to_expiry) / WINDOW_SECONDS))
    bias = (btc_return_bps / RETURN_BPS_FOR_FULL_CONVICTION) * time_factor
    return float(max(0.0, min(1.0, 0.5 + bias)))


def attach_perp_context(derived: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    if derived.empty:
        return derived

    if trades.empty:
        return _empty_cross_columns(derived)

    open_prices = _window_open_prices(trades, derived["slug"])
    if not open_prices:
        return _empty_cross_columns(derived)

    frame = derived.sort_values("timestamp").copy()
    trade_prices = trades[["timestamp", "price"]].sort_values("timestamp")
    frame = pd.merge_asof(
        frame,
        trade_prices.rename(columns={"price": "btc_price"}),
        on="timestamp",
        direction="backward",
    )
    frame["window_open_price"] = frame["slug"].map(open_prices)
    frame["btc_return_bps"] = (
        (frame["btc_price"] - frame["window_open_price"]) / frame["window_open_price"] * 10_000.0
    ).where(frame["btc_price"].notna() & frame["window_open_price"].notna())

    frame["perp_implied_up"] = frame.apply(
        lambda row: _perp_implied_up(
            float(row["btc_return_bps"]),
            row.get("seconds_to_expiry"),
        )
        if pd.notna(row.get("btc_return_bps"))
        else pd.NA,
        axis=1,
    )
    frame["pm_perp_divergence"] = (
        frame["implied_prob"] - frame["perp_implied_up"]
    ).where(frame["outcome"] == "Up")
    return frame


def load_binance_trades_day(data_root: Path, day: str) -> pd.DataFrame:
    trades_path = (
        symbol_parquet_root(data_root, BINANCE_FUTURES, "trades", BINANCE_SYMBOL) / f"{day}.parquet"
    )
    if not trades_path.exists():
        return pd.DataFrame()
    return pd.read_parquet(trades_path)
