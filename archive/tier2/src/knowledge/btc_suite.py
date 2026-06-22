#!/usr/bin/env python3
"""Compute BTC knowledge objects from derived and normalized data (Layer 4)."""

from __future__ import annotations

import logging
import math
from pathlib import Path

import pandas as pd

from datasets import (
    BINANCE_FUTURES,
    KNOWLEDGE,
    KNOWLEDGE_SYMBOL,
    POLYMARKET,
    POLYMARKET_SYMBOL,
    symbol_parquet_root,
)
from metrics.knowledge_registry import (
    btc_knowledge_conviction,
    btc_knowledge_fear,
    btc_knowledge_momentum,
    btc_knowledge_positioning,
)

logger = logging.getLogger(__name__)

KNOWLEDGE_COLUMNS = [
    "timestamp",
    "symbol",
    "momentum",
    "positioning",
    "fear",
    "conviction",
]

RECENT_TRADE_LOOKBACK_MS = 5 * 60 * 1000
FEAR_LIQUIDATION_BASELINE = 5.0


def _clamp_score(value: float) -> float:
    return max(0.0, min(100.0, value))


def _momentum_score(btc_return_bps: float) -> float:
    return _clamp_score(50.0 + 50.0 * math.tanh(btc_return_bps / 50.0))


def _positioning_score(trades: pd.DataFrame, timestamp_ms: int) -> float:
    if trades.empty:
        return 50.0

    window = trades[
        (trades["timestamp"] <= timestamp_ms)
        & (trades["timestamp"] >= timestamp_ms - RECENT_TRADE_LOOKBACK_MS)
    ]
    if window.empty:
        return 50.0

    buy_volume = window.loc[window["side"] == "buy", "amount"].sum()
    sell_volume = window.loc[window["side"] == "sell", "amount"].sum()
    total = buy_volume + sell_volume
    if total <= 0:
        return 50.0
    return _clamp_score(100.0 * float(buy_volume / total))


def _fear_score(liquidations: pd.DataFrame, timestamp_ms: int) -> float:
    if liquidations.empty:
        return 0.0

    window = liquidations[
        (liquidations["timestamp"] <= timestamp_ms)
        & (liquidations["timestamp"] >= timestamp_ms - RECENT_TRADE_LOOKBACK_MS)
    ]
    count = len(window)
    return _clamp_score(100.0 * (count / FEAR_LIQUIDATION_BASELINE))


def _conviction_score(
    divergence: float | None,
    liquidity: float | None,
    yes_no_deviation: float | None,
) -> float:
    if divergence is None or pd.isna(divergence):
        return 50.0

    alignment = max(0.0, 1.0 - abs(float(divergence)))
    liquidity_weight = 1.0
    if liquidity is not None and pd.notna(liquidity):
        liquidity_weight = min(1.0, float(liquidity) / 500.0)

    consistency = 1.0
    if yes_no_deviation is not None and pd.notna(yes_no_deviation):
        consistency = max(0.0, 1.0 - float(yes_no_deviation) / 0.1)

    return _clamp_score(100.0 * alignment * liquidity_weight * consistency)


def _latest_up_row(derived: pd.DataFrame) -> pd.Series | None:
    up_rows = derived[derived["outcome"] == "Up"]
    if up_rows.empty:
        return None
    return up_rows.sort_values("timestamp").iloc[-1]


def compute_knowledge_row(
    *,
    derived: pd.DataFrame,
    trades: pd.DataFrame,
    liquidations: pd.DataFrame,
) -> dict[str, object] | None:
    latest = _latest_up_row(derived)
    if latest is None:
        return None

    timestamp_ms = int(latest["timestamp"])
    btc_return = latest.get("btc_return_bps")
    momentum = _momentum_score(float(btc_return)) if pd.notna(btc_return) else 50.0
    positioning = _positioning_score(trades, timestamp_ms)
    fear = _fear_score(liquidations, timestamp_ms)
    conviction = _conviction_score(
        latest.get("pm_perp_divergence"),
        latest.get("top_of_book_liquidity"),
        latest.get("yes_no_deviation"),
    )

    return {
        "timestamp": timestamp_ms,
        "symbol": KNOWLEDGE_SYMBOL,
        "momentum": momentum,
        "positioning": positioning,
        "fear": fear,
        "conviction": conviction,
    }


def _publish_knowledge_metrics(row: dict[str, object]) -> None:
    btc_knowledge_momentum.set(float(row["momentum"]))
    btc_knowledge_positioning.set(float(row["positioning"]))
    btc_knowledge_fear.set(float(row["fear"]))
    btc_knowledge_conviction.set(float(row["conviction"]))


def _write_knowledge_day(row: dict[str, object], out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([row]).astype(
        {
            "timestamp": "int64",
            "symbol": "string",
            "momentum": "float64",
            "positioning": "float64",
            "fear": "float64",
            "conviction": "float64",
        }
    )
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        frame = pd.concat([existing, frame], ignore_index=True)
        frame = frame.drop_duplicates(subset=["timestamp"], keep="last")
        frame = frame.sort_values("timestamp")
    frame.to_parquet(out_path, index=False)
    return len(frame)


def compute_knowledge_day(data_root: Path, day: str) -> int:
    derived_path = (
        symbol_parquet_root(data_root, POLYMARKET, "prediction_derived", POLYMARKET_SYMBOL)
        / f"{day}.parquet"
    )
    if not derived_path.exists():
        return 0

    derived = pd.read_parquet(derived_path)
    trades_path = symbol_parquet_root(data_root, BINANCE_FUTURES, "trades", "BTCUSDT") / f"{day}.parquet"
    liq_path = (
        symbol_parquet_root(data_root, BINANCE_FUTURES, "liquidations", "BTCUSDT") / f"{day}.parquet"
    )
    trades = pd.read_parquet(trades_path) if trades_path.exists() else pd.DataFrame()
    liquidations = pd.read_parquet(liq_path) if liq_path.exists() else pd.DataFrame()

    row = compute_knowledge_row(derived=derived, trades=trades, liquidations=liquidations)
    if row is None:
        return 0

    out_path = symbol_parquet_root(data_root, KNOWLEDGE, "btc_signals", KNOWLEDGE_SYMBOL) / f"{day}.parquet"
    rows_written = _write_knowledge_day(row, out_path)
    _publish_knowledge_metrics(row)
    logger.info("Knowledge objects for %s: %s rows -> %s", day, rows_written, out_path)
    return rows_written


def compute_knowledge_once(data_root: Path) -> dict[str, int]:
    derived_root = symbol_parquet_root(
        data_root, POLYMARKET, "prediction_derived", POLYMARKET_SYMBOL
    )
    if not derived_root.exists():
        return {"days_processed": 0, "rows_written": 0}

    stats = {"days_processed": 0, "rows_written": 0}
    for derived_path in sorted(derived_root.glob("*.parquet")):
        day = derived_path.stem
        rows = compute_knowledge_day(data_root, day)
        if rows:
            stats["days_processed"] += 1
            stats["rows_written"] += rows

    return stats
