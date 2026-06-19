#!/usr/bin/env python3
"""Derive Polymarket metrics from normalized parquet (Layer 3)."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from datasets import POLYMARKET, POLYMARKET_SYMBOL, symbol_parquet_root
from derive_cross_market import CROSS_COLUMNS, attach_perp_context, load_binance_trades_day
from metrics.polymarket_registry import (
    polymarket_derived_stale_seconds,
    polymarket_implied_prob_up,
    polymarket_seconds_to_expiry,
    polymarket_spread_up,
    polymarket_top_of_book_liquidity_up,
    polymarket_yes_no_deviation,
    cross_btc_price,
    cross_btc_return_bps,
    cross_perp_implied_up,
    cross_pm_perp_divergence,
)

logger = logging.getLogger(__name__)

STALE_THRESHOLD_SECONDS = 30.0

DERIVED_COLUMNS = [
    "timestamp",
    "symbol",
    "slug",
    "outcome",
    "asset_id",
    "best_bid",
    "best_ask",
    "mid",
    "spread",
    "implied_prob",
    "seconds_to_expiry",
    "yes_no_sum",
    "yes_no_deviation",
    "seconds_since_last_update",
    "top_bid_liquidity",
    "top_ask_liquidity",
    "top_of_book_liquidity",
    "is_stale",
    "exchange",
]

OUTPUT_COLUMNS = [*DERIVED_COLUMNS, *CROSS_COLUMNS]


def _parse_end_date_ms(end_date: str) -> int | None:
    if not end_date:
        return None
    normalized = end_date.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(normalized).timestamp() * 1000)
    except ValueError:
        return None


def _metadata_context(metadata: pd.DataFrame) -> pd.DataFrame:
    if metadata.empty:
        return metadata

    subscriptions = metadata[metadata["event_type"] == "market_subscription"].copy()
    if subscriptions.empty:
        subscriptions = metadata.copy()

    subscriptions = subscriptions.sort_values("timestamp")
    subscriptions["end_date_ms"] = subscriptions["end_date"].map(_parse_end_date_ms)
    return subscriptions[
        ["timestamp", "slug", "yes_token_id", "no_token_id", "end_date_ms"]
    ]


def _attach_metadata(price_rows: pd.DataFrame, metadata: pd.DataFrame) -> pd.DataFrame:
    if price_rows.empty:
        return price_rows

    context = _metadata_context(metadata)
    if context.empty:
        price_rows = price_rows.copy()
        price_rows["slug"] = ""
        price_rows["yes_token_id"] = ""
        price_rows["no_token_id"] = ""
        price_rows["end_date_ms"] = pd.NA
        return price_rows

    return pd.merge_asof(
        price_rows.sort_values("timestamp"),
        context.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )


def _outcome_label(asset_id: str, yes_token_id: str, no_token_id: str) -> str:
    if asset_id and asset_id == yes_token_id:
        return "Up"
    if asset_id and asset_id == no_token_id:
        return "Down"
    return "Unknown"


def _top_of_book_liquidity(orderbook: pd.DataFrame) -> pd.DataFrame:
    if orderbook.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "asset_id",
                "outcome",
                "top_bid_liquidity",
                "top_ask_liquidity",
                "top_of_book_liquidity",
            ]
        )

    top_levels = orderbook[orderbook["level_index"] == 0].copy()
    if top_levels.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "asset_id",
                "outcome",
                "top_bid_liquidity",
                "top_ask_liquidity",
                "top_of_book_liquidity",
            ]
        )

    grouped = (
        top_levels.groupby(["timestamp", "asset_id", "side"], as_index=False)["quantity"]
        .sum()
    )
    bids = grouped[grouped["side"] == "bid"].rename(columns={"quantity": "top_bid_liquidity"})
    asks = grouped[grouped["side"] == "ask"].rename(columns={"quantity": "top_ask_liquidity"})

    liquidity = pd.merge(
        bids[["timestamp", "asset_id", "top_bid_liquidity"]],
        asks[["timestamp", "asset_id", "top_ask_liquidity"]],
        on=["timestamp", "asset_id"],
        how="outer",
    )
    liquidity["top_bid_liquidity"] = liquidity["top_bid_liquidity"].fillna(0.0)
    liquidity["top_ask_liquidity"] = liquidity["top_ask_liquidity"].fillna(0.0)
    liquidity["top_of_book_liquidity"] = (
        liquidity["top_bid_liquidity"] + liquidity["top_ask_liquidity"]
    )
    return liquidity


def _attach_liquidity(derived: pd.DataFrame, orderbook: pd.DataFrame) -> pd.DataFrame:
    liquidity = _top_of_book_liquidity(orderbook)
    if liquidity.empty:
        derived = derived.copy()
        derived["top_bid_liquidity"] = pd.NA
        derived["top_ask_liquidity"] = pd.NA
        derived["top_of_book_liquidity"] = pd.NA
        return derived

    frame = derived.sort_values("timestamp").copy()
    frame["asset_id"] = frame["asset_id"].astype("string")
    liquidity = liquidity.sort_values("timestamp").copy()
    liquidity["asset_id"] = liquidity["asset_id"].astype("string")

    enriched = pd.merge_asof(
        frame,
        liquidity,
        on="timestamp",
        by="asset_id",
        direction="backward",
    )
    return enriched


def _attach_consistency(derived: pd.DataFrame) -> pd.DataFrame:
    if derived.empty:
        return derived

    up_down = (
        derived[derived["outcome"].isin(["Up", "Down"])]
        .pivot_table(index="timestamp", columns="outcome", values="implied_prob", aggfunc="last")
        .reset_index()
    )
    if "Up" not in up_down.columns or "Down" not in up_down.columns:
        derived = derived.copy()
        derived["yes_no_sum"] = pd.NA
        derived["yes_no_deviation"] = pd.NA
        return derived

    up_down["yes_no_sum"] = up_down["Up"] + up_down["Down"]
    up_down["yes_no_deviation"] = (up_down["yes_no_sum"] - 1.0).abs()
    return derived.merge(
        up_down[["timestamp", "yes_no_sum", "yes_no_deviation"]],
        on="timestamp",
        how="left",
    )


def _attach_staleness(derived: pd.DataFrame) -> pd.DataFrame:
    if derived.empty:
        return derived

    frame = derived.sort_values("timestamp").copy()
    frame["seconds_since_last_update"] = (
        frame.groupby(["slug", "outcome"], dropna=False)["timestamp"].diff() / 1000.0
    )
    frame["is_stale"] = frame["seconds_since_last_update"] > STALE_THRESHOLD_SECONDS
    return frame


def compute_derived_rows(
    price_rows: pd.DataFrame,
    metadata: pd.DataFrame,
    orderbook: pd.DataFrame,
) -> pd.DataFrame:
    if price_rows.empty:
        return pd.DataFrame(columns=DERIVED_COLUMNS)

    usable = price_rows[
        price_rows["best_bid"].notna() & price_rows["best_ask"].notna()
    ].copy()
    if usable.empty:
        return pd.DataFrame(columns=DERIVED_COLUMNS)

    usable["best_bid"] = usable["best_bid"].astype(float)
    usable["best_ask"] = usable["best_ask"].astype(float)
    enriched = _attach_metadata(usable, metadata)

    enriched["mid"] = (enriched["best_bid"] + enriched["best_ask"]) / 2.0
    enriched["spread"] = enriched["best_ask"] - enriched["best_bid"]
    enriched["implied_prob"] = enriched["mid"]
    enriched["outcome"] = enriched.apply(
        lambda row: _outcome_label(
            str(row.get("asset_id", "")),
            str(row.get("yes_token_id", "")),
            str(row.get("no_token_id", "")),
        ),
        axis=1,
    )
    enriched["seconds_to_expiry"] = (
        (enriched["end_date_ms"] - enriched["timestamp"]) / 1000.0
    ).where(enriched["end_date_ms"].notna())

    derived = enriched.assign(exchange=POLYMARKET)
    derived["slug"] = derived["slug"].fillna("").astype(str)
    derived = _attach_consistency(derived)
    derived = _attach_staleness(derived)
    derived = _attach_liquidity(derived, orderbook)
    return derived[DERIVED_COLUMNS]


def _write_derived_day(derived: pd.DataFrame, out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    frame = derived.astype(
        {
            "timestamp": "int64",
            "symbol": "string",
            "slug": "string",
            "outcome": "string",
            "asset_id": "string",
            "best_bid": "float64",
            "best_ask": "float64",
            "mid": "float64",
            "spread": "float64",
            "implied_prob": "float64",
            "seconds_to_expiry": "Float64",
            "yes_no_sum": "Float64",
            "yes_no_deviation": "Float64",
            "seconds_since_last_update": "Float64",
            "top_bid_liquidity": "Float64",
            "top_ask_liquidity": "Float64",
            "top_of_book_liquidity": "Float64",
            "is_stale": "boolean",
            "exchange": "string",
            "btc_price": "Float64",
            "window_open_price": "Float64",
            "btc_return_bps": "Float64",
            "perp_implied_up": "Float64",
            "pm_perp_divergence": "Float64",
        }
    )
    frame.to_parquet(out_path, index=False)
    return len(frame)


def _publish_latest_metrics(derived: pd.DataFrame) -> None:
    if derived.empty:
        return

    latest_ts = int(derived["timestamp"].max())
    polymarket_derived_stale_seconds.set(max(0.0, time.time() - latest_ts / 1000.0))

    up_rows = derived[derived["outcome"] == "Up"]
    if up_rows.empty:
        latest = derived.sort_values("timestamp").iloc[-1]
    else:
        latest = up_rows.sort_values("timestamp").iloc[-1]

    polymarket_implied_prob_up.set(float(latest["implied_prob"]))
    polymarket_spread_up.set(float(latest["spread"]))
    expiry = latest.get("seconds_to_expiry")
    if pd.notna(expiry):
        polymarket_seconds_to_expiry.set(float(expiry))

    deviation = latest.get("yes_no_deviation")
    if pd.notna(deviation):
        polymarket_yes_no_deviation.set(float(deviation))

    liquidity = latest.get("top_of_book_liquidity")
    if pd.notna(liquidity):
        polymarket_top_of_book_liquidity_up.set(float(liquidity))

    btc_price = latest.get("btc_price")
    if pd.notna(btc_price):
        cross_btc_price.set(float(btc_price))

    btc_return = latest.get("btc_return_bps")
    if pd.notna(btc_return):
        cross_btc_return_bps.set(float(btc_return))

    perp_implied = latest.get("perp_implied_up")
    if pd.notna(perp_implied):
        cross_perp_implied_up.set(float(perp_implied))

    divergence = latest.get("pm_perp_divergence")
    if pd.notna(divergence):
        cross_pm_perp_divergence.set(float(divergence))


def derive_polymarket_day(data_root: Path, day: str) -> int:
    symbol = POLYMARKET_SYMBOL
    price_path = symbol_parquet_root(data_root, POLYMARKET, "prediction_price", symbol) / f"{day}.parquet"
    metadata_path = (
        symbol_parquet_root(data_root, POLYMARKET, "prediction_metadata", symbol) / f"{day}.parquet"
    )
    orderbook_path = (
        symbol_parquet_root(data_root, POLYMARKET, "prediction_orderbook", symbol) / f"{day}.parquet"
    )
    out_path = symbol_parquet_root(data_root, POLYMARKET, "prediction_derived", symbol) / f"{day}.parquet"

    if not price_path.exists():
        return 0

    price_rows = pd.read_parquet(price_path)
    metadata = pd.read_parquet(metadata_path) if metadata_path.exists() else pd.DataFrame()
    orderbook = pd.read_parquet(orderbook_path) if orderbook_path.exists() else pd.DataFrame()
    derived = compute_derived_rows(price_rows, metadata, orderbook)
    if derived.empty:
        return 0

    trades = load_binance_trades_day(data_root, day)
    derived = attach_perp_context(derived, trades)
    derived = derived[OUTPUT_COLUMNS]

    rows_written = _write_derived_day(derived, out_path)
    _publish_latest_metrics(derived)
    logger.info("Derived Polymarket metrics for %s: %s rows -> %s", day, rows_written, out_path)
    return rows_written


def derive_polymarket_once(data_root: Path) -> dict[str, int]:
    price_root = symbol_parquet_root(data_root, POLYMARKET, "prediction_price", POLYMARKET_SYMBOL)
    if not price_root.exists():
        return {"days_processed": 0, "rows_written": 0}

    stats = {"days_processed": 0, "rows_written": 0}
    for price_path in sorted(price_root.glob("*.parquet")):
        day = price_path.stem
        rows = derive_polymarket_day(data_root, day)
        if rows:
            stats["days_processed"] += 1
            stats["rows_written"] += rows

    return stats
