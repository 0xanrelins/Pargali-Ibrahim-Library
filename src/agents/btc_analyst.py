#!/usr/bin/env python3
"""Rule-based BTC analyst — catalog + derived + knowledge (Layer 5 v1)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from catalog.db import CatalogConfig, connect_catalog, load_catalog_config
from datasets import (
    KNOWLEDGE,
    KNOWLEDGE_SYMBOL,
    POLYMARKET,
    POLYMARKET_SYMBOL,
    symbol_parquet_root,
)


@dataclass(frozen=True)
class BtcMarketContext:
    dataset_count: int
    pm_implied_up: float | None
    pm_perp_divergence: float | None
    perp_implied_up: float | None
    btc_price: float | None
    btc_return_bps: float | None
    seconds_to_expiry: float | None
    yes_no_deviation: float | None
    momentum: float | None
    positioning: float | None
    fear: float | None
    conviction: float | None


def _latest_parquet_day(root: Path) -> str | None:
    if not root.exists():
        return None
    files = sorted(root.glob("*.parquet"))
    if not files:
        return None
    return files[-1].stem


def _latest_up_row(derived_root: Path) -> pd.Series | None:
    day = _latest_parquet_day(derived_root)
    if day is None:
        return None
    frame = pd.read_parquet(derived_root / f"{day}.parquet")
    up_rows = frame[frame["outcome"] == "Up"]
    if up_rows.empty:
        return None
    return up_rows.sort_values("timestamp").iloc[-1]


def _latest_knowledge_row(knowledge_root: Path) -> pd.Series | None:
    day = _latest_parquet_day(knowledge_root)
    if day is None:
        return None
    frame = pd.read_parquet(knowledge_root / f"{day}.parquet")
    if frame.empty:
        return None
    return frame.sort_values("timestamp").iloc[-1]


def _catalog_dataset_count(config: CatalogConfig) -> int:
    if not config.db_path.exists():
        return 0
    connection = connect_catalog(config)
    try:
        row = connection.execute("SELECT COUNT(*) FROM datasets").fetchone()
        return int(row[0]) if row else 0
    finally:
        connection.close()


def load_context(
    *,
    project_root: Path | None = None,
    config: CatalogConfig | None = None,
) -> BtcMarketContext:
    cfg = config or load_catalog_config(project_root)
    data_root = cfg.data_root

    derived_root = symbol_parquet_root(
        data_root, POLYMARKET, "prediction_derived", POLYMARKET_SYMBOL
    )
    knowledge_root = symbol_parquet_root(
        data_root, KNOWLEDGE, "btc_signals", KNOWLEDGE_SYMBOL
    )

    derived = _latest_up_row(derived_root)
    knowledge = _latest_knowledge_row(knowledge_root)

    def _float(value: object) -> float | None:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        return float(value)

    return BtcMarketContext(
        dataset_count=_catalog_dataset_count(cfg),
        pm_implied_up=_float(derived.get("implied_prob")) if derived is not None else None,
        pm_perp_divergence=_float(derived.get("pm_perp_divergence")) if derived is not None else None,
        perp_implied_up=_float(derived.get("perp_implied_up")) if derived is not None else None,
        btc_price=_float(derived.get("btc_price")) if derived is not None else None,
        btc_return_bps=_float(derived.get("btc_return_bps")) if derived is not None else None,
        seconds_to_expiry=_float(derived.get("seconds_to_expiry")) if derived is not None else None,
        yes_no_deviation=_float(derived.get("yes_no_deviation")) if derived is not None else None,
        momentum=_float(knowledge.get("momentum")) if knowledge is not None else None,
        positioning=_float(knowledge.get("positioning")) if knowledge is not None else None,
        fear=_float(knowledge.get("fear")) if knowledge is not None else None,
        conviction=_float(knowledge.get("conviction")) if knowledge is not None else None,
    )


def _pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _score(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.0f}/100"


def _bps(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.1f} bps"


def _price(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.1f}"


def _seconds(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.0f}s"


def brief(context: BtcMarketContext) -> str:
    divergence_note = "aligned with perp"
    if context.pm_perp_divergence is not None:
        if context.pm_perp_divergence > 0.05:
            divergence_note = "more bullish than perp"
        elif context.pm_perp_divergence < -0.05:
            divergence_note = "less bullish than perp"

    return (
        f"BTC Suite briefing ({context.dataset_count} catalog datasets). "
        f"Polymarket Up implied {_pct(context.pm_implied_up)} "
        f"({divergence_note}, divergence {_pct(context.pm_perp_divergence)}). "
        f"Perp implied Up {_pct(context.perp_implied_up)}, "
        f"BTC {_price(context.btc_price)} ({_bps(context.btc_return_bps)} since window open), "
        f"expiry in {_seconds(context.seconds_to_expiry)}. "
        f"Knowledge: momentum {_score(context.momentum)}, "
        f"positioning {_score(context.positioning)}, "
        f"fear {_score(context.fear)}, "
        f"conviction {_score(context.conviction)}."
    )


def ask(question: str, context: BtcMarketContext) -> str:
    normalized = question.strip().lower()

    if any(token in normalized for token in ("pm ne diyor", "polymarket", "implied", "olasilik")):
        return (
            f"Polymarket Up implied probability is {_pct(context.pm_implied_up)}. "
            f"YES/NO deviation is {_pct(context.yes_no_deviation)}."
        )

    if any(token in normalized for token in ("divergence", "sapma", "fark")):
        return (
            f"PM–perp divergence is {_pct(context.pm_perp_divergence)} "
            f"(PM {_pct(context.pm_implied_up)} vs perp {_pct(context.perp_implied_up)})."
        )

    if "momentum" in normalized:
        return f"BTC momentum score is {_score(context.momentum)} (return {_bps(context.btc_return_bps)})."

    if any(token in normalized for token in ("fear", "korku", "likidasyon")):
        return f"BTC fear score is {_score(context.fear)}."

    if any(token in normalized for token in ("conviction", "inanç", "guven")):
        return f"Cross-venue conviction score is {_score(context.conviction)}."

    if any(token in normalized for token in ("positioning", "pozisyon", "flow")):
        return f"BTC positioning score is {_score(context.positioning)}."

    if any(token in normalized for token in ("fiyat", "price", "btc")):
        return (
            f"BTC perp price is {_price(context.btc_price)} "
            f"({_bps(context.btc_return_bps)} since 15m window open)."
        )

    if any(token in normalized for token in ("expiry", "sure", "kalan")):
        return f"Active BTC 15m market expires in {_seconds(context.seconds_to_expiry)}."

    if any(token in normalized for token in ("ozet", "özet", "brief", "durum")):
        return brief(context)

    return (
        f"{brief(context)}\n\n"
        "Try: pm ne diyor | divergence | momentum | fear | conviction | btc fiyat"
    )
