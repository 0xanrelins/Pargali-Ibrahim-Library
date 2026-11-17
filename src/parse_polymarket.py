#!/usr/bin/env python3
"""Parse raw Polymarket JSONL lines into normalized row dicts."""

from __future__ import annotations

import logging
from pathlib import Path

from datasets import (
    POLYMARKET,
    POLYMARKET_CATEGORIES,
    POLYMARKET_SYMBOL,
    PolymarketCategory,
    iter_jsonl_paths as dataset_iter_jsonl_paths,
)
from json_codec import loads as json_loads
from polymarket.discovery import is_btc_15m_metadata

logger = logging.getLogger(__name__)

EventCategory = PolymarketCategory
ParsedRow = dict[str, object]
EXCHANGE = POLYMARKET
SYMBOL = POLYMARKET_SYMBOL

ALL_CATEGORIES: tuple[EventCategory, ...] = POLYMARKET_CATEGORIES

METADATA_EVENTS = frozenset({"new_market", "market_resolved", "market_subscription"})
PRICE_EVENTS = frozenset({"price_change", "best_bid_ask", "tick_size_change"})
TRADE_EVENTS = frozenset({"last_trade_price"})
ORDERBOOK_EVENTS = frozenset({"book"})


def _parse_timestamp_ms(payload: dict[str, object]) -> int:
    for key in ("timestamp", "t", "time"):
        raw = payload.get(key)
        if raw is None:
            continue
        value = int(str(raw))
        if value < 10_000_000_000:
            return value * 1000
        return value
    return 0


def _outcome_label(asset_id: str, yes_token_id: str | None, no_token_id: str | None) -> str:
    if yes_token_id and asset_id == yes_token_id:
        return "Up"
    if no_token_id and asset_id == no_token_id:
        return "Down"
    return "Unknown"


class OutcomeTokenRegistry:
    """Maps Polymarket asset_id → Up/Down from metadata subscription rows."""

    def __init__(self) -> None:
        self._by_asset: dict[str, str] = {}

    def update_from_metadata(self, row: ParsedRow) -> None:
        yes_token_id = str(row.get("yes_token_id", ""))
        no_token_id = str(row.get("no_token_id", ""))
        if yes_token_id:
            self._by_asset[yes_token_id] = "Up"
        if no_token_id:
            self._by_asset[no_token_id] = "Down"

    def label(self, asset_id: str) -> str:
        if not asset_id:
            return "Unknown"
        return self._by_asset.get(asset_id, "Unknown")

    def to_mapping(self) -> dict[str, str]:
        return dict(self._by_asset)

    def merge_from_mapping(self, mapping: dict[str, str]) -> None:
        self._by_asset.update(mapping)


def _resolve_outcome(
    asset_id: str,
    *,
    token_registry: OutcomeTokenRegistry | None = None,
    yes_token_id: str | None = None,
    no_token_id: str | None = None,
) -> str:
    if yes_token_id or no_token_id:
        return _outcome_label(asset_id, yes_token_id, no_token_id)
    if token_registry is not None:
        return token_registry.label(asset_id)
    return "Unknown"


def parse_metadata_row(payload: dict[str, object]) -> ParsedRow:
    event_type = str(payload.get("event_type", payload.get("type", "unknown")))
    row: ParsedRow = {
        "timestamp": _parse_timestamp_ms(payload),
        "symbol": SYMBOL,
        "event_type": event_type,
        "slug": str(payload.get("slug", "")),
        "condition_id": str(payload.get("condition_id", payload.get("market", ""))),
        "question": str(payload.get("question", "")),
        "end_date": str(payload.get("end_date", "")),
        "yes_token_id": str(payload.get("yes_token_id", "")),
        "no_token_id": str(payload.get("no_token_id", "")),
        "winning_outcome": "",
        "winning_asset_id": "",
        "resolved_source": "",
        "exchange": EXCHANGE,
    }
    if event_type == "market_resolved":
        row["winning_outcome"] = str(payload.get("winning_outcome", ""))
        row["winning_asset_id"] = str(payload.get("winning_asset_id", ""))
        row["resolved_source"] = str(payload.get("resolved_source", "ws"))
    return row


def parse_price_row(
    payload: dict[str, object],
    *,
    token_registry: OutcomeTokenRegistry | None = None,
) -> list[ParsedRow]:
    event_type = str(payload.get("event_type", ""))
    timestamp = _parse_timestamp_ms(payload)
    asset_id = str(payload.get("asset_id", ""))
    rows: list[ParsedRow] = []

    if event_type == "best_bid_ask":
        rows.append(
            {
                "timestamp": timestamp,
                "symbol": SYMBOL,
                "event_type": event_type,
                "asset_id": asset_id,
                "outcome": _resolve_outcome(asset_id, token_registry=token_registry),
                "best_bid": float(payload.get("best_bid", 0) or 0),
                "best_ask": float(payload.get("best_ask", 0) or 0),
                "spread": float(payload.get("spread", 0) or 0),
                "price": None,
                "side": "",
                "exchange": EXCHANGE,
            }
        )
        return rows

    if event_type == "price_change":
        changes = payload.get("price_changes")
        if not isinstance(changes, list):
            return rows
        for change in changes:
            if not isinstance(change, dict):
                continue
            change_asset_id = str(change.get("asset_id", asset_id))
            rows.append(
                {
                    "timestamp": timestamp,
                    "symbol": SYMBOL,
                    "event_type": event_type,
                    "asset_id": change_asset_id,
                    "outcome": _resolve_outcome(change_asset_id, token_registry=token_registry),
                    "best_bid": float(change.get("best_bid", 0) or 0),
                    "best_ask": float(change.get("best_ask", 0) or 0),
                    "spread": float(change.get("spread", 0) or 0),
                    "price": float(change.get("price", 0) or 0),
                    "side": str(change.get("side", "")),
                    "exchange": EXCHANGE,
                }
            )
        return rows

    rows.append(
        {
            "timestamp": timestamp,
            "symbol": SYMBOL,
            "event_type": event_type,
            "asset_id": asset_id,
            "outcome": _resolve_outcome(asset_id, token_registry=token_registry),
            "best_bid": None,
            "best_ask": None,
            "spread": None,
            "price": float(payload.get("price", 0) or 0),
            "side": str(payload.get("side", "")),
            "exchange": EXCHANGE,
        }
    )
    return rows


def parse_trade_row(
    payload: dict[str, object],
    *,
    token_registry: OutcomeTokenRegistry | None = None,
) -> ParsedRow:
    asset_id = str(payload.get("asset_id", ""))
    return {
        "timestamp": _parse_timestamp_ms(payload),
        "symbol": SYMBOL,
        "asset_id": asset_id,
        "outcome": _resolve_outcome(asset_id, token_registry=token_registry),
        "price": float(payload.get("price", 0) or 0),
        "side": str(payload.get("side", "")),
        "size": float(payload.get("size", 0) or 0),
        "exchange": EXCHANGE,
    }


def parse_orderbook_rows(
    payload: dict[str, object],
    *,
    token_registry: OutcomeTokenRegistry | None = None,
) -> list[ParsedRow]:
    timestamp = _parse_timestamp_ms(payload)
    asset_id = str(payload.get("asset_id", ""))
    rows: list[ParsedRow] = []

    for side_name, side_key in (("bid", "bids"), ("ask", "asks")):
        levels = payload.get(side_key)
        if not isinstance(levels, list):
            continue
        for level_index, level in enumerate(levels):
            if not isinstance(level, dict):
                continue
            rows.append(
                {
                    "timestamp": timestamp,
                    "symbol": SYMBOL,
                    "asset_id": asset_id,
                    "outcome": _resolve_outcome(asset_id, token_registry=token_registry),
                    "side": side_name,
                    "level_index": level_index,
                    "price": float(level.get("price", 0) or 0),
                    "quantity": float(level.get("size", 0) or 0),
                    "exchange": EXCHANGE,
                }
            )
    return rows


def classify_category(event_type: str) -> EventCategory | None:
    if event_type in METADATA_EVENTS:
        return "prediction_metadata"
    if event_type in PRICE_EVENTS:
        return "prediction_price"
    if event_type in TRADE_EVENTS:
        return "prediction_trades"
    if event_type in ORDERBOOK_EVENTS:
        return "prediction_orderbook"
    return None


def parse_polymarket_lines(
    raw_line: str,
    *,
    token_registry: OutcomeTokenRegistry | None = None,
) -> list[tuple[EventCategory, ParsedRow]]:
    line = raw_line.strip()
    if not line:
        return []

    try:
        payload = json_loads(line)
    except ValueError as exc:
        logger.warning("Skipping invalid Polymarket JSON line: %s", exc)
        return []

    if not isinstance(payload, dict):
        return []

    event_type = str(payload.get("event_type", payload.get("type", "")))
    category = classify_category(event_type)
    if category is None:
        return []

    if category == "prediction_metadata":
        if not is_btc_15m_metadata(payload):
            return []
        row = parse_metadata_row(payload)
        if token_registry is not None:
            token_registry.update_from_metadata(row)
        return [(category, row)]
    if category == "prediction_price":
        return [(category, row) for row in parse_price_row(payload, token_registry=token_registry)]
    if category == "prediction_trades":
        return [(category, parse_trade_row(payload, token_registry=token_registry))]
    return [(category, row) for row in parse_orderbook_rows(payload, token_registry=token_registry)]


def seed_token_registry_from_raw(raw_root: Path) -> OutcomeTokenRegistry:
    """Load yes/no token ids from all raw metadata JSONL files."""
    registry = OutcomeTokenRegistry()
    for jsonl_path in iter_jsonl_paths(raw_root, "prediction_metadata"):
        for raw_line in jsonl_path.read_text(encoding="utf-8").splitlines():
            parse_polymarket_lines(raw_line, token_registry=registry)
    return registry


def iter_jsonl_paths(raw_root: Path, category: EventCategory) -> list[Path]:
    return dataset_iter_jsonl_paths(raw_root, category)
