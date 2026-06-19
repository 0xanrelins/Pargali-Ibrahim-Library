"""Parse raw Binance futures JSONL lines into normalized row dicts (Plan 2)."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from datasets import (
    BINANCE_FUTURES,
    BINANCE_FUTURES_CATEGORIES,
    BinanceCategory,
    iter_jsonl_paths as dataset_iter_jsonl_paths,
)
from unicorn_fy import UnicornFy

logger = logging.getLogger(__name__)
TARGET_SYMBOLS = frozenset({"BTCUSDT"})
ORDERBOOK_SYMBOLS = frozenset({"BTCUSDT"})

EventCategory = BinanceCategory
ParsedRow = dict[str, object]

ALL_CATEGORIES: tuple[EventCategory, ...] = BINANCE_FUTURES_CATEGORIES
EXCHANGE = BINANCE_FUTURES


def unwrap_payload(raw_line: str) -> dict[str, object]:
    payload = json.loads(raw_line)
    if not isinstance(payload, dict):
        raise ValueError("JSON root must be an object")

    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def normalize_raw_line(raw_line: str) -> dict[str, object]:
    return UnicornFy.binance_com_futures_websocket(raw_line)


def trade_side_from_market_maker(is_market_maker: bool) -> str:
    return "sell" if is_market_maker else "buy"


def parse_trade_row(normalized: dict[str, object]) -> ParsedRow:
    return {
        "timestamp": int(normalized["trade_time"]),
        "symbol": str(normalized["symbol"]),
        "agg_trade_id": int(normalized["aggregate_trade_id"]),
        "price": float(normalized["price"]),
        "amount": float(normalized["quantity"]),
        "side": trade_side_from_market_maker(bool(normalized["is_market_maker"])),
        "exchange": EXCHANGE,
    }


def parse_liquidation_row(normalized: dict[str, object]) -> ParsedRow:
    return {
        "timestamp": int(normalized["event_time"]),
        "symbol": str(normalized["symbol"]),
        "side": str(normalized["side"]),
        "price": float(normalized["price"]),
        "avg_price": float(normalized["avg_price"]),
        "quantity": float(normalized["original_quantity"]),
        "filled_qty": float(normalized["cumulative_filled_quantity"]),
        "exchange": EXCHANGE,
    }


def _parse_depth_levels(
    levels: object,
    *,
    side: str,
    timestamp: int,
    event_time: int,
    transaction_time: int,
    symbol: str,
    first_update_id: int,
    final_update_id: int,
    previous_final_update_id: int | None,
) -> list[ParsedRow]:
    if not isinstance(levels, list):
        return []

    rows: list[ParsedRow] = []
    for level in levels:
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            continue
        rows.append(
            {
                "timestamp": timestamp,
                "event_time": event_time,
                "transaction_time": transaction_time,
                "symbol": symbol,
                "first_update_id": first_update_id,
                "final_update_id": final_update_id,
                "previous_final_update_id": previous_final_update_id,
                "side": side,
                "price": float(level[0]),
                "quantity": float(level[1]),
                "exchange": EXCHANGE,
            }
        )
    return rows


def parse_orderbook_diff_rows(data: dict[str, object]) -> list[ParsedRow]:
    symbol = str(data.get("s", ""))
    if symbol not in ORDERBOOK_SYMBOLS:
        return []

    event_time = int(data.get("E", 0))
    transaction_time = int(data.get("T", event_time))
    timestamp = transaction_time if transaction_time else event_time
    first_update_id = int(data.get("U", 0))
    final_update_id = int(data.get("u", 0))
    previous_raw = data.get("pu")
    previous_final_update_id = int(previous_raw) if previous_raw is not None else None

    rows: list[ParsedRow] = []
    rows.extend(
        _parse_depth_levels(
            data.get("b"),
            side="bid",
            timestamp=timestamp,
            event_time=event_time,
            transaction_time=transaction_time,
            symbol=symbol,
            first_update_id=first_update_id,
            final_update_id=final_update_id,
            previous_final_update_id=previous_final_update_id,
        )
    )
    rows.extend(
        _parse_depth_levels(
            data.get("a"),
            side="ask",
            timestamp=timestamp,
            event_time=event_time,
            transaction_time=transaction_time,
            symbol=symbol,
            first_update_id=first_update_id,
            final_update_id=final_update_id,
            previous_final_update_id=previous_final_update_id,
        )
    )
    return rows


def parse_orderbook_snapshot_rows(data: dict[str, object]) -> list[ParsedRow]:
    symbol = str(data.get("symbol", ""))
    if symbol not in ORDERBOOK_SYMBOLS:
        return []

    timestamp = int(data["captured_at_ms"])
    last_update_id = int(data.get("lastUpdateId", 0))
    rows: list[ParsedRow] = []

    for side_name, levels_key in (("bid", "bids"), ("ask", "asks")):
        levels = data.get(levels_key, [])
        if not isinstance(levels, list):
            continue
        for level_index, level in enumerate(levels):
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            rows.append(
                {
                    "timestamp": timestamp,
                    "last_update_id": last_update_id,
                    "symbol": symbol,
                    "side": side_name,
                    "level_index": level_index,
                    "price": float(level[0]),
                    "quantity": float(level[1]),
                    "exchange": EXCHANGE,
                }
            )
    return rows


def parse_raw_lines(raw_line: str) -> list[tuple[EventCategory, ParsedRow]]:
    line = raw_line.strip()
    if not line:
        return []

    try:
        payload = unwrap_payload(line)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.warning("Skipping invalid JSON line: %s", exc)
        return []

    if not isinstance(payload, dict):
        logger.warning("Skipping non-dict payload: %r", type(payload))
        return []

    event_type = payload.get("e")
    if event_type == "depthUpdate":
        return [
            ("orderbook_diff", row)
            for row in parse_orderbook_diff_rows(payload)
        ]

    if "captured_at_ms" in payload and "bids" in payload and "asks" in payload:
        return [
            ("orderbook_snapshots", row)
            for row in parse_orderbook_snapshot_rows(payload)
        ]

    single = parse_raw_line(line)
    if single is None:
        return []
    category, row = single
    return [(category, row)]


def parse_raw_line(raw_line: str) -> tuple[EventCategory, ParsedRow] | None:
    line = raw_line.strip()
    if not line:
        return None

    try:
        normalized = normalize_raw_line(line)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.warning("Skipping invalid JSON line: %s", exc)
        return None

    if not isinstance(normalized, dict):
        logger.warning("Skipping non-dict normalized payload: %r", type(normalized))
        return None

    event_type = normalized.get("event_type")
    if event_type == "aggTrade":
        row = parse_trade_row(normalized)
        if row["symbol"] not in TARGET_SYMBOLS:
            return None
        return "trades", row

    if event_type == "forceOrder":
        row = parse_liquidation_row(normalized)
        if row["symbol"] not in TARGET_SYMBOLS:
            return None
        return "liquidations", row

    return None


def iter_jsonl_paths(raw_root: Path, category: EventCategory) -> list[Path]:
    return dataset_iter_jsonl_paths(raw_root, category)


def dry_run(raw_root: Path) -> dict[str, int]:
    stats = {
        "lines_read": 0,
        "trades_parsed": 0,
        "liquidations_parsed": 0,
        "orderbook_diff_parsed": 0,
        "orderbook_snapshots_parsed": 0,
        "skipped": 0,
        "errors": 0,
    }

    for category in ALL_CATEGORIES:
        for jsonl_path in iter_jsonl_paths(raw_root, category):
            for raw_line in jsonl_path.read_text(encoding="utf-8").splitlines():
                stats["lines_read"] += 1
                try:
                    parsed_rows = parse_raw_lines(raw_line)
                except (KeyError, TypeError, ValueError) as exc:
                    stats["errors"] += 1
                    logger.warning("Failed to parse line in %s: %s", jsonl_path, exc)
                    continue

                if not parsed_rows:
                    stats["skipped"] += 1
                    continue

                for parsed_category, _row in parsed_rows:
                    if parsed_category == "trades":
                        stats["trades_parsed"] += 1
                    elif parsed_category == "liquidations":
                        stats["liquidations_parsed"] += 1
                    elif parsed_category == "orderbook_diff":
                        stats["orderbook_diff_parsed"] += 1
                    else:
                        stats["orderbook_snapshots_parsed"] += 1

    return stats


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    project_root = Path(__file__).resolve().parent.parent
    raw_root = project_root / "raw" / BINANCE_FUTURES
    stats = dry_run(raw_root)

    logger.info("Dry-run complete for %s", raw_root)
    for key, value in stats.items():
        logger.info("%s: %s", key, value)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
