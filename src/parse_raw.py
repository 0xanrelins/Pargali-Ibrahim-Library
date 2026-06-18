"""Parse raw Binance futures JSONL lines into normalized row dicts (Plan 2)."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Literal

from unicorn_fy import UnicornFy

logger = logging.getLogger(__name__)

EXCHANGE = "binance_futures"
TARGET_SYMBOLS = frozenset({
    "BTCUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ETHUSDT",
    "HYPEUSDT",
    "BNBUSDT",
})

EventCategory = Literal["trades", "liquidations"]
ParsedRow = dict[str, object]


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
    category_dir = raw_root / category
    if not category_dir.exists():
        return []
    return sorted(category_dir.glob("*/*.jsonl"))


def dry_run(raw_root: Path) -> dict[str, int]:
    stats = {
        "lines_read": 0,
        "trades_parsed": 0,
        "liquidations_parsed": 0,
        "skipped": 0,
        "errors": 0,
    }

    for category in ("trades", "liquidations"):
        for jsonl_path in iter_jsonl_paths(raw_root, category):
            for raw_line in jsonl_path.read_text(encoding="utf-8").splitlines():
                stats["lines_read"] += 1
                try:
                    parsed = parse_raw_line(raw_line)
                except (KeyError, TypeError, ValueError) as exc:
                    stats["errors"] += 1
                    logger.warning("Failed to parse line in %s: %s", jsonl_path, exc)
                    continue

                if parsed is None:
                    stats["skipped"] += 1
                    continue

                parsed_category, _row = parsed
                if parsed_category == "trades":
                    stats["trades_parsed"] += 1
                else:
                    stats["liquidations_parsed"] += 1

    return stats


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    project_root = Path(__file__).resolve().parent.parent
    raw_root = project_root / "raw" / "binance_futures"
    stats = dry_run(raw_root)

    logger.info("Dry-run complete for %s", raw_root)
    for key, value in stats.items():
        logger.info("%s: %s", key, value)

    sample_trade = parse_raw_line(
        '{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"a":1,"s":"BTCUSDT",'
        '"p":"1.0","q":"2","nq":"2","f":1,"l":1,"T":1000,"m":true}}'
    )
    sample_liq = parse_raw_line(
        '{"e":"forceOrder","E":2000,"o":{"s":"DOGEUSDT","S":"SELL","o":"LIMIT","f":"IOC",'
        '"q":"10","p":"0.1","ap":"0.09","X":"FILLED","l":"10","z":"10","T":1999}}'
    )
    logger.info("Sample trade row: %s", sample_trade)
    logger.info("Sample liquidation row: %s", sample_liq)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
