"""Tests for streaming raw JSONL parsing.

The reference is the pre-change loop (``read_text().splitlines()``) implemented
in ``_legacy_parse`` below; the streaming implementation must return the same
rows, with or without ``orjson`` installed. The only intended difference is
U+2028/U+0085 inside a JSON string, which ``splitlines`` treated as a line
break and streaming correctly keeps inside the row.
"""

from __future__ import annotations

import json
import math
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import json_codec
from normalizer.parser import parse_jsonl_file
from parse_polymarket import (
    OutcomeTokenRegistry,
    parse_polymarket_lines,
)
from parse_raw import parse_raw_lines

EXCHANGE = "binance_futures"
CATEGORY = "orderbook_diff"
POLYMARKET_EXCHANGE = "polymarket"


def _legacy_parse(
    jsonl_path: Path,
    *,
    exchange: str,
    category: str,
    token_registry: OutcomeTokenRegistry | None = None,
) -> list[dict[str, object]]:
    parser = parse_polymarket_lines if exchange == POLYMARKET_EXCHANGE else parse_raw_lines
    rows: list[dict[str, object]] = []
    for raw_line in jsonl_path.read_text(encoding="utf-8").splitlines():
        if exchange == POLYMARKET_EXCHANGE:
            parsed = parser(raw_line, token_registry=token_registry)
        else:
            parsed = parser(raw_line)
        for row_category, row in parsed:
            if row_category != category:
                continue
            rows.append(row)
    return rows


def _depth_line(*, event_time: int, first_update_id: int) -> str:
    return json.dumps(
        {
            "stream": "btcusdt@depth",
            "data": {
                "e": "depthUpdate",
                "E": event_time,
                "T": event_time + 1,
                "s": "BTCUSDT",
                "U": first_update_id,
                "u": first_update_id + 1,
                "b": [["68000.10", "0.5"]],
                "a": [["68001.20", "1.5"]],
            },
        }
    )


def _trade_line() -> str:
    return json.dumps(
        {
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "E": 1_762_000_002_000,
                "s": "BTCUSDT",
                "a": 100,
                "p": "68000.5",
                "q": "0.01",
                "f": 1,
                "l": 1,
                "T": 1_762_000_002_000,
                "m": True,
            },
        }
    )


LINES = [
    _depth_line(event_time=1_762_000_000_000, first_update_id=10),
    "",
    "   ",
    '{"stream": "btcusdt@depth", "data": ',
    _trade_line(),  # parses as a trade, filtered out of an orderbook_diff file
    _depth_line(event_time=1_762_000_001_000, first_update_id=20),
]


class ParserStreamingTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.root = Path(self._tmpdir.name)

    def _write(self, text: str) -> Path:
        path = self.root / "batch.jsonl"
        path.write_bytes(text.encode("utf-8"))
        return path

    def test_streaming_matches_splitlines(self) -> None:
        path = self._write("\n".join(LINES) + "\n")
        self.assertEqual(
            parse_jsonl_file(path, exchange=EXCHANGE, category=CATEGORY),
            _legacy_parse(path, exchange=EXCHANGE, category=CATEGORY),
        )

    def test_streaming_matches_splitlines_without_trailing_newline(self) -> None:
        path = self._write("\n".join(LINES))
        self.assertEqual(
            parse_jsonl_file(path, exchange=EXCHANGE, category=CATEGORY),
            _legacy_parse(path, exchange=EXCHANGE, category=CATEGORY),
        )

    def test_streaming_matches_splitlines_with_crlf(self) -> None:
        path = self._write("\r\n".join(LINES) + "\r\n")
        self.assertEqual(
            parse_jsonl_file(path, exchange=EXCHANGE, category=CATEGORY),
            _legacy_parse(path, exchange=EXCHANGE, category=CATEGORY),
        )

    def test_expected_rows(self) -> None:
        path = self._write("\n".join(LINES) + "\n")
        rows = parse_jsonl_file(path, exchange=EXCHANGE, category=CATEGORY)
        self.assertEqual(len(rows), 4)
        self.assertEqual([row["first_update_id"] for row in rows], [10, 10, 20, 20])
        self.assertEqual([row["side"] for row in rows], ["bid", "ask", "bid", "ask"])

    def test_empty_file_returns_no_rows(self) -> None:
        path = self._write("")
        self.assertEqual(parse_jsonl_file(path, exchange=EXCHANGE, category=CATEGORY), [])

    def test_orjson_fallback_matches_orjson(self) -> None:
        path = self._write("\n".join(LINES) + "\n")
        with_orjson = parse_jsonl_file(path, exchange=EXCHANGE, category=CATEGORY)

        self.assertIsNotNone(json_codec.orjson, "orjson is installed in this environment")
        with mock.patch.object(json_codec, "orjson", None):
            without_orjson = parse_jsonl_file(path, exchange=EXCHANGE, category=CATEGORY)

        self.assertEqual(without_orjson, with_orjson)

    def test_invalid_json_is_skipped_with_and_without_orjson(self) -> None:
        path = self._write(
            '{"broken": \n' + _depth_line(event_time=1, first_update_id=1) + "\n"
        )
        expected = parse_jsonl_file(path, exchange=EXCHANGE, category=CATEGORY)
        self.assertEqual(len(expected), 2)
        with mock.patch.object(json_codec, "orjson", None):
            self.assertEqual(
                parse_jsonl_file(path, exchange=EXCHANGE, category=CATEGORY), expected
            )

    def test_stdlib_only_json_extensions_still_parse(self) -> None:
        """orjson is strict JSON; lines with NaN/Infinity must not be lost."""
        payload = json_codec.loads('{"value": NaN, "other": Infinity}')
        self.assertTrue(math.isnan(payload["value"]))
        self.assertEqual(payload["other"], float("inf"))

        with mock.patch.object(json_codec, "orjson", None):
            self.assertEqual(
                json_codec.loads('{"value": 1.5}'), {"value": 1.5}
            )

    def test_unicode_line_separators_stay_inside_one_json_string(self) -> None:
        """Streaming splits only on newlines, unlike ``str.splitlines``.

        This is the one intentional parser difference: U+2028/U+0085 inside a
        JSON string are no longer treated as line breaks, so the row survives.
        """
        payload = _depth_line(event_time=1_762_000_000_000, first_update_id=10)
        payload = payload.replace('"btcusdt@depth"', '"btcusdt@depth\u2028\u0085x"')
        path = self._write(payload + "\n")

        rows = parse_jsonl_file(path, exchange=EXCHANGE, category=CATEGORY)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["symbol"], "BTCUSDT")
        self.assertEqual(_legacy_parse(path, exchange=EXCHANGE, category=CATEGORY), [])


POLYMARKET_METADATA = {
    "event_type": "new_market",
    "slug": "btc-updown-15m-1762000000",
    "condition_id": "0xcondition",
    "question": "BTC Up or Down 15m?",
    "end_date": "2026-11-01T12:15:00Z",
    "yes_token_id": "0xyes",
    "no_token_id": "0xno",
    "timestamp": 1_762_000_000_000,
}
POLYMARKET_PRICE = {
    "event_type": "price_change",
    "timestamp": 1_762_000_001_500,
    "price_changes": [
        {
            "asset_id": "0xyes",
            "best_bid": "0.42",
            "best_ask": "0.45",
            "spread": "0.03",
            "price": "0.43",
            "side": "buy",
        },
        {
            "asset_id": "0xno",
            "best_bid": "0.55",
            "best_ask": "0.58",
            "spread": "0.03",
            "price": "0.56",
            "side": "sell",
        },
    ],
}
POLYMARKET_BEST_BID_ASK = {
    "event_type": "best_bid_ask",
    "timestamp": 1_762_000_002_000,
    "asset_id": "0xyes",
    "best_bid": "0.44",
    "best_ask": "0.46",
    "spread": "0.02",
}
POLYMARKET_TRADE = {
    "event_type": "last_trade_price",
    "timestamp": 1_762_000_003_000,
    "asset_id": "0xno",
    "price": "0.57",
    "side": "BUY",
    "size": "12.5",
}
POLYMARKET_BOOK = {
    "event_type": "book",
    "timestamp": 1_762_000_004_000,
    "asset_id": "0xyes",
    "bids": [
        {"price": "0.41", "size": "100"},
        {"price": "0.40", "size": "250"},
    ],
    "asks": [{"price": "0.47", "size": "80"}],
}
POLYMARKET_LINES = [
    json.dumps(POLYMARKET_METADATA),
    "",
    '{"broken": ',
    json.dumps(POLYMARKET_PRICE),
    json.dumps(POLYMARKET_BEST_BID_ASK),
    json.dumps(POLYMARKET_TRADE),
    json.dumps(POLYMARKET_BOOK),
    json.dumps({"event_type": "unknown_event"}),
]


class PolymarketParserParityTest(unittest.TestCase):
    """The Polymarket path must stream to exactly the old ``splitlines`` rows."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.root = Path(self._tmpdir.name)
        self.path = self.root / "polymarket.jsonl"
        self.path.write_text("\n".join(POLYMARKET_LINES) + "\n", encoding="utf-8")

    def test_each_category_matches_splitlines(self) -> None:
        categories = (
            "prediction_metadata",
            "prediction_price",
            "prediction_trades",
            "prediction_orderbook",
        )
        for category in categories:
            with self.subTest(category=category):
                new_registry = OutcomeTokenRegistry()
                legacy_registry = OutcomeTokenRegistry()

                streamed = parse_jsonl_file(
                    self.path,
                    exchange=POLYMARKET_EXCHANGE,
                    category=category,
                    token_registry=new_registry,
                )
                legacy = _legacy_parse(
                    self.path,
                    exchange=POLYMARKET_EXCHANGE,
                    category=category,
                    token_registry=legacy_registry,
                )

                self.assertTrue(streamed, f"{category} fixture produced no rows")
                self.assertEqual(streamed, legacy)
                self.assertEqual(new_registry.to_mapping(), legacy_registry.to_mapping())

    def test_price_rows_use_the_seeded_token_registry(self) -> None:
        registry = OutcomeTokenRegistry()
        rows = parse_jsonl_file(
            self.path,
            exchange=POLYMARKET_EXCHANGE,
            category="prediction_price",
            token_registry=registry,
        )
        self.assertEqual(
            [(row["asset_id"], row["outcome"]) for row in rows],
            [("0xyes", "Up"), ("0xno", "Down"), ("0xyes", "Up")],
        )



if __name__ == "__main__":
    unittest.main()
