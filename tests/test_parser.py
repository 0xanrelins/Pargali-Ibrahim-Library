"""Tests for streaming raw JSONL parsing.

The reference is the pre-change loop (``read_text().splitlines()``) implemented
in ``_legacy_parse`` below; the streaming implementation must return exactly the
same rows, with or without ``orjson`` installed.
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
from parse_raw import parse_raw_lines

EXCHANGE = "binance_futures"
CATEGORY = "orderbook_diff"


def _legacy_parse(jsonl_path: Path, *, exchange: str, category: str) -> list[dict[str, object]]:
    parser = parse_raw_lines
    rows: list[dict[str, object]] = []
    for raw_line in jsonl_path.read_text(encoding="utf-8").splitlines():
        for row_category, row in parser(raw_line):
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



if __name__ == "__main__":
    unittest.main()
