"""Tests for the Arrow parquet batch writer.

Parity is asserted against the pre-change writer vendored verbatim in
``tests/legacy_duckdb_writer.py``: a reusable in-memory DuckDB session,
``row.get(column)`` column building, the ``pa.Table.from_pylist`` fast path
against the fixed schema, typed ``INSERT ... SELECT`` with DuckDB ``TRY_CAST``
and ``COPY ... COMPRESSION ZSTD`` into a ``.tmp`` file plus ``os.replace``.
DuckDB stays the reference for cast semantics; the writer itself no longer
uses it.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from legacy_duckdb_writer import close_batch_writer as legacy_close_batch_writer
from legacy_duckdb_writer import write_parquet_batch as legacy_write_parquet_batch

from normalizer import writer as writer_module
from normalizer.parser import parse_jsonl_file
from normalizer.schemas import CATEGORY_SPECS, category_column_specs
from normalizer.writer import (
    PARQUET_COMPRESSION,
    ParquetBatchWriter,
    arrow_schema,
    close_batch_writer,
    get_batch_writer,
    rows_to_arrow,
    write_parquet_batch,
)

PREDICTION_PRICE = "prediction_price"
ORDERBOOK_DIFF = "orderbook_diff"
TRADES = "trades"


def _price_row(index: int) -> dict[str, object]:
    return {
        "timestamp": 1_762_000_000_000 + index,
        "symbol": "BTC_15M",
        "event_type": "price_change",
        "asset_id": f"0x{index:04x}",
        "outcome": "Up" if index % 2 else "Down",
        "best_bid": 0.42 + index / 10_000,
        "best_ask": 0.45 + index / 10_000,
        "spread": 0.03,
        "price": None if index % 3 == 0 else 0.43 + index / 10_000,
        "side": "buy" if index % 2 else "sell",
        "exchange": "polymarket",
    }


def _dense_rows(count: int = 250) -> list[dict[str, object]]:
    return [_price_row(index) for index in range(count)]


def _sparse_rows() -> list[dict[str, object]]:
    """Missing columns, extra keys, ints where the contract says DOUBLE."""
    rows = _dense_rows(4)
    del rows[0]["best_ask"]
    del rows[0]["spread"]
    rows[1].pop("price")
    rows[2]["unknown_extra_column"] = "ignored"
    rows[3]["best_bid"] = 1  # int into a DOUBLE column
    rows.append(
        {
            "timestamp": 1_762_000_000_500.0,  # float into a BIGINT column
            "symbol": "BTC_15M",
            "event_type": "best_bid_ask",
            "asset_id": None,
            "outcome": None,
            "best_bid": None,
            "best_ask": None,
            "spread": None,
            "price": None,
            "side": None,
            "exchange": "polymarket",
        }
    )
    return rows


def _stringy_rows() -> list[dict[str, object]]:
    """Numeric values as strings: exercises the TRY_CAST-equivalent coercion."""
    rows = _dense_rows(5)
    for row in rows:
        for column, column_type in category_column_specs(PREDICTION_PRICE):
            value = row.get(column)
            if column_type != "VARCHAR" and value is not None:
                row[column] = str(value)
    return rows


def _trade_rows(count: int = 5) -> list[dict[str, object]]:
    return [
        {
            "timestamp": 1_762_000_000_000 + index,
            "symbol": "BTCUSDT",
            "agg_trade_id": index,
            "price": 68_000.5 + index,
            "amount": 0.01 * (index + 1),
            "side": "BUY",
            "exchange": "binance_futures",
        }
        for index in range(count)
    ]


def _depth_update_line(
    *,
    event_time: int,
    transaction_time: int,
    first_update_id: int,
    final_update_id: int,
    previous_final_update_id: int | None,
    bids: list[list[str]],
    asks: list[list[str]],
) -> str:
    data: dict[str, object] = {
        "e": "depthUpdate",
        "E": event_time,
        "T": transaction_time,
        "s": "BTCUSDT",
        "U": first_update_id,
        "u": final_update_id,
        "b": bids,
        "a": asks,
    }
    if previous_final_update_id is not None:
        data["pu"] = previous_final_update_id
    return json.dumps({"stream": "btcusdt@depth", "data": data})


def _trade_line() -> str:
    data = {
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
    }
    return json.dumps({"stream": "btcusdt@aggTrade", "data": data})


class WriterTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.root = Path(self._tmpdir.name)
        close_batch_writer()
        self.addCleanup(close_batch_writer)
        self.addCleanup(legacy_close_batch_writer)
        self._con = duckdb.connect(":memory:")
        self.addCleanup(self._con.close)

    def describe(self, path: Path) -> list[tuple[str, str]]:
        rows = self._con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
        return [(name, column_type) for name, column_type, *_rest in rows]

    def count(self, path: Path) -> int:
        return self._con.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(path)]
        ).fetchone()[0]

    def except_all(self, left: Path, right: Path, column: str | None = None) -> int:
        projection = f'"{column}"' if column is not None else "*"
        return self._con.execute(
            f"SELECT count(*) FROM (SELECT {projection} FROM read_parquet(?) "
            f"EXCEPT ALL SELECT {projection} FROM read_parquet(?))",
            [str(left), str(right)],
        ).fetchone()[0]

    def assert_rows_identical(self, left: Path, right: Path) -> None:
        self.assertEqual(self.except_all(left, right), 0, f"{left.name} has rows {right.name} lacks")
        self.assertEqual(self.except_all(right, left), 0, f"{right.name} has rows {left.name} lacks")

    def assert_matches_legacy(self, rows: list[dict[str, object]], category: str) -> None:
        legacy_path = self.root / f"legacy_{category}.parquet"
        new_path = self.root / f"new_{category}.parquet"

        self.assertEqual(legacy_write_parquet_batch(legacy_path, rows, category), len(rows))
        self.assertEqual(write_parquet_batch(new_path, rows, category), len(rows))

        self.assertEqual(
            self.describe(new_path),
            list(category_column_specs(category)),
            "written schema must match the category contract",
        )
        self.assertEqual(self.describe(legacy_path), self.describe(new_path))
        self.assertEqual(self.count(legacy_path), self.count(new_path))
        self.assertEqual(self.count(new_path), len(rows))
        self.assert_rows_identical(legacy_path, new_path)
        for column, _type in category_column_specs(category):
            self.assertEqual(
                self.except_all(legacy_path, new_path, column),
                0,
                f"column {column} differs from the legacy writer",
            )


class WriterParityTest(WriterTestBase):
    """The new writer must produce schema- and value-identical batches."""

    def test_parity_dense_rows(self) -> None:
        self.assert_matches_legacy(_dense_rows(), PREDICTION_PRICE)

    def test_parity_sparse_and_extra_keys(self) -> None:
        self.assert_matches_legacy(_sparse_rows(), PREDICTION_PRICE)

    def test_parity_numeric_strings_use_coercion(self) -> None:
        self.assert_matches_legacy(_stringy_rows(), PREDICTION_PRICE)

    def test_parity_other_category(self) -> None:
        self.assert_matches_legacy(_trade_rows(), TRADES)

    def test_empty_batch_matches_legacy(self) -> None:
        for category in (TRADES, PREDICTION_PRICE):
            with self.subTest(category=category):
                legacy_path = self.root / f"empty_legacy_{category}.parquet"
                new_path = self.root / f"empty_new_{category}.parquet"
                self.assertEqual(
                    legacy_write_parquet_batch(legacy_path, [], category), 0
                )
                self.assertEqual(write_parquet_batch(new_path, [], category), 0)
                self.assertTrue(new_path.exists())
                self.assertEqual(self.describe(legacy_path), self.describe(new_path))
                self.assertEqual(self.describe(new_path), list(category_column_specs(category)))
                self.assertEqual(self.count(new_path), 0)

    def test_all_categories_write_empty_batches(self) -> None:
        for category in CATEGORY_SPECS:
            with self.subTest(category=category):
                path = self.root / f"contract_{category}.parquet"
                self.assertEqual(write_parquet_batch(path, [], category), 0)
                self.assertEqual(self.describe(path), list(category_column_specs(category)))


class WriterTryCastTest(WriterTestBase):
    """Uncastable values become NULL, castable values are converted, like TRY_CAST."""

    def test_numeric_strings_are_cast_to_numbers(self) -> None:
        rows = [_price_row(0)]
        rows[0]["timestamp"] = "1762000000123"
        rows[0]["best_bid"] = "0.4321"
        rows[0]["price"] = "1e3"
        self.assert_matches_legacy(rows, PREDICTION_PRICE)

    def test_invalid_numeric_strings_become_null(self) -> None:
        rows = [_price_row(0), _price_row(1)]
        rows[0]["timestamp"] = "not-a-number"
        rows[1]["timestamp"] = "12.5"
        table = rows_to_arrow(rows, PREDICTION_PRICE)
        self.assertEqual(table.column("timestamp").to_pylist(), [None, 13])
        self.assert_matches_legacy(rows, PREDICTION_PRICE)

    def test_floats_in_integer_columns_truncate_like_the_old_arrow_path(self) -> None:
        """The old writer's ``from_pylist`` truncated floats before DuckDB saw them."""
        rows = _dense_rows(5)
        rows[0]["timestamp"] = 2.5
        rows[1]["timestamp"] = -2.5
        rows[2]["timestamp"] = 1.5
        rows[3]["timestamp"] = 2.6
        rows[4]["timestamp"] = 1_762_000_000_000.0
        table = rows_to_arrow(rows, PREDICTION_PRICE)
        self.assertEqual(
            table.column("timestamp").to_pylist(),
            [2, -2, 1, 2, 1_762_000_000_000],
        )
        self.assert_matches_legacy(rows, PREDICTION_PRICE)

    def test_out_of_range_floats_in_integer_columns_become_null(self) -> None:
        rows = _dense_rows(4)
        rows[0]["timestamp"] = 9.223372036854776e18  # int64 max + 1 as a float
        rows[1]["timestamp"] = 1e30
        rows[2]["timestamp"] = -1e30
        rows[3]["timestamp"] = 1_762_000_000_000.0
        table = rows_to_arrow(rows, PREDICTION_PRICE)
        self.assertEqual(
            table.column("timestamp").to_pylist(),
            [None, None, None, 1_762_000_000_000],
        )
        self.assert_matches_legacy(rows, PREDICTION_PRICE)

    def test_nan_and_infinity_in_integer_columns_become_null(self) -> None:
        rows = _dense_rows(3)
        rows[0]["timestamp"] = float("nan")
        rows[1]["timestamp"] = float("inf")
        rows[2]["timestamp"] = 1_762_000_000_000.0
        table = rows_to_arrow(rows, PREDICTION_PRICE)
        self.assertEqual(table.column("timestamp").to_pylist(), [None, None, 1_762_000_000_000])
        self.assert_matches_legacy(rows, PREDICTION_PRICE)

    def test_invalid_strings_in_double_columns_become_null(self) -> None:
        rows = [_price_row(0), _price_row(1)]
        rows[0]["price"] = "nope"
        rows[1]["price"] = " 0.25 "
        table = rows_to_arrow(rows, PREDICTION_PRICE)
        self.assertEqual(table.column("price").to_pylist(), [None, 0.25])
        self.assert_matches_legacy(rows, PREDICTION_PRICE)

    def test_non_strings_are_rendered_like_duckdb_varchar(self) -> None:
        rows = _dense_rows(3)
        for index, row in enumerate(rows):
            row["side"] = index  # int -> '0', '1', '2'
            row["exchange"] = index % 2 == 0  # bool -> 'true', 'false'
        table = rows_to_arrow(rows, PREDICTION_PRICE)
        self.assertEqual(table.column("side").to_pylist(), ["0", "1", "2"])
        self.assertEqual(table.column("exchange").to_pylist(), ["true", "false", "true"])
        self.assert_matches_legacy(rows, PREDICTION_PRICE)

    def test_integer_overflow_fails_the_batch_like_the_old_writer(self) -> None:
        """A Python int outside int64 must raise, not silently become NULL.

        The old ``from_pylist`` fast path raised ``OverflowError``, so the batch
        failed and the raw file was never marked processed.
        """
        for column in ("timestamp", "best_bid"):
            for value in (2**63, -(2**63) - 1, 2**70, -(2**70)):
                with self.subTest(column=column, value=value):
                    rows = [_price_row(0)]
                    rows[0][column] = value
                    new_path = self.root / f"overflow_new_{column}.parquet"
                    legacy_path = self.root / f"overflow_legacy_{column}.parquet"

                    with self.assertRaises(OverflowError):
                        write_parquet_batch(new_path, rows, PREDICTION_PRICE)
                    with self.assertRaises(OverflowError):
                        legacy_write_parquet_batch(
                            legacy_path, rows, PREDICTION_PRICE
                        )

                    self.assertFalse(new_path.exists())
                    self.assertFalse(
                        new_path.with_name(f"{new_path.name}.tmp").exists()
                    )

    def test_string_grammar_matches_duckdb_for_flagged_forms(self) -> None:
        """Signed hex and Unicode digits are NULL, as in DuckDB's TRY_CAST."""
        rows = _dense_rows(4)
        rows[0]["timestamp"] = "-0x10"
        rows[1]["timestamp"] = "+0x10"
        rows[2]["timestamp"] = "\u0661\u0662\u0663"  # Arabic-Indic digits
        rows[3]["timestamp"] = "\uff11\uff12\uff13"  # full-width digits
        table = rows_to_arrow(rows, PREDICTION_PRICE)
        self.assertEqual(table.column("timestamp").to_pylist(), [None] * 4)
        self.assert_matches_legacy(rows, PREDICTION_PRICE)

    def test_ascii_decimal_and_exponent_strings_are_cast(self) -> None:
        values = ["123", " 42 ", "+3", "-3", "00123", "12.5", "-12.5", ".5", "5.", "1e3", "1.5e3", "1.e3"]
        rows = _dense_rows(len(values))
        for row, value in zip(rows, values, strict=True):
            row["timestamp"] = value
        table = rows_to_arrow(rows, PREDICTION_PRICE)
        self.assertEqual(
            table.column("timestamp").to_pylist(),
            [123, 42, 3, -3, 123, 13, -13, 1, 5, 1000, 1500, 1000],
        )
        self.assert_matches_legacy(rows, PREDICTION_PRICE)

    def test_unsigned_hex_and_underscores_are_intentionally_stricter(self) -> None:
        """DuckDB accepts these; the new grammar only accepts ASCII decimal/exponent."""
        rows = _dense_rows(3)
        rows[0]["timestamp"] = "0x10"
        rows[1]["timestamp"] = " 0x10"
        rows[2]["timestamp"] = "1_000"
        table = rows_to_arrow(rows, PREDICTION_PRICE)
        self.assertEqual(table.column("timestamp").to_pylist(), [None, None, None])

        legacy_path = self.root / "stricter_legacy.parquet"
        legacy_write_parquet_batch(legacy_path, rows, PREDICTION_PRICE)
        self.assertEqual(
            pq.read_table(legacy_path).column("timestamp").to_pylist(),
            [16, 16, 1000],
            "documents the intentional difference from DuckDB's VARCHAR cast",
        )

    def test_rows_to_arrow_always_uses_the_contract_schema(self) -> None:
        table = rows_to_arrow(_stringy_rows(), PREDICTION_PRICE)
        self.assertEqual(table.schema, arrow_schema(PREDICTION_PRICE))


MATRIX_INT_VALUES: tuple[object, ...] = (
    None,
    True,
    False,
    0,
    1,
    -1,
    2**62,
    -(2**62),
    2**63 - 1,
    -(2**63),
    2**63,
    -(2**63) - 1,
    2**70,
    -(2**70),
    0.0,
    2.5,
    -2.5,
    1.5,
    2.6,
    -1.5,
    -1.9,
    1.9,
    1e18,
    9.223372036854776e18,
    9.3e18,
    1e30,
    -1e30,
    float("nan"),
    float("inf"),
    float("-inf"),
    "123",
    " 42 ",
    "+3",
    "-3",
    "+0",
    "-0",
    "000",
    "12.5",
    "-12.5",
    "+12.5",
    "1e3",
    "-1e3",
    "1.5e3",
    "00123",
    "",
    "   ",
    "not-a-number",
    "-0x10",
    "+0x10",
    "\u0661\u0662\u0663",
    "\uff11\uff12\uff13",
    "9223372036854775807",
    "9223372036854775808",
    "-9223372036854775809",
    "1e400",
    "inf",
    "nan",
    "\t7\n",
)

MATRIX_FLOAT_VALUES: tuple[object, ...] = (
    None,
    True,
    False,
    0,
    1,
    -1,
    2**70,
    -(2**70),
    0.0,
    0.5,
    1.5,
    -1.5,
    1e308,
    float("nan"),
    float("inf"),
    float("-inf"),
    "-0.0",
    "0.5",
    " 0.25 ",
    ".5",
    "5.",
    "+1.5",
    "1e3",
    "1e400",
    "nope",
    "inf",
    "nan",
    "-inf",
    "-0x10",
    "+0x10",
    "\u0661\u0662\u0663",
    "\uff11\uff12\uff13",
    "",
    "   ",
)

MATRIX_STRING_VALUES: tuple[object, ...] = (
    None,
    True,
    False,
    0,
    1,
    -5,
    2**70,
    0.5,
    1.0,
    1e30,
    float("nan"),
    float("inf"),
    -0.0,
    "",
    "buy",
    "SELL",
    "\u0661\u0662\u0663",
    "\uff11\uff12\uff13",
    "  spaced  ",
    "0x10",
)


class WriterCastMatrixTest(WriterTestBase):
    """One row per value: the vendored old writer and the new writer must agree.

    Exceptions compare by type (both fail the batch) and values compare through
    ``repr`` so NaN and ``-0.0`` are compared meaningfully. Unsigned hex and
    underscore strings are *not* in this matrix: the new ASCII-only grammar is
    intentionally stricter than DuckDB there (covered by
    ``WriterTryCastTest.test_unsigned_hex_and_underscores_are_intentionally_stricter``).
    """

    def _written_value(self, path: Path, column: str) -> str:
        return repr(pq.read_table(path).column(column).to_pylist()[0])

    def _result(
        self,
        write: object,
        path: Path,
        row: dict[str, object],
        column: str,
    ) -> tuple[str, str]:
        try:
            write(path, [row], PREDICTION_PRICE)  # type: ignore[operator]
        except Exception as exc:  # noqa: BLE001 - the failure mode is the assertion
            return ("error", type(exc).__name__)
        return ("value", self._written_value(path, column))

    def test_cast_matrix_matches_the_vendored_old_writer(self) -> None:
        cases = (
            [("timestamp", value) for value in MATRIX_INT_VALUES]
            + [("best_bid", value) for value in MATRIX_FLOAT_VALUES]
            + [("side", value) for value in MATRIX_STRING_VALUES]
        )
        self.assertGreaterEqual(len(cases), 109)

        differences: list[tuple[str, object, tuple[str, str], tuple[str, str]]] = []
        for index, (column, value) in enumerate(cases):
            row = _price_row(index)
            row[column] = value
            legacy = self._result(
                legacy_write_parquet_batch,
                self.root / f"matrix_legacy_{index}.parquet",
                row,
                column,
            )
            current = self._result(
                write_parquet_batch,
                self.root / f"matrix_new_{index}.parquet",
                row,
                column,
            )
            if legacy != current:
                differences.append((column, value, legacy, current))

        self.assertEqual(differences, [], "cast behaviour differs from the old writer")

class WriterFormatTest(WriterTestBase):
    def test_batch_is_zstd_compressed(self) -> None:
        path = self.root / "compressed.parquet"
        write_parquet_batch(path, _dense_rows(50), PREDICTION_PRICE)
        compressions = {
            row[0]
            for row in self._con.execute(
                "SELECT DISTINCT compression FROM parquet_metadata(?)", [str(path)]
            ).fetchall()
        }
        self.assertEqual(compressions, {PARQUET_COMPRESSION.upper()})

    def test_parent_directories_are_created(self) -> None:
        path = self.root / "data" / "prediction_price" / "polymarket" / "BTC_15M" / "01-02-03.parquet"
        write_parquet_batch(path, _dense_rows(3), PREDICTION_PRICE)
        self.assertTrue(path.exists())
        self.assertEqual(self.count(path), 3)

    def test_rewriting_same_path_replaces_contents(self) -> None:
        path = self.root / "batch.parquet"
        write_parquet_batch(path, _dense_rows(10), PREDICTION_PRICE)
        write_parquet_batch(path, _dense_rows(4), PREDICTION_PRICE)
        self.assertEqual(self.count(path), 4)
        self.assertFalse(path.with_name(f"{path.name}.tmp").exists())

    def test_stale_tmp_file_is_replaced(self) -> None:
        path = self.root / "batch.parquet"
        tmp_path = path.with_name(f"{path.name}.tmp")
        tmp_path.write_bytes(b"stale partial batch")
        write_parquet_batch(path, _dense_rows(3), PREDICTION_PRICE)
        self.assertEqual(self.count(path), 3)
        self.assertFalse(tmp_path.exists())

    def test_rows_to_arrow_uses_contract_schema(self) -> None:
        table = rows_to_arrow(_dense_rows(10), PREDICTION_PRICE)
        self.assertEqual(table.schema, arrow_schema(PREDICTION_PRICE))
        self.assertEqual(table.num_rows, 10)

    def test_writer_module_does_not_import_duckdb(self) -> None:
        self.assertFalse(hasattr(writer_module, "duckdb"))


class WriterRawJsonlTest(WriterTestBase):
    """End-to-end: one raw JSONL batch → parquet with exact schema and values."""

    def _write_raw_file(self) -> Path:
        raw_path = self.root / "raw" / "orderbook_diff" / "11-55-05.jsonl"
        raw_path.parent.mkdir(parents=True)
        lines = [
            _depth_update_line(
                event_time=1_762_000_000_000,
                transaction_time=1_762_000_000_005,
                first_update_id=10,
                final_update_id=12,
                previous_final_update_id=9,
                bids=[["68000.10", "0.500"]],
                asks=[["68001.20", "1.500"]],
            ),
            "",
            '{"stream": "btcusdt@depth", "data": ',
            _trade_line(),
            _depth_update_line(
                event_time=1_762_000_001_000,
                transaction_time=1_762_000_001_000,
                first_update_id=13,
                final_update_id=14,
                previous_final_update_id=None,
                bids=[["67999.00", "0.250"]],
                asks=[],
            ),
        ]
        raw_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return raw_path

    def test_raw_batch_is_normalized_exactly(self) -> None:
        raw_path = self._write_raw_file()
        rows = parse_jsonl_file(raw_path, exchange="binance_futures", category=ORDERBOOK_DIFF)
        self.assertEqual(len(rows), 3, "blank, invalid and wrong-category lines are skipped")

        out_path = self.root / "out" / "11-55-05.parquet"
        self.assertEqual(write_parquet_batch(out_path, rows, ORDERBOOK_DIFF), 3)

        table = pq.read_table(out_path)
        self.assertEqual(table.schema, arrow_schema(ORDERBOOK_DIFF))
        self.assertEqual(
            table.schema.types,
            [
                pa.int64(),
                pa.int64(),
                pa.int64(),
                pa.string(),
                pa.int64(),
                pa.int64(),
                pa.int64(),
                pa.string(),
                pa.float64(),
                pa.float64(),
                pa.string(),
            ],
        )
        self.assertEqual(
            table.to_pylist(),
            [
                {
                    "timestamp": 1_762_000_000_005,
                    "event_time": 1_762_000_000_000,
                    "transaction_time": 1_762_000_000_005,
                    "symbol": "BTCUSDT",
                    "first_update_id": 10,
                    "final_update_id": 12,
                    "previous_final_update_id": 9,
                    "side": "bid",
                    "price": 68_000.10,
                    "quantity": 0.500,
                    "exchange": "binance_futures",
                },
                {
                    "timestamp": 1_762_000_000_005,
                    "event_time": 1_762_000_000_000,
                    "transaction_time": 1_762_000_000_005,
                    "symbol": "BTCUSDT",
                    "first_update_id": 10,
                    "final_update_id": 12,
                    "previous_final_update_id": 9,
                    "side": "ask",
                    "price": 68_001.20,
                    "quantity": 1.500,
                    "exchange": "binance_futures",
                },
                {
                    "timestamp": 1_762_000_001_000,
                    "event_time": 1_762_000_001_000,
                    "transaction_time": 1_762_000_001_000,
                    "symbol": "BTCUSDT",
                    "first_update_id": 13,
                    "final_update_id": 14,
                    "previous_final_update_id": None,
                    "side": "bid",
                    "price": 67_999.00,
                    "quantity": 0.250,
                    "exchange": "binance_futures",
                },
            ],
        )

    def test_empty_raw_file_keeps_the_contract_schema(self) -> None:
        raw_path = self.root / "raw" / "orderbook_diff" / "empty.jsonl"
        raw_path.parent.mkdir(parents=True)
        raw_path.write_text("\n\n", encoding="utf-8")
        rows = parse_jsonl_file(raw_path, exchange="binance_futures", category=ORDERBOOK_DIFF)
        out_path = self.root / "out" / "empty.parquet"
        self.assertEqual(write_parquet_batch(out_path, rows, ORDERBOOK_DIFF), 0)
        table = pq.read_table(out_path)
        self.assertEqual(table.schema, arrow_schema(ORDERBOOK_DIFF))
        self.assertEqual(table.num_rows, 0)


class WriterSessionTest(WriterTestBase):
    """One writer instance must serve many batches without state leaking between them."""

    def test_single_session_writes_many_batches(self) -> None:
        paths = [self.root / f"batch_{index}.parquet" for index in range(3)]
        writer = ParquetBatchWriter()
        with writer:
            for index, path in enumerate(paths):
                self.assertEqual(
                    writer.write_batch(path, _dense_rows(index + 1), PREDICTION_PRICE),
                    index + 1,
                )

        for index, path in enumerate(paths):
            self.assertEqual(self.count(path), index + 1)
            self.assertEqual(
                self._con.execute(
                    "SELECT min(timestamp), max(timestamp) FROM read_parquet(?)", [str(path)]
                ).fetchone(),
                (1_762_000_000_000, 1_762_000_000_000 + index),
            )

    def test_single_session_serves_multiple_categories(self) -> None:
        first = self.root / "price_1.parquet"
        trades = self.root / "trades_1.parquet"
        second = self.root / "price_2.parquet"

        writer = ParquetBatchWriter()
        with writer:
            writer.write_batch(first, _dense_rows(2), PREDICTION_PRICE)
            writer.write_batch(trades, _trade_rows(7), TRADES)
            writer.write_batch(second, _dense_rows(3), PREDICTION_PRICE)

        self.assertEqual(self.describe(first), self.describe(second))
        self.assertEqual(self.describe(trades), list(category_column_specs(TRADES)))
        self.assertEqual((self.count(first), self.count(trades), self.count(second)), (2, 7, 3))
        self.assertEqual(
            self._con.execute(
                "SELECT count(DISTINCT symbol) FROM read_parquet(?)", [str(trades)]
            ).fetchone()[0],
            1,
        )

    def test_empty_batch_between_batches_does_not_leak_rows(self) -> None:
        writer = ParquetBatchWriter()
        with writer:
            writer.write_batch(self.root / "filled.parquet", _dense_rows(5), PREDICTION_PRICE)
            writer.write_batch(self.root / "empty.parquet", [], PREDICTION_PRICE)
            writer.write_batch(self.root / "refilled.parquet", _dense_rows(2), PREDICTION_PRICE)

        self.assertEqual(self.count(self.root / "filled.parquet"), 5)
        self.assertEqual(self.count(self.root / "empty.parquet"), 0)
        self.assertEqual(self.count(self.root / "refilled.parquet"), 2)

    def test_default_writer_is_shared_and_closable(self) -> None:
        self.assertIs(get_batch_writer(), get_batch_writer())
        writer = get_batch_writer()
        close_batch_writer()
        with self.assertRaises(RuntimeError):
            writer.write_batch(self.root / "closed.parquet", _dense_rows(1), PREDICTION_PRICE)

    def test_default_writer_is_recreated_after_close(self) -> None:
        first = get_batch_writer()
        close_batch_writer()
        self.assertIsNot(first, get_batch_writer())


class WriterAtomicityTest(WriterTestBase):
    """A failed write must leave the previously published batch untouched."""

    def test_failed_replace_keeps_previous_batch_and_cleans_tmp(self) -> None:
        path = self.root / "batch.parquet"
        write_parquet_batch(path, _dense_rows(6), PREDICTION_PRICE)
        original = path.read_bytes()

        with mock.patch.object(writer_module.os, "replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                write_parquet_batch(path, _dense_rows(2), PREDICTION_PRICE)

        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(self.count(path), 6)
        self.assertFalse(path.with_name(f"{path.name}.tmp").exists())

    def test_write_after_failure_uses_fresh_writer(self) -> None:
        path = self.root / "batch.parquet"
        write_parquet_batch(path, _dense_rows(9), PREDICTION_PRICE)

        with mock.patch.object(writer_module.os, "replace", side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                write_parquet_batch(path, _dense_rows(2), PREDICTION_PRICE)

        self.assertIsNone(writer_module._default_writer)  # discarded, not poisoned
        self.assertEqual(write_parquet_batch(path, _dense_rows(3), PREDICTION_PRICE), 3)
        self.assertEqual(self.count(path), 3)

    def test_failed_conversion_does_not_publish_partial_batch(self) -> None:
        path = self.root / "batch.parquet"
        write_parquet_batch(path, _dense_rows(4), PREDICTION_PRICE)
        original = path.read_bytes()

        with mock.patch.object(
            writer_module, "rows_to_arrow", side_effect=RuntimeError("conversion failed")
        ):
            with self.assertRaises(RuntimeError):
                write_parquet_batch(path, _dense_rows(4), PREDICTION_PRICE)

        self.assertEqual(path.read_bytes(), original)
        self.assertFalse(path.with_name(f"{path.name}.tmp").exists())

    def test_failed_parquet_write_leaves_no_partial_file(self) -> None:
        path = self.root / "batch.parquet"

        def broken_write(table: pa.Table, where: object, **_kwargs: object) -> None:
            Path(where).write_bytes(b"half a parquet file")
            raise OSError("disk full")

        with mock.patch.object(writer_module.pq, "write_table", side_effect=broken_write):
            with self.assertRaises(OSError):
                write_parquet_batch(path, _dense_rows(3), PREDICTION_PRICE)

        self.assertFalse(path.exists(), "a failed write must not publish a batch")
        self.assertFalse(path.with_name(f"{path.name}.tmp").exists(), ".tmp must be removed")

    def test_failed_parquet_write_keeps_previous_batch(self) -> None:
        path = self.root / "batch.parquet"
        write_parquet_batch(path, _dense_rows(6), PREDICTION_PRICE)
        original = path.read_bytes()

        def broken_write(table: pa.Table, where: object, **_kwargs: object) -> None:
            Path(where).write_bytes(b"half a parquet file")
            raise OSError("disk full")

        with mock.patch.object(writer_module.pq, "write_table", side_effect=broken_write):
            with self.assertRaises(OSError):
                write_parquet_batch(path, _dense_rows(2), PREDICTION_PRICE)

        self.assertEqual(path.read_bytes(), original)
        self.assertFalse(path.with_name(f"{path.name}.tmp").exists())


if __name__ == "__main__":
    unittest.main()
