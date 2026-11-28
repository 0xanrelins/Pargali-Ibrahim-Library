"""Tests for the one-off per-minute -> hourly archive merge tool.

Every test builds a throwaway archive under ``tempfile``; no repository or real
archive directory is touched.
"""

from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Sequence

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "src"))

from merge_archive_hourly import main  # noqa: E402
from normalizer.merged import MERGED_BATCH_PATTERN, MERGED_NAME_SEPARATOR  # noqa: E402
from normalizer.writer import MERGED_SOURCES_METADATA_KEY  # noqa: E402

DAY = "2026-01-01"
DATASET = Path("trades/binance_futures/BTCUSDT")
SCHEMA = pa.schema(
    [
        ("timestamp", pa.int64()),
        ("price", pa.float64()),
        ("side", pa.string()),
    ]
)


def trade_row(timestamp: int, price: float, side: str) -> dict[str, object]:
    return {"timestamp": timestamp, "price": price, "side": side}


class ArchiveMergeTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.root = Path(self._tmpdir.name)
        self.source = self.root / "source"
        self.out = self.root / "out"

    def write_source(
        self,
        stamp: str,
        rows: Sequence[dict[str, object]],
        *,
        day: str = DAY,
        dataset: Path = DATASET,
        schema: pa.Schema = SCHEMA,
    ) -> Path:
        directory = self.source / dataset / day
        directory.mkdir(parents=True, exist_ok=True)
        table = (
            pa.Table.from_pylist(list(rows), schema=schema)
            if rows
            else schema.empty_table()
        )
        path = directory / f"{stamp}.parquet"
        pq.write_table(table, path)
        return path

    def target(self, name: str, *, day: str = DAY, dataset: Path = DATASET) -> Path:
        return self.out / dataset / day / name

    def run_tool(
        self, *arguments: str, out: Path | None = None
    ) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main([str(self.source), "--out", str(out or self.out), *arguments])
        return code, stdout.getvalue(), stderr.getvalue()

    def report_entries(self) -> dict[str, dict[str, object]]:
        path = self.out / "_merge_report.jsonl"
        self.assertTrue(path.is_file())
        entries: dict[str, dict[str, object]] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            entry = json.loads(line)
            entries[entry["target"]] = entry
        return entries

    def test_writes_one_file_per_hour_with_merged_names_and_preserves_order(self) -> None:
        self.write_source("09-00-00", [trade_row(1, 1.0, "BUY"), trade_row(2, 2.0, "SELL")])
        self.write_source("09-15-00", [])
        self.write_source("09-59-59", [trade_row(3, 3.0, "BUY")])
        self.write_source("10-00-00", [trade_row(4, 4.0, "SELL")])
        source_before = {
            path: (path.read_bytes(), path.stat().st_mtime_ns)
            for path in sorted(self.source.rglob("*.parquet"))
        }

        code, stdout, stderr = self.run_tool("--apply")

        self.assertEqual(code, 0, stderr)
        first = self.target("09-00-00--09-59-59.parquet")
        second = self.target("10-00-00--10-00-00.parquet")
        self.assertTrue(first.is_file())
        self.assertTrue(second.is_file())
        self.assertIsNotNone(MERGED_BATCH_PATTERN.fullmatch(first.name))
        self.assertNotEqual(first.name, second.name)
        self.assertEqual(
            first.name, f"09-00-00{MERGED_NAME_SEPARATOR}09-59-59.parquet"
        )
        self.assertEqual(
            pq.read_table(first).to_pylist(),
            [
                trade_row(1, 1.0, "BUY"),
                trade_row(2, 2.0, "SELL"),
                trade_row(3, 3.0, "BUY"),
            ],
        )
        self.assertEqual(pq.read_schema(first).names, SCHEMA.names)
        self.assertEqual(
            [str(field.type) for field in pq.read_schema(first)],
            [str(field.type) for field in SCHEMA],
        )

        metadata = pq.read_metadata(first).metadata or {}
        self.assertEqual(
            json.loads(metadata[MERGED_SOURCES_METADATA_KEY]),
            [
                f"trades/binance_futures/BTCUSDT/{DAY}/09-00-00.parquet",
                f"trades/binance_futures/BTCUSDT/{DAY}/09-15-00.parquet",
                f"trades/binance_futures/BTCUSDT/{DAY}/09-59-59.parquet",
            ],
        )

        entries = self.report_entries()
        first_entry = entries["trades/binance_futures/BTCUSDT/2026-01-01/09-00-00--09-59-59.parquet"]
        self.assertEqual(first_entry["rows"], 3)
        self.assertEqual(first_entry["source_count"], 3)
        self.assertEqual(first_entry["timestamp_min"], 1)
        self.assertEqual(first_entry["timestamp_max"], 3)
        self.assertEqual(first_entry["status"], "written")
        self.assertIn("written", stdout)

        self.assertEqual(
            {
                path: (path.read_bytes(), path.stat().st_mtime_ns)
                for path in sorted(self.source.rglob("*.parquet"))
            },
            source_before,
        )

    def test_zero_row_sources_produce_a_valid_empty_output(self) -> None:
        self.write_source("11-00-00", [])
        self.write_source("11-59-59", [])

        code, _, stderr = self.run_tool("--apply")

        self.assertEqual(code, 0, stderr)
        target = self.target("11-00-00--11-59-59.parquet")
        self.assertTrue(target.is_file())
        self.assertEqual(pq.read_metadata(target).num_rows, 0)
        self.assertEqual(pq.read_schema(target).names, SCHEMA.names)
        entry = self.report_entries()[
            "trades/binance_futures/BTCUSDT/2026-01-01/11-00-00--11-59-59.parquet"
        ]
        self.assertEqual(entry["rows"], 0)
        self.assertIsNone(entry["timestamp_min"])
        self.assertIsNone(entry["timestamp_max"])

    def test_dry_run_is_the_default_and_writes_nothing(self) -> None:
        self.write_source("09-00-00", [trade_row(1, 1.0, "BUY")])

        code, stdout, stderr = self.run_tool()

        self.assertEqual(code, 0, stderr)
        self.assertFalse(self.out.exists())
        self.assertIn("would write", stdout)
        self.assertIn("mode=dry-run", stdout)

        code, stdout, stderr = self.run_tool("--dry-run")

        self.assertEqual(code, 0, stderr)
        self.assertFalse(self.out.exists())
        self.assertIn("would write", stdout)

    def test_second_apply_run_reuses_outputs_and_leaves_the_report_alone(self) -> None:
        self.write_source("09-00-00", [trade_row(1, 1.0, "BUY")])
        code, _, stderr = self.run_tool("--apply")
        self.assertEqual(code, 0, stderr)

        target = self.target("09-00-00--09-00-00.parquet")
        report = self.out / "_merge_report.jsonl"
        target_mtime = target.stat().st_mtime_ns
        report_bytes = report.read_bytes()
        report_mtime = report.stat().st_mtime_ns

        code, stdout, stderr = self.run_tool("--apply")

        self.assertEqual(code, 0, stderr)
        self.assertIn("(reused)", stdout)
        self.assertNotIn("(written)", stdout)
        self.assertEqual(target.stat().st_mtime_ns, target_mtime)
        self.assertEqual(report.read_bytes(), report_bytes)
        self.assertEqual(report.stat().st_mtime_ns, report_mtime)

    def test_overlapping_roots_are_refused(self) -> None:
        self.write_source("09-00-00", [trade_row(1, 1.0, "BUY")])

        for out in (self.source / "out", self.root):
            with self.subTest(out=out):
                code, _, stderr = self.run_tool("--apply", out=out)
                self.assertEqual(code, 1)
                self.assertIn("overlap", stderr)
                self.assertFalse((self.source / "out").exists())

    def test_existing_output_with_a_different_row_count_is_not_overwritten(self) -> None:
        self.write_source("09-00-00", [trade_row(1, 1.0, "BUY")])
        code, _, stderr = self.run_tool("--apply")
        self.assertEqual(code, 0, stderr)
        target = self.target("09-00-00--09-00-00.parquet")
        before = target.read_bytes()

        self.write_source("09-30-00", [trade_row(2, 2.0, "SELL")])
        code, _, stderr = self.run_tool("--apply")

        self.assertEqual(code, 1)
        self.assertIn("2026-01-01 09:00", stderr)
        self.assertIn("row count", stderr)
        self.assertEqual(target.read_bytes(), before)

    def test_existing_output_without_recorded_sources_is_not_reused(self) -> None:
        self.write_source("09-00-00", [trade_row(1, 1.0, "BUY")])
        target = self.target("09-00-00--09-00-00.parquet")
        target.parent.mkdir(parents=True)
        pq.write_table(
            pa.Table.from_pylist([trade_row(1, 1.0, "BUY")], schema=SCHEMA),
            target,
        )

        code, _, stderr = self.run_tool("--apply")

        self.assertEqual(code, 1)
        self.assertIn("no recorded source set", stderr)

    def test_a_merged_name_in_the_source_is_refused(self) -> None:
        directory = self.source / DATASET / DAY
        directory.mkdir(parents=True)
        pq.write_table(SCHEMA.empty_table(), directory / "09-00-00--09-59-59.parquet")

        code, _, stderr = self.run_tool("--apply")

        self.assertEqual(code, 1)
        self.assertIn("unexpected parquet file name", stderr)
        self.assertFalse(self.out.exists())


if __name__ == "__main__":
    unittest.main()
