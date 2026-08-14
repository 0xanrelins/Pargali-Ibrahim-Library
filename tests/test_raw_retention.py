"""Tests for conservative raw JSONL retention."""

from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from datasets import BINANCE_FUTURES, POLYMARKET, parquet_batch_path
from metrics.pipeline_registry import (
    raw_retention_bytes_freed_total,
    raw_retention_files_deleted_total,
    raw_retention_would_delete_bytes,
    raw_retention_would_delete_files,
)
from normalizer.raw_retention import RawRetentionConfig, RawRetentionScheduler, run_raw_retention


def _write_jsonl(path: Path, *, age_hours: float = 48.0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"event":"test"}\n', encoding="utf-8")
    old_mtime = time.time() - (age_hours * 3600)
    os.utime(path, (old_mtime, old_mtime))


def _write_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"PAR1PAR1")


def _binance_trade_paths(root: Path, batch_stamp: str = "12-00-00") -> tuple[Path, Path]:
    date = "2026-08-14"
    jsonl = (
        root
        / "raw"
        / BINANCE_FUTURES
        / "trades"
        / date
        / f"{batch_stamp}.jsonl"
    )
    parquet = parquet_batch_path(
        root / "data",
        BINANCE_FUTURES,
        "trades",
        "BTCUSDT",
        date,
        batch_stamp,
    )
    return jsonl, parquet


def _polymarket_metadata_paths(root: Path, batch_stamp: str = "12-00-00") -> tuple[Path, Path]:
    date = "2026-08-14"
    jsonl = (
        root
        / "raw"
        / POLYMARKET
        / "prediction_metadata"
        / date
        / f"{batch_stamp}.jsonl"
    )
    parquet = parquet_batch_path(
        root / "data",
        POLYMARKET,
        "prediction_metadata",
        "BTC_15M",
        date,
        batch_stamp,
    )
    return jsonl, parquet


class RawRetentionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.data_root = self.root / "data"
        self.state_path = self.root / "state" / "processed.json"
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps({"processed_files": []}), encoding="utf-8")

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_dry_run_deletes_nothing_and_does_not_increment_delete_counters(self) -> None:
        jsonl, parquet = _binance_trade_paths(self.root)
        _write_jsonl(jsonl)
        _write_parquet(parquet)
        self.state_path.write_text(
            json.dumps({"processed_files": [f"raw/{BINANCE_FUTURES}/trades/2026-08-14/12-00-00.jsonl"]}),
            encoding="utf-8",
        )

        deleted_before = raw_retention_files_deleted_total._value.get()
        bytes_before = raw_retention_bytes_freed_total._value.get()

        stats = run_raw_retention(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            dry_run=True,
        )

        self.assertTrue(jsonl.exists())
        self.assertEqual(stats["would_delete"], 1)
        self.assertEqual(stats["deleted"], 0)
        self.assertEqual(raw_retention_files_deleted_total._value.get(), deleted_before)
        self.assertEqual(raw_retention_bytes_freed_total._value.get(), bytes_before)
        self.assertEqual(raw_retention_would_delete_files._value.get(), 1.0)
        self.assertGreater(raw_retention_would_delete_bytes._value.get(), 0.0)

    def test_live_delete_requires_parquet_and_grace(self) -> None:
        jsonl, parquet = _binance_trade_paths(self.root)
        _write_jsonl(jsonl, age_hours=48.0)
        _write_parquet(parquet)
        self.state_path.write_text(
            json.dumps({"processed_files": [f"raw/{BINANCE_FUTURES}/trades/2026-08-14/12-00-00.jsonl"]}),
            encoding="utf-8",
        )

        stats = run_raw_retention(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            dry_run=False,
        )

        self.assertFalse(jsonl.exists())
        self.assertEqual(stats["deleted"], 1)
        self.assertGreater(stats["bytes_freed"], 0)

    def test_no_parquet_means_no_delete(self) -> None:
        jsonl, _parquet = _binance_trade_paths(self.root)
        _write_jsonl(jsonl, age_hours=48.0)
        self.state_path.write_text(
            json.dumps({"processed_files": [f"raw/{BINANCE_FUTURES}/trades/2026-08-14/12-00-00.jsonl"]}),
            encoding="utf-8",
        )

        stats = run_raw_retention(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            dry_run=False,
        )

        self.assertTrue(jsonl.exists())
        self.assertEqual(stats["deleted"], 0)
        self.assertEqual(stats["skipped_no_parquet"], 1)

    def test_grace_period_blocks_delete(self) -> None:
        jsonl, parquet = _binance_trade_paths(self.root)
        _write_jsonl(jsonl, age_hours=1.0)
        _write_parquet(parquet)
        self.state_path.write_text(
            json.dumps({"processed_files": [f"raw/{BINANCE_FUTURES}/trades/2026-08-14/12-00-00.jsonl"]}),
            encoding="utf-8",
        )

        stats = run_raw_retention(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            dry_run=False,
            grace_hours=24.0,
        )

        self.assertTrue(jsonl.exists())
        self.assertEqual(stats["deleted"], 0)
        self.assertEqual(stats["skipped_grace"], 1)

    def test_backlog_blocks_entire_run(self) -> None:
        jsonl, parquet = _binance_trade_paths(self.root, batch_stamp="12-00-00")
        pending, _pending_parquet = _binance_trade_paths(self.root, batch_stamp="12-01-00")
        _write_jsonl(jsonl, age_hours=48.0)
        _write_jsonl(pending, age_hours=48.0)
        _write_parquet(parquet)
        self.state_path.write_text(
            json.dumps({"processed_files": [f"raw/{BINANCE_FUTURES}/trades/2026-08-14/12-00-00.jsonl"]}),
            encoding="utf-8",
        )

        stats = run_raw_retention(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            dry_run=False,
        )

        self.assertTrue(jsonl.exists())
        self.assertTrue(pending.exists())
        self.assertEqual(stats["run_skipped_backlog"], 1)
        self.assertEqual(stats["deleted"], 0)

    def test_polymarket_live_delete_with_parquet_and_grace(self) -> None:
        jsonl, parquet = _polymarket_metadata_paths(self.root)
        _write_jsonl(jsonl, age_hours=48.0)
        _write_parquet(parquet)
        self.state_path.write_text(
            json.dumps(
                {"processed_files": [f"raw/{POLYMARKET}/prediction_metadata/2026-08-14/12-00-00.jsonl"]}
            ),
            encoding="utf-8",
        )

        stats = run_raw_retention(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            dry_run=False,
        )

        self.assertFalse(jsonl.exists())
        self.assertEqual(stats["deleted"], 1)

    def test_polymarket_counted_in_dry_run_candidates(self) -> None:
        jsonl, parquet = _polymarket_metadata_paths(self.root)
        _write_jsonl(jsonl, age_hours=48.0)
        _write_parquet(parquet)
        self.state_path.write_text(
            json.dumps(
                {"processed_files": [f"raw/{POLYMARKET}/prediction_metadata/2026-08-14/12-00-00.jsonl"]}
            ),
            encoding="utf-8",
        )

        stats = run_raw_retention(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            dry_run=True,
        )

        self.assertTrue(jsonl.exists())
        self.assertEqual(stats["would_delete"], 1)
        self.assertEqual(stats["deleted"], 0)

    def test_polymarket_no_parquet_blocks_delete(self) -> None:
        jsonl, _parquet = _polymarket_metadata_paths(self.root)
        _write_jsonl(jsonl, age_hours=48.0)
        self.state_path.write_text(
            json.dumps(
                {"processed_files": [f"raw/{POLYMARKET}/prediction_metadata/2026-08-14/12-00-00.jsonl"]}
            ),
            encoding="utf-8",
        )

        stats = run_raw_retention(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            dry_run=False,
        )

        self.assertTrue(jsonl.exists())
        self.assertEqual(stats["deleted"], 0)
        self.assertEqual(stats["skipped_no_parquet"], 1)

    def test_polymarket_grace_blocks_delete(self) -> None:
        jsonl, parquet = _polymarket_metadata_paths(self.root)
        _write_jsonl(jsonl, age_hours=1.0)
        _write_parquet(parquet)
        self.state_path.write_text(
            json.dumps(
                {"processed_files": [f"raw/{POLYMARKET}/prediction_metadata/2026-08-14/12-00-00.jsonl"]}
            ),
            encoding="utf-8",
        )

        stats = run_raw_retention(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            dry_run=False,
            grace_hours=24.0,
        )

        self.assertTrue(jsonl.exists())
        self.assertEqual(stats["deleted"], 0)
        self.assertEqual(stats["skipped_grace"], 1)

    def test_polymarket_backlog_blocks_delete(self) -> None:
        jsonl, parquet = _polymarket_metadata_paths(self.root, batch_stamp="12-00-00")
        pending, _ = _polymarket_metadata_paths(self.root, batch_stamp="12-01-00")
        _write_jsonl(jsonl, age_hours=48.0)
        _write_jsonl(pending, age_hours=48.0)
        _write_parquet(parquet)
        self.state_path.write_text(
            json.dumps(
                {"processed_files": [f"raw/{POLYMARKET}/prediction_metadata/2026-08-14/12-00-00.jsonl"]}
            ),
            encoding="utf-8",
        )

        stats = run_raw_retention(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            dry_run=False,
        )

        self.assertTrue(jsonl.exists())
        self.assertTrue(pending.exists())
        self.assertEqual(stats["run_skipped_backlog"], 1)
        self.assertEqual(stats["deleted"], 0)

    def test_empty_raw_directories_removed_after_delete(self) -> None:
        jsonl, parquet = _binance_trade_paths(self.root)
        date_dir = jsonl.parent
        _write_jsonl(jsonl, age_hours=48.0)
        _write_parquet(parquet)
        self.state_path.write_text(
            json.dumps({"processed_files": [f"raw/{BINANCE_FUTURES}/trades/2026-08-14/12-00-00.jsonl"]}),
            encoding="utf-8",
        )

        run_raw_retention(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            dry_run=False,
        )

        self.assertFalse(date_dir.exists())


class RawRetentionSchedulerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.data_root = self.root / "data"
        self.state_path = self.root / "state" / "processed.json"
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps({"processed_files": []}), encoding="utf-8")

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def _scheduler(self, *, interval_sec: float = 86_400.0, dry_run: bool = True) -> RawRetentionScheduler:
        return RawRetentionScheduler(
            config=RawRetentionConfig(
                enabled=True,
                dry_run=dry_run,
                interval_sec=interval_sec,
            ),
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
        )

    def test_backlog_skip_retries_on_next_call_without_throttle(self) -> None:
        jsonl, parquet = _binance_trade_paths(self.root, batch_stamp="12-00-00")
        pending, _ = _binance_trade_paths(self.root, batch_stamp="12-01-00")
        _write_jsonl(jsonl, age_hours=48.0)
        _write_jsonl(pending, age_hours=48.0)
        _write_parquet(parquet)
        self.state_path.write_text(
            json.dumps({"processed_files": [f"raw/{BINANCE_FUTURES}/trades/2026-08-14/12-00-00.jsonl"]}),
            encoding="utf-8",
        )
        scheduler = self._scheduler(interval_sec=86_400.0)

        first = scheduler.maybe_run_after_normalize()
        second = scheduler.maybe_run_after_normalize()

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None and second is not None
        self.assertEqual(first["run_skipped_backlog"], 1)
        self.assertEqual(second["run_skipped_backlog"], 1)
        self.assertEqual(second.get("run_skipped_throttle", 0), 0)

    def test_backlog_clear_runs_retention_on_next_call(self) -> None:
        jsonl, parquet = _binance_trade_paths(self.root, batch_stamp="12-00-00")
        pending, pending_parquet = _binance_trade_paths(self.root, batch_stamp="12-01-00")
        _write_jsonl(jsonl, age_hours=48.0)
        _write_jsonl(pending, age_hours=48.0)
        _write_parquet(parquet)
        _write_parquet(pending_parquet)
        self.state_path.write_text(
            json.dumps({"processed_files": [f"raw/{BINANCE_FUTURES}/trades/2026-08-14/12-00-00.jsonl"]}),
            encoding="utf-8",
        )
        scheduler = self._scheduler(dry_run=False)

        blocked = scheduler.maybe_run_after_normalize()
        self.assertIsNotNone(blocked)
        assert blocked is not None
        self.assertEqual(blocked["run_skipped_backlog"], 1)

        self.state_path.write_text(
            json.dumps(
                {
                    "processed_files": [
                        f"raw/{BINANCE_FUTURES}/trades/2026-08-14/12-00-00.jsonl",
                        f"raw/{BINANCE_FUTURES}/trades/2026-08-14/12-01-00.jsonl",
                    ]
                }
            ),
            encoding="utf-8",
        )

        cleared = scheduler.maybe_run_after_normalize()
        self.assertIsNotNone(cleared)
        assert cleared is not None
        self.assertEqual(cleared.get("run_skipped_backlog", 0), 0)
        self.assertGreaterEqual(cleared["deleted"], 1)
        self.assertFalse(jsonl.exists())
        self.assertFalse(pending.exists())

    def test_evaluated_run_throttled_until_interval_elapses(self) -> None:
        jsonl, parquet = _binance_trade_paths(self.root)
        _write_jsonl(jsonl, age_hours=48.0)
        _write_parquet(parquet)
        self.state_path.write_text(
            json.dumps({"processed_files": [f"raw/{BINANCE_FUTURES}/trades/2026-08-14/12-00-00.jsonl"]}),
            encoding="utf-8",
        )
        scheduler = self._scheduler(interval_sec=3600.0, dry_run=True)

        first = scheduler.maybe_run_after_normalize()
        second = scheduler.maybe_run_after_normalize()

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None and second is not None
        self.assertEqual(first.get("run_skipped_backlog", 0), 0)
        self.assertEqual(first["would_delete"], 1)
        self.assertEqual(second.get("run_skipped_throttle"), 1)


if __name__ == "__main__":
    unittest.main()
