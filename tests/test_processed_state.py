"""Tests for processed-state batching, pruning, atomic writes and crash recovery."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from datasets import BINANCE_FUTURES, parquet_batch_path
from normalizer import batch as batch_module
from normalizer import runner
from normalizer.state import load_processed_files, save_processed_files

BTC_SYMBOL = "BTCUSDT"


class ProcessedStateTestBase(unittest.TestCase):
    """Run ``normalize_once`` against a throwaway project root."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.root = Path(self._tmpdir.name)
        self.data_root = self.root / "data"
        self.state_path = self.root / "state" / "processed.json"
        self.tokens_path = self.root / "state" / "polymarket_tokens.json"

        paths_patcher = mock.patch.object(
            runner,
            "project_paths",
            return_value=(self.root, self.data_root, self.state_path, self.tokens_path),
        )
        paths_patcher.start()
        self.addCleanup(paths_patcher.stop)

        self.normalized: list[Path] = []

        def fake_normalize(jsonl_path: Path, **_kwargs: object) -> tuple[Path, int]:
            self.normalized.append(jsonl_path)
            return self.data_root / "fake" / f"{jsonl_path.stem}.parquet", 1

        batch_patcher = mock.patch.object(
            runner,
            "normalize_jsonl_batch",
            side_effect=fake_normalize,
        )
        self.normalize_mock = batch_patcher.start()
        self.addCleanup(batch_patcher.stop)

    def write_raw(
        self,
        category: str = "trades",
        date: str = "2026-08-14",
        stamp: str = "12-00-00",
    ) -> Path:
        path = self.root / "raw" / BINANCE_FUTURES / category / date / f"{stamp}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"event":"test"}\n', encoding="utf-8")
        return path

    def rel(self, path: Path) -> str:
        return path.relative_to(self.root).as_posix()

    def seed_state(self, raw_paths: list[str]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(
            json.dumps({"processed_files": raw_paths}),
            encoding="utf-8",
        )


class NormalizeOnceStateWriteTests(ProcessedStateTestBase):
    def test_batch_saves_processed_state_once(self) -> None:
        raw_paths = [
            self.write_raw(),
            self.write_raw(stamp="12-01-00"),
            self.write_raw(category="liquidations"),
        ]

        with mock.patch.object(
            runner,
            "save_processed_files",
            side_effect=save_processed_files,
        ) as save_mock:
            stats = runner.normalize_once()

        self.assertEqual(save_mock.call_count, 1)
        self.assertEqual(stats["raw_files_processed"], 3)
        self.assertEqual(
            load_processed_files(self.state_path),
            {self.rel(path) for path in raw_paths},
        )

    def test_prune_drops_entries_for_deleted_raw_files(self) -> None:
        kept = [self.write_raw(), self.write_raw(category="liquidations")]
        deleted = [
            "raw/binance_futures/trades/2026-01-01/00-00-00.jsonl",
            "raw/polymarket/prediction_trades/2026-01-01/00-00-00.jsonl",
        ]
        self.seed_state([self.rel(path) for path in kept] + deleted)

        stats = runner.normalize_once()

        self.assertEqual(stats["raw_files_processed"], 0)
        self.assertEqual(
            load_processed_files(self.state_path),
            {self.rel(path) for path in kept},
        )

    def test_existing_batch_parquet_is_recovered_without_renormalizing(self) -> None:
        jsonl_path = self.write_raw()
        parquet_path = parquet_batch_path(
            self.data_root,
            BINANCE_FUTURES,
            "trades",
            BTC_SYMBOL,
            "2026-08-14",
            "12-00-00",
        )
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        parquet_path.write_bytes(b"PAR1PAR1")

        stats = runner.normalize_once()

        self.assertEqual(stats["raw_files_skipped"], 1)
        self.assertEqual(self.normalize_mock.call_count, 0)
        self.assertEqual(
            load_processed_files(self.state_path),
            {self.rel(jsonl_path)},
        )

    def test_failed_write_does_not_mark_raw_file_processed(self) -> None:
        """A batch that raises must stay unprocessed so a retry can pick it up."""
        jsonl_path = self.write_raw(category="orderbook_diff")
        jsonl_path.write_text(
            json.dumps(
                {
                    "stream": "btcusdt@depth",
                    "data": {
                        "e": "depthUpdate",
                        "E": 1_762_000_000_000,
                        "T": 1_762_000_000_001,
                        "s": "BTCUSDT",
                        "U": 10,
                        "u": 11,
                        "b": [["68000.10", "0.5"]],
                        "a": [["68001.20", "1.5"]],
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        self.normalize_mock.side_effect = batch_module.normalize_jsonl_batch

        with mock.patch.object(
            batch_module,
            "write_parquet_batch",
            side_effect=OverflowError("Python int too large to convert to C long"),
        ):
            stats = runner.normalize_once()

        self.assertEqual(stats["raw_files_seen"], 1)
        self.assertEqual(stats["raw_files_failed"], 1)
        self.assertEqual(stats["raw_files_processed"], 0)
        self.assertEqual(stats["parquet_batches_written"], 0)
        self.assertEqual(load_processed_files(self.state_path), set())

        out_path = parquet_batch_path(
            self.data_root,
            BINANCE_FUTURES,
            "orderbook_diff",
            BTC_SYMBOL,
            "2026-08-14",
            "12-00-00",
        )
        self.assertFalse(out_path.exists())
        self.assertFalse(out_path.with_name(f"{out_path.name}.tmp").exists())

    def test_stop_mid_batch_saves_once_without_pruning(self) -> None:
        first = self.write_raw()
        self.write_raw(stamp="12-01-00")
        missing = "raw/binance_futures/trades/2026-01-01/00-00-00.jsonl"
        self.seed_state([missing])

        stop_flag = {"requested": False}

        def fake_normalize(jsonl_path: Path, **_kwargs: object) -> tuple[Path, int]:
            self.normalized.append(jsonl_path)
            stop_flag["requested"] = True
            return self.data_root / "fake" / f"{jsonl_path.stem}.parquet", 1

        self.normalize_mock.side_effect = fake_normalize

        with mock.patch.object(
            runner,
            "save_processed_files",
            side_effect=save_processed_files,
        ) as save_mock:
            stats = runner.normalize_once(
                should_stop=lambda: stop_flag["requested"],
            )

        self.assertEqual(save_mock.call_count, 1)
        self.assertEqual(stats["raw_files_processed"], 1)
        self.assertEqual(len(self.normalized), 1)
        state = load_processed_files(self.state_path)
        self.assertIn(self.rel(first), state)
        self.assertIn(missing, state)


class SaveProcessedFilesAtomicTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.state_path = Path(self._tmpdir.name) / "state" / "processed.json"

    def _dir_entries(self) -> list[str]:
        return sorted(path.name for path in self.state_path.parent.iterdir())

    def test_failed_write_keeps_previous_state_parseable(self) -> None:
        save_processed_files(self.state_path, {"raw/a.jsonl"})
        before = self.state_path.read_bytes()

        with mock.patch(
            "normalizer.state.os.fsync",
            side_effect=OSError(28, "No space left on device"),
        ):
            with self.assertRaises(OSError):
                save_processed_files(
                    self.state_path,
                    {"raw/a.jsonl", "raw/b.jsonl"},
                )

        self.assertEqual(self.state_path.read_bytes(), before)
        self.assertEqual(
            load_processed_files(self.state_path),
            {"raw/a.jsonl"},
        )
        self.assertEqual(self._dir_entries(), ["processed.json"])

    def test_write_replaces_target_atomically_and_leaves_no_tmp_file(self) -> None:
        save_processed_files(self.state_path, {"raw/a.jsonl"})

        with mock.patch(
            "normalizer.state.os.replace",
            wraps=os.replace,
        ) as replace_mock:
            save_processed_files(self.state_path, {"raw/b.jsonl"})

        self.assertEqual(replace_mock.call_count, 1)
        tmp_path, target_path = replace_mock.call_args.args
        self.assertEqual(Path(target_path), self.state_path)
        self.assertEqual(Path(tmp_path).parent, self.state_path.parent)
        self.assertNotEqual(Path(tmp_path), self.state_path)
        self.assertEqual(
            load_processed_files(self.state_path),
            {"raw/b.jsonl"},
        )
        self.assertEqual(self._dir_entries(), ["processed.json"])


if __name__ == "__main__":
    unittest.main()
