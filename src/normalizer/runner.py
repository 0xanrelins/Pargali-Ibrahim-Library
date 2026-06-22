"""Orchestrate raw JSONL → immutable normalized parquet batches."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from datasets import POLYMARKET, REGISTERED_SOURCES, all_categories, iter_jsonl_paths, raw_root
from metrics.pipeline_registry import (
    parquet_failed_conversions_total,
    parquet_files_processed_total,
    parquet_last_batch_duration_seconds,
    parquet_last_batch_files_processed,
    parquet_last_batch_timestamp,
    parquet_raw_backlog_files,
)
from normalizer.batch import normalize_jsonl_batch
from normalizer.paths import batch_parquet_path_from_jsonl, project_paths, relative_to_project
from normalizer.state import load_processed_files, save_polymarket_tokens, save_processed_files
from normalizer.tokens import (
    invalidate_polymarket_outcome_batches,
    mark_polymarket_outcome_backfill_done,
    prepare_polymarket_token_registry,
)

logger = logging.getLogger(__name__)

NORMALIZE_INTERVAL_SEC = 60


def collect_backlog_metrics(
    *,
    project_root: Path,
    state_path: Path,
) -> dict[str, int]:
    processed_files = load_processed_files(state_path)
    backlog = {category: 0 for category in all_categories()}

    for source in REGISTERED_SOURCES.values():
        if not source.has_parquet_pipeline:
            continue
        exchange_raw_root = raw_root(project_root, source.exchange)
        for category in source.categories:
            for jsonl_path in iter_jsonl_paths(exchange_raw_root, category):
                rel_path = relative_to_project(jsonl_path, project_root)
                if rel_path not in processed_files:
                    backlog[category] += 1
            parquet_raw_backlog_files.labels(category=category).set(backlog[category])

    return backlog


def normalize_once() -> dict[str, int]:
    batch_started = time.monotonic()
    project_root, data_root, state_path, tokens_path = project_paths()
    processed_files = load_processed_files(state_path)

    token_registry, backfill_outcomes = prepare_polymarket_token_registry(
        project_root=project_root,
        tokens_path=tokens_path,
    )
    if backfill_outcomes:
        invalidate_polymarket_outcome_batches(
            project_root=project_root,
            data_root=data_root,
            state_path=state_path,
            processed_files=processed_files,
        )
        mark_polymarket_outcome_backfill_done(tokens_path)

    stats = {
        "raw_files_seen": 0,
        "raw_files_processed": 0,
        "raw_files_skipped": 0,
        "raw_files_failed": 0,
        "rows_written": 0,
        "parquet_batches_written": 0,
    }
    written_paths: set[Path] = set()

    collect_backlog_metrics(project_root=project_root, state_path=state_path)

    for source in REGISTERED_SOURCES.values():
        if not source.has_parquet_pipeline:
            continue

        exchange_raw_root = raw_root(project_root, source.exchange)
        for category in source.categories:
            for jsonl_path in iter_jsonl_paths(exchange_raw_root, category):
                stats["raw_files_seen"] += 1
                rel_path = relative_to_project(jsonl_path, project_root)

                if rel_path in processed_files:
                    stats["raw_files_skipped"] += 1
                    continue

                out_path = batch_parquet_path_from_jsonl(
                    jsonl_path,
                    project_root=project_root,
                    data_root=data_root,
                )
                if out_path.exists():
                    processed_files.add(rel_path)
                    save_processed_files(state_path, processed_files)
                    stats["raw_files_skipped"] += 1
                    logger.info(
                        "Recovered processed state for existing batch: %s",
                        rel_path,
                    )
                    continue

                try:
                    mapping_before = (
                        token_registry.to_mapping()
                        if source.exchange == POLYMARKET
                        else None
                    )
                    written_path, row_count = normalize_jsonl_batch(
                        jsonl_path,
                        project_root=project_root,
                        data_root=data_root,
                        token_registry=(
                            token_registry if source.exchange == POLYMARKET else None
                        ),
                    )
                    if (
                        source.exchange == POLYMARKET
                        and category == "prediction_metadata"
                        and token_registry.to_mapping() != mapping_before
                    ):
                        save_polymarket_tokens(tokens_path, token_registry.to_mapping())
                except Exception:
                    stats["raw_files_failed"] += 1
                    parquet_failed_conversions_total.labels(reason="normalize").inc()
                    logger.exception("Failed to normalize raw file: %s", jsonl_path)
                    continue

                processed_files.add(rel_path)
                save_processed_files(state_path, processed_files)

                stats["raw_files_processed"] += 1
                stats["rows_written"] += row_count
                written_paths.add(written_path)
                parquet_files_processed_total.inc()

    stats["parquet_batches_written"] = len(written_paths)

    collect_backlog_metrics(project_root=project_root, state_path=state_path)

    parquet_last_batch_timestamp.set(time.time())
    parquet_last_batch_duration_seconds.set(time.monotonic() - batch_started)
    parquet_last_batch_files_processed.set(stats["raw_files_processed"])

    return stats
