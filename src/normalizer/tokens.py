"""Polymarket asset_id → Up/Down registry sync for the normalizer."""

from __future__ import annotations

import logging
from pathlib import Path

from datasets import POLYMARKET, iter_jsonl_paths, raw_root
from parse_polymarket import OutcomeTokenRegistry, seed_token_registry_from_raw

from normalizer.paths import batch_parquet_path_from_jsonl, relative_to_project
from normalizer.state import load_polymarket_tokens, save_polymarket_tokens, save_processed_files

logger = logging.getLogger(__name__)

PM_OUTCOME_CATEGORIES: frozenset[str] = frozenset(
    {"prediction_price", "prediction_trades", "prediction_orderbook"}
)

OUTCOME_BACKFILL_FLAG = "polymarket_outcome_backfill_done"


def _outcome_backfill_flag_path(tokens_path: Path) -> Path:
    return tokens_path.parent / OUTCOME_BACKFILL_FLAG


def needs_polymarket_outcome_backfill(tokens_path: Path) -> bool:
    """True once when token registry exists but outcome batches were never rewritten."""
    if not tokens_path.exists() or not load_polymarket_tokens(tokens_path):
        return False
    return not _outcome_backfill_flag_path(tokens_path).exists()


def mark_polymarket_outcome_backfill_done(tokens_path: Path) -> None:
    _outcome_backfill_flag_path(tokens_path).touch()


def prepare_polymarket_token_registry(
    *,
    project_root: Path,
    tokens_path: Path,
) -> tuple[OutcomeTokenRegistry, bool]:
    """Load persisted registry, merge all raw metadata, optionally flag outcome backfill."""
    before = load_polymarket_tokens(tokens_path)
    pm_raw_root = raw_root(project_root, POLYMARKET)

    registry = OutcomeTokenRegistry()
    registry.merge_from_mapping(before)
    registry.merge_from_mapping(seed_token_registry_from_raw(pm_raw_root).to_mapping())

    after = registry.to_mapping()
    needs_backfill = needs_polymarket_outcome_backfill(tokens_path)
    if after != before:
        save_polymarket_tokens(tokens_path, after)
        logger.info(
            "Polymarket token registry updated: %d → %d asset ids",
            len(before),
            len(after),
        )

    return registry, needs_backfill


def invalidate_polymarket_outcome_batches(
    *,
    project_root: Path,
    data_root: Path,
    state_path: Path,
    processed_files: set[str],
) -> int:
    """Drop processed state + parquet for PM price/trade/orderbook so outcomes can be rewritten."""
    pm_raw_root = raw_root(project_root, POLYMARKET)
    invalidated = 0

    for category in PM_OUTCOME_CATEGORIES:
        for jsonl_path in iter_jsonl_paths(pm_raw_root, category):
            rel_path = relative_to_project(jsonl_path, project_root)
            out_path = batch_parquet_path_from_jsonl(
                jsonl_path,
                project_root=project_root,
                data_root=data_root,
            )
            if out_path.exists():
                out_path.unlink()
                invalidated += 1
            processed_files.discard(rel_path)

    if invalidated:
        save_processed_files(state_path, processed_files)
        logger.info(
            "Invalidated %d Polymarket outcome parquet batches for token registry backfill",
            invalidated,
        )

    return invalidated
