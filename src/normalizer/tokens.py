"""Polymarket asset_id → Up/Down registry sync for the normalizer."""

from __future__ import annotations

import logging
from pathlib import Path

from datasets import POLYMARKET, iter_jsonl_paths, raw_root
from parse_polymarket import OutcomeTokenRegistry, seed_token_registry_from_raw

from normalizer.merged import is_merged_batch_name, read_merged_sources
from normalizer.paths import batch_parquet_path_from_jsonl, relative_to_project
from normalizer.state import (
    ProcessedState,
    load_polymarket_tokens,
    save_polymarket_tokens,
    save_processed_state,
)

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


def _raw_category(rel_path: str) -> str | None:
    parts = rel_path.split("/")
    if len(parts) != 5 or parts[0] != "raw":
        return None
    return parts[2]


def invalidate_polymarket_outcome_batches(
    *,
    project_root: Path,
    data_root: Path,
    state_path: Path,
    state: ProcessedState,
) -> int:
    """Drop processed state + parquet for PM price/trade/orderbook so outcomes can be rewritten.

    A merged batch can be shared by several raw files, so it is deleted once and
    every raw source loses its mapping together. If any source of a merged batch
    is no longer on disk, that batch is kept: its rows cannot be rebuilt from raw
    anymore, and deleting it would lose them. Per-file (legacy) batches keep the
    old behavior.
    """
    pm_raw_root = raw_root(project_root, POLYMARKET)
    deleted_batches: set[Path] = set()
    state_changed = False

    batch_sources: dict[str, set[str]] = {}
    for raw_path, batch_path in state.batches.items():
        if _raw_category(raw_path) in PM_OUTCOME_CATEGORIES:
            batch_sources.setdefault(batch_path, set()).add(raw_path)

    def discard(raw_path: str) -> None:
        nonlocal state_changed
        if raw_path in state.processed_files:
            state_changed = True
        state.processed_files.discard(raw_path)
        if state.batches.pop(raw_path, None) is not None:
            state_changed = True

    for batch_rel in sorted(batch_sources):
        mapped_sources = batch_sources[batch_rel]
        batch_path = project_root / batch_rel
        if not batch_path.exists():
            for raw_path in mapped_sources:
                discard(raw_path)
            continue

        if is_merged_batch_name(batch_path.name):
            listed_sources = read_merged_sources(batch_path)
            if listed_sources is None:
                logger.warning(
                    "Keeping Polymarket outcome batch %s: its raw source list "
                    "cannot be read, so the batch is not safe to delete",
                    batch_rel,
                )
                continue
            missing = [
                source
                for source in listed_sources
                if not (project_root / source).exists()
            ]
            if missing:
                logger.warning(
                    "Keeping Polymarket outcome batch %s: %d of its raw sources "
                    "are gone, so it cannot be rebuilt (missing: %s)",
                    batch_rel,
                    len(missing),
                    missing[:5],
                )
                continue
            sources_to_discard = [
                source
                for source in listed_sources
                if state.batches.get(source) == batch_rel
            ]
        else:
            sources_to_discard = sorted(mapped_sources)

        batch_path.unlink()
        deleted_batches.add(batch_path)
        state_changed = True
        for raw_path in sources_to_discard:
            discard(raw_path)

    # Legacy per-file batches that predate the mapping.
    for category in PM_OUTCOME_CATEGORIES:
        for jsonl_path in iter_jsonl_paths(pm_raw_root, category):
            rel_path = relative_to_project(jsonl_path, project_root)
            if rel_path in state.batches:
                continue
            per_file_path = batch_parquet_path_from_jsonl(
                jsonl_path,
                project_root=project_root,
                data_root=data_root,
            )
            if per_file_path.exists():
                per_file_path.unlink()
                deleted_batches.add(per_file_path)
                state_changed = True
            discard(rel_path)

    if deleted_batches or state_changed:
        save_processed_state(state_path, state.processed_files, state.batches)
        logger.info(
            "Invalidated %d Polymarket outcome parquet batches for token registry backfill",
            len(deleted_batches),
        )

    return len(deleted_batches)
