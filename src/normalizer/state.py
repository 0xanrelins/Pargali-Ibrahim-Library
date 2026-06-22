"""Normalizer idempotency and optional Polymarket token registry persistence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TypedDict


class ProcessedState(TypedDict):
    processed_files: list[str]


class PolymarketTokenState(TypedDict):
    asset_to_outcome: dict[str, str]


def load_processed_files(state_path: Path) -> set[str]:
    if not state_path.exists():
        return set()

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    files = payload.get("processed_files", [])
    if not isinstance(files, list):
        raise ValueError(f"Invalid processed state file: {state_path}")
    return {str(item) for item in files}


def save_processed_files(state_path: Path, processed_files: set[str]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload: ProcessedState = {
        "processed_files": sorted(processed_files),
    }
    state_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_polymarket_tokens(state_path: Path) -> dict[str, str]:
    """Load asset_id → outcome map. Empty dict when file is missing (PM registry deferred)."""
    if not state_path.exists():
        return {}

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    mapping = payload.get("asset_to_outcome", {})
    if not isinstance(mapping, dict):
        raise ValueError(f"Invalid polymarket token state file: {state_path}")
    return {str(asset_id): str(outcome) for asset_id, outcome in mapping.items()}


def save_polymarket_tokens(state_path: Path, asset_to_outcome: dict[str, str]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload: PolymarketTokenState = {
        "asset_to_outcome": dict(sorted(asset_to_outcome.items())),
    }
    state_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
