"""Parse raw JSONL batch files into normalized row dicts."""

from __future__ import annotations

import logging
from pathlib import Path

from datasets import POLYMARKET
from parse_polymarket import OutcomeTokenRegistry, parse_polymarket_lines
from parse_raw import ParsedRow, parse_raw_lines

logger = logging.getLogger(__name__)


def parse_jsonl_file(
    jsonl_path: Path,
    *,
    exchange: str,
    category: str,
    token_registry: OutcomeTokenRegistry | None = None,
) -> list[ParsedRow]:
    """Parse all lines in one raw JSONL batch; keep rows matching the file category."""
    parser = parse_polymarket_lines if exchange == POLYMARKET else parse_raw_lines
    rows: list[ParsedRow] = []

    for raw_line in jsonl_path.read_text(encoding="utf-8").splitlines():
        if exchange == POLYMARKET:
            parsed = parser(raw_line, token_registry=token_registry)
        else:
            parsed = parser(raw_line)

        for row_category, row in parsed:
            if row_category != category:
                logger.warning(
                    "Skipping row category %s in %s (expected %s)",
                    row_category,
                    jsonl_path,
                    category,
                )
                continue
            rows.append(row)

    return rows
