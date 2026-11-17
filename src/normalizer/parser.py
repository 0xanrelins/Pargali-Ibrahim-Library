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

    # Stream the raw file instead of materializing a full ``read_text()`` copy
    # plus a ``splitlines()`` list: batches are read once, top to bottom.
    # This differs from ``str.splitlines()`` only for U+2028/U+0085 inside a
    # JSON string: they are no longer treated as line breaks, which is correct
    # for JSONL (a JSON string may contain them unescaped).
    with jsonl_path.open("r", encoding="utf-8") as jsonl_file:
        for raw_line in jsonl_file:
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
