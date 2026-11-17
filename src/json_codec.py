"""Decode raw JSONL lines with ``orjson`` when it is installed.

``orjson`` is optional: without it the normalizer falls back to the standard
library ``json`` module. ``orjson`` is strict JSON, so lines it rejects are
retried with the stdlib parser; that keeps every input the previous code
accepted (``NaN``/``Infinity`` and friends) parsing exactly as before. Both
paths raise ``ValueError`` for genuinely invalid JSON, so callers keep the same
error handling either way.
"""

from __future__ import annotations

import json

try:
    import orjson
except ImportError:  # pragma: no cover - orjson is optional
    orjson = None


def loads(line: str) -> object:
    """Decode one JSON document (one raw JSONL line)."""
    if orjson is not None:
        try:
            return orjson.loads(line)
        except ValueError:
            return json.loads(line)
    return json.loads(line)
