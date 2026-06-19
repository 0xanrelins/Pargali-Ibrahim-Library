"""Discover active Polymarket BTC 15m Up/Down markets via Gamma API."""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

logger = logging.getLogger(__name__)

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
WINDOW_SECONDS = 900
SLUG_PREFIX = "btc-updown-15m"


@dataclass(frozen=True)
class Btc15mMarket:
    slug: str
    condition_id: str
    question: str
    window_timestamp: int
    end_date: str
    yes_token_id: str
    no_token_id: str

    @property
    def token_ids(self) -> tuple[str, str]:
        return self.yes_token_id, self.no_token_id


def current_window_timestamp(now: float | None = None) -> int:
    """Unix timestamp aligned to the current 15-minute window start."""
    ts = now if now is not None else time.time()
    return int(ts // WINDOW_SECONDS) * WINDOW_SECONDS


def btc_15m_slug(window_timestamp: int) -> str:
    return f"{SLUG_PREFIX}-{window_timestamp}"


def _parse_token_ids(raw: object) -> tuple[str, str]:
    if isinstance(raw, str):
        token_ids = json.loads(raw)
    elif isinstance(raw, list):
        token_ids = raw
    else:
        raise ValueError(f"Unexpected clobTokenIds payload: {raw!r}")

    if not isinstance(token_ids, list) or len(token_ids) < 2:
        raise ValueError(f"Expected two token ids, got: {token_ids!r}")

    return str(token_ids[0]), str(token_ids[1])


def _fetch_market_by_slug(slug: str) -> Btc15mMarket | None:
    query = urllib.parse.urlencode({"slug": slug})
    url = f"{GAMMA_API_BASE}/markets?{query}"
    request = urllib.request.Request(url, headers={"User-Agent": "btc-intelligence-platform/1.0"})

    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as exc:
        logger.warning("Gamma API request failed for slug=%s: %s", slug, exc)
        return None

    if not isinstance(payload, list) or not payload:
        return None

    market = payload[0]
    if not isinstance(market, dict):
        return None

    if market.get("closed") or not market.get("active", True):
        return None

    try:
        yes_token_id, no_token_id = _parse_token_ids(market.get("clobTokenIds"))
    except ValueError as exc:
        logger.warning("Invalid token ids for slug=%s: %s", slug, exc)
        return None

    window_timestamp = int(slug.rsplit("-", maxsplit=1)[-1])
    return Btc15mMarket(
        slug=str(market.get("slug", slug)),
        condition_id=str(market.get("conditionId", "")),
        question=str(market.get("question", "")),
        window_timestamp=window_timestamp,
        end_date=str(market.get("endDate", "")),
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
    )


def fetch_btc_15m_market(
    *,
    window_timestamp: int | None = None,
    look_ahead_windows: int = 1,
) -> Btc15mMarket | None:
    """Return the active BTC 15m market for the current (or next) window."""
    base_ts = window_timestamp if window_timestamp is not None else current_window_timestamp()

    for offset in range(look_ahead_windows + 1):
        slug = btc_15m_slug(base_ts + offset * WINDOW_SECONDS)
        market = _fetch_market_by_slug(slug)
        if market is not None:
            logger.info(
                "Resolved BTC 15m market slug=%s condition_id=%s",
                market.slug,
                market.condition_id,
            )
            return market

    logger.warning("No active BTC 15m market found for window=%s", base_ts)
    return None
