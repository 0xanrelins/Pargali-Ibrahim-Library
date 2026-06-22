"""Prometheus gauges for BTC knowledge objects."""

from __future__ import annotations

from prometheus_client import Gauge

btc_knowledge_momentum = Gauge(
    "mdp_btc_knowledge_momentum",
    "BTC momentum score from 0 (bearish) to 100 (bullish).",
)

btc_knowledge_positioning = Gauge(
    "mdp_btc_knowledge_positioning",
    "BTC positioning score from 0 (short-heavy) to 100 (long-heavy).",
)

btc_knowledge_fear = Gauge(
    "mdp_btc_knowledge_fear",
    "BTC fear score from 0 (calm) to 100 (stressed).",
)

btc_knowledge_conviction = Gauge(
    "mdp_btc_knowledge_conviction",
    "Cross-venue conviction score from 0 (conflicted) to 100 (aligned).",
)
