"""Prometheus metrics for the Polymarket collector."""

from __future__ import annotations

from prometheus_client import Counter, Gauge

polymarket_connected = Gauge(
    "mdp_polymarket_connected",
    "Polymarket WebSocket connection state (1=connected, 0=disconnected).",
)

polymarket_last_message_timestamp = Gauge(
    "mdp_polymarket_last_message_timestamp_seconds",
    "Unix timestamp of the last Polymarket WebSocket message.",
)

polymarket_last_message_age_seconds = Gauge(
    "mdp_polymarket_last_message_age_seconds",
    "Seconds since the last Polymarket WebSocket message.",
)

polymarket_reconnect_total = Counter(
    "mdp_polymarket_reconnect_total",
    "Total Polymarket WebSocket reconnect attempts.",
)

polymarket_errors_total = Counter(
    "mdp_polymarket_errors_total",
    "Total Polymarket collector errors.",
    ["reason"],
)

polymarket_messages_total = Counter(
    "mdp_polymarket_messages_total",
    "Total Polymarket WebSocket messages received.",
    ["event_type"],
)

polymarket_active_subscriptions = Gauge(
    "mdp_polymarket_active_subscriptions",
    "Number of active Polymarket asset subscriptions.",
)

polymarket_active_market_slug = Gauge(
    "mdp_polymarket_active_market_window_timestamp",
    "Unix timestamp of the active BTC 15m market window.",
)

polymarket_market_rotations_total = Counter(
    "mdp_polymarket_market_rotations_total",
    "Total BTC 15m market rotations handled.",
)

polymarket_implied_prob_up = Gauge(
    "mdp_polymarket_implied_prob_up",
    "Latest implied probability for the Up outcome (0-1).",
)

polymarket_spread_up = Gauge(
    "mdp_polymarket_spread_up",
    "Latest bid-ask spread for the Up outcome.",
)

polymarket_seconds_to_expiry = Gauge(
    "mdp_polymarket_seconds_to_expiry",
    "Seconds until the active BTC 15m market expires.",
)

polymarket_yes_no_deviation = Gauge(
    "mdp_polymarket_yes_no_deviation",
    "Absolute deviation of Up+Down implied probabilities from 1.0.",
)

polymarket_derived_stale_seconds = Gauge(
    "mdp_polymarket_derived_stale_seconds",
    "Seconds since the latest derived Polymarket price row was produced.",
)

polymarket_top_of_book_liquidity_up = Gauge(
    "mdp_polymarket_top_of_book_liquidity_up",
    "Top-of-book bid+ask liquidity for the Up outcome.",
)

cross_btc_price = Gauge(
    "mdp_cross_btc_price",
    "Latest Binance BTCUSDT perp trade price aligned to Polymarket derived rows.",
)

cross_btc_return_bps = Gauge(
    "mdp_cross_btc_return_bps",
    "BTC return in basis points since the active 15m window open.",
)

cross_perp_implied_up = Gauge(
    "mdp_cross_perp_implied_up",
    "Heuristic Up probability implied by Binance perp price vs window open.",
)

cross_pm_perp_divergence = Gauge(
    "mdp_cross_pm_perp_divergence",
    "Polymarket implied Up probability minus perp-implied Up probability.",
)
