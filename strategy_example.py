"""
strategy_example.py -- PLACEHOLDER detectors for the portfolio edition.

The production detection logic (signal rules, thresholds, scoring weights,
structure/level confluence and the exit tuning that goes with them) is
private and is not included in this repository.

What is here is a deliberately simple stand-in with the same interface, so
that the rest of the system -- async scanning, filtering, the AI review,
alerting, logging, backtesting and the research tooling -- runs end to end.
It is NOT the strategy, and it is not intended to be profitable.

Interface
---------
A detector returns None (no signal) or a tuple:

    (score, signal_type, stop_anchor_price, notes)

    score             0-100, used by the confluence gate
    signal_type       label stored with the signal
    stop_anchor_price price the stop is derived from
    notes             one-line human-readable explanation
"""

from typing import Optional, Tuple

# WEEX kline layout (11 fields), duplicated here to keep this module
# dependency-free and importable on its own.
K_OPEN_TIME, K_OPEN, K_HIGH, K_LOW, K_CLOSE, K_BASE_VOL = 0, 1, 2, 3, 4, 5
K_CLOSE_TIME, K_QUOTE_VOL, K_TRADES = 6, 7, 8
K_TAKER_BUY_BASE, K_TAKER_BUY_QUOTE = 9, 10

Signal = Tuple[float, str, float, str]


def _buy_ratio(candle: list) -> float:
    """Share of quote volume bought by aggressive takers (0.5 = balanced)."""
    quote = float(candle[K_QUOTE_VOL])
    if quote <= 0:
        return 0.5
    return float(candle[K_TAKER_BUY_QUOTE]) / quote


def detect_breakout(closed, conf, o, h, l, c, velocity, trade_expansion,
                    params: dict) -> Optional[Signal]:
    """
    Placeholder rule: a green candle that closes above the highest high of
    the previous N candles, on a volume spike with net aggressive buying.

    params: lookback (int), velocity_min (float), buy_ratio_min (float)
    """
    if c <= o:
        return None

    lookback = closed[-(params["lookback"] + 1):-1]
    if len(lookback) < 10:
        return None

    resistance = max(float(x[K_HIGH]) for x in lookback)
    if c <= resistance:
        return None

    buy_ratio = _buy_ratio(conf)
    if velocity < params["velocity_min"] or buy_ratio < params["buy_ratio_min"]:
        return None

    # Simple, transparent score: 50 base + up to 30 for volume + up to 20 for flow.
    score = 50.0
    score += min((velocity - params["velocity_min"]) / params["velocity_min"], 1.0) * 30.0
    score += min(max(buy_ratio - 0.5, 0.0) / 0.25, 1.0) * 20.0

    notes = (f"Placeholder breakout | {velocity:.1f}x volume | "
             f"{buy_ratio * 100:.0f}% taker buys")
    return score, "EXAMPLE_BREAKOUT_LONG", l, notes


def detect_ramp(closed, current_price, velocity, trade_expansion,
                params: dict) -> Optional[Signal]:
    """
    The production system has a second, slower detector. It is omitted from
    this edition, so this placeholder never fires.
    """
    return None
