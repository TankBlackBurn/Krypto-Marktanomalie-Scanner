"""
rsi_indicators.py -- Ultimate RSI and RSI divergence detection.

Pure computation, no network and no bot wiring. Imported by rsi_test.py to
measure whether these actually predict anything, and available to
data_pipeline.py afterwards IF the test says they earn a place.

ULTIMATE RSI (LuxAlgo formulation)
Classic RSI measures average gain vs average loss over a fixed lookback.
Ultimate RSI instead measures movement relative to the ROLLING RANGE: when
a new high is made, the full range counts as upward movement rather than
just the bar-to-bar delta. The effect is that it responds to genuine range
expansion rather than to a series of small green bars, which is the usual
complaint about RSI on crypto -- it pins to 70+ during grinding drift and
stops distinguishing anything.

  upper = highest(src, length)
  lower = lowest(src, length)
  r     = upper - lower
  d     = src - src[1]
  diff  = r        if upper > upper[1]     (new high: full range is up-move)
          -r       if lower < lower[1]     (new low: full range is down-move)
          d        otherwise               (inside range: plain delta)
  num   = rma(diff, length)
  den   = rma(abs(diff), length)
  arsi  = num / den * 50 + 50

RSI DIVERGENCE
Regular bullish: price makes a LOWER low, RSI makes a HIGHER low
                 -> selling is losing force, potential reversal up
Regular bearish: price makes a HIGHER high, RSI makes a LOWER high
                 -> buying is losing force, potential reversal down

Detection uses confirmed pivots: a pivot low at index i requires i to be
the lowest point within `left` bars before and `right` bars after. This
means a pivot is only confirmed `right` bars AFTER it happens -- that lag
is real and unavoidable, and it is deliberately preserved here rather than
quietly ignored. A backtest that treats pivots as known at the moment they
form is looking into the future and will produce results that cannot be
reproduced live.
"""

from typing import List, Optional, Sequence


# --------------------------------------------------------------------------
# primitives
# --------------------------------------------------------------------------

def rma(values: Sequence[float], length: int) -> List[Optional[float]]:
    """
    Wilder's smoothing (running moving average), the same one classic RSI
    uses. Seeded with a simple mean of the first `length` values so the
    series does not carry startup bias forward.
    """
    out: List[Optional[float]] = [None] * len(values)
    if len(values) < length or length <= 0:
        return out
    seed = sum(values[:length]) / length
    out[length - 1] = seed
    prev = seed
    alpha = 1.0 / length
    for i in range(length, len(values)):
        prev = prev + alpha * (values[i] - prev)
        out[i] = prev
    return out


def ema(values: Sequence[float], length: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if len(values) < length or length <= 0:
        return out
    seed = sum(values[:length]) / length
    out[length - 1] = seed
    prev = seed
    k = 2.0 / (length + 1.0)
    for i in range(length, len(values)):
        prev = values[i] * k + prev * (1 - k)
        out[i] = prev
    return out


def classic_rsi(closes: Sequence[float], length: int = 14) -> List[Optional[float]]:
    """Standard Wilder RSI. Kept so the test can compare Ultimate RSI
    against the ordinary version rather than assuming it is better."""
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    if n < length + 1:
        return out
    gains, losses = [], []
    for i in range(1, n):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = rma(gains, length)
    avg_l = rma(losses, length)
    for i in range(len(gains)):
        g, l = avg_g[i], avg_l[i]
        if g is None or l is None:
            continue
        if l == 0:
            out[i + 1] = 100.0
        else:
            rs = g / l
            out[i + 1] = 100.0 - (100.0 / (1.0 + rs))
    return out


def ultimate_rsi(closes: Sequence[float], length: int = 14,
                 smooth: int = 14) -> tuple:
    """
    Returns (arsi, signal). Both lists are the same length as `closes`,
    with None where there is not yet enough history.
    """
    n = len(closes)
    arsi: List[Optional[float]] = [None] * n
    if n < length + 1:
        return arsi, [None] * n

    diffs: List[float] = []
    diff_index: List[int] = []   # which candle each diff belongs to

    for i in range(length, n):
        window = closes[i - length + 1: i + 1]
        prev_window = closes[i - length: i]
        upper, lower = max(window), min(window)
        prev_upper, prev_lower = max(prev_window), min(prev_window)
        r = upper - lower
        d = closes[i] - closes[i - 1]

        if upper > prev_upper:
            diff = r
        elif lower < prev_lower:
            diff = -r
        else:
            diff = d

        diffs.append(diff)
        diff_index.append(i)

    num = rma(diffs, length)
    den = rma([abs(x) for x in diffs], length)

    for k, idx in enumerate(diff_index):
        nu, de = num[k], den[k]
        if nu is None or de is None or de == 0:
            continue
        arsi[idx] = (nu / de) * 50.0 + 50.0

    vals = [v for v in arsi if v is not None]
    signal: List[Optional[float]] = [None] * n
    if len(vals) >= smooth:
        first = next(i for i, v in enumerate(arsi) if v is not None)
        sm = ema(vals, smooth)
        for offset, v in enumerate(sm):
            signal[first + offset] = v

    return arsi, signal


# --------------------------------------------------------------------------
# pivots and divergence
# --------------------------------------------------------------------------

def pivot_lows(values: Sequence[Optional[float]], left: int = 5,
               right: int = 5) -> List[int]:
    """
    Indices where values[i] is the strict minimum of its neighbourhood.
    Confirmed only `right` bars later -- callers must respect that lag.
    """
    out = []
    n = len(values)
    for i in range(left, n - right):
        v = values[i]
        if v is None:
            continue
        window = values[i - left: i + right + 1]
        if any(x is None for x in window):
            continue
        if v == min(window) and window.count(v) == 1:
            out.append(i)
    return out


def pivot_highs(values: Sequence[Optional[float]], left: int = 5,
                right: int = 5) -> List[int]:
    out = []
    n = len(values)
    for i in range(left, n - right):
        v = values[i]
        if v is None:
            continue
        window = values[i - left: i + right + 1]
        if any(x is None for x in window):
            continue
        if v == max(window) and window.count(v) == 1:
            out.append(i)
    return out


def _nearest_pivot(target: int, pivots: Sequence[int],
                   tolerance: int) -> Optional[int]:
    """
    Closest pivot index within +/- tolerance of `target`, or None.

    Oscillator pivots rarely land on the exact same bar as price pivots --
    momentum usually turns a bar or two before or after price does. An
    exact-index match therefore almost never fires, which would produce a
    null result caused by the detector rather than by the market. A small
    tolerance window is what makes this measure the real thing.
    """
    best, best_dist = None, tolerance + 1
    for p in pivots:
        d = abs(p - target)
        if d <= tolerance and d < best_dist:
            best, best_dist = p, d
    return best


def find_divergences(lows: Sequence[float], highs: Sequence[float],
                     osc: Sequence[Optional[float]],
                     left: int = 5, right: int = 5,
                     max_bars_between: int = 60,
                     tolerance: int = 3) -> List[dict]:
    """
    Regular divergences between price pivots and oscillator pivots.

    Returns dicts with:
      kind          'bullish' | 'bearish'
      pivot_index   candle index of the SECOND price pivot
      confirmed_at  candle index where it becomes knowable
      prev_index    candle index of the first price pivot

    confirmed_at accounts for BOTH the price pivot and the matched
    oscillator pivot -- whichever confirms later is when the divergence is
    actually knowable. Acting earlier would read the future.
    """
    results = []

    p_lows = pivot_lows(lows, left, right)
    o_lows = pivot_lows(osc, left, right)
    for a, b in zip(p_lows, p_lows[1:]):
        if b - a > max_bars_between:
            continue
        oa = _nearest_pivot(a, o_lows, tolerance)
        ob = _nearest_pivot(b, o_lows, tolerance)
        if oa is None or ob is None or oa == ob:
            continue
        if osc[oa] is None or osc[ob] is None:
            continue
        # price lower low, oscillator higher low
        if lows[b] < lows[a] and osc[ob] > osc[oa]:
            results.append({
                "kind": "bullish",
                "prev_index": a,
                "pivot_index": b,
                "confirmed_at": max(b, ob) + right,
                "price_delta_pct": (lows[b] - lows[a]) / lows[a] * 100.0,
                "osc_delta": osc[ob] - osc[oa],
            })

    p_highs = pivot_highs(highs, left, right)
    o_highs = pivot_highs(osc, left, right)
    for a, b in zip(p_highs, p_highs[1:]):
        if b - a > max_bars_between:
            continue
        oa = _nearest_pivot(a, o_highs, tolerance)
        ob = _nearest_pivot(b, o_highs, tolerance)
        if oa is None or ob is None or oa == ob:
            continue
        if osc[oa] is None or osc[ob] is None:
            continue
        # price higher high, oscillator lower high
        if highs[b] > highs[a] and osc[ob] < osc[oa]:
            results.append({
                "kind": "bearish",
                "prev_index": a,
                "pivot_index": b,
                "confirmed_at": max(b, ob) + right,
                "price_delta_pct": (highs[b] - highs[a]) / highs[a] * 100.0,
                "osc_delta": osc[ob] - osc[oa],
            })

    return results