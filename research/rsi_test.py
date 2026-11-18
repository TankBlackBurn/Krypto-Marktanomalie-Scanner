"""
rsi_test.py -- do Ultimate RSI and RSI divergence predict anything?

Same methodology as pattern_test.py / key_level_test.py / funding_test.py /
depth_test.py: classify EVERY candle across every liquid pair, then compare
each variant's forward returns against the base rate of all candles. Six
hypotheses have now been through this harness and come back null. That is
not a reason to skip the test -- it is the reason to run it.

    python -m research.rsi_test                 all three timeframes
    python -m research.rsi_test --tf 15m        one timeframe only
    python -m research.rsi_test --forward 8     forward window in candles

WHAT IS TESTED
  1. Ultimate RSI level bands (oversold / neutral / overbought)
  2. Ultimate RSI crossing its signal line
  3. Regular bullish divergence, on both Ultimate RSI and classic RSI
  4. Divergence COMBINED with the bot's existing volume trigger -- the
     question that actually matters, since the bot already fires on volume
     and the only useful addition is one that improves those entries

TIMEFRAMES
15m matches the live interval. 1h and 4h are tested because divergence is
conventionally held to be more reliable on higher timeframes; if that is
true it should show up as a larger lift there.

LOOKAHEAD
Divergences are entered at `confirmed_at` (pivot + right bars), never at the
pivot itself. A pivot is not knowable until `right` bars after it forms.
Scoring at the pivot would read the future and produce a result that cannot
be reproduced live -- which is the single easiest way to manufacture a fake
edge in a test like this.
"""

import asyncio
import json
import os
import statistics
import sys
from collections import defaultdict

import aiohttp

from data_pipeline import (
    WeexFuturesPipeline,
    K_HIGH, K_LOW, K_CLOSE, K_QUOTE_VOL,
)
from research.rsi_indicators import ultimate_rsi, classic_rsi, find_divergences

TIMEFRAMES = ["15m", "1h", "4h"]
CONCURRENCY = 12
BIG_MOVE_PCT = 3.0
MIN_HISTORY = 60
PIVOT_LEFT = 5
PIVOT_RIGHT = 5

URSI_LENGTH = 14
URSI_SMOOTH = 14


def load_config():
    if os.path.exists("config.json"):
        with open("config.json", "r") as f:
            return json.load(f)
    return {}


class RsiTest:
    def __init__(self, config, timeframe, forward):
        self.p = WeexFuturesPipeline(config)
        self.timeframe = timeframe
        self.forward = forward
        self.samples = defaultdict(list)        # label -> max forward gain %
        self.forward_close = defaultdict(list)  # label -> forward close return %

    def _record(self, label, entry, future):
        if not future or entry <= 0:
            return
        peak = max(float(x[K_HIGH]) for x in future)
        final = float(future[-1][K_CLOSE])
        self.samples[label].append((peak - entry) / entry * 100.0)
        self.forward_close[label].append((final - entry) / entry * 100.0)

    async def run_symbol(self, session, symbol):
        try:
            url = (f"{self.p.klines_url}?symbol={symbol}"
                   f"&interval={self.timeframe}&limit=200")
            async with session.get(url, timeout=self.p.request_timeout) as r:
                if r.status != 200:
                    return
                rows = self.p._normalize_klines(await r.json())
        except Exception:
            return

        if len(rows) < MIN_HISTORY + self.forward + 10:
            return

        _, closed = self.p._split_live(rows)
        if len(closed) < MIN_HISTORY + self.forward + 10:
            return

        closes = [float(c[K_CLOSE]) for c in closed]
        highs = [float(c[K_HIGH]) for c in closed]
        lows = [float(c[K_LOW]) for c in closed]
        vols = [float(c[K_QUOTE_VOL]) for c in closed]

        arsi, asignal = ultimate_rsi(closes, URSI_LENGTH, URSI_SMOOTH)
        crsi = classic_rsi(closes, 14)

        # ---- level bands and signal-line crosses, evaluated per candle ----
        for i in range(MIN_HISTORY, len(closed) - self.forward):
            hist_vol = vols[max(0, i - 50):i]
            if len(hist_vol) < 40:
                continue
            avg_vol = sum(hist_vol) / len(hist_vol)
            if avg_vol <= 0 or avg_vol * 4 < self.p.min_volume_usd:
                continue      # same tradeability floor as live

            entry = closes[i]
            future = closed[i + 1: i + 1 + self.forward]
            velocity = vols[i] / avg_vol

            self._record("ALL CANDLES (base rate)", entry, future)

            v = arsi[i]
            if v is not None:
                if v <= 30:
                    self._record("uRSI <= 30 (oversold)", entry, future)
                elif v <= 45:
                    self._record("uRSI 30-45", entry, future)
                elif v < 55:
                    self._record("uRSI 45-55 (neutral)", entry, future)
                elif v < 70:
                    self._record("uRSI 55-70", entry, future)
                else:
                    self._record("uRSI >= 70 (overbought)", entry, future)

                # crossing up through the signal line
                prev, prev_sig = arsi[i - 1], asignal[i - 1]
                sig = asignal[i]
                if (prev is not None and sig is not None and prev_sig is not None
                        and prev <= prev_sig and v > sig):
                    self._record("uRSI crosses ABOVE signal", entry, future)
                    if velocity >= self.p.breakout_velocity:
                        self._record("uRSI cross above + volume >= 2x",
                                     entry, future)

            # the bot's existing trigger, for direct comparison
            if velocity >= self.p.breakout_velocity:
                self._record("volume >= 2x (bot's current trigger)",
                             entry, future)

        # ---- divergences, entered only at confirmation ----
        for osc, tag in ((arsi, "uRSI"), (crsi, "classic RSI")):
            divs = find_divergences(lows, highs, osc,
                                    left=PIVOT_LEFT, right=PIVOT_RIGHT)
            for d in divs:
                idx = d["confirmed_at"]           # NOT pivot_index
                if idx < MIN_HISTORY or idx >= len(closed) - self.forward:
                    continue
                hist_vol = vols[max(0, idx - 50):idx]
                if len(hist_vol) < 40:
                    continue
                avg_vol = sum(hist_vol) / len(hist_vol)
                if avg_vol <= 0 or avg_vol * 4 < self.p.min_volume_usd:
                    continue

                entry = closes[idx]
                future = closed[idx + 1: idx + 1 + self.forward]
                velocity = vols[idx] / avg_vol

                if d["kind"] == "bullish":
                    self._record(f"{tag} bullish divergence", entry, future)
                    if velocity >= self.p.breakout_velocity:
                        self._record(f"{tag} bullish div + volume >= 2x",
                                     entry, future)
                else:
                    self._record(f"{tag} bearish divergence", entry, future)

    async def run(self):
        connector = aiohttp.TCPConnector(limit=40)
        async with aiohttp.ClientSession(connector=connector) as session:
            symbols = await self.p.fetch_active_futures_pairs(session)
            if not symbols:
                print("No symbols returned.")
                return False

            print(f"[{self.timeframe}] scanning {len(symbols)} pairs...")
            sem = asyncio.Semaphore(CONCURRENCY)

            async def bound(s):
                async with sem:
                    await self.run_symbol(session, s)

            done = 0
            for start in range(0, len(symbols), 60):
                chunk = symbols[start:start + 60]
                await asyncio.gather(*(bound(s) for s in chunk))
                done += len(chunk)
                print(f"  ...{done}/{len(symbols)}", end="\r")
            print(" " * 40, end="\r")
        return True

    def report(self):
        base = self.samples.get("ALL CANDLES (base rate)", [])
        if not base:
            print(f"[{self.timeframe}] no candles collected.\n")
            return

        base_big = sum(1 for g in base if g >= BIG_MOVE_PCT) / len(base)
        base_fwd = statistics.mean(
            self.forward_close["ALL CANDLES (base rate)"])

        print("=" * 82)
        print(f"TIMEFRAME {self.timeframe}   forward window {self.forward} candles"
              f"   big move = +{BIG_MOVE_PCT}%")
        print("=" * 82)
        print(f"{'variant':<40}{'n':>7}{'>3% up':>9}{'lift':>8}{'mean fwd':>11}")
        print("-" * 82)

        order = [
            "ALL CANDLES (base rate)",
            "uRSI <= 30 (oversold)",
            "uRSI 30-45",
            "uRSI 45-55 (neutral)",
            "uRSI 55-70",
            "uRSI >= 70 (overbought)",
            "uRSI crosses ABOVE signal",
            "uRSI bullish divergence",
            "classic RSI bullish divergence",
            "uRSI bearish divergence",
            "classic RSI bearish divergence",
            "volume >= 2x (bot's current trigger)",
            "uRSI cross above + volume >= 2x",
            "uRSI bullish div + volume >= 2x",
            "classic RSI bullish div + volume >= 2x",
        ]

        for label in order:
            vals = self.samples.get(label)
            if not vals or len(vals) < 30:
                if vals:
                    print(f"{label:<40}{len(vals):>7}   too few to judge")
                continue
            big = sum(1 for g in vals if g >= BIG_MOVE_PCT) / len(vals)
            lift = big / base_big if base_big > 0 else 0.0
            fwd = statistics.mean(self.forward_close[label])
            flag = ""
            if label != "ALL CANDLES (base rate)" and lift >= 1.3 and fwd > base_fwd:
                flag = "  <-- above bar"
            print(f"{label:<40}{len(vals):>7}{big*100:>8.1f}%"
                  f"{lift:>7.2f}x{fwd:>+10.2f}%{flag}")

        print("-" * 82)
        print(f"base rate: {base_big*100:.1f}% of candles see +{BIG_MOVE_PCT}% "
              f"within {self.forward} candles | mean fwd {base_fwd:+.2f}%")
        print()


async def main():
    config = load_config()
    tfs = TIMEFRAMES
    forward = 8

    if "--tf" in sys.argv:
        tfs = [sys.argv[sys.argv.index("--tf") + 1]]
    if "--forward" in sys.argv:
        forward = int(sys.argv[sys.argv.index("--forward") + 1])

    print("=" * 82)
    print("ULTIMATE RSI / DIVERGENCE BASE-RATE TEST")
    print("=" * 82)
    print("Divergences are entered at confirmation (pivot + 5 bars), never at")
    print("the pivot -- a pivot is not knowable until 5 bars after it forms.")
    print("Compare every variant against 'volume >= 2x', the bot's current")
    print("trigger. Beating the base rate is not enough; to earn a place in")
    print("the entry logic a variant has to beat what is already there.")
    print("=" * 82 + "\n")

    for tf in tfs:
        t = RsiTest(config, tf, forward)
        ok = await t.run()
        if ok:
            t.report()

    print("=" * 82)
    print("HOW TO READ THIS")
    print("  lift = variant's big-move rate / base rate. 1.0x means the")
    print("  variant tells you nothing. The bar for wiring anything into")
    print("  entries: lift >= 1.3x AND mean forward return above base AND")
    print("  it must beat the plain volume trigger already in the bot.")
    print("  Anything under that is noise dressed up as a signal.")
    print("=" * 82)


if __name__ == "__main__":
    asyncio.run(main())