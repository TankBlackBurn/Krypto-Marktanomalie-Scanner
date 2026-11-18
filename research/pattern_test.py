"""
pattern_test.py -- does the capitulation candle actually predict anything?

missed_pumps.py kept showing the same thing: the candle before a big move is
red, closing at or near its low. Tempting conclusion -- build a detector for
that pattern.

The problem is that missed_pumps.py ONLY looks at coins that already pumped.
It measures the pattern on winners exclusively. It cannot show you the
hundreds of coins that printed an identical candle and kept falling. Acting
on that observation without this test is how people build falling-knife
catchers that look brilliant in review and lose money live.

This runs the unbiased version: classify EVERY candle across every liquid
pair, then measure what happened next. Compare the pattern's forward returns
against the base rate of all candles.

    python -m research.pattern_test

The number that matters is LIFT. If 4% of all candles are followed by a >3%
move, and 4.1% of capitulation candles are, the pattern is worthless no
matter how compelling it looked on the winners. Lift needs to be
substantially above 1.0x to mean anything, and even then the edge has to
survive costs.
"""

import asyncio
import json
import os
import aiohttp
import statistics
from collections import defaultdict

from data_pipeline import (
    WeexFuturesPipeline,
    K_OPEN, K_HIGH, K_LOW, K_CLOSE, K_QUOTE_VOL,
)

FORWARD = 8
MIN_HISTORY = 55
CONCURRENCY = 12
BIG_MOVE_PCT = 3.0


def load_config():
    if os.path.exists("config.json"):
        with open("config.json", "r") as f:
            return json.load(f)
    return {}


# Each variant is (label, predicate). The predicate receives a dict of
# precomputed candle features and returns True if the candle qualifies.
VARIANTS = [
    ("ALL CANDLES (base rate)",
     lambda f: True),

    ("red, close in bottom 25% of range",
     lambda f: f["red"] and f["close_pos"] <= 0.25),

    ("red, close in bottom 10% of range",
     lambda f: f["red"] and f["close_pos"] <= 0.10),

    ("red, bottom 25%, volume >= 2x avg",
     lambda f: f["red"] and f["close_pos"] <= 0.25 and f["velocity"] >= 2.0),

    ("red, bottom 25%, volume >= 3x avg",
     lambda f: f["red"] and f["close_pos"] <= 0.25 and f["velocity"] >= 3.0),

    ("red, bottom 25%, vol >= 2x, near 24h low",
     lambda f: f["red"] and f["close_pos"] <= 0.25 and f["velocity"] >= 2.0
               and f["pct_above_low"] <= 5.0),

    ("long lower wick (>=60% of range)",
     lambda f: f["lower_wick_frac"] >= 0.60),

    ("long lower wick + volume >= 2x",
     lambda f: f["lower_wick_frac"] >= 0.60 and f["velocity"] >= 2.0),

    ("--- for contrast: green, strong close, vol >= 2x",
     lambda f: not f["red"] and f["close_pos"] >= 0.75 and f["velocity"] >= 2.0),
]


class PatternTest:
    def __init__(self, config):
        self.p = WeexFuturesPipeline(config)
        # label -> list of forward max-gain percentages
        self.samples = defaultdict(list)
        self.forward_close = defaultdict(list)

    @staticmethod
    def features(conf, avg_quote, window_low):
        o = float(conf[K_OPEN])
        h = float(conf[K_HIGH])
        l = float(conf[K_LOW])
        c = float(conf[K_CLOSE])
        rng = h - l
        if rng <= 0 or c <= 0:
            return None
        body_low = min(o, c)
        return {
            "red": c < o,
            "close_pos": (c - l) / rng,
            "lower_wick_frac": (body_low - l) / rng,
            "velocity": float(conf[K_QUOTE_VOL]) / avg_quote if avg_quote > 0 else 0.0,
            "pct_above_low": (c - window_low) / window_low * 100.0 if window_low > 0 else 0.0,
            "close": c,
        }

    async def run_symbol(self, session, symbol):
        try:
            url = (f"{self.p.klines_url}?symbol={symbol}"
                   f"&interval={self.p.interval}&limit={self.p.candle_limit}")
            async with session.get(url, timeout=self.p.request_timeout) as r:
                if r.status != 200:
                    return
                rows = self.p._normalize_klines(await r.json())
            if len(rows) < MIN_HISTORY + FORWARD + 5:
                return

            _, closed = self.p._split_live(rows)

            for i in range(MIN_HISTORY, len(closed) - FORWARD):
                hist = closed[: i + 1]
                history = hist[-51:-1]
                if len(history) < 40:
                    continue

                avg_quote = sum(float(x[K_QUOTE_VOL]) for x in history) / len(history)
                if avg_quote <= 0:
                    continue
                # Same tradeability floor as live. No point measuring a
                # pattern on markets you could never execute in.
                if avg_quote * 4 < self.p.min_volume_usd:
                    continue

                window_low = min(float(x[K_LOW]) for x in hist[-96:])
                f = self.features(closed[i], avg_quote, window_low)
                if f is None:
                    continue

                future = closed[i + 1: i + 1 + FORWARD]
                if not future:
                    continue
                entry = f["close"]
                peak = max(float(x[K_HIGH]) for x in future)
                final = float(future[-1][K_CLOSE])

                max_gain = (peak - entry) / entry * 100.0
                close_ret = (final - entry) / entry * 100.0

                for label, pred in VARIANTS:
                    try:
                        if pred(f):
                            self.samples[label].append(max_gain)
                            self.forward_close[label].append(close_ret)
                    except Exception:
                        pass
        except Exception:
            pass

    async def run(self):
        print("=" * 76)
        print("PATTERN BASE-RATE TEST")
        print("=" * 76)
        print(f"  interval {self.p.interval} | liquidity floor "
              f"${self.p.min_volume_usd:,.0f}/h | forward window {FORWARD} candles")
        print("=" * 76 + "\n")

        connector = aiohttp.TCPConnector(limit=40)
        async with aiohttp.ClientSession(connector=connector) as session:
            symbols = await self.p.fetch_active_futures_pairs(session)
            if not symbols:
                print("No symbols returned.")
                return

            print(f"Classifying every candle across {len(symbols)} pairs...\n")
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
        self.report()

    def report(self):
        base_label = VARIANTS[0][0]
        base = self.samples.get(base_label, [])
        if not base:
            print("No candles collected. Check the liquidity floor.")
            return

        base_big = sum(1 for g in base if g >= BIG_MOVE_PCT) / len(base)
        base_mean_close = statistics.mean(self.forward_close[base_label])

        print("=" * 76)
        print(f"RESULTS  (forward window = {FORWARD} candles)")
        print("=" * 76)
        print(f"\n{'pattern':<44}{'n':>8}{'>3% move':>10}{'lift':>7}{'fwd ret':>9}")
        print("-" * 76)

        for label, _ in VARIANTS:
            sample = self.samples.get(label, [])
            if len(sample) < 20:
                print(f"{label:<44}{len(sample):>8}     too few to judge")
                continue

            big_rate = sum(1 for g in sample if g >= BIG_MOVE_PCT) / len(sample)
            lift = big_rate / base_big if base_big > 0 else 0.0
            mean_close = statistics.mean(self.forward_close[label])

            marker = ""
            if label != base_label:
                if lift >= 1.5 and mean_close > base_mean_close:
                    marker = "  <-- worth investigating"
                elif lift < 1.1:
                    marker = "  (no lift)"

            print(f"{label:<44}{len(sample):>8}{big_rate*100:>9.1f}%"
                  f"{lift:>6.2f}x{mean_close:>+8.2f}%{marker}")

        print("-" * 76)
        print(f"\nBase rate: {base_big*100:.1f}% of all candles are followed by a "
              f">{BIG_MOVE_PCT}% move")
        print(f"           within {FORWARD} candles. Mean forward return "
              f"{base_mean_close:+.2f}%.")
        print("\nHOW TO READ THIS")
        print("  lift 1.0x  = the pattern tells you nothing beyond the base rate.")
        print("  lift 1.5x+ = worth investigating, IF forward return is also")
        print("               better than base. A pattern can raise the odds of a")
        print("               big move while still losing money on average,")
        print("               because it also raises the odds of a big drop.")
        print("\n  'fwd ret' is mean return at the end of the window -- the honest")
        print("  measure. Max-gain figures flatter every pattern, because they")
        print("  assume you sell at the exact top.")
        print("\n  Nothing here is tradeable until it clears costs. At a 2% stop")
        print("  your round-trip cost is ~0.06R; the forward return has to beat")
        print("  that consistently, out of sample, before it means anything.")
        print("=" * 76)


if __name__ == "__main__":
    asyncio.run(PatternTest(load_config()).run())