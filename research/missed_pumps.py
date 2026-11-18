"""
missed_pumps.py -- the reverse backtest.

backtest.py asks "what did the bot fire on, and did it work?"
This asks the more useful question: "what actually pumped, and what did
the bot think about it at the moment before it went?"

For every pair it finds the largest forward move in the last ~25 hours,
ranks by size, then replays the pipeline at the candle immediately before
that move began and reports the exact stage that rejected it.

    python -m research.missed_pumps

If the biggest movers were all killed by one filter, that filter is your
problem and the output will name it.
"""

import asyncio
import json
import os
import aiohttp
from collections import Counter

from data_pipeline import (
    WeexFuturesPipeline,
    K_OPEN, K_HIGH, K_LOW, K_CLOSE, K_QUOTE_VOL, K_TRADES,
)

FORWARD = 8        # candles to measure the move over (2 hours)
MIN_HISTORY = 55
TOP_N = 25
CONCURRENCY = 12


def load_config():
    if os.path.exists("config.json"):
        with open("config.json", "r") as f:
            return json.load(f)
    print("WARNING: config.json not found -- running on pipeline defaults.\n")
    return {}


class MissedPumpFinder:
    def __init__(self, config):
        # BUG FIX: this used to pass {} so config.json was never read and
        # every run silently used hardcoded defaults.
        self.p = WeexFuturesPipeline(config)
        self.movers = []

    # ------------------------------------------------------------ diagnosis

    def diagnose(self, closed, i):
        """Replay every gate at candle i. Return (stage, detail)."""
        hist_slice = closed[: i + 1]
        conf = hist_slice[-1]
        history = hist_slice[-51:-1]
        if len(history) < 40:
            return "insufficient history", ""

        o, h, l, c = (float(conf[k]) for k in (K_OPEN, K_HIGH, K_LOW, K_CLOSE))
        conf_quote = float(conf[K_QUOTE_VOL])
        conf_trades = float(conf[K_TRADES])

        avg_quote = sum(float(x[K_QUOTE_VOL]) for x in history) / len(history)
        if avg_quote <= 0:
            return "no volume data", ""

        hourly = avg_quote * 4
        if hourly < self.p.min_volume_usd:
            return "liquidity floor", f"${hourly:,.0f}/h < ${self.p.min_volume_usd:,.0f}/h"

        velocity = conf_quote / avg_quote
        avg_trades = sum(float(x[K_TRADES]) for x in history) / len(history)
        trade_exp = conf_trades / avg_trades if avg_trades > 0 else 1.0

        lookback = getattr(self.p, "extension_lookback_candles", 96)
        window_low = min(float(x[K_LOW]) for x in hist_slice[-lookback:])
        extension = (c - window_low) / window_low * 100.0
        if extension > self.p.max_extension_pct:
            return "extension filter", (f"already +{extension:.1f}% off {lookback}-candle low "
                                        f"(cap {self.p.max_extension_pct}%)")

        # Which detector conditions failed?
        buy_ratio = self.p._buy_ratio(conf)
        lookback = hist_slice[-(self.p.breakout_lookback + 1):-1]
        resistance = max(float(x[K_HIGH]) for x in lookback) if lookback else 0.0
        size = h - l
        strong = ((c - l) / size) if size > 0 else 0.0

        breakout = self.p._detect_breakout(hist_slice, conf, o, h, l, c, velocity, trade_exp)
        ramp = self.p._detect_ramp(hist_slice, c, velocity, trade_exp)
        result = breakout or ramp

        if result is None:
            fails = []
            if c <= o:
                fails.append("red candle")
            if strong < 0.55:
                fails.append(f"weak close {strong:.2f}")
            if c <= resistance:
                fails.append(f"no breakout ({c:.6g} vs {resistance:.6g})")
            if velocity < self.p.breakout_velocity:
                fails.append(f"velocity {velocity:.1f}x < {self.p.breakout_velocity}x")
            if buy_ratio < self.p.breakout_buy_ratio:
                fails.append(f"taker buys {buy_ratio:.2f} < {self.p.breakout_buy_ratio}")
            pooled = self.p._pooled_buy_ratio(hist_slice[-self.p.ramp_window:])
            if pooled < self.p.ramp_buy_ratio:
                fails.append(f"pooled buys {pooled:.2f} < {self.p.ramp_buy_ratio}")
            return "no detector match", "; ".join(fails[:3])

        score, sig_type, sl_anchor, _ = result

        sl = sl_anchor * 0.995
        if c <= sl:
            return "stop above entry", ""
        sl_pct = (c - sl) / c * 100.0
        if sl_pct > self.p.max_sl_percent:
            return "stop too wide", f"{sl_pct:.2f}% > cap {self.p.max_sl_percent}%"
        if sl_pct < self.p.min_sl_percent:
            return "stop too narrow", f"{sl_pct:.2f}% < floor {self.p.min_sl_percent}%"

        if score < self.p.min_score:
            return "below score gate", f"scored {score:.0f}, gate {self.p.min_score:.0f}"

        return "PASSED", f"score {score:.0f}, {sig_type}"

    # -------------------------------------------------------------- scanning

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

            best_move, best_i = 0.0, None
            for i in range(MIN_HISTORY, len(closed) - FORWARD):
                entry = float(closed[i][K_CLOSE])
                if entry <= 0:
                    continue
                peak = max(float(x[K_HIGH]) for x in closed[i + 1: i + 1 + FORWARD])
                move = (peak - entry) / entry * 100.0
                if move > best_move:
                    best_move, best_i = move, i

            if best_i is None or best_move < 3.0:
                return

            stage, detail = self.diagnose(closed, best_i)
            self.movers.append({
                "symbol": symbol,
                "move": best_move,
                "stage": stage,
                "detail": detail,
            })
        except Exception:
            pass

    async def run(self):
        connector = aiohttp.TCPConnector(limit=40)
        async with aiohttp.ClientSession(connector=connector) as session:
            symbols = await self.p.fetch_active_futures_pairs(session)
            if not symbols:
                print("No symbols returned.")
                return

            print(f"CONFIG: interval={self.p.interval} | "
                  f"floor=${self.p.min_volume_usd:,.0f}/h | "
                  f"ext_cap={self.p.max_extension_pct}% | "
                  f"gate={self.p.min_score}")
            print(f"Finding the biggest 2-hour moves across {len(symbols)} pairs")
            print("in the last ~25h, then replaying the pipeline at the candle")
            print("immediately before each move began...\n")

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
        print("=" * 78)
        print("BIGGEST ACTUAL MOVERS -- AND WHY THE BOT DID OR DIDN'T CATCH THEM")
        print("=" * 78)

        if not self.movers:
            print("\nNo pair moved more than 3% in any 2-hour window in the last 25h.")
            print("That would be an unusually flat market -- worth re-running later.")
            return

        self.movers.sort(key=lambda m: m["move"], reverse=True)
        top = self.movers[:TOP_N]

        print(f"\nPairs that moved >3% in a 2h window: {len(self.movers)}")
        print(f"Showing top {len(top)}:\n")
        print(f"  {'symbol':<16}{'move':>9}  {'verdict':<20} detail")
        print("  " + "-" * 74)
        for m in top:
            mark = "OK " if m["stage"] == "PASSED" else "   "
            print(f"{mark}{m['symbol']:<16}{m['move']:>8.1f}%  {m['stage']:<20} {m['detail'][:34]}")

        print("\n" + "-" * 78)
        print("REJECTION BREAKDOWN across all >3% movers:\n")
        stages = Counter(m["stage"] for m in self.movers)
        total = len(self.movers)
        for stage, n in stages.most_common():
            bar = "#" * int(n / total * 40)
            print(f"  {stage:<22} {n:>4} ({n/total*100:>4.0f}%) {bar}")

        caught = stages.get("PASSED", 0)
        print(f"\n  Caught: {caught}/{total} ({caught/total*100:.1f}%) of real movers.")

        dominant = stages.most_common(1)[0]
        if dominant[0] != "PASSED":
            print(f"\n  Dominant rejection: '{dominant[0]}' killed {dominant[1]} of {total}.")
            print("  That is the single filter standing between you and these moves.")

        print("=" * 78)


if __name__ == "__main__":
    asyncio.run(MissedPumpFinder(load_config()).run())