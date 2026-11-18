"""
backtest.py -- replay the detectors over recent history across every pair.

FIXED: earlier versions instantiated WeexFuturesPipeline({}) with an empty
dict, so config.json was never read and every run silently used hardcoded
defaults. If you changed settings and the output did not move, that was why.
The config actually in use is now printed at the top of every run.

    python -m research.backtest

Limitations, stated plainly:
  - ~25h at 15m is a very small sample. Ten signals is not a track record.
  - Entry is modelled at the signal candle's close; live you enter seconds
    later at whatever the book gives you.
  - Funding is not modelled. Fees and a slippage estimate ARE, because at
    sub-1% stops they are a large fraction of R and ignoring them makes a
    losing system look breakeven.
  - Only currently-listed pairs are in the sample (survivorship bias).
"""

import asyncio
import json
import os
import aiohttp
import statistics
from collections import Counter, defaultdict

from data_pipeline import (
    WeexFuturesPipeline,
    K_OPEN, K_HIGH, K_LOW, K_CLOSE, K_QUOTE_VOL, K_TRADES,
)

FORWARD_CANDLES = 8
MIN_HISTORY = 55
CONCURRENCY = 12

# Round-trip taker cost estimate. Two market orders plus slippage.
ROUND_TRIP_COST_PCT = 0.12


def load_config():
    if os.path.exists("config.json"):
        with open("config.json", "r") as f:
            return json.load(f)
    print("WARNING: config.json not found -- running on pipeline defaults.\n")
    return {}


class Backtester:
    def __init__(self, config):
        self.p = WeexFuturesPipeline(config)
        self.signals = []
        self.near_misses = []
        self.stage_deaths = Counter()

    def show_config(self):
        print("=" * 70)
        print("CONFIG IN USE  (if this doesn't match config.json, the file isn't loading)")
        print("=" * 70)
        print(f"  interval                 {self.p.interval}  (limit {self.p.candle_limit})")
        print(f"  min_volume_usd           ${self.p.min_volume_usd:,.0f}/h")
        print(f"  max_extension_pct        {self.p.max_extension_pct}%")
        print(f"  max_sl_percent           {self.p.max_sl_percent}%")
        print(f"  breakout_velocity        {self.p.breakout_velocity}x")
        print(f"  breakout_buy_ratio       {self.p.breakout_buy_ratio}")
        print(f"  ramp_min_velocity        {self.p.ramp_min_velocity}x")
        print(f"  ramp_buy_ratio           {self.p.ramp_buy_ratio}")
        print(f"  min_confluence_score     {self.p.min_score}")
        print(f"  tp1 / tp2 R-multiples    {self.p.tp1_r} / {self.p.tp2_r}")
        print(f"  modelled round-trip cost {ROUND_TRIP_COST_PCT}%")
        print("=" * 70 + "\n")

    def evaluate_at(self, closed, i):
        hist = closed[: i + 1]
        conf = hist[-1]
        history = hist[-51:-1]
        if len(history) < 40:
            return None

        o, h, l, c = (float(conf[k]) for k in (K_OPEN, K_HIGH, K_LOW, K_CLOSE))
        avg_quote = sum(float(x[K_QUOTE_VOL]) for x in history) / len(history)
        if avg_quote <= 0:
            return None
        if avg_quote * 4 < self.p.min_volume_usd:
            self.stage_deaths["liquidity floor"] += 1
            return None

        velocity = float(conf[K_QUOTE_VOL]) / avg_quote
        avg_trades = sum(float(x[K_TRADES]) for x in history) / len(history)
        trade_exp = float(conf[K_TRADES]) / avg_trades if avg_trades > 0 else 1.0

        lookback = getattr(self.p, "extension_lookback_candles", 96)
        window_low = min(float(x[K_LOW]) for x in hist[-lookback:])
        extension = (c - window_low) / window_low * 100.0
        if extension > self.p.max_extension_pct:
            self.stage_deaths["already extended"] += 1
            return None

        result = (
            self.p._detect_breakout(hist, conf, o, h, l, c, velocity, trade_exp)
            or self.p._detect_ramp(hist, c, velocity, trade_exp)
        )
        if result is None:
            self.stage_deaths["no detector match"] += 1
            return None

        score, sig_type, sl_anchor, notes = result
        sl = sl_anchor * 0.995
        if c <= sl:
            return None
        risk = c - sl
        sl_pct = risk / c * 100.0
        if sl_pct > self.p.max_sl_percent or sl_pct < self.p.min_sl_percent:
            self.stage_deaths["stop too wide/narrow"] += 1
            return None

        return {
            "score": score, "type": sig_type, "entry": c, "sl": sl,
            "tp1": c + risk * self.p.tp1_r, "tp2": c + risk * self.p.tp2_r,
            "sl_pct": sl_pct, "velocity": velocity, "notes": notes,
        }

    def forward_outcome(self, sig, future):
        """Return (label, R_multiple, max_favourable_pct)."""
        best = sig["entry"]
        for candle in future:
            hi, lo = float(candle[K_HIGH]), float(candle[K_LOW])
            best = max(best, hi)
            # Conservative: if both are touched in one candle, assume the stop.
            if lo <= sig["sl"]:
                return "STOP", -1.0, (best - sig["entry"]) / sig["entry"] * 100.0
            if hi >= sig["tp2"]:
                return "TP2", self.p.tp2_r, (best - sig["entry"]) / sig["entry"] * 100.0
            if hi >= sig["tp1"]:
                return "TP1", self.p.tp1_r, (best - sig["entry"]) / sig["entry"] * 100.0
        # Still open: mark to market at the last close.
        final = float(future[-1][K_CLOSE]) if future else sig["entry"]
        r = (final - sig["entry"]) / (sig["entry"] - sig["sl"])
        return "OPEN", r, (best - sig["entry"]) / sig["entry"] * 100.0

    async def run_symbol(self, session, symbol):
        try:
            url = (f"{self.p.klines_url}?symbol={symbol}"
                   f"&interval={self.p.interval}&limit={self.p.candle_limit}")
            async with session.get(url, timeout=self.p.request_timeout) as r:
                if r.status != 200:
                    return
                rows = self.p._normalize_klines(await r.json())
            if len(rows) < MIN_HISTORY + FORWARD_CANDLES + 5:
                return

            _, closed = self.p._split_live(rows)
            last_fire = -999

            for i in range(MIN_HISTORY, len(closed) - FORWARD_CANDLES):
                sig = self.evaluate_at(closed, i)
                if sig is None:
                    continue
                sig["symbol"] = symbol

                if sig["score"] < self.p.min_score:
                    self.near_misses.append(sig)
                    continue

                # Per-symbol cooldown, mirroring live behaviour.
                if i - last_fire < self.p.symbol_cooldown_candles:
                    continue
                last_fire = i

                future = closed[i + 1: i + 1 + FORWARD_CANDLES]
                label, r_mult, mfe = self.forward_outcome(sig, future)
                sig["outcome"] = label
                sig["r_multiple"] = r_mult
                sig["max_gain_pct"] = mfe
                # Cost expressed in R: at a 0.7% stop, 0.12% fees = 0.17R.
                sig["cost_r"] = ROUND_TRIP_COST_PCT / sig["sl_pct"]
                sig["net_r"] = r_mult - sig["cost_r"]
                # Candle open time -- lets rolling_backtest.py dedupe the
                # same setup seen across overlapping daily runs (each run
                # covers ~25h, so consecutive days overlap by ~24h).
                sig["entry_ts"] = int(closed[i][0])
                self.signals.append(sig)
        except Exception:
            pass

    async def run(self):
        self.show_config()
        connector = aiohttp.TCPConnector(limit=40)
        async with aiohttp.ClientSession(connector=connector) as session:
            symbols = await self.p.fetch_active_futures_pairs(session)
            if not symbols:
                print("No symbols returned. Check the API before anything else.")
                return

            print(f"Replaying {self.p.candle_limit} x {self.p.interval} candles "
                  f"across {len(symbols)} pairs...\n")
            sem = asyncio.Semaphore(CONCURRENCY)

            async def bound(s):
                async with sem:
                    await self.run_symbol(session, s)

            done = 0
            for start in range(0, len(symbols), 60):
                chunk = symbols[start:start + 60]
                await asyncio.gather(*(bound(s) for s in chunk))
                done += len(chunk)
                print(f"  ...{done}/{len(symbols)} pairs", end="\r")

        print(" " * 50, end="\r")
        self.report()

    def report(self):
        print("=" * 70)
        print("RESULTS")
        print("=" * 70)

        if not self.signals and not self.near_misses:
            print("\nNothing matched a detector at any score. Stage breakdown:\n")
            for stage, n in self.stage_deaths.most_common():
                print(f"   {stage:<26} {n:>8}")
            return

        print(f"\nSignals at or above gate ({self.p.min_score}): {len(self.signals)}")
        print(f"Matched a detector but scored below gate:  {len(self.near_misses)}")

        if self.near_misses and not self.signals:
            scores = sorted((s["score"] for s in self.near_misses), reverse=True)
            print(f"\n  Best near-miss scores: {', '.join(f'{s:.0f}' for s in scores[:10])}")
            print(f"  -> detectors are matching; the gate is the binding constraint.")
            return

        if not self.signals:
            return

        outcomes = Counter(s["outcome"] for s in self.signals)
        total = len(self.signals)

        print("\n  Outcome within forward window:")
        for k in ("TP2", "TP1", "OPEN", "STOP"):
            n = outcomes.get(k, 0)
            print(f"     {k:<6} {n:>4}  ({n / total * 100:.0f}%)")

        # ---- the number that actually decides this ----
        gross = [s["r_multiple"] for s in self.signals]
        net = [s["net_r"] for s in self.signals]
        costs = [s["cost_r"] for s in self.signals]

        print("\n  EXPECTANCY (R per trade -- this is the number that matters)")
        print(f"     gross            {statistics.mean(gross):+.3f} R")
        print(f"     modelled cost    {statistics.mean(costs):-.3f} R")
        print(f"     net              {statistics.mean(net):+.3f} R")

        if statistics.mean(net) > 0:
            print(f"\n     Positive on this sample. Sample size is {total} --")
            print(f"     treat as a hypothesis, not a result. Forward test it.")
        else:
            print(f"\n     Negative expectancy. On this sample the system loses")
            print(f"     money per trade after costs.")

        wins = outcomes.get("TP1", 0) + outcomes.get("TP2", 0)
        print(f"\n  Reached at least TP1: {wins}/{total} ({wins / total * 100:.0f}%)")

        gains = [s["max_gain_pct"] for s in self.signals]
        print(f"  Max favourable excursion: median {statistics.median(gains):+.2f}% | "
              f"best {max(gains):+.2f}%")

        # ---- concentration check ----
        by_symbol = Counter(s["symbol"] for s in self.signals)
        top_sym, top_n = by_symbol.most_common(1)[0]
        print(f"\n  Distinct symbols: {len(by_symbol)} across {total} signals")
        if top_n / total > 0.3:
            print(f"  WARNING: {top_sym} produced {top_n}/{total} "
                  f"({top_n/total*100:.0f}%) of all signals.")
            print(f"  One instrument dominating the set is concentration, not edge.")

        print("\n  Top 10 by score:")
        print(f"     {'symbol':<14}{'score':>6}{'vel':>7}{'stop%':>7}  "
              f"{'outcome':<7}{'netR':>7}")
        for s in sorted(self.signals, key=lambda x: x["score"], reverse=True)[:10]:
            print(f"     {s['symbol']:<14}{s['score']:>6.0f}{s['velocity']:>7.1f}"
                  f"{s['sl_pct']:>7.2f}  {s['outcome']:<7}{s['net_r']:>+7.2f}")

        by_type = Counter(s["type"] for s in self.signals)
        print("\n  By detector:")
        for t, n in by_type.items():
            print(f"     {t:<32} {n}")

        print("\n" + "=" * 70)
        print("In-sample on recent history. Parameters that fit this window")
        print("are not evidence they fit the next one.")
        print("=" * 70)


if __name__ == "__main__":
    asyncio.run(Backtester(load_config()).run())