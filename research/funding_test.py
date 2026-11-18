"""
funding_test.py -- does funding rate predict pumps?

The first genuinely non-OHLCV test in this project. Five price-derived
hypotheses have come back null; this one uses positioning data instead --
who is long, who is short, and who is paying to stay there.

WHY FUNDING COULD LEAD PRICE WHERE CANDLES CANNOT
Funding is the periodic payment between longs and shorts. Deeply negative
funding means shorts are crowded and paying longs to hold the other side.
That is fuel: any upward push forces shorts to cover, which pushes price
further. Unlike a candle pattern, this describes positioning that has
already accumulated -- it is a statement about the future, not the past.

DATA
/capi/v3/market/fundingRate returns 100 historical entries per symbol,
each with its own markPrice. Forward returns come from the series itself,
so no kline join is needed and the sample is ~95,000 observations rather
than the 10,856 candles the OHLCV tests were limited to.

    python -m research.funding_test              all symbols (slow, ~20 min)
    python -m research.funding_test --limit 300  faster sample
    python -m research.funding_test --top 40     also show what funding did before
                                        the 40 biggest moves

RATE LIMITS
exchangeInfo reports 500 requests per 10 minutes. This throttles to stay
under that, which is why a full run takes ~20 minutes. Let it run.
"""

import asyncio
import json
import os
import statistics
import sys
import time
from collections import defaultdict

import aiohttp

BASE = "https://api-contract.weex.com"
FUNDING_URL = f"{BASE}/capi/v3/market/fundingRate"
EXCHANGE_INFO = f"{BASE}/capi/v3/market/exchangeInfo"

# 500 requests / 10 min. Stay well under.
REQUESTS_PER_MINUTE = 42
CONCURRENCY = 3

FORWARD_PERIODS = [1, 2, 3]   # funding intervals ahead
BIG_MOVE_PCT = 30.0           # the move size actually being hunted
MODEST_MOVE_PCT = 5.0

# Funding is quoted as a fraction: -0.00189180 == -0.189%.
BANDS = [
    ("funding <= -0.10%  (shorts crowded)", -9.99, -0.0010),
    ("funding -0.10% to -0.03%", -0.0010, -0.0003),
    ("funding -0.03% to +0.03%  (neutral)", -0.0003, 0.0003),
    ("funding +0.03% to +0.10%", 0.0003, 0.0010),
    ("funding >= +0.10%  (longs crowded)", 0.0010, 9.99),
]


async def fetch_symbols(session):
    async with session.get(EXCHANGE_INFO,
                           timeout=aiohttp.ClientTimeout(total=25)) as r:
        data = await r.json(content_type=None)
    out = []
    for s in data.get("symbols", []):
        sym = str(s.get("symbol", ""))
        if sym.upper().endswith("USDT"):
            out.append(sym)
    return out


async def fetch_funding(session, symbol):
    try:
        async with session.get(FUNDING_URL, params={"symbol": symbol},
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                return None
            body = await r.json(content_type=None)
    except Exception:
        return None

    if isinstance(body, dict):
        for k in ("data", "result", "list"):
            if isinstance(body.get(k), list):
                body = body[k]
                break
    if not isinstance(body, list) or len(body) < 10:
        return None

    rows = []
    for e in body:
        if not isinstance(e, dict):
            continue
        try:
            rows.append({
                "t": int(e["fundingTime"]),
                "rate": float(e["fundingRate"]),
                "px": float(e["markPrice"]),
            })
        except (KeyError, TypeError, ValueError):
            continue

    rows.sort(key=lambda x: x["t"])
    return rows if len(rows) >= 10 else None


class FundingTest:
    def __init__(self):
        self.by_band = defaultdict(list)      # band -> forward returns (k=1)
        self.by_band_max = defaultdict(list)  # band -> best forward over k
        self.all_fwd = []
        self.movers = []                      # (move%, symbol, rate, delta)
        self.by_delta = defaultdict(list)
        self.intervals = []

    def ingest(self, symbol, rows):
        gaps = [rows[i + 1]["t"] - rows[i]["t"] for i in range(len(rows) - 1)]
        if gaps:
            self.intervals.append(statistics.median(gaps) / 3600000.0)

        maxk = max(FORWARD_PERIODS)
        for i in range(len(rows) - maxk):
            rate = rows[i]["rate"]
            px = rows[i]["px"]
            if px <= 0:
                continue

            fwds = []
            for k in FORWARD_PERIODS:
                nxt = rows[i + k]["px"]
                if nxt > 0:
                    fwds.append((nxt - px) / px * 100.0)
            if not fwds:
                continue

            fwd1 = fwds[0]
            best = max(fwds)
            self.all_fwd.append(fwd1)

            for label, lo, hi in BANDS:
                if lo <= rate < hi:
                    self.by_band[label].append(fwd1)
                    self.by_band_max[label].append(best)
                    break

            # Funding CHANGE, not just level. A rate collapsing toward
            # negative may matter more than one that has sat there.
            if i > 0:
                delta = rate - rows[i - 1]["rate"]
                if delta <= -0.0005:
                    self.by_delta["falling fast (<= -0.05%)"].append(best)
                elif delta >= 0.0005:
                    self.by_delta["rising fast (>= +0.05%)"].append(best)
                else:
                    self.by_delta["stable"].append(best)

                self.movers.append((best, symbol, rate, delta))

    async def run(self, limit=None, show_top=0):
        print("=" * 78)
        print("FUNDING RATE TEST -- DOES POSITIONING LEAD PRICE?")
        print("=" * 78)

        connector = aiohttp.TCPConnector(limit=CONCURRENCY)
        async with aiohttp.ClientSession(connector=connector) as session:
            symbols = await fetch_symbols(session)
            if not symbols:
                print("Could not fetch the symbol list.")
                return
            if limit:
                symbols = symbols[:limit]

            est = len(symbols) / REQUESTS_PER_MINUTE
            print(f"\n{len(symbols)} symbols | throttled to {REQUESTS_PER_MINUTE}/min "
                  f"to respect the 500-per-10-min limit")
            print(f"Estimated runtime: {est:.0f} minutes. Leave it running.\n")

            delay = 60.0 / REQUESTS_PER_MINUTE
            ok = 0
            start = time.time()

            for idx, sym in enumerate(symbols, 1):
                rows = await fetch_funding(session, sym)
                if rows:
                    self.ingest(sym, rows)
                    ok += 1
                if idx % 25 == 0:
                    el = (time.time() - start) / 60
                    print(f"  {idx}/{len(symbols)} symbols | {ok} with data | "
                          f"{el:.1f} min elapsed", end="\r")
                await asyncio.sleep(delay)

        print(" " * 70, end="\r")
        print(f"Collected funding history for {ok} symbols.\n")
        self.report(show_top)

    def report(self, show_top=0):
        if len(self.all_fwd) < 200:
            print(f"Only {len(self.all_fwd)} observations -- too few to judge.")
            return

        interval_h = statistics.median(self.intervals) if self.intervals else 0
        base_mean = statistics.mean(self.all_fwd)
        base_big = sum(1 for r in self.all_fwd if r >= BIG_MOVE_PCT) / len(self.all_fwd)
        base_mod = sum(1 for r in self.all_fwd if r >= MODEST_MOVE_PCT) / len(self.all_fwd)

        print("=" * 78)
        print(f"RESULTS  ({len(self.all_fwd):,} observations | funding interval "
              f"~{interval_h:.1f}h | forward = 1 interval)")
        print("=" * 78)
        print(f"\n{'band':<40}{'n':>8}{'>5%':>8}{'>30%':>8}{'mean fwd':>11}")
        print("-" * 78)

        for label, _, _ in BANDS:
            vals = self.by_band.get(label, [])
            if len(vals) < 30:
                print(f"{label:<40}{len(vals):>8}   too few")
                continue
            mod = sum(1 for r in vals if r >= MODEST_MOVE_PCT) / len(vals)
            big = sum(1 for r in vals if r >= BIG_MOVE_PCT) / len(vals)
            mean = statistics.mean(vals)
            mark = ""
            if mean > base_mean + 0.5 and big > base_big:
                mark = "  <--"
            print(f"{label:<40}{len(vals):>8}{mod*100:>7.1f}%{big*100:>7.2f}%"
                  f"{mean:>+10.2f}%{mark}")

        print("-" * 78)
        print(f"{'ALL (base rate)':<40}{len(self.all_fwd):>8}"
              f"{base_mod*100:>7.1f}%{base_big*100:>7.2f}%{base_mean:>+10.2f}%")

        if self.by_delta:
            print(f"\n{'funding CHANGE since previous period':<40}{'n':>8}"
                  f"{'>5%':>8}{'>30%':>8}{'mean':>11}")
            print("-" * 78)
            for label in ("falling fast (<= -0.05%)", "stable", "rising fast (>= +0.05%)"):
                vals = self.by_delta.get(label, [])
                if len(vals) < 30:
                    continue
                mod = sum(1 for r in vals if r >= MODEST_MOVE_PCT) / len(vals)
                big = sum(1 for r in vals if r >= BIG_MOVE_PCT) / len(vals)
                print(f"{label:<40}{len(vals):>8}{mod*100:>7.1f}%"
                      f"{big*100:>7.2f}%{statistics.mean(vals):>+10.2f}%")

        # The reverse view: what was funding doing before the biggest moves?
        if show_top and self.movers:
            self.movers.sort(key=lambda x: -x[0])
            print(f"\n{'BEFORE THE BIGGEST MOVES':<78}")
            print("-" * 78)
            print(f"{'move':>9}{'symbol':>16}{'funding then':>16}{'change':>14}")
            for move, sym, rate, delta in self.movers[:show_top]:
                print(f"{move:>+8.1f}%{sym:>16}{rate*100:>15.4f}%{delta*100:>+13.4f}%")

            top_rates = [m[2] for m in self.movers[:show_top]]
            all_rates = [m[2] for m in self.movers]
            print(f"\n  median funding before the top {show_top} moves: "
                  f"{statistics.median(top_rates)*100:+.4f}%")
            print(f"  median funding across all observations:     "
                  f"{statistics.median(all_rates)*100:+.4f}%")
            print("\n  If these two are similar, funding was NOT elevated before")
            print("  the big moves and this whole line of attack is dead. That")
            print("  comparison is the single most informative number here --")
            print("  the band table can look encouraging while this one says no.")

        print("\n" + "=" * 78)
        print("HOW TO READ THIS")
        print("  The 'shorts crowded' band needs a mean forward return clearly")
        print("  above base AND a higher rate of >30% moves. Both, not either.")
        print("\n  A band can show more big moves while losing money on average,")
        print("  because crowded shorts also mark coins in genuine decline --")
        print("  same trap as the volume spikes in pattern_test.py.")
        print("\n  This is in-sample across ~33 days. A positive result here")
        print("  needs re-running on fresh data before anything gets built.")
        print("=" * 78)


if __name__ == "__main__":
    lim = None
    top = 0
    if "--limit" in sys.argv:
        lim = int(sys.argv[sys.argv.index("--limit") + 1])
    if "--top" in sys.argv:
        top = int(sys.argv[sys.argv.index("--top") + 1])
    asyncio.run(FundingTest().run(limit=lim, show_top=top))