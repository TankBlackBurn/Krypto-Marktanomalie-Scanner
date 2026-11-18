"""
depth_collector.py -- poll order book depth and record book thinness.

COW moved 39% on $128k because the book above price was thin -- $71 average
trade size before the spike, $1,415 during it. That is a footprint, but it
only appeared inside the ignition candle. This asks whether thinness is
visible BEFORE a move, which candles cannot show at all.

DEPTH HAS NO HISTORY ENDPOINT
Unlike funding, WEEX only exposes the current book -- there is no lookback.
Same situation as the hype collector: this has to run forward and
accumulate, there is no way to backtest it on the past.

    python -m research.depth_collector              run continuously
    python -m research.depth_collector --status      check how much is collected

Params confirmed from https://www.weex.com/api-doc/contract/Market_API/GetDepthData
symbol required, limit accepts ONLY 15 or 200 (this is what returned 400
before -- 50 is not a valid value). Using 200 for the richer book; weight
is flat per the docs, so there is no cost to requesting the larger side.

WHAT IS RECORDED, PER SNAPSHOT
  midprice              (best_bid + best_ask) / 2
  bid_notional / ask_notional   total $ resting in the returned book
  notional_to_move_up_2pct      $ needed to walk the ask side up 2% from
                                 mid. This is the actual "how easily could
                                 this get swept" number -- COW's ignition
                                 candle needed about $128k to move 39%
                                 through a thin book. A LOW number here on
                                 a liquid-looking coin is the anomaly.
  notional_to_move_down_2pct    same, bid side, for symmetry/context

RATE LIMITING
The exchangeInfo docs example shows 2000 req/10sec, but the LIVE probe run
earlier in this project showed 500/10min -- a very different number, and
the one actually observed beats a documentation example that may be stale.
This keeps the same 42/min throttle already proven safe for funding_test.py
rather than trusting the more generous documented figure.
"""

import asyncio
import json
import os
import sqlite3
import sys
import time

import aiohttp

BASE = "https://api-contract.weex.com"
DEPTH_URL = f"{BASE}/capi/v3/market/depth"
EXCHANGE_INFO = f"{BASE}/capi/v3/market/exchangeInfo"
DB_PATH = "db/depth.db"

REQUESTS_PER_MINUTE = 42
MOVE_PCT = 2.0   # the "how much $ to move price this far" threshold


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS depth_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                ts INTEGER NOT NULL,
                midprice REAL,
                bid_notional REAL,
                ask_notional REAL,
                notional_up_2pct REAL,
                notional_down_2pct REAL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_depth_sym_ts
            ON depth_snapshots(symbol, ts)
        """)
        conn.commit()


async def fetch_symbols(session):
    async with session.get(EXCHANGE_INFO,
                           timeout=aiohttp.ClientTimeout(total=25)) as r:
        data = await r.json(content_type=None)
    return [s["symbol"] for s in data.get("symbols", [])
            if str(s.get("symbol", "")).upper().endswith("USDT")]


def notional_to_move(levels, from_price, target_price, side):
    """
    Walk the book from best price outward, summing notional until the
    target is reached. Returns None if the returned depth (15 or 200
    levels) is not deep enough to reach the target -- that itself is
    informative (an even thinner book than the metric can measure) but is
    kept as None rather than guessed at.
    """
    total = 0.0
    for price_s, size_s in levels:
        try:
            price, size = float(price_s), float(size_s)
        except (TypeError, ValueError):
            continue
        if side == "up" and price > target_price:
            break
        if side == "down" and price < target_price:
            break
        total += price * size
    else:
        return None  # exhausted the book without reaching target
    return total


async def poll_symbol(session, symbol):
    try:
        async with session.get(
            DEPTH_URL, params={"symbol": symbol, "limit": "200"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                return None
            body = await r.json(content_type=None)
    except Exception:
        return None

    bids, asks = body.get("bids") or [], body.get("asks") or []
    if not bids or not asks:
        return None

    try:
        best_bid, best_ask = float(bids[0][0]), float(asks[0][0])
    except (TypeError, ValueError, IndexError):
        return None
    if best_bid <= 0 or best_ask <= 0:
        return None

    mid = (best_bid + best_ask) / 2.0
    bid_notional = sum(float(p) * float(s) for p, s in bids
                       if _safe(p) and _safe(s))
    ask_notional = sum(float(p) * float(s) for p, s in asks
                       if _safe(p) and _safe(s))

    up_target = mid * (1 + MOVE_PCT / 100)
    down_target = mid * (1 - MOVE_PCT / 100)

    return {
        "symbol": symbol,
        "ts": int(time.time()),
        "midprice": mid,
        "bid_notional": bid_notional,
        "ask_notional": ask_notional,
        "notional_up_2pct": notional_to_move(asks, mid, up_target, "up"),
        "notional_down_2pct": notional_to_move(bids, mid, down_target, "down"),
    }


def _safe(x):
    try:
        float(x)
        return True
    except (TypeError, ValueError):
        return False


def print_status():
    if not os.path.exists(DB_PATH):
        print("No data collected yet. Run without --status to start.")
        return
    with sqlite3.connect(DB_PATH) as conn:
        total = conn.execute("SELECT COUNT(*) FROM depth_snapshots").fetchone()[0]
        if not total:
            print("Database exists but is empty.")
            return
        lo, hi = conn.execute(
            "SELECT MIN(ts), MAX(ts) FROM depth_snapshots").fetchone()
        span_h = (hi - lo) / 3600.0
        symbols = conn.execute(
            "SELECT COUNT(DISTINCT symbol) FROM depth_snapshots").fetchone()[0]
        thinnest = conn.execute("""
            SELECT symbol, notional_up_2pct FROM depth_snapshots
            WHERE notional_up_2pct IS NOT NULL
            ORDER BY notional_up_2pct ASC LIMIT 10
        """).fetchall()

    print("=" * 58)
    print("DEPTH COLLECTOR STATUS")
    print("=" * 58)
    print(f"Snapshots: {total:,}  |  symbols seen: {symbols}  |  span: {span_h:.1f}h")
    if thinnest:
        print("\nThinnest books seen so far (lowest $ to move price 2%):")
        for sym, notional in thinnest:
            print(f"   {sym:<14} ${notional:>12,.0f}")
    if span_h < 24:
        print(f"\nOnly {span_h:.0f}h collected. depth_test.py needs at least")
        print("24-48h before results mean anything -- same constraint as")
        print("the hype collector, for the same reason: no history endpoint.")
    else:
        print(f"\n{span_h:.0f}h collected -- enough to run depth_test.py.")
    print("=" * 58)


async def collect_loop():
    init_db()
    connector = aiohttp.TCPConnector(limit=4)
    delay = 60.0 / REQUESTS_PER_MINUTE

    async with aiohttp.ClientSession(connector=connector) as session:
        print("Fetching symbol list...")
        symbols = await fetch_symbols(session)
        if not symbols:
            print("No symbols. Aborting.")
            return
        print(f"  {len(symbols)} symbols")

        cycle_min = len(symbols) / REQUESTS_PER_MINUTE
        print(f"Throttled to {REQUESTS_PER_MINUTE}/min -> one full sweep "
              f"takes ~{cycle_min:.0f} minutes.")
        print("Leave this running. Check progress with:")
        print("  python -m research.depth_collector --status\n")

        cycle = 0
        while True:
            cycle += 1
            saved = 0
            start = time.time()

            rows = []
            for sym in symbols:
                snap = await poll_symbol(session, sym)
                if snap:
                    rows.append((
                        snap["symbol"], snap["ts"], snap["midprice"],
                        snap["bid_notional"], snap["ask_notional"],
                        snap["notional_up_2pct"], snap["notional_down_2pct"],
                    ))
                    saved += 1
                await asyncio.sleep(delay)

            if rows:
                with sqlite3.connect(DB_PATH) as conn:
                    conn.executemany("""
                        INSERT INTO depth_snapshots
                        (symbol, ts, midprice, bid_notional, ask_notional,
                         notional_up_2pct, notional_down_2pct)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, rows)
                    conn.commit()

            elapsed = (time.time() - start) / 60
            print(f"[cycle {cycle}] {saved}/{len(symbols)} symbols saved "
                  f"in {elapsed:.1f} min")


if __name__ == "__main__":
    if "--status" in sys.argv:
        print_status()
    else:
        try:
            asyncio.run(collect_loop())
        except KeyboardInterrupt:
            print("\nStopped. Data saved in db/depth.db -- run again to resume.")