"""
check_depth.py -- verify the raw shape of WEEX's order book response.

depth_collector.py is reporting $0 notional-to-move-2% on dozens of
unrelated symbols (MOODENGETH, BEAT, BUTTCOIN, VCX, ...). Two different
explanations produce that same number, and they mean opposite things:

  1. GENUINE: those books really are that sparse -- the first visible ask
     already sits past a 2% move from mid, so the metric is doing its job
     and correctly reporting an extremely thin book.

  2. BUG: the asks array is not sorted ascending (best/cheapest ask first)
     the way notional_to_move() assumes. If it is sorted the other way,
     or unsorted, the walk breaks on the very first entry for nearly every
     symbol regardless of actual liquidity, and every $0 is meaningless.

This prints the raw top 10 levels on both sides for a liquid major (BTC,
where a sorting bug would be obvious at a glance) and two of the $0
symbols from your list, plus a step-by-step trace of exactly why the
notional calculation stopped where it did.

    python tools/check_depth.py
    python tools/check_depth.py MOODENGETH BEAT VCX
"""

import asyncio
import sys

import aiohttp

DEPTH_URL = "https://api-contract.weex.com/capi/v3/market/depth"


async def check(session, symbol):
    print("=" * 78)
    print(f"{symbol}")
    print("=" * 78)

    try:
        async with session.get(
            DEPTH_URL, params={"symbol": symbol, "limit": "200"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                print(f"  HTTP {r.status}")
                return
            body = await r.json(content_type=None)
    except Exception as e:
        print(f"  fetch failed: {e}")
        return

    bids, asks = body.get("bids") or [], body.get("asks") or []
    if not bids or not asks:
        print(f"  empty book -- bids={len(bids)} asks={len(asks)}")
        return

    print(f"\n  RAW top 10 BIDS as returned (index 0 first):")
    for i, (p, s) in enumerate(bids[:10]):
        print(f"    [{i}] price={p:<14} size={s}")

    print(f"\n  RAW top 10 ASKS as returned (index 0 first):")
    for i, (p, s) in enumerate(asks[:10]):
        print(f"    [{i}] price={p:<14} size={s}")

    try:
        best_bid, best_ask = float(bids[0][0]), float(asks[0][0])
    except (ValueError, IndexError):
        print("\n  Could not parse best bid/ask.")
        return

    mid = (best_bid + best_ask) / 2.0
    spread_pct = (best_ask - best_bid) / mid * 100.0
    target = mid * 1.02

    print(f"\n  best_bid={best_bid}  best_ask={best_ask}")
    print(f"  mid={mid:.8g}   spread={spread_pct:.3f}%   2% target={target:.8g}")

    if best_ask > target:
        print(f"\n  VERDICT: best_ask ({best_ask}) already exceeds the 2% target")
        print(f"  on its own -- spread alone is {spread_pct:.2f}%. If this also")
        print(f"  happens on BTC, the array is not sorted the way the code")
        print(f"  assumes -- that is the bug. If this ONLY happens on illiquid")
        print(f"  alts and BTC looks normal below, the books are genuinely")
        print(f"  this thin and the $0 readings are real.")
        return

    print(f"\n  Walking the ask side toward the 2% target:")
    total = 0.0
    for i, (p, s) in enumerate(asks[:15]):
        try:
            price, size = float(p), float(s)
        except ValueError:
            continue
        if price > target:
            print(f"    [{i}] price={price:<12} > target -- STOP. "
                  f"Accumulated so far: ${total:,.2f}")
            break
        total += price * size
        print(f"    [{i}] price={price:<12} size={size:<12} "
              f"running total=${total:,.2f}")
    else:
        print(f"    Walked all shown levels without reaching target. "
              f"Total: ${total:,.2f}")


async def main():
    symbols = [a.upper() for a in sys.argv[1:]] or ["BTC", "MOODENGETH", "BEAT"]
    symbols = [s if s.endswith("USDT") else s + "USDT" for s in symbols]

    async with aiohttp.ClientSession() as session:
        for sym in symbols:
            await check(session, sym)
            print()


if __name__ == "__main__":
    asyncio.run(main())