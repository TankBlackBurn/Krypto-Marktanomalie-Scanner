"""
check_order.py -- settle the candle ordering question.

Prints the timestamp at each end of the array plus the gaps between
consecutive elements. If index 0 is newer than index 1, the array is
descending and data_pipeline.py is reading 40-hour-old candles.

    python tools/check_order.py
"""

import asyncio
import aiohttp
from datetime import datetime, timezone

URL = ("https://api-contract.weex.com/capi/v3/market/klines"
       "?symbol=BTCUSDT&interval=15m&limit=100")


def fmt(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")


async def main():
    async with aiohttp.ClientSession() as s:
        async with s.get(URL, timeout=aiohttp.ClientTimeout(total=15)) as r:
            k = await r.json()

    print(f"total candles: {len(k)}\n")
    print("idx |  open time  | close  | gap to next")
    print("-" * 48)

    for i in [0, 1, 2, 3, 48, 49, 96, 97, 98, 99]:
        if i >= len(k):
            continue
        gap = ""
        if i + 1 < len(k):
            delta = (k[i + 1][0] - k[i][0]) / 60000.0
            gap = f"{delta:+.0f} min"
        print(f"{i:>3} | {fmt(k[i][0])} | {k[i][4]:>9} | {gap}")

    print()
    span = abs(max(c[0] for c in k) - min(c[0] for c in k)) / 3600000.0
    print(f"total span covered: {span:.1f} hours  (should be ~25h for 100x 15m)")

    ascending = all(k[i][0] < k[i + 1][0] for i in range(len(k) - 1))
    descending = all(k[i][0] > k[i + 1][0] for i in range(len(k) - 1))

    print()
    if ascending:
        print("VERDICT: strictly ascending (oldest first).")
        print("  -> klines[-2] really is the last closed candle. Ordering is fine.")
    elif descending:
        print("VERDICT: strictly descending (newest first).")
        print("  -> klines[-2] is the OLDEST candle. Reverse the list on fetch.")
    else:
        print("VERDICT: neither strictly ascending nor descending.")
        print("  -> mixed ordering, most likely descending with the live candle")
        print("     appended at the end. Sort by field [0] before using.")

    # Show what the pipeline currently grabs as its "confirmation candle"
    print()
    print(f"pipeline's klines[-2] -> {fmt(k[-2][0])}  (it thinks this is 'just closed')")
    newest = max(k, key=lambda c: c[0])
    print(f"actual newest candle  -> {fmt(newest[0])}")


if __name__ == "__main__":
    asyncio.run(main())