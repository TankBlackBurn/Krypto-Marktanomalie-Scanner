"""
api_discovery.py -- find out what non-price data WEEX actually exposes.

Every test in this project so far has been a transformation of OHLCV, and
all five came back null. Price data cannot tell you about positioning:
who is long, who is short, who is paying to stay there, and how thin the
book is above spot. That information exists on the exchange -- your own
COW screenshot showed a funding rate -- but it is not in a candle.

This probes candidate endpoints and reports which respond, what shape the
payload is, and which fields look useful. Nothing is built on guessed URLs
until this says they exist.

    python tools/api_discovery.py              probe with BTCUSDT
    python tools/api_discovery.py COWUSDT      probe with a specific symbol

WHY A SYMBOL MATTERS: some endpoints only return data for symbols with
open positions or active funding, so a thin altcoin can 404 where BTC
succeeds. Worth probing both.
"""

import asyncio
import json
import sys

import aiohttp

BASE = "https://api-contract.weex.com"

# Candidate paths. WEEX's contract API follows a common template, so these
# are the conventional spellings -- but conventional is not confirmed, which
# is the entire point of running this.
CANDIDATES = [
    ("funding rate (current)", "/capi/v3/market/current-fundRate", {"symbol": "{sym}"}),
    ("funding rate (alt spelling)", "/capi/v3/market/fundingRate", {"symbol": "{sym}"}),
    ("funding rate (history)", "/capi/v3/market/historyFundRate", {"symbol": "{sym}", "pageSize": "20"}),
    ("open interest", "/capi/v3/market/open_interest", {"symbol": "{sym}"}),
    ("open interest (alt)", "/capi/v3/market/openInterest", {"symbol": "{sym}"}),
    ("order book depth", "/capi/v3/market/depth", {"symbol": "{sym}", "limit": "50"}),
    ("order book (alt)", "/capi/v3/market/orderBook", {"symbol": "{sym}", "limit": "50"}),
    ("ticker (single)", "/capi/v3/market/ticker", {"symbol": "{sym}"}),
    ("tickers (all)", "/capi/v3/market/tickers", {}),
    ("recent trades", "/capi/v3/market/trades", {"symbol": "{sym}", "limit": "50"}),
    ("long/short ratio", "/capi/v3/market/longShortRatio", {"symbol": "{sym}"}),
    ("account ratio", "/capi/v3/market/account-ratio", {"symbol": "{sym}"}),
    ("mark price", "/capi/v3/market/mark-price", {"symbol": "{sym}"}),
    ("index price", "/capi/v3/market/index", {"symbol": "{sym}"}),
    ("contracts / instruments", "/capi/v3/market/contracts", {}),
    ("exchange info (known good)", "/capi/v3/market/exchangeInfo", {}),
]

# Fields that would actually be useful if we find them.
INTERESTING = [
    "openinterest", "open_interest", "oi", "holdingamount", "holding",
    "fundingrate", "funding_rate", "fundrate", "rate",
    "longshort", "long_short", "longaccount", "shortaccount", "ratio",
    "bids", "asks", "bid", "ask", "depth",
    "marketprice", "markprice", "indexprice",
]


def describe(obj, depth=0, max_depth=3):
    """Compact structural summary -- keys and types, not a data dump."""
    pad = "    " * (depth + 1)
    if depth > max_depth:
        return f"{pad}..."
    if isinstance(obj, dict):
        out = []
        for k, v in list(obj.items())[:14]:
            flag = "  <-- USEFUL" if str(k).lower().replace("-", "").replace("_", "") in \
                [i.replace("_", "") for i in INTERESTING] else ""
            if isinstance(v, (dict, list)):
                out.append(f"{pad}{k}: {type(v).__name__}{flag}")
                out.append(describe(v, depth + 1, max_depth))
            else:
                shown = str(v)[:60]
                out.append(f"{pad}{k}: {shown}{flag}")
        if len(obj) > 14:
            out.append(f"{pad}... {len(obj) - 14} more keys")
        return "\n".join(out)
    if isinstance(obj, list):
        if not obj:
            return f"{pad}(empty list)"
        out = [f"{pad}list of {len(obj)}, first element:"]
        out.append(describe(obj[0], depth + 1, max_depth))
        return "\n".join(out)
    return f"{pad}{str(obj)[:60]}"


async def probe(session, label, path, params, symbol):
    url = BASE + path
    filled = {k: v.replace("{sym}", symbol) for k, v in params.items()}
    try:
        async with session.get(url, params=filled,
                               timeout=aiohttp.ClientTimeout(total=12)) as r:
            status = r.status
            try:
                body = await r.json(content_type=None)
            except Exception:
                text = await r.text()
                return label, status, None, text[:120]
            return label, status, body, None
    except asyncio.TimeoutError:
        return label, None, None, "timeout"
    except Exception as e:
        return label, None, None, f"{type(e).__name__}: {e}"


async def main():
    symbol = sys.argv[1].upper() if len(sys.argv) > 1 else "BTCUSDT"
    if not symbol.endswith("USDT"):
        symbol += "USDT"

    print("=" * 74)
    print(f"WEEX API DISCOVERY  --  probing with {symbol}")
    print("=" * 74)
    print("\nLooking for positioning data that is NOT in the candles:")
    print("open interest, funding rate, long/short ratio, order book depth.\n")

    working, dead = [], []

    async with aiohttp.ClientSession() as session:
        for label, path, params in CANDIDATES:
            lbl, status, body, err = await probe(session, label, path, params, symbol)

            if status == 200 and body is not None:
                # A 200 with an error code in the payload is still a failure.
                if isinstance(body, dict):
                    code = str(body.get("code", body.get("status", ""))).lower()
                    if code and code not in ("0", "00000", "200", "success", "ok"):
                        msg = body.get("msg") or body.get("message") or code
                        print(f"  [{status}] {label:<32} rejected: {str(msg)[:40]}")
                        dead.append((label, path, str(msg)[:60]))
                        continue
                print(f"  [200] {label:<32} OK")
                working.append((label, path, params, body))
            elif status:
                print(f"  [{status}] {label:<32} {str(err or '')[:40]}")
                dead.append((label, path, f"HTTP {status}"))
            else:
                print(f"  [---] {label:<32} {err}")
                dead.append((label, path, err))

            await asyncio.sleep(0.25)

    print("\n" + "=" * 74)
    print(f"WORKING ENDPOINTS: {len(working)} of {len(CANDIDATES)}")
    print("=" * 74)

    for label, path, params, body in working:
        print(f"\n--- {label}")
        print(f"    {path}   params={list(params.keys())}")
        print(describe(body))

    if dead:
        print("\n" + "=" * 74)
        print("NOT AVAILABLE")
        print("=" * 74)
        for label, path, reason in dead:
            print(f"  {label:<34} {reason}")

    print("\n" + "=" * 74)
    print("Send this whole output back. Anything marked USEFUL is a data")
    print("source the candles do not contain -- that is what determines")
    print("what gets built next. If nothing positioning-related responds,")
    print("say so plainly rather than building on a guessed endpoint.")
    print("=" * 74)


if __name__ == "__main__":
    asyncio.run(main())