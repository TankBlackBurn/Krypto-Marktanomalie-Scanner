"""
signal_tracker.py -- what actually happened to the signals the bot sent you?

This is the real forward test. backtest.py replays history the parameters
were tuned on; this measures live signals on data nothing has touched.
That distinction is the whole ballgame -- an in-sample backtest can look
fine while live results are flat, and only this tells you which you have.

    python signal_tracker.py            resolve pending signals, show stats
    python signal_tracker.py --open     just list what's still open
    python signal_tracker.py --raw      also show the pre-dedup numbers

Requires main.py to have been running (it writes db/live_signals.db).

Outcome rules mirror backtest.py so the two are comparable:
  STOP  price traded at or below the stop before touching TP1
  TP1   price reached TP1 first
  TP2   price reached TP2 first
  OPEN  neither, still inside the evaluation window
  EXPIRED  neither after the window closed -- marked to market

A signal is only resolved once its evaluation window has fully elapsed,
so nothing gets scored early on a half-formed move.

DEDUPLICATION
main.py enforces a per-symbol cooldown now, but signals logged before that
fix can still contain clusters -- CLO fired 7 times on one move before the
cooldown was wired in, and a mean computed over that data is measuring one
move seven times, not seven independent trades. This applies the same
window retroactively so every run reports the honest figure by default,
rather than requiring a separate dedup_report.py pass to catch it.
"""

import asyncio
import json
import os
import sqlite3
import statistics
import sys
import time

import aiohttp

DB_PATH = "db/live_signals.db"
KLINES = "https://api-contract.weex.com/capi/v3/market/klines"

EVAL_WINDOW_HOURS = 2          # matches backtest's 8 x 15m forward window
ROUND_TRIP_COST_PCT = 0.12

K_OPEN_TIME, K_HIGH, K_LOW, K_CLOSE = 0, 2, 3, 4


def cooldown_seconds() -> int:
    """Mirror main.py's live cooldown window so this stays in sync with
    whatever is actually suppressing repeat fires. Falls back to the same
    8-candle/15m default main.py uses if config.json is unavailable."""
    candles, interval_min = 8, 15
    try:
        if os.path.exists("config.json"):
            with open("config.json") as f:
                cfg = json.load(f)
            candles = int(cfg.get("bot_settings", {}).get("symbol_cooldown_candles", 8))
            interval = cfg.get("bot_settings", {}).get("interval", "15m")
            interval_min = {"1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
                            "1h": 60, "4h": 240}.get(interval, 15)
    except Exception:
        pass
    return candles * interval_min * 60


def dedupe(rows, ts_index, symbol_index):
    """Keep the FIRST signal in each per-symbol cooldown window. Keeping
    the best of a cluster would be picking winners after the fact."""
    window = cooldown_seconds()
    ordered = sorted(rows, key=lambda r: r[ts_index] or 0)
    last_fire = {}
    kept, dropped = [], []
    for r in ordered:
        sym, ts = r[symbol_index], (r[ts_index] or 0)
        prev = last_fire.get(sym)
        if prev is not None and (ts - prev) < window:
            dropped.append(r)
            continue
        last_fire[sym] = ts
        kept.append(r)
    return kept, dropped


def normalize(raw):
    if isinstance(raw, dict):
        for key in ("data", "result", "list", "klines"):
            if isinstance(raw.get(key), list):
                raw = raw[key]
                break
    if not isinstance(raw, list):
        return []
    rows = [c for c in raw if isinstance(c, list) and len(c) > K_CLOSE]
    rows.sort(key=lambda c: int(c[K_OPEN_TIME]))
    return rows


async def fetch_since(session, symbol, since_ms):
    try:
        url = f"{KLINES}?symbol={symbol}USDT&interval=15m&limit=100"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                return []
            rows = normalize(await r.json())
    except Exception:
        return []
    return [c for c in rows if int(c[K_OPEN_TIME]) >= since_ms]


def classify(candles, entry, sl, tp1, tp2):
    """Conservative: if a candle spans both stop and target, assume the stop."""
    peak = entry
    for c in candles:
        hi, lo = float(c[K_HIGH]), float(c[K_LOW])
        peak = max(peak, hi)
        if lo <= sl:
            return "STOP", peak
        if hi >= tp2:
            return "TP2", peak
        if hi >= tp1:
            return "TP1", peak
    return None, peak


async def resolve():
    if not os.path.exists(DB_PATH):
        print("No db/live_signals.db yet. Run main.py first -- it creates this")
        print("as signals fire. If main.py has been running with no signals,")
        print("there is simply nothing to resolve yet.")
        return

    with sqlite3.connect(DB_PATH) as conn:
        pending = conn.execute("""
            SELECT id, symbol, entry_price, sl_price, tp1_price, tp2_price,
                   signalled_at, sl_percent
            FROM live_signals WHERE outcome = 'PENDING'
        """).fetchall()

    if not pending:
        print("No pending signals to resolve.\n")
        report()
        return

    now = int(time.time())
    window = EVAL_WINDOW_HOURS * 3600
    updates = []

    async with aiohttp.ClientSession() as session:
        for sid, symbol, entry, sl, tp1, tp2, ts, sl_pct in pending:
            age = now - (ts or now)
            candles = await fetch_since(session, symbol, (ts or now) * 1000)
            outcome, peak = classify(candles, entry, sl, tp1, tp2)

            if outcome is None:
                if age < window:
                    continue                      # still inside the window
                outcome = "EXPIRED"

            # R multiple, consistent with backtest.py
            risk = entry - sl
            if outcome == "STOP":
                r = -1.0
            elif outcome == "TP2":
                r = (tp2 - entry) / risk if risk > 0 else 0.0
            elif outcome == "TP1":
                r = (tp1 - entry) / risk if risk > 0 else 0.0
            else:
                final = float(candles[-1][K_CLOSE]) if candles else entry
                r = (final - entry) / risk if risk > 0 else 0.0

            cost_r = ROUND_TRIP_COST_PCT / sl_pct if sl_pct else 0.0
            updates.append((outcome, now, peak, r - cost_r, sid))
            print(f"  resolved {symbol:<14} {outcome:<8} net {r - cost_r:+.2f}R")

    if updates:
        with sqlite3.connect(DB_PATH) as conn:
            conn.executemany("""
                UPDATE live_signals
                SET outcome = ?, resolved_at = ?, peak_price = ?, net_r = ?
                WHERE id = ?
            """, updates)
            conn.commit()
        print(f"\n{len(updates)} signal(s) resolved.\n")
    else:
        print("  nothing ready to resolve yet (all still inside the window)\n")

    report()


def report():
    show_raw = "--raw" in sys.argv

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT symbol, signal_type, confluence_score, sl_percent, velocity,
                   extension_pct, ai_verdict, outcome, net_r, entry_price,
                   peak_price, signalled_at
            FROM live_signals WHERE outcome != 'PENDING'
        """).fetchall()
        still_open = conn.execute(
            "SELECT COUNT(*) FROM live_signals WHERE outcome = 'PENDING'").fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM live_signals").fetchone()[0]
        blocked = conn.execute(
            "SELECT COUNT(*) FROM live_signals "
            "WHERE outcome = 'BLOCKED_FUNDING'").fetchone()[0]

    print("=" * 72)
    print("LIVE SIGNAL FORWARD TEST")
    print("=" * 72)
    print(f"\nTotal signals emitted: {total}   resolved: {len(rows)}   pending: {still_open}")
    if blocked:
        print(f"Excluded by the funding filter: {blocked}  "
              f"(logged, not traded — audit these later)")

    if not rows:
        print("\nNothing resolved yet. Come back after signals have had")
        print(f"{EVAL_WINDOW_HOURS}h to play out.")
        print("=" * 72)
        return

    SYM, TYPE, SCORE, SLPCT, VEL, EXT, AI, OUT, NETR, ENTRY, PEAK, TS = range(12)

    resolved_rows, cluster_rows = dedupe(rows, TS, SYM)
    if cluster_rows:
        print(f"\n{len(cluster_rows)} signal(s) collapsed as repeat fires on the same "
              f"symbol within the cooldown window\n(same move, not independent trades) "
              f"-- excluded from the stats below.")
        if show_raw:
            by_sym = {}
            for r in cluster_rows:
                by_sym.setdefault(r[SYM], []).append(r[NETR])
            for sym, vals in sorted(by_sym.items(), key=lambda x: -len(x[1])):
                clean = [v for v in vals if v is not None]
                avg = statistics.mean(clean) if clean else 0.0
                print(f"    {sym:<12} {len(vals)} extra   mean {avg:+.2f} R")

    rows = resolved_rows  # everything below reads on deduplicated data

    outcomes = {}
    for r in rows:
        outcomes[r[OUT]] = outcomes.get(r[OUT], 0) + 1
    print("\n  Outcomes: " + ", ".join(f"{k} {v}" for k, v in sorted(outcomes.items())))

    net_rs = [r[NETR] for r in rows if r[NETR] is not None]
    if net_rs:
        mean = statistics.mean(net_rs)
        print(f"\nLIVE EXPECTANCY  (deduplicated -- one signal per symbol per move)")
        print(f"  mean net R:   {mean:+.3f}   (n={len(net_rs)})")
        if len(net_rs) > 1:
            se = statistics.stdev(net_rs) / (len(net_rs) ** 0.5)
            lo, hi = mean - 2 * se, mean + 2 * se
            print(f"  std error:    {se:.3f}")
            print(f"  95% band:     {lo:+.3f} to {hi:+.3f}")
            if lo <= 0 <= hi:
                print("  -> band includes zero: not yet distinguishable from no edge.")
            elif lo > 0:
                print("  -> band excludes zero. Genuinely notable at this sample size.")

    if show_raw and cluster_rows:
        all_rs = [r[NETR] for r in (resolved_rows + cluster_rows) if r[NETR] is not None]
        if all_rs:
            raw_mean = statistics.mean(all_rs)
            print(f"\n  AS LOGGED (repeats counted): mean {raw_mean:+.3f} R  (n={len(all_rs)})")
            print(f"  Gap from deduplicated: {raw_mean - mean:+.3f} R -- this much of the")
            print(f"  'as logged' figure was cluster inflation, not independent edge.")

    # Does the confluence score actually predict anything?
    scored = [(r[SCORE], r[NETR]) for r in rows if r[SCORE] is not None and r[NETR] is not None]
    if len(scored) >= 6:
        scored.sort()
        half = len(scored) // 2
        low_half = statistics.mean(v for _, v in scored[:half])
        high_half = statistics.mean(v for _, v in scored[-half:])
        print(f"\nDOES THE SCORE PREDICT OUTCOME?  (deduplicated)")
        print(f"  lower-scoring half:  {low_half:+.3f} R  "
              f"(scores {scored[0][0]:.0f}-{scored[half-1][0]:.0f})")
        print(f"  higher-scoring half: {high_half:+.3f} R  "
              f"(scores {scored[-half][0]:.0f}-{scored[-1][0]:.0f})")
        if high_half <= low_half:
            print("  -> higher scores did NOT do better. The score is not")
            print("     currently ranking signals usefully.")

    # Is the AI advisory adding anything?
    by_verdict = {}
    for r in rows:
        if r[AI] and r[NETR] is not None:
            by_verdict.setdefault(r[AI], []).append(r[NETR])
    if len(by_verdict) > 1:
        print(f"\nAI VERDICT vs OUTCOME  (deduplicated)")
        for v, vals in sorted(by_verdict.items()):
            print(f"  {v:<10} n={len(vals):<3} mean {statistics.mean(vals):+.3f} R")

    print(f"\n{'symbol':<14}{'score':>6}{'stop%':>7}{'ext%':>7}  "
          f"{'AI':<9}{'outcome':<9}{'netR':>7}")
    print("-" * 72)
    for r in sorted(rows, key=lambda x: -(x[SCORE] or 0)):
        print(f"{r[SYM]:<14}{(r[SCORE] or 0):>6.0f}{(r[SLPCT] or 0):>7.2f}{(r[EXT] or 0):>7.1f}  "
              f"{(r[AI] or '-'):<9}{r[OUT]:<9}{(r[NETR] or 0):>+7.2f}")

    print("\n" + "=" * 72)
    print("This is out-of-sample: these signals fired on data no parameter")
    print("was fitted to. It is worth more than any backtest number, and it")
    print("is also slower to accumulate. Judge on 30+ resolved signals, not 5.")
    if cluster_rows and not show_raw:
        print("Run with --raw to see the pre-deduplication comparison.")
    print("=" * 72)


def list_open():
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT symbol, entry_price, sl_price, tp1_price, confluence_score, signalled_at
            FROM live_signals WHERE outcome = 'PENDING' ORDER BY signalled_at DESC
        """).fetchall()
    if not rows:
        print("No open signals.")
        return
    now = int(time.time())
    print(f"{'symbol':<14}{'entry':>12}{'stop':>12}{'tp1':>12}{'score':>7}{'age':>8}")
    print("-" * 66)
    for sym, e, sl, tp1, sc, ts in rows:
        age_m = (now - (ts or now)) // 60
        print(f"{sym:<14}{e:>12.6g}{sl:>12.6g}{tp1:>12.6g}{(sc or 0):>7.0f}{age_m:>7}m")


if __name__ == "__main__":
    if "--open" in sys.argv:
        list_open()
    else:
        asyncio.run(resolve())