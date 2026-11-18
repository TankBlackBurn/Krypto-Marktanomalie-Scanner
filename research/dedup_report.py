"""
dedup_report.py -- re-read the live signal log with clustered repeats
collapsed to one signal per symbol per move.

WHY THIS EXISTS
symbol_cooldown_candles was defined in config and honoured by backtest.py,
but never enforced on the live path. Result: CLO fired 7 times on a single
price move (21% of a 33-signal sample), all winners, all the same outcome
counted 7 times. Any mean computed over that data is measuring one move,
not 33 independent trades.

main.py now enforces the cooldown, so this will not recur. But the signals
already collected are still useful -- they just need the clusters
collapsed. This applies the same window retroactively and reports both
figures side by side so the difference is visible rather than assumed.

    python -m research.dedup_report
    python -m research.dedup_report --cooldown 8      window in candles (default 8)
    python -m research.dedup_report --interval 15     candle minutes (default 15)
"""

import os
import sqlite3
import statistics
import sys
from collections import defaultdict

DB_PATH = "db/live_signals.db"


def main():
    cooldown_candles = 8
    interval_min = 15
    if "--cooldown" in sys.argv:
        cooldown_candles = int(sys.argv[sys.argv.index("--cooldown") + 1])
    if "--interval" in sys.argv:
        interval_min = int(sys.argv[sys.argv.index("--interval") + 1])
    window = cooldown_candles * interval_min * 60

    if not os.path.exists(DB_PATH):
        print("No db/live_signals.db found.")
        return

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT symbol, confluence_score, signalled_at, outcome, net_r, ai_verdict
            FROM live_signals
            WHERE outcome NOT IN ('PENDING', 'BLOCKED_FUNDING')
              AND net_r IS NOT NULL
            ORDER BY signalled_at ASC
        """).fetchall()

    if not rows:
        print("No resolved signals yet.")
        return

    # Keep the FIRST signal of each cluster -- that is the one the live
    # cooldown would have let through. Keeping the best would be picking
    # winners after the fact.
    last_fire = {}
    kept, dropped = [], []
    for sym, score, ts, outcome, net_r, verdict in rows:
        prev = last_fire.get(sym)
        if prev is not None and (ts - prev) < window:
            dropped.append((sym, score, outcome, net_r))
            continue
        last_fire[sym] = ts
        kept.append((sym, score, outcome, net_r, verdict))

    print("=" * 72)
    print("DEDUPLICATED LIVE FORWARD TEST")
    print(f"cooldown {cooldown_candles} candles x {interval_min}m = "
          f"{window // 60}min window")
    print("=" * 72)

    raw_rs = [r[4] for r in rows]
    kept_rs = [k[3] for k in kept]

    def block(label, vals):
        if not vals:
            print(f"\n  {label}: no data")
            return
        mean = statistics.mean(vals)
        line = f"\n  {label}\n    n {len(vals)}   mean {mean:+.3f} R"
        if len(vals) > 1:
            se = statistics.stdev(vals) / (len(vals) ** 0.5)
            lo, hi = mean - 2 * se, mean + 2 * se
            line += f"\n    95% band {lo:+.3f} to {hi:+.3f}"
            line += ("\n    -> spans zero" if lo <= 0 <= hi
                     else "\n    -> excludes zero")
        print(line)

    block("AS LOGGED (clusters counted repeatedly)", raw_rs)
    block("DEDUPLICATED (one per symbol per move)", kept_rs)

    if dropped:
        by_sym = defaultdict(list)
        for sym, score, outcome, net_r in dropped:
            by_sym[sym].append(net_r)
        print(f"\n  Collapsed {len(dropped)} repeat signal(s):")
        for sym, vals in sorted(by_sym.items(), key=lambda x: -len(x[1])):
            print(f"    {sym:<12} {len(vals)} extra   "
                  f"mean {statistics.mean(vals):+.2f} R")

    # Score split on clean data only -- the contaminated version was
    # largely measuring "did CLO pump", since CLO occupied most of the
    # upper half by itself.
    scored = [(k[1], k[3]) for k in kept if k[1] is not None]
    if len(scored) >= 8:
        scored.sort()
        h = len(scored) // 2
        lo_mean = statistics.mean(v for _, v in scored[:h])
        hi_mean = statistics.mean(v for _, v in scored[-h:])
        print(f"\n  SCORE vs OUTCOME (deduplicated)")
        print(f"    lower half ({scored[0][0]:.0f}-{scored[h-1][0]:.0f}): "
              f"{lo_mean:+.3f} R")
        print(f"    upper half ({scored[-h][0]:.0f}-{scored[-1][0]:.0f}): "
              f"{hi_mean:+.3f} R")
        if hi_mean <= lo_mean:
            print("    -> higher scores did NOT do better on clean data.")
        else:
            print(f"    -> gap {hi_mean - lo_mean:+.3f} R")
        if len(scored) < 30:
            print(f"    ({len(scored)} signals -- provisional.)")

    by_ai = defaultdict(list)
    for k in kept:
        if k[4]:
            by_ai[k[4]].append(k[3])
    if len(by_ai) > 1:
        print(f"\n  AI VERDICT (deduplicated)")
        for v, vals in sorted(by_ai.items()):
            print(f"    {v:<10} n={len(vals):<3} mean {statistics.mean(vals):+.3f} R")

    print("\n" + "=" * 72)
    print("The deduplicated figure is the honest one. Signals from the same")
    print("move share an outcome -- counting them separately inflates both")
    print("the sample size and whichever direction that move happened to go.")
    print("=" * 72)


if __name__ == "__main__":
    main()