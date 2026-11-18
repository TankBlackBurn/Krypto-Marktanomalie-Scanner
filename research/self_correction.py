"""
self_correction.py -- performance review. Deliberately NOT an auto-tuner.

    python -m research.self_correction

WHY THIS NO LONGER ADJUSTS PARAMETERS AUTOMATICALLY

The original version tightened velocity and buy-pressure thresholds by 10%
whenever the trailing 10-trade loss rate hit 40%. Three problems, in
increasing order of seriousness:

1. It tuned settings the pipeline never reads. It stored
   buy_pressure_ratio = 1.5 while the pipeline's breakout_buy_ratio is a
   fraction between 0 and 1 (e.g. 0.6). Nothing consumed the table it wrote to.

2. It only ever ratcheted TIGHTER. There was no path back: losses raised
   the thresholds, wins never lowered them. Run long enough and every
   parameter pins at its ceiling and the bot stops producing signals at
   all -- silently, with no error, looking exactly like a quiet market.

3. Ten trades cannot distinguish a bad system from a normal losing streak.
   Measured spread on this strategy is roughly 0.78R per trade, so the
   standard error on a 10-trade mean is about 0.25R. A 40% loss rate over
   10 trades is completely ordinary for a system with genuine positive
   expectancy. Auto-tightening on that is fitting to noise in real time --
   the same error every backtest in this project was built to avoid, only
   now with money attached and no way to inspect it afterwards.

So this reports instead. It shows what the trades did, with honest
confidence bands, and flags a parameter worth reconsidering only when the
sample is large enough for the observation to mean something. You make the
change; you keep the audit trail of why.
"""

import os
import sqlite3
import statistics
import sys

TRADES_DB = "db/trading_ledger.db"
SIGNALS_DB = "db/live_signals.db"

# Below this, report but do not draw conclusions.
MIN_SAMPLE_FOR_INFERENCE = 30


def _fetch(db, query):
    if not os.path.exists(db):
        return []
    try:
        with sqlite3.connect(db) as conn:
            return conn.execute(query).fetchall()
    except Exception:
        return []


def _stats_block(label, values):
    if not values:
        print(f"  {label}: no data")
        return None
    mean = statistics.mean(values)
    print(f"\n  {label}")
    print(f"    n            {len(values)}")
    print(f"    mean         {mean:+.3f} R")
    if len(values) > 1:
        sd = statistics.stdev(values)
        se = sd / (len(values) ** 0.5)
        lo, hi = mean - 2 * se, mean + 2 * se
        print(f"    std dev      {sd:.3f}")
        print(f"    95% band     {lo:+.3f} to {hi:+.3f}")
        if lo <= 0 <= hi:
            print(f"    -> band spans zero: not distinguishable from no edge.")
        elif lo > 0:
            print(f"    -> band excludes zero.")
        else:
            print(f"    -> band entirely below zero.")
    if len(values) < MIN_SAMPLE_FOR_INFERENCE:
        print(f"    (under {MIN_SAMPLE_FOR_INFERENCE} trades — treat as provisional)")
    return mean


def review_executed_trades():
    print("=" * 70)
    print("EXECUTED TRADES  (db/trading_ledger.db)")
    print("=" * 70)

    rows = _fetch(TRADES_DB, """
        SELECT symbol, side, entry_price, initial_stop, exit_price,
               realized_r, status, trail_step
        FROM trades WHERE status IN ('WIN','LOSS')
    """)
    if not rows:
        print("\n  No closed trades recorded.")
        print("  Expected while auto_execute_trades is false — nothing has")
        print("  been executed, so there is nothing to review here yet.")
        return

    rs = [r[5] for r in rows if r[5] is not None]
    wins = sum(1 for r in rows if r[6] == 'WIN')
    print(f"\n  Closed trades: {len(rows)}   wins: {wins}   "
          f"losses: {len(rows) - wins}   win rate: {wins/len(rows)*100:.0f}%")
    _stats_block("Realized R", rs)

    reached_tp1 = sum(1 for r in rows if (r[7] or 0) >= 1)
    print(f"\n  Reached TP1 (banked a partial): {reached_tp1}/{len(rows)}")

    print(f"\n  {'symbol':<14}{'side':<7}{'status':<7}{'R':>8}")
    print("  " + "-" * 40)
    for r in sorted(rows, key=lambda x: (x[5] or 0)):
        print(f"  {r[0]:<14}{(r[1] or ''):<7}{r[6]:<7}{(r[5] or 0):>+8.2f}")


def review_signals():
    print("\n" + "=" * 70)
    print("SIGNAL RECORD  (db/live_signals.db)")
    print("=" * 70)

    rows = _fetch(SIGNALS_DB, """
        SELECT symbol, confluence_score, sl_percent, velocity, buy_pressure,
               extension_pct, ai_verdict, outcome, net_r, signal_type
        FROM live_signals WHERE outcome NOT IN ('PENDING')
    """)
    if not rows:
        print("\n  No resolved signals yet. Run signal_tracker.py after")
        print("  signals have had time to play out.")
        return

    rs = [r[8] for r in rows if r[8] is not None]
    print(f"\n  Resolved signals: {len(rows)}")
    _stats_block("Net R per signal", rs)

    # Does the confluence score rank anything?
    scored = [(r[1], r[8]) for r in rows if r[1] is not None and r[8] is not None]
    if len(scored) >= 8:
        scored.sort()
        h = len(scored) // 2
        lo_mean = statistics.mean(v for _, v in scored[:h])
        hi_mean = statistics.mean(v for _, v in scored[-h:])
        print(f"\n  CONFLUENCE SCORE vs OUTCOME")
        print(f"    lower half  ({scored[0][0]:.0f}-{scored[h-1][0]:.0f}): "
              f"{lo_mean:+.3f} R")
        print(f"    upper half  ({scored[-h][0]:.0f}-{scored[-1][0]:.0f}): "
              f"{hi_mean:+.3f} R")
        if hi_mean <= lo_mean:
            print("    -> higher scores did NOT perform better. The score is")
            print("       not ranking signals usefully at this sample size.")
        else:
            print(f"    -> higher scores did better by {hi_mean - lo_mean:+.3f} R.")
        if len(scored) < MIN_SAMPLE_FOR_INFERENCE:
            print(f"       ({len(scored)} signals — provisional either way.)")

    # By detector.
    by_type = {}
    for r in rows:
        if r[9] and r[8] is not None:
            by_type.setdefault(r[9], []).append(r[8])
    if len(by_type) > 1:
        print(f"\n  BY DETECTOR")
        for t, vals in sorted(by_type.items()):
            print(f"    {t:<32} n={len(vals):<3} mean {statistics.mean(vals):+.3f} R")

    # By AI verdict.
    by_ai = {}
    for r in rows:
        if r[6] and r[8] is not None:
            by_ai.setdefault(r[6], []).append(r[8])
    if len(by_ai) > 1:
        print(f"\n  BY AI VERDICT")
        for v, vals in sorted(by_ai.items()):
            print(f"    {v:<10} n={len(vals):<3} mean {statistics.mean(vals):+.3f} R")

    # Stop width, since it has come up repeatedly.
    wide = [r[8] for r in rows if r[2] and r[2] >= 2.0 and r[8] is not None]
    tight = [r[8] for r in rows if r[2] and r[2] < 2.0 and r[8] is not None]
    if len(wide) >= 4 and len(tight) >= 4:
        print(f"\n  STOP WIDTH")
        print(f"    stops >= 2%   n={len(wide):<3} mean {statistics.mean(wide):+.3f} R")
        print(f"    stops <  2%   n={len(tight):<3} mean {statistics.mean(tight):+.3f} R")


def suggestions():
    print("\n" + "=" * 70)
    print("SUGGESTIONS")
    print("=" * 70)

    n_signals = len(_fetch(SIGNALS_DB,
                           "SELECT 1 FROM live_signals WHERE outcome NOT IN ('PENDING')"))
    n_trades = len(_fetch(TRADES_DB,
                          "SELECT 1 FROM trades WHERE status IN ('WIN','LOSS')"))
    total = n_signals + n_trades

    if total < MIN_SAMPLE_FOR_INFERENCE:
        print(f"\n  {total} resolved outcomes on record. Nothing to suggest below")
        print(f"  {MIN_SAMPLE_FOR_INFERENCE} — any pattern visible in a sample this small is")
        print("  as likely to be noise as signal, and acting on it is how a")
        print("  working system gets tuned into a broken one.")
        print("\n  Keep main.py running and check back in a few days.")
        return

    print(f"\n  {total} resolved outcomes. Sections above are now worth reading")
    print("  as evidence. Change ONE parameter at a time, note the date, and")
    print("  reset the rolling log so results are not mixed across configs.")


if __name__ == "__main__":
    review_executed_trades()
    review_signals()
    suggestions()
    print()