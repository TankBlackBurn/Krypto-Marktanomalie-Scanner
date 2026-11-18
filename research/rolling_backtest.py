"""
rolling_backtest.py -- accumulate backtest signals across many days.

The WEEX klines endpoint caps at 100 candles per call, so any single
backtest run is stuck looking at ~25 hours of 15m history no matter how
the code is written. That's why every run so far has landed on n=1, n=4,
n=21 -- not because setups are rare, but because the WINDOW is small.

This doesn't fix the window. It works around it: run this once a day (or
put it on a schedule) and it appends today's signals to a persistent log,
deduplicated by (symbol, entry_ts) so the ~24h of overlap between
consecutive daily runs doesn't double-count. After a week you have ~7x
the sample of any single run, without needing more history than the API
will ever give you in one call.

    python -m research.rolling_backtest            run today's backtest, log it,
                                           print AGGREGATE stats across
                                           every day logged so far

    python -m research.rolling_backtest --report   just show the aggregate,
                                           don't run a new backtest

Ten independent daily runs is still a small sample for a strategy this
noisy. Treat "aggregate net R turned positive after N days" as encouraging,
not as a green light -- the honest bar is holding up over several weeks,
ideally spanning both a trending and a choppy market.
"""

import asyncio
import json
import os
import sqlite3
import sys
import time
import statistics
from datetime import datetime, timezone

from research.backtest import Backtester, load_config

DB_PATH = "db/backtest_log.db"


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                entry_ts INTEGER NOT NULL,
                run_date TEXT NOT NULL,
                score REAL, type TEXT, sl_pct REAL, velocity REAL,
                outcome TEXT, r_multiple REAL, cost_r REAL, net_r REAL,
                max_gain_pct REAL,
                config_snapshot TEXT,
                UNIQUE(symbol, entry_ts)
            )
        """)
        conn.commit()


def store(signals, config_snapshot: str) -> int:
    if not signals:
        return 0
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = [
        (s["symbol"], s["entry_ts"], today, s["score"], s["type"],
         s["sl_pct"], s["velocity"], s["outcome"], s["r_multiple"],
         s["cost_r"], s["net_r"], s["max_gain_pct"], config_snapshot)
        for s in signals
    ]
    with sqlite3.connect(DB_PATH) as conn:
        before = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        conn.executemany("""
            INSERT OR IGNORE INTO signals
            (symbol, entry_ts, run_date, score, type, sl_pct, velocity,
             outcome, r_multiple, cost_r, net_r, max_gain_pct, config_snapshot)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        after = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    return after - before


def report():
    if not os.path.exists(DB_PATH):
        print("No log yet. Run without --report first.")
        return

    with sqlite3.connect(DB_PATH) as conn:
        total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        if not total:
            print("Log exists but is empty.")
            return

        days = conn.execute("SELECT COUNT(DISTINCT run_date) FROM signals").fetchone()[0]
        date_range = conn.execute("SELECT MIN(run_date), MAX(run_date) FROM signals").fetchone()
        rows = conn.execute(
            "SELECT symbol, sl_pct, outcome, r_multiple, net_r, max_gain_pct, run_date FROM signals"
        ).fetchall()
        by_symbol = conn.execute("""
            SELECT symbol, COUNT(*) c FROM signals GROUP BY symbol ORDER BY c DESC LIMIT 10
        """).fetchall()
        configs = conn.execute(
            "SELECT DISTINCT config_snapshot FROM signals"
        ).fetchall()

    print("=" * 70)
    print("ROLLING BACKTEST -- AGGREGATE ACROSS ALL LOGGED RUNS")
    print("=" * 70)
    print(f"\nDistinct daily runs: {days}  ({date_range[0]} to {date_range[1]})")
    print(f"Total unique signals: {total}")

    if len(configs) > 1:
        print(f"\n  WARNING: {len(configs)} different config snapshots are mixed in")
        print("  this log. Changing settings between runs invalidates the")
        print("  aggregate -- results below combine signals produced under")
        print("  DIFFERENT rules. Treat this run as the start of a fresh log")
        print("  if you changed config.json recently.")

    net_rs = [r[4] for r in rows]
    print(f"\nAGGREGATE EXPECTANCY")
    print(f"  mean net R:      {statistics.mean(net_rs):+.3f}")
    if len(net_rs) > 1:
        stdev = statistics.stdev(net_rs)
        se = stdev / (len(net_rs) ** 0.5)
        print(f"  std dev:         {stdev:.3f}")
        print(f"  std error:       {se:.3f}  (mean +/- 2*SE is a rough 95% band: "
              f"{statistics.mean(net_rs) - 2*se:+.3f} to {statistics.mean(net_rs) + 2*se:+.3f})")

    outcomes = {}
    for r in rows:
        outcomes[r[2]] = outcomes.get(r[2], 0) + 1
    print(f"\n  Outcomes: " + ", ".join(f"{k} {v}" for k, v in outcomes.items()))

    by_sym_r = {}
    for r in rows:
        by_sym_r.setdefault(r[0], []).append(r[4])
    top_sym, top_n = max(by_sym_r.items(), key=lambda x: len(x[1]))
    if len(top_n) / total > 0.3:
        print(f"\n  WARNING: {top_sym} produced {len(top_n)}/{total} "
              f"({len(top_n)/total*100:.0f}%) of all signals. Aggregate is")
        print("  concentration risk, not a diversified track record.")

    print(f"\n  Signals per day (top 10 symbols):")
    for sym, c in by_symbol:
        print(f"     {sym:<14} {c}")

    if days < 7:
        print(f"\n  Only {days} day(s) logged. Keep running this daily --")
        print("  results before ~2 weeks of daily runs are still a small sample.")

    print("=" * 70)


async def run_and_log():
    init_db()
    cfg = load_config()
    bt = Backtester(cfg)
    await bt.run()

    snapshot = json.dumps({
        "interval": bt.p.interval, "min_volume_usd": bt.p.min_volume_usd,
        "max_extension_pct": bt.p.max_extension_pct,
        "max_sl_percent": bt.p.max_sl_percent, "min_sl_percent": bt.p.min_sl_percent,
        "min_score": bt.p.min_score,
    }, sort_keys=True)

    added = store(bt.signals, snapshot)
    print(f"\n{added} new signal(s) added to the rolling log "
          f"({len(bt.signals) - added} were duplicates of a previous day).")

    report()


if __name__ == "__main__":
    if "--report" in sys.argv:
        report()
    else:
        asyncio.run(run_and_log())