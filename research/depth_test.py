"""
depth_test.py -- does book thinness predict pumps?

Same lift methodology as pattern_test.py / key_level_test.py / funding_test.py:
bucket every snapshot by a book-thinness metric, then compare forward
returns against the base rate. Four hypotheses have failed this test
already; treat this one with the same skepticism, not less.

Requires depth_collector.py to have been running for at least 24h --
there is no history endpoint, so this can only measure what has been
collected forward from when the collector started.

    python -m research.depth_test
    python -m research.depth_test --hours 4      forward window (default 2h)

WHAT WOULD ACTUALLY JUSTIFY BUILDING ON THIS
The thinnest-book band needs a mean forward return clearly above base AND
a higher rate of big moves. A thin book could just as easily precede a
dump as a pump -- thinness cuts both ways, same trap as every volume-based
signal tested so far. Watch the SIGNED forward return, not just magnitude.
"""

import os
import sqlite3
import statistics
import sys
from collections import defaultdict

DB_PATH = "db/depth.db"
BIG_MOVE_PCT = 5.0

# Percentile bands, computed on the fly since "thin" is relative --
# a $50k book is normal for a microcap and alarming for a major.
BAND_LABELS = [
    "thinnest 10%  (notional to move 2%)",
    "10-25th percentile",
    "25-75th percentile (typical)",
    "75-90th percentile",
    "thickest 10%",
]


def load(hours_forward):
    if not os.path.exists(DB_PATH):
        print("No db/depth.db found. Run depth_collector.py first.")
        return None, 0.0

    with sqlite3.connect(DB_PATH) as conn:
        span_row = conn.execute(
            "SELECT MIN(ts), MAX(ts) FROM depth_snapshots").fetchone()
        rows = conn.execute("""
            SELECT symbol, ts, midprice, notional_up_2pct, notional_down_2pct
            FROM depth_snapshots
            WHERE notional_up_2pct IS NOT NULL AND midprice > 0
            ORDER BY symbol, ts
        """).fetchall()

    span_hours = 0.0
    if span_row and span_row[0] and span_row[1]:
        span_hours = (span_row[1] - span_row[0]) / 3600.0

    if not rows:
        return None, span_hours

    by_symbol = defaultdict(list)
    for sym, ts, mid, up, down in rows:
        by_symbol[sym].append((ts, mid, up, down))

    window_s = hours_forward * 3600
    observations = []  # (notional_up, notional_down, fwd_return_pct)

    for sym, series in by_symbol.items():
        series.sort()
        for i, (ts, mid, up, down) in enumerate(series):
            target_t = ts + window_s
            # Nearest snapshot at or after the forward horizon, within a
            # generous tolerance -- poll spacing is not perfectly regular.
            fwd = None
            for j in range(i + 1, len(series)):
                if series[j][0] >= target_t:
                    if series[j][0] - target_t < window_s * 0.5:
                        fwd = series[j][1]
                    break
            if fwd is None or mid <= 0:
                continue
            fwd_ret = (fwd - mid) / mid * 100.0
            observations.append((up, down, fwd_ret))

    return observations, span_hours


def report(observations, hours_forward, span_hours):
    if len(observations) < 200:
        print(f"Only {len(observations)} observations with a resolved forward "
              f"point -- too few. Let the collector run longer.")
        return

    print("=" * 78)
    if span_hours < 24:
        print(f"PRELIMINARY -- only {span_hours:.1f}h collected, the docstring's own")
        print(f"stated minimum is 24h. The sample-size check below (200 obs) does")
        print(f"NOT verify this; it was possible to get a full, confident-looking")
        print(f"table at 20h with 34,000+ observations because polling many symbols")
        print(f"quickly produces volume fast even when the calendar time is short.")
        print(f"Treat everything below as provisional until this clears 24h.")
        print("=" * 78)

    ups = sorted(o[0] for o in observations)
    n = len(ups)
    p10, p25, p75, p90 = (ups[int(n * q)] for q in (0.10, 0.25, 0.75, 0.90))

    bands = [
        (BAND_LABELS[0], lambda u: u <= p10),
        (BAND_LABELS[1], lambda u: p10 < u <= p25),
        (BAND_LABELS[2], lambda u: p25 < u <= p75),
        (BAND_LABELS[3], lambda u: p75 < u <= p90),
        (BAND_LABELS[4], lambda u: u > p90),
    ]

    all_fwd = [o[2] for o in observations]
    base_mean = statistics.mean(all_fwd)
    base_big_up = sum(1 for r in all_fwd if r >= BIG_MOVE_PCT) / len(all_fwd)
    base_big_down = sum(1 for r in all_fwd if r <= -BIG_MOVE_PCT) / len(all_fwd)

    print(f"\n{'band':<38}{'n':>7}{'>5% up':>9}{'>5% dn':>9}{'mean fwd':>11}")
    print("-" * 78)
    for label, pred in bands:
        vals = [o[2] for o in observations if pred(o[0])]
        if len(vals) < 20:
            print(f"{label:<38}{len(vals):>7}   too few")
            continue
        big_up = sum(1 for r in vals if r >= BIG_MOVE_PCT) / len(vals)
        big_dn = sum(1 for r in vals if r <= -BIG_MOVE_PCT) / len(vals)
        mean = statistics.mean(vals)
        mark = "  <-- worth investigating" if mean > base_mean + 0.5 and big_up > base_big_up else ""
        print(f"{label:<38}{len(vals):>7}{big_up*100:>8.1f}%{big_dn*100:>8.1f}%"
              f"{mean:>+10.2f}%{mark}")

    print("-" * 78)
    print(f"{'ALL (base rate)':<38}{len(all_fwd):>7}{base_big_up*100:>8.1f}%"
          f"{base_big_down*100:>8.1f}%{base_mean:>+10.2f}%")

    print(f"\np10={p10:,.0f}  p25={p25:,.0f}  p75={p75:,.0f}  p90={p90:,.0f}  "
          f"(USD notional to move price {2.0}% through the ask side)")

    print("\n" + "=" * 78)
    print("HOW TO READ THIS")
    print("  'thinnest 10%' needs mean fwd return above base AND more UP moves")
    print("  specifically, not just more moves. A thin book that gets swept")
    print("  down as often as up is a volatility marker, same trap as every")
    print("  other signal tested in this project -- not a pump predictor.")
    print("=" * 78)


if __name__ == "__main__":
    hrs = 2
    if "--hours" in sys.argv:
        hrs = float(sys.argv[sys.argv.index("--hours") + 1])
    obs, span_hours = load(hrs)
    if obs is not None:
        report(obs, hrs, span_hours)