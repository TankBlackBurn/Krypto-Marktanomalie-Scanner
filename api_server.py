"""
api_server.py -- read-only window into the bot, for the phone.

Run this NEXT TO main.py, from the same folder, at the same time. It opens
every database in SQLite read-only mode and never writes to any of them, so
it cannot corrupt state, cannot lock a table the bot is trying to write, and
cannot place a trade. The worst it can do is show you a stale number.

    pip install fastapi uvicorn
    python api_server.py

On first run it writes mobile_token.txt and prints the URL to open on your
phone. Both stay the same across restarts.

WHY READ-ONLY MODE SPECIFICALLY
sqlite3.connect("file:db/x.db?mode=ro", uri=True) is enforced by SQLite
itself, not by convention. Even a bug in this file that tried to run an
UPDATE would be refused by the driver. Given main.py is holding real
positions open, that guarantee is worth more than the two lines it costs.

ENDPOINTS
  /api/health        is the bot alive, when did it last emit a signal
  /api/signals       recent signals with full detail
  /api/performance   the forward-test stats from signal_tracker.py
  /api/trades        open and closed positions
  /api/collectors    how much depth/hype data has accumulated
  /api/config        bot_settings only -- credentials are never served
"""

import json
import os
import secrets
import socket
import sqlite3
import statistics
import sys
import time

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# Paths are relative, matching how main.py addresses them. Run from the bot
# folder or nothing will be found.
SIGNALS_DB = "db/live_signals.db"
TRADES_DB = "db/trading_ledger.db"
DEPTH_DB = "db/depth.db"
HYPE_DB = "db/hype.db"
CONFIG_FILE = "config.json"
TOKEN_FILE = "mobile_token.txt"
STATIC_DIR = "mobile"
PORT = 8787

ROUND_TRIP_COST_PCT = 0.12   # same assumption signal_tracker.py uses

app = FastAPI(title="Sniper Mobile", docs_url=None, redoc_url=None)


# --------------------------------------------------------------- auth

def load_token() -> str:
    if os.path.exists(TOKEN_FILE):
        tok = open(TOKEN_FILE).read().strip()
        if tok:
            return tok
    tok = secrets.token_urlsafe(18)
    with open(TOKEN_FILE, "w") as f:
        f.write(tok)
    return tok


TOKEN = load_token()


def check_auth(request: Request):
    supplied = (request.headers.get("x-token")
                or request.query_params.get("token", ""))
    # Constant-time compare so the token can't be recovered by timing the
    # response. Cheap, and this port may end up reachable from the LAN.
    if not secrets.compare_digest(supplied, TOKEN):
        raise HTTPException(status_code=401, detail="Bad or missing token")


# ------------------------------------------------------------ db access

def q(db_path, sql, params=()):
    """Read-only query. Returns [] for a missing db, missing table, or any
    driver error, because a phone screen showing an empty section beats one
    showing a stack trace."""
    if not os.path.exists(db_path):
        return []
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True,
                             timeout=5) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute(sql, params).fetchall()]
    except sqlite3.Error:
        return []


def columns(db_path, table) -> set:
    return {r["name"] for r in q(db_path, f"PRAGMA table_info({table})")}


def band(values):
    """mean, standard error, and the rough 95% interval. Identical maths to
    signal_tracker.py so the phone and the terminal never disagree."""
    if not values:
        return None
    mean = statistics.mean(values)
    out = {"n": len(values), "mean": mean, "se": None, "lo": None, "hi": None,
           "verdict": "single"}
    if len(values) > 1:
        se = statistics.stdev(values) / (len(values) ** 0.5)
        lo, hi = mean - 2 * se, mean + 2 * se
        out.update({"se": se, "lo": lo, "hi": hi})
        if lo <= 0 <= hi:
            out["verdict"] = "spans_zero"
        elif lo > 0:
            out["verdict"] = "positive"
        else:
            out["verdict"] = "negative"
    return out


def split_halves(pairs):
    """Sort by the first element, compare mean outcome of bottom vs top half."""
    if len(pairs) < 6:
        return None
    pairs = sorted(pairs)
    h = len(pairs) // 2
    lo = statistics.mean(v for _, v in pairs[:h])
    hi = statistics.mean(v for _, v in pairs[-h:])
    return {
        "low_mean": lo, "high_mean": hi, "delta": hi - lo,
        "low_range": [pairs[0][0], pairs[h - 1][0]],
        "high_range": [pairs[-h][0], pairs[-1][0]],
        "n": len(pairs),
    }


# ------------------------------------------------------------- endpoints

@app.get("/api/health")
def health(request: Request):
    check_auth(request)
    rows = q(SIGNALS_DB, "SELECT MAX(signalled_at) t FROM live_signals")
    last = rows[0]["t"] if rows and rows[0]["t"] else None
    pending = q(SIGNALS_DB,
                "SELECT COUNT(*) c FROM live_signals WHERE outcome='PENDING'")
    return {
        "server_time": int(time.time()),
        "last_signal_at": last,
        "pending": pending[0]["c"] if pending else 0,
        "databases": {
            "signals": os.path.exists(SIGNALS_DB),
            "trades": os.path.exists(TRADES_DB),
            "depth": os.path.exists(DEPTH_DB),
            "hype": os.path.exists(HYPE_DB),
        },
    }


@app.get("/api/signals")
def signals(request: Request, limit: int = 60):
    check_auth(request)
    rows = q(SIGNALS_DB, """
        SELECT symbol, signal_type, entry_price, sl_price, sl_percent,
               tp1_price, tp2_price, confluence_score, velocity, buy_pressure,
               funding_rate, extension_pct, suggested_leverage, ai_verdict,
               executed, signalled_at, outcome, resolved_at, peak_price, net_r
        FROM live_signals
        ORDER BY signalled_at DESC LIMIT ?
    """, (min(limit, 300),))
    for r in rows:
        # Peak gain is the number that tells you whether a stopped-out trade
        # was actually close to working. It is not stored, only derivable.
        if r.get("peak_price") and r.get("entry_price"):
            r["peak_gain_pct"] = (r["peak_price"] - r["entry_price"]) / r["entry_price"] * 100
        else:
            r["peak_gain_pct"] = None
    return {"signals": rows}


@app.get("/api/performance")
def performance(request: Request):
    check_auth(request)
    rows = q(SIGNALS_DB, """
        SELECT symbol, signal_type, confluence_score, sl_percent, velocity,
               extension_pct, ai_verdict, outcome, net_r
        FROM live_signals WHERE outcome != 'PENDING'
    """)
    totals = q(SIGNALS_DB, """
        SELECT COUNT(*) total,
               SUM(outcome='PENDING') pending,
               SUM(outcome='BLOCKED_FUNDING') blocked
        FROM live_signals
    """)
    t = totals[0] if totals else {"total": 0, "pending": 0, "blocked": 0}

    # BLOCKED_FUNDING signals were never entered, so they carry no net_r and
    # must not dilute expectancy. They are counted separately for auditing.
    scored = [r for r in rows if r["outcome"] != "BLOCKED_FUNDING"]
    net_rs = [r["net_r"] for r in scored if r["net_r"] is not None]

    outcomes = {}
    for r in scored:
        outcomes[r["outcome"]] = outcomes.get(r["outcome"], 0) + 1

    by_verdict, by_type, by_stop = {}, {}, {"wide": [], "tight": []}
    for r in scored:
        if r["net_r"] is None:
            continue
        if r["ai_verdict"]:
            by_verdict.setdefault(r["ai_verdict"], []).append(r["net_r"])
        if r["signal_type"]:
            by_type.setdefault(r["signal_type"], []).append(r["net_r"])
        if r["sl_percent"]:
            by_stop["wide" if r["sl_percent"] >= 2.0 else "tight"].append(r["net_r"])

    def summarise(d):
        return {k: {"n": len(v), "mean": statistics.mean(v)}
                for k, v in sorted(d.items()) if v}

    return {
        "total_emitted": t["total"] or 0,
        "pending": t["pending"] or 0,
        "blocked_funding": t["blocked"] or 0,
        "resolved": len(scored),
        "outcomes": outcomes,
        "expectancy": band(net_rs),
        "score_split": split_halves(
            [(r["confluence_score"], r["net_r"]) for r in scored
             if r["confluence_score"] is not None and r["net_r"] is not None]),
        "by_ai_verdict": summarise(by_verdict),
        "by_detector": summarise(by_type),
        "by_stop_width": summarise({k: v for k, v in by_stop.items() if len(v) >= 4}),
    }


@app.get("/api/trades")
def trades(request: Request):
    check_auth(request)
    cols = columns(TRADES_DB, "trades")
    if not cols:
        return {"open": [], "closed": [], "note": "No trades table yet."}

    # main.py creates this table with one schema and trade_monitor.py adds
    # columns to it later. Selecting * and filtering in Python means this
    # keeps working whichever version of the table is on disk.
    rows = q(TRADES_DB, "SELECT * FROM trades ORDER BY trade_id DESC LIMIT 200")
    open_rows = [r for r in rows if (r.get("status") or "").upper()
                 in ("PENDING", "OPEN", "ACTIVE")]
    closed = [r for r in rows if (r.get("status") or "").upper()
              in ("WIN", "LOSS", "CLOSED", "STOPPED")]

    realized = [r.get("realized_r") for r in closed
                if r.get("realized_r") is not None]
    wins = sum(1 for r in closed if (r.get("status") or "").upper() == "WIN")

    return {
        "open": open_rows,
        "closed": closed[:60],
        "has_realized_r": "realized_r" in cols,
        "summary": {
            "closed_count": len(closed),
            "wins": wins,
            "losses": len(closed) - wins,
            "realized": band(realized),
        },
    }


@app.get("/api/collectors")
def collectors(request: Request):
    check_auth(request)

    def span(db, table, ts_col="ts"):
        rows = q(db, f"SELECT COUNT(*) n, MIN({ts_col}) lo, MAX({ts_col}) hi "
                     f"FROM {table}")
        if not rows or not rows[0]["n"]:
            return {"rows": 0, "hours": 0, "last_seen": None}
        r = rows[0]
        return {
            "rows": r["n"],
            "hours": (r["hi"] - r["lo"]) / 3600.0 if r["hi"] and r["lo"] else 0,
            "last_seen": r["hi"],
        }

    depth = span(DEPTH_DB, "depth_snapshots")
    syms = q(DEPTH_DB, "SELECT COUNT(DISTINCT symbol) c FROM depth_snapshots")
    depth["symbols"] = syms[0]["c"] if syms else 0
    depth["thinnest"] = q(DEPTH_DB, """
        SELECT symbol, notional_up_2pct v FROM depth_snapshots
        WHERE notional_up_2pct IS NOT NULL
        ORDER BY notional_up_2pct ASC LIMIT 8
    """)

    return {
        "depth": depth,
        "trending": span(HYPE_DB, "trending_snapshots"),
        "biz": span(HYPE_DB, "biz_mentions"),
        # depth_test.py wants 24h+, hype_test.py wants 48h+. Surfacing the
        # bar here saves running the test only to be told it's too early.
        "thresholds": {"depth_hours": 24, "hype_hours": 48},
    }


@app.get("/api/config")
def config(request: Request):
    check_auth(request)
    if not os.path.exists(CONFIG_FILE):
        return {"error": "config.json not found"}
    with open(CONFIG_FILE) as f:
        cfg = json.load(f)
    # api_credentials is deliberately never read. The phone has no business
    # holding exchange keys, and a read-only viewer has no use for them.
    return {"bot_settings": cfg.get("bot_settings", {})}


# ---------------------------------------------------------------- static

if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index():
    path = os.path.join(STATIC_DIR, "index.html")
    if not os.path.exists(path):
        raise HTTPException(404, f"Put index.html in ./{STATIC_DIR}/")
    return FileResponse(path)


@app.get("/manifest.json")
def manifest():
    return FileResponse(os.path.join(STATIC_DIR, "manifest.json"))


def lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


if __name__ == "__main__":
    import uvicorn

    if not os.path.exists(SIGNALS_DB):
        print(f"WARNING: {SIGNALS_DB} not found. Run this from the bot folder")
        print("(the one containing main.py and config.json).\n")

    ip = lan_ip()
    print("=" * 62)
    print("SNIPER MOBILE -- read-only")
    print("=" * 62)
    print(f"\nOn your phone, open:\n\n    http://{ip}:{PORT}/?token={TOKEN}\n")
    print("Phone must be on the same wifi. To reach it from anywhere,")
    print("install Tailscale on both and use the Tailscale IP instead.")
    print(f"\nToken saved in {TOKEN_FILE} -- delete that file to rotate it.")
    print("=" * 62)

    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")