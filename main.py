import asyncio
import json
import logging
import os
import sys
import sqlite3
import time

from data_pipeline import WeexFuturesPipeline
from executor import WeexExecutor
from trade_monitor import TradeMonitor
from ai_analyst import ClaudeAnalyst
from funding_filter import FundingFilter

from telegram_bridge import TelegramBridge

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SniperBrain")

CONFIG_FILE = "config.json"
DB_PATH = "db/trading_ledger.db"
# Every signal is recorded here regardless of whether it executed.
# Without this, signal-only mode leaves no record at all and the
# forward test is just memory.
SIGNAL_LOG_PATH = "db/live_signals.db"

# Candle interval -> minutes, so symbol_cooldown_candles (expressed in
# candles) can be converted into a wall-clock window. Live scanning runs
# on a 30s timer, not per-candle, so the cooldown has to be time-based.
INTERVAL_MINUTES = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "12h": 720, "1d": 1440,
}

if os.path.exists(CONFIG_FILE):
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
    TELEGRAM_BOT_TOKEN = config["api_credentials"].get("telegram_bot_token", "")
    TELEGRAM_CHAT_ID = config["api_credentials"].get("telegram_chat_id", "")
    SCAN_INTERVAL_SECONDS = config["bot_settings"].get("scan_interval_seconds", 30)
    EXECUTE_SCORE = float(config["bot_settings"].get("execute_score", 75))
else:
    logger.error("config.json missing.")
    exit(1)


def load_last_fire_times() -> dict:
    """
    Reload the most recent signal time per symbol, so a restart does not
    reset every cooldown to zero and let a still-running move re-fire.

    Without this, CLO produced 7 of 33 signals (21% of the sample) on a
    single price move -- the ramp detector re-firing on consecutive
    candles. Those are not 7 independent trades; they are one outcome
    counted 7 times, and they corrupt every downstream statistic.
    """
    out = {}
    try:
        if not os.path.exists(SIGNAL_LOG_PATH):
            return out
        with sqlite3.connect(SIGNAL_LOG_PATH) as conn:
            rows = conn.execute(
                "SELECT symbol, MAX(signalled_at) FROM live_signals "
                "WHERE symbol IS NOT NULL GROUP BY symbol"
            ).fetchall()
        out = {sym: ts for sym, ts in rows if ts}
    except Exception as e:
        logger.warning(f"Could not reload cooldown state: {e}")
    return out


def load_seen_candles() -> set:
    """Reload already-reported candle_ids so a restart doesn't re-alert."""
    try:
        if not os.path.exists(SIGNAL_LOG_PATH):
            return set()
        with sqlite3.connect(SIGNAL_LOG_PATH) as conn:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='live_signals'")
            if not cur.fetchone():
                return set()
            rows = conn.execute(
                "SELECT candle_id FROM live_signals WHERE candle_id IS NOT NULL"
            ).fetchall()
        return {r[0] for r in rows}
    except Exception as e:
        logger.error(f"Could not reload seen candles: {e}")
        return set()


def ensure_signal_schema():
    """
    Create or migrate db/live_signals.db BEFORE anything writes to it.

    CREATE TABLE IF NOT EXISTS is a no-op once the table already exists --
    it checks the table name, not its columns. That is exactly what broke
    logging here: funding_rate was added to the CREATE statement after the
    table already existed with 19 rows under the old schema, so every
    INSERT since then failed with "no column named funding_rate", was
    caught by log_live_signal's own try/except, and only ever surfaced as
    a quiet error log. The Telegram alert fires from a separate code path,
    so it looked like everything was working -- it wasn't. Signals since
    that point were sent to Telegram and never persisted.

    Called once at startup instead. New installs get the full schema;
    existing databases get missing columns ADDed, data untouched.
    """
    os.makedirs(os.path.dirname(SIGNAL_LOG_PATH), exist_ok=True)
    with sqlite3.connect(SIGNAL_LOG_PATH) as conn:
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='live_signals'"
        ).fetchone()

        if not exists:
            conn.execute("""
                CREATE TABLE live_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    candle_id TEXT UNIQUE,
                    symbol TEXT, signal_type TEXT,
                    entry_price REAL, sl_price REAL, sl_percent REAL,
                    tp1_price REAL, tp2_price REAL,
                    confluence_score REAL, velocity REAL, buy_pressure REAL,
                    funding_rate REAL,
                    extension_pct REAL, suggested_leverage INTEGER,
                    ai_verdict TEXT, executed INTEGER DEFAULT 0,
                    signalled_at INTEGER,
                    outcome TEXT DEFAULT 'PENDING',
                    resolved_at INTEGER, peak_price REAL, net_r REAL
                )
            """)
            conn.commit()
            logger.info("Created db/live_signals.db")
            return

        cols = [r[1] for r in conn.execute("PRAGMA table_info(live_signals)").fetchall()]
        # Every column ever added to the schema after the table could
        # already exist in the wild goes here -- this list only grows.
        additions = {"funding_rate": "REAL"}
        migrated = [c for c in additions if c not in cols]
        for col in migrated:
            conn.execute(f"ALTER TABLE live_signals ADD COLUMN {col} {additions[col]}")
        if migrated:
            conn.commit()
            logger.info(f"Auto-migrated db/live_signals.db: added {migrated}")


def log_live_signal(anomaly: dict, executed: bool, ai_verdict: str,
                    blocked_funding: bool = False):
    """Record every signal the bot emits, executed or not."""
    try:
        with sqlite3.connect(SIGNAL_LOG_PATH) as conn:
            conn.execute("""
                INSERT OR IGNORE INTO live_signals
                (candle_id, symbol, signal_type, entry_price, sl_price, sl_percent,
                 tp1_price, tp2_price, confluence_score, velocity, buy_pressure,
                 funding_rate, extension_pct, suggested_leverage, ai_verdict,
                 executed, signalled_at, outcome)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                anomaly.get("candle_id"), anomaly.get("symbol"),
                anomaly.get("signal_type"), anomaly.get("price_usd"),
                anomaly.get("sl_price"), anomaly.get("sl_percent"),
                anomaly.get("tp1_price"), anomaly.get("tp2_price"),
                anomaly.get("confluence_score"), anomaly.get("micro_velocity"),
                anomaly.get("buy_pressure"), anomaly.get("funding_rate"),
                anomaly.get("extension_pct"),
                anomaly.get("suggested_leverage"), ai_verdict,
                1 if executed else 0, int(time.time()),
                "BLOCKED_FUNDING" if blocked_funding else "PENDING",
            ))
            conn.commit()
    except Exception as e:
        # If this ever fires again it will now say exactly why -- no more
        # silent schema-mismatch drops.
        logger.error(f"Failed to log live signal: {type(e).__name__}: {e}")


def log_sniper_trade(anomaly: dict):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, side TEXT, chain TEXT,
                entry_price REAL, stop_loss REAL, take_profit REAL,
                tp1_price REAL, tp2_price REAL,
                status TEXT DEFAULT 'PENDING',
                trail_step INTEGER DEFAULT 0,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            INSERT INTO trades (symbol, side, chain, entry_price, stop_loss, take_profit, tp1_price, tp2_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            anomaly['symbol'],
            anomaly.get('side', 'LONG').upper(),
            anomaly.get('chain', 'WEEX Futures'),
            anomaly['price_usd'],
            anomaly['sl_price'],
            anomaly.get('tp_price', 0),
            anomaly.get('tp1_price', 0),
            anomaly.get('tp2_price', 0),
        ))
        conn.commit()


async def run_trade_monitor_loop(monitor: TradeMonitor):
    while True:
        try:
            await monitor.check_active_trades()
            await asyncio.sleep(15)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Trade Monitor Error: {e}")
            await asyncio.sleep(15)


async def main_loop():
    logger.info("Initializing WEEX Sniper Breakout Engine...")

    pipeline = WeexFuturesPipeline(config)
    executor = WeexExecutor(config)
    analyst = ClaudeAnalyst(config)
    funding = FundingFilter(config)
    telegram = TelegramBridge(bot_token=TELEGRAM_BOT_TOKEN, chat_id=TELEGRAM_CHAT_ID)

    monitor = TradeMonitor(DB_PATH, telegram, config)
    monitor_task = asyncio.create_task(run_trade_monitor_loop(monitor))

    # Must run before load_seen_candles() or any log_live_signal() call --
    # this is what silently broke last time it was skipped.
    ensure_signal_schema()

    logger.info(f"Signal log: {SIGNAL_LOG_PATH} (every signal recorded, executed or not)")
    auto = "ON" if executor.enabled else "OFF (signals only)"
    await telegram._send_raw_message(
        f"<b>WEEX SNIPER BOT ONLINE</b>\n"
        f"Execution: {auto} | Score gate: {EXECUTE_SCORE} | "
        f"AI: {analyst.mode} | Funding filter: {funding.mode}"
    )

    # One signal per symbol per candle. Without this, a setup re-fires on
    # every 30s scan for as long as its candle is the most recent one.
    seen_candles = load_seen_candles()
    if seen_candles:
        logger.info(f"Reloaded {len(seen_candles)} previously-reported candles; they will not re-alert.")

    # Per-symbol cooldown. symbol_cooldown_candles has existed in config and
    # been honoured by backtest.py all along, but was never enforced on the
    # live path -- which is exactly why backtests looked clean while live
    # produced 7 CLO signals on one move.
    interval_min = INTERVAL_MINUTES.get(pipeline.interval, 15)
    cooldown_seconds = pipeline.symbol_cooldown_candles * interval_min * 60
    last_fire = load_last_fire_times()
    logger.info(
        f"Symbol cooldown: {pipeline.symbol_cooldown_candles} candles "
        f"({cooldown_seconds // 60}min at {pipeline.interval}) | "
        f"{len(last_fire)} symbols with prior signals loaded"
    )

    try:
        while True:
            logger.info("Scanning WEEX...")
            anomalies = await pipeline.scan_market()

            new_anomalies = [a for a in anomalies if a.get("candle_id") not in seen_candles]
            for a in new_anomalies:
                seen_candles.add(a["candle_id"])
            if len(seen_candles) > 5000:
                seen_candles = set(list(seen_candles)[-2000:])

            # Drop symbols still inside their cooldown window. Done here
            # rather than after the AI review so a suppressed repeat costs
            # neither an Anthropic call nor a funding lookup.
            now_ts = int(time.time())
            passed, suppressed = [], []
            for a in new_anomalies:
                sym = a.get("symbol")
                prev = last_fire.get(sym)
                if prev and (now_ts - prev) < cooldown_seconds:
                    suppressed.append(sym)
                    continue
                passed.append(a)
                last_fire[sym] = now_ts
            new_anomalies = passed

            logger.info(
                f"Scan complete. {len(anomalies)} setups, {len(new_anomalies)} new. "
                f"Next scan in {SCAN_INTERVAL_SECONDS}s."
            )
            if suppressed:
                logger.info(
                    f"Cooldown suppressed {len(suppressed)} repeat signal(s): "
                    f"{', '.join(sorted(set(suppressed))[:8])}"
                )

            for anomaly in new_anomalies:
                symbol = anomaly["symbol"]
                score = anomaly.get("confluence_score", 0)
                sig_type = anomaly.get("signal_type", "")

                logger.info(f"{sig_type} on {symbol} | confluence {score}/100")

                # Funding check first: a blocked signal should not spend an
                # Anthropic API call on a review nobody will read.
                fund_ok, fund_rate, fund_note = await funding.check(symbol)
                anomaly["funding_rate"] = fund_rate
                blocked_funding = not fund_ok

                if blocked_funding:
                    logger.info(f"BLOCKED {symbol} on funding: {fund_note}")
                    # Still recorded, with outcome BLOCKED_FUNDING, so the
                    # filter can be audited later. A filter you cannot check
                    # is a filter you cannot trust.
                    log_live_signal(anomaly, False, "", blocked_funding=True)
                    continue

                review = await analyst.review(anomaly)
                verdict = review["verdict"]

                # Advisory mode annotates only. Gate mode can veto.
                blocked = analyst.can_block and verdict == "AVOID"

                executed = False
                if blocked:
                    logger.info(f"BLOCKED {symbol} by AI gate: {review['concern']}")
                    anomaly["skip_reason"] = f"AI gate: {review['concern']}"
                elif score >= EXECUTE_SCORE:
                    logger.info(f"EXECUTING {symbol} (confluence {score} >= {EXECUTE_SCORE})")
                    executed = await executor.execute_scalp_trade(anomaly)
                    if executed:
                        log_sniper_trade(anomaly)
                    else:
                        anomaly["skip_reason"] = "Execution disabled, or exchange rejected the order."
                else:
                    logger.info(f"BELOW GATE {symbol} (confluence {score} < {EXECUTE_SCORE})")
                    anomaly["skip_reason"] = f"Confluence {score} below execute threshold {EXECUTE_SCORE}."

                log_live_signal(anomaly, executed, verdict)

                note = f"Confluence {score}/100. {review['note']}"
                if fund_note:
                    note += f" | {fund_note}"

                try:
                    await telegram.send_sniper_signal(anomaly, note, executed)
                except Exception as e:
                    logger.error(f"Telegram formatted send failed: {e}")
                    await telegram._send_raw_message(
                        f"<b>{sig_type}</b>: {symbol}\nConfluence {score}/100"
                    )

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    except asyncio.CancelledError:
        logger.info("Main loop cancellation requested.")
    finally:
        logger.info("Shutting down cleanly...")
        monitor_task.cancel()
        await executor.close()
        await monitor.close()
        await analyst.close()
        await funding.close()
        try:
            await telegram.close()
        except Exception:
            pass
        await asyncio.sleep(0.5)


if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\nSniper Bot stopped cleanly.")