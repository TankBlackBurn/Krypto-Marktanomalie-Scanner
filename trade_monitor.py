import sqlite3
import ccxt.async_support as ccxt
import logging
import json
import os
import asyncio

logger = logging.getLogger("SniperTradeMonitor")


class TradeMonitor:
    """
    Manages open positions: partial profit-taking, stop migration, and
    recording how each trade actually ended.

    Fixes over the previous version:
      - Breakeven stop was entry * 1.002 (0.2%). That is inside the spread
        plus normal noise on most alts, so it scratched winners almost
        immediately after TP1. Now derived from the trade's own risk.
      - Trades were marked 'CLOSED' with no outcome, so nothing downstream
        ever learned whether a signal worked. Now records WIN/LOSS, the
        approximate exit, and realized R.
      - The legacy TP1 backfill put TP1 BELOW entry on a long, which would
        have triggered an instant "profit" partial close.
      - _normalize_symbol stripped every occurrence of "USDT", so a base
        asset containing those letters was mangled.
      - After a partial close, the replacement stop was sized from the
        PRE-close position, so a partial fill left the size wrong.
      - If the replacement stop failed to place, the position was left
        completely unprotected and only a debug line was written.
    """

    def __init__(self, db_path: str, telegram_bridge, config: dict = None):
        self.db_path = db_path
        self.telegram = telegram_bridge
        self.exchange = None

        cfg = (config or {}).get("bot_settings", {})
        # Where the stop goes after TP1, as a fraction of the original risk.
        # 0.25 means "lock in a quarter of what you were risking" -- far
        # enough from entry to survive noise, unlike a 0.2% breakeven.
        self.breakeven_r = float(cfg.get("breakeven_r_multiple", 0.3))
        self.tp1_close_ratio = float(cfg.get("tp1_close_ratio", 0.5))
        self.tp2_close_ratio = float(cfg.get("tp2_close_ratio", 0.50))

        # "fixed"    -- close the remainder at TP2 (old behaviour, caps upside)
        # "trailing" -- bank a partial at TP1, then let the rest run behind a
        #               ratcheting stop with no upper limit. This lets an
        #               outlier move be worth more than a fixed target.
        self.exit_mode = str(cfg.get("exit_mode", "trailing")).lower()
        # Stop sits this many R behind the highest price seen. E.g. at a 2% stop
        # and 2.0R, that is 4% behind the peak.
        self.trail_distance_r = float(cfg.get("trail_distance_r", 1.5))
        # Don't rewrite the exchange stop for trivial improvements -- each
        # move costs a cancel + create round trip.
        self.trail_update_pct = float(cfg.get("trail_update_threshold_pct", 0.5))

        self._ensure_schema()
        self._init_exchange()

    # ------------------------------------------------------------- schema

    def _ensure_schema(self):
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='trades'")
                exists = cursor.fetchone()

                if not exists:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS trades (
                            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            symbol TEXT, side TEXT DEFAULT 'LONG', chain TEXT,
                            entry_price REAL, stop_loss REAL, take_profit REAL,
                            tp1_price REAL, tp2_price REAL,
                            initial_stop REAL, peak_price REAL,
                            status TEXT DEFAULT 'PENDING',
                            trail_step INTEGER DEFAULT 0,
                            exit_price REAL, realized_r REAL, closed_at DATETIME,
                            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    conn.commit()
                    return

                cursor.execute("PRAGMA table_info(trades)")
                cols = [r[1] for r in cursor.fetchall()]
                additions = {
                    "side": "TEXT DEFAULT 'LONG'",
                    "tp1_price": "REAL",
                    "tp2_price": "REAL",
                    "trail_step": "INTEGER DEFAULT 0",
                    # New: needed to compute R and to place a sane breakeven.
                    "initial_stop": "REAL",
                    "peak_price": "REAL",
                    "exit_price": "REAL",
                    "realized_r": "REAL",
                    "closed_at": "DATETIME",
                }
                for col, decl in additions.items():
                    if col not in cols:
                        cursor.execute(f"ALTER TABLE trades ADD COLUMN {col} {decl}")
                        logger.info(f"Auto-migrated database: added '{col}'.")
                conn.commit()
        except Exception as e:
            logger.error(f"Schema auto-migration error: {e}")

    def _init_exchange(self):
        config_path = "config.json"
        if os.path.exists(config_path):
            try:
                with open(config_path, "r") as f:
                    config = json.load(f)
                creds = config.get("api_credentials", {})
                self.exchange = ccxt.weex({
                    'apiKey': creds.get("weex_api_key", ""),
                    'secret': creds.get("weex_secret_key", ""),
                    'password': creds.get("weex_api_passphrase", ""),
                    'enableRateLimit': True,
                    'options': {'defaultType': 'swap'},
                })
            except Exception as e:
                logger.error(f"Failed to initialize exchange in TradeMonitor: {e}")

    # -------------------------------------------------------------- utils

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        """
        'BTC/USDT:USDT' -> 'BTC'.  The old version called
        .replace("USDT", "") which stripped EVERY occurrence, so a base
        asset containing those letters came out mangled.
        """
        if not symbol:
            return ""
        s = symbol.upper().strip()
        for suffix in ("/USDT:USDT", ":USDT", "/USDT", "USDT"):
            if s.endswith(suffix):
                return s[: -len(suffix)].replace("/", "").replace(":", "")
        return s.replace("/", "").replace(":", "")

    def _execute_query(self, query: str, params: tuple = ()):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.cursor().execute(query, params)
                conn.commit()
        except Exception as e:
            logger.error(f"Database write failed: {e}")

    async def _move_stop(self, ccxt_sym, norm_sym, side, amount, new_sl) -> bool:
        """Cancel the resting stop and place a new one. True if it stuck."""
        if amount <= 0:
            return False
        close_side = 'sell' if side == 'LONG' else 'buy'
        for attempt in range(3):
            try:
                await self.exchange.cancel_all_orders(ccxt_sym)
                await asyncio.sleep(0.4)
                px = float(self.exchange.price_to_precision(ccxt_sym, new_sl))
                await self.exchange.create_order(
                    ccxt_sym, 'market', close_side, amount, None,
                    {'stopLossPrice': px, 'reduceOnly': True})
                return True
            except Exception as e:
                logger.warning(f"Stop move attempt {attempt+1} failed on {ccxt_sym}: {e}")
                await asyncio.sleep(0.8)

        # Three failures means the cancel probably went through and the
        # replacement did not. Say so loudly -- this is a live unhedged
        # position, not a logging curiosity.
        logger.error(f"{ccxt_sym} may be UNPROTECTED -- stop could not be moved.")
        try:
            await self.telegram._send_raw_message(
                f"<b>WARNING — {norm_sym}</b>\nCould not move the stop after three "
                f"attempts. This position may be running with NO stop on the "
                f"exchange. Check and set one manually.")
        except Exception:
            pass
        return False

    async def _close_out(self, rowid, sym, entry, initial_stop, trail_step, side):
        """
        Position no longer on the exchange. Record how it ended.

        The exit price is an approximation -- we detect the close on a
        polling cycle, not at fill time, so the mark can be seconds to a
        minute stale. Good enough to classify WIN/LOSS and estimate R; not
        good enough to reconcile against exchange PnL to the cent.
        """
        exit_price = entry
        try:
            ticker = await self.exchange.fetch_ticker(f"{sym}/USDT:USDT")
            exit_price = float(ticker.get('last') or entry)
        except Exception:
            pass

        risk = abs(entry - initial_stop) if initial_stop else 0.0
        if risk > 0:
            raw = (exit_price - entry) if str(side).upper() == "LONG" else (entry - exit_price)
            realized_r = raw / risk
        else:
            realized_r = 0.0

        # A trade that reached TP1 banked a partial regardless of where the
        # remainder came out, so don't label it a loss on the tail alone.
        if trail_step >= 1:
            status = "WIN"
        else:
            status = "WIN" if realized_r > 0 else "LOSS"

        self._execute_query(
            """UPDATE trades SET status = ?, exit_price = ?, realized_r = ?,
               closed_at = CURRENT_TIMESTAMP WHERE rowid = ?""",
            (status, exit_price, realized_r, rowid),
        )
        logger.info(f"{sym} closed: {status} | approx exit {exit_price} | "
                    f"{realized_r:+.2f}R (trail step {trail_step})")

    # ------------------------------------------------------------- monitor

    async def check_active_trades(self):
        if not self.exchange:
            return

        try:
            with sqlite3.connect(self.db_path) as conn:
                pending = conn.cursor().execute(
                    """SELECT rowid, symbol, side, entry_price, stop_loss,
                              tp1_price, tp2_price, trail_step, take_profit,
                              initial_stop, peak_price
                       FROM trades WHERE status = 'PENDING'"""
                ).fetchall()
        except Exception as db_err:
            logger.error(f"Trade Monitor DB Read Error: {db_err}")
            return

        if not pending:
            return

        try:
            if not self.exchange.markets:
                await self.exchange.load_markets()
            positions_list = await self.exchange.fetch_positions()
            active_map = {
                self._normalize_symbol(p.get('symbol', '')): p
                for p in positions_list
                if float(p.get('contracts', 0) or 0) > 0
            }
        except Exception as e:
            logger.debug(f"Position fetch failed: {e}")
            return

        for (rowid, sym, side, entry, sl, tp1, tp2,
             trail_raw, fallback_tp, initial_stop, peak_raw) in pending:

            norm_sym = self._normalize_symbol(sym)
            ccxt_sym = f"{norm_sym}/USDT:USDT"
            trade_side = str(side or "LONG").upper()
            trail_step = int(trail_raw or 0)

            # Capture the original stop the first time we see this trade.
            # Everything downstream (R, breakeven) depends on it, and
            # stop_loss gets overwritten as the trade progresses.
            if initial_stop is None:
                initial_stop = sl
                self._execute_query(
                    "UPDATE trades SET initial_stop = ? WHERE rowid = ?",
                    (initial_stop, rowid))

            # Legacy rows with no TP levels. The old backfill put TP1 BELOW
            # entry on a long, which reads as instantly hit.
            if tp1 is None:
                base = fallback_tp or entry
                if trade_side == "LONG":
                    tp1 = min(base, entry * 1.05) if base > entry else entry * 1.02
                    tp2 = max(base, tp1 * 1.02)
                else:
                    tp1 = max(base, entry * 0.95) if base < entry else entry * 0.98
                    tp2 = min(base, tp1 * 0.98)
                self._execute_query(
                    "UPDATE trades SET tp1_price = ?, tp2_price = ? WHERE rowid = ?",
                    (tp1, tp2, rowid))

            if norm_sym not in active_map:
                await self._close_out(rowid, norm_sym, entry, initial_stop,
                                      trail_step, trade_side)
                continue

            pos_size = float(active_map[norm_sym].get('contracts', 0))

            try:
                ticker = await self.exchange.fetch_ticker(ccxt_sym)
                current_price = float(ticker.get('last', entry))
                risk = abs(entry - initial_stop) if initial_stop else 0.0

                partial_ratio = 0.0
                new_trail = trail_step
                new_sl = sl
                alert = ""

                # --- trailing management (runs before target checks) ---
                # Once TP1 is banked the remainder rides a stop that only ever
                # moves in your favour. No upper target, so an outlier is not
                # capped at TP2 the way the old fixed exit capped it.
                peak = max(float(peak_raw or entry), current_price)
                if peak != (peak_raw or 0):
                    self._execute_query(
                        "UPDATE trades SET peak_price = ? WHERE rowid = ?",
                        (peak, rowid))

                if (self.exit_mode == "trailing" and trail_step >= 1
                        and risk > 0):
                    gap = risk * self.trail_distance_r
                    trail_sl = (peak - gap) if trade_side == "LONG" else (peak + gap)
                    improves = ((trail_sl > sl) if trade_side == "LONG"
                                else (trail_sl < sl))
                    material = abs(trail_sl - sl) / entry * 100.0 >= self.trail_update_pct
                    if improves and material:
                        moved = await self._move_stop(ccxt_sym, norm_sym, trade_side,
                                                      pos_size, trail_sl)
                        if moved:
                            self._execute_query(
                                "UPDATE trades SET stop_loss = ? WHERE rowid = ?",
                                (trail_sl, rowid))
                            locked = (abs(trail_sl - entry) / risk)
                            direction = "above" if trail_sl > entry else "below"
                            logger.info(
                                f"{norm_sym} trail -> {trail_sl:.6g} "
                                f"({locked:.2f}R {direction} entry, peak {peak:.6g})")
                        continue

                if trade_side == "LONG":
                    if current_price >= tp2 and trail_step < 2:
                        if self.exit_mode == "trailing":
                            # Bank a slice, then let the trail carry the rest.
                            partial_ratio = self.tp2_close_ratio
                            new_trail = 2
                            new_sl = max(sl, peak - risk * self.trail_distance_r)
                            alert = (f"<b>{norm_sym} TP2 HIT</b> — closing "
                                     f"{self.tp2_close_ratio:.0%}, remainder now "
                                     f"trailing {self.trail_distance_r:.1f}R behind "
                                     f"the high. No upper target.")
                        else:
                            partial_ratio = self.tp2_close_ratio
                            new_trail, new_sl = 2, tp1
                            alert = (f"<b>{norm_sym} TP2 HIT</b> — closing "
                                     f"{self.tp2_close_ratio:.0%} of remainder. "
                                     f"Stop moved to TP1 (${new_sl:.6g}).")
                    elif current_price >= tp1 and trail_step < 1:
                        partial_ratio = self.tp1_close_ratio
                        new_trail = 1
                        # Was entry * 1.002 -- 0.2%, inside the spread plus
                        # noise, so it scratched winners on the first wiggle.
                        new_sl = entry + (risk * self.breakeven_r) if risk else entry * 1.002
                        alert = (f"<b>{norm_sym} TP1 HIT</b> — closing "
                                 f"{self.tp1_close_ratio:.0%}. Stop moved to "
                                 f"${new_sl:.6g} (+{self.breakeven_r:.2f}R locked).")
                else:
                    if current_price <= tp2 and trail_step < 2:
                        partial_ratio = self.tp2_close_ratio
                        new_trail, new_sl = 2, tp1
                        alert = (f"<b>{norm_sym} TP2 HIT</b> — closing "
                                 f"{self.tp2_close_ratio:.0%} of remainder. "
                                 f"Stop moved to TP1 (${new_sl:.6g}).")
                    elif current_price <= tp1 and trail_step < 1:
                        partial_ratio = self.tp1_close_ratio
                        new_trail = 1
                        new_sl = entry - (risk * self.breakeven_r) if risk else entry * 0.998
                        alert = (f"<b>{norm_sym} TP1 HIT</b> — closing "
                                 f"{self.tp1_close_ratio:.0%}. Stop moved to "
                                 f"${new_sl:.6g} (+{self.breakeven_r:.2f}R locked).")

                if partial_ratio <= 0:
                    continue

                close_side = 'sell' if trade_side == 'LONG' else 'buy'
                close_amount = float(self.exchange.amount_to_precision(
                    ccxt_sym, pos_size * partial_ratio))
                if close_amount <= 0:
                    continue

                await self.exchange.create_order(
                    ccxt_sym, 'market', close_side, close_amount, None,
                    {'reduceOnly': True})

                # Re-read the position instead of assuming pos_size minus
                # what we asked for -- a partial fill would otherwise leave
                # the replacement stop covering the wrong size.
                await asyncio.sleep(0.6)
                rem_amount = 0.0
                try:
                    refreshed = await self.exchange.fetch_positions([ccxt_sym])
                    for p in refreshed:
                        if self._normalize_symbol(p.get('symbol', '')) == norm_sym:
                            rem_amount = float(p.get('contracts', 0) or 0)
                            break
                except Exception:
                    rem_amount = max(0.0, pos_size - close_amount)

                sl_ok = False
                if rem_amount > 0:
                    for attempt in range(3):
                        try:
                            await self.exchange.cancel_all_orders(ccxt_sym)
                            await asyncio.sleep(0.4)
                            formatted_sl = float(
                                self.exchange.price_to_precision(ccxt_sym, new_sl))
                            await self.exchange.create_order(
                                ccxt_sym, 'market', close_side, rem_amount, None,
                                {'stopLossPrice': formatted_sl, 'reduceOnly': True})
                            sl_ok = True
                            break
                        except Exception as e:
                            logger.warning(
                                f"SL replace attempt {attempt+1} failed on {ccxt_sym}: {e}")
                            await asyncio.sleep(0.8)

                self._execute_query(
                    "UPDATE trades SET trail_step = ?, stop_loss = ? WHERE rowid = ?",
                    (new_trail, new_sl, rowid))

                if rem_amount > 0 and not sl_ok:
                    # The old code logged this at error level and moved on,
                    # leaving a live position with no protection at all.
                    alert += ("\n\n<b>WARNING: could not re-place the stop after "
                              "three attempts. This position is currently UNPROTECTED "
                              "on the exchange — set a stop manually now.</b>")
                    logger.error(f"{ccxt_sym} left without a stop after partial close.")

                await self.telegram._send_raw_message(alert)

            except Exception as e:
                logger.debug(f"Evaluation skipped for {ccxt_sym}: {e}")

    async def close(self):
        if self.exchange:
            await self.exchange.close()