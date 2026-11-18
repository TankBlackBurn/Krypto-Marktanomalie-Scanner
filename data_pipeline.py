"""
data_pipeline.py -- asynchronous market scanner for WEEX USDT futures.

Fetches candles for every listed pair concurrently, normalizes the exchange
payload, applies liquidity and extension filters, hands each candidate to the
strategy module, and builds a fully specified signal (stop, targets, derived
leverage) for anything that passes.

Portfolio edition: detection is delegated to strategy_example.py, a
placeholder. All numeric defaults below are neutral placeholders, not tuned
values.
"""

import asyncio
import aiohttp
import logging
import math
import time
from typing import List, Dict, Optional

import strategy_example as strategy

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("SniperPipeline")

# WEEX returns Binance-format klines with 11 fields. Quote volume and taker-buy
# flow are the most informative fields in the payload.
K_OPEN_TIME, K_OPEN, K_HIGH, K_LOW, K_CLOSE, K_BASE_VOL = 0, 1, 2, 3, 4, 5
K_CLOSE_TIME, K_QUOTE_VOL, K_TRADES = 6, 7, 8
K_TAKER_BUY_BASE, K_TAKER_BUY_QUOTE = 9, 10

MIN_FIELDS = K_TAKER_BUY_QUOTE + 1


class WeexFuturesPipeline:
    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        s = config.get("bot_settings", {})

        self.exchange_info_url = "https://api-contract.weex.com/capi/v3/market/exchangeInfo"
        self.klines_url = "https://api-contract.weex.com/capi/v3/market/klines"
        self.request_timeout = aiohttp.ClientTimeout(total=8)

        # Liquidity floor, derived from order size rather than a flat number:
        # the order should be a small fraction of the pair's hourly volume,
        # otherwise the bot's own order would BE the market. An explicit
        # min_volume_usd in config overrides the derived value.
        explicit_floor = s.get("min_volume_usd")
        if explicit_floor is not None:
            self.min_volume_usd = float(explicit_floor)
        else:
            margin = float(s.get("margin_per_trade_usd", 2.0))
            target_lev = float(s.get("leverage", 10))
            max_share = float(s.get("max_pct_of_hourly_volume", 0.01))
            notional = margin * target_lev
            self.min_volume_usd = (notional / max_share) if max_share > 0 else 15_000.0

        # Detector parameters (placeholders -- consumed by strategy_example.py).
        self.breakout_velocity = float(s.get("breakout_velocity", 3.0))
        self.breakout_buy_ratio = float(s.get("breakout_buy_ratio", 0.60))
        self.breakout_lookback = int(s.get("breakout_lookback", 20))

        # Reserved for the second detector, which is not part of this edition.
        self.ramp_window = int(s.get("ramp_window", 12))
        self.ramp_buy_ratio = float(s.get("ramp_buy_ratio", 0.60))
        self.ramp_volume_expansion = float(s.get("ramp_volume_expansion", 1.5))
        self.ramp_min_velocity = float(s.get("ramp_min_velocity", 1.0))

        # Extension filter: skip setups already far off the recent low, which
        # are chases rather than entries.
        self.extension_lookback_candles = int(s.get("extension_lookback_candles", 48))
        self.max_extension_pct = float(s.get("max_extension_pct", 30.0))

        # Risk. A stop wider than this is not tradeable on leverage; a stop
        # narrower than the floor is dominated by fee drag and wick noise.
        self.max_sl_percent = float(s.get("max_sl_percent", 5.0))
        self.min_sl_percent = float(s.get("min_sl_percent", 0.5))

        self.tp1_r = float(s.get("tp1_r_multiple", 1.5))
        self.tp2_r = float(s.get("tp2_r_multiple", 3.0))

        # Quality gate applied to the strategy's score.
        self.min_score = float(s.get("min_confluence_score", 60.0))

        # Risk budget used to derive a leverage suggestion from stop distance.
        self.risk_fraction_of_margin = float(s.get("risk_fraction_of_margin", 0.25))

        # Candle timeframe and history length (the endpoint caps at 100).
        self.interval = str(s.get("interval", "15m"))
        self.candle_limit = int(s.get("candle_limit", 100))

        # Per-symbol cooldown in candles, so one instrument cannot dominate
        # the signal set by re-firing on the same move.
        self.symbol_cooldown_candles = int(s.get("symbol_cooldown_candles", 8))

    # ------------------------------------------------------------------ utils

    @staticmethod
    def _smart_round(price: float) -> float:
        if price <= 0:
            return price
        if price >= 100:
            return round(price, 2)
        if price >= 1:
            return round(price, 4)
        if price >= 0.0001:
            return round(price, 6)
        return round(price, min(int(-math.floor(math.log10(abs(price)))) + 4, 10))

    @staticmethod
    def _normalize_klines(raw) -> List[list]:
        """
        WEEX ships closed candles newest-first, then appends the live forming
        candle at the end. Sorting by open time restores the chronological
        ordering the rest of this file assumes.
        """
        if isinstance(raw, dict):
            for key in ("data", "result", "list", "klines"):
                if isinstance(raw.get(key), list):
                    raw = raw[key]
                    break
        if not isinstance(raw, list):
            return []
        rows = [c for c in raw if isinstance(c, list) and len(c) >= MIN_FIELDS]
        rows.sort(key=lambda c: int(c[K_OPEN_TIME]))
        return rows

    @staticmethod
    def _split_live(rows: List[list]):
        """Separate the still-forming candle from the confirmed history."""
        if not rows:
            return None, []
        now_ms = time.time() * 1000.0
        if float(rows[-1][K_CLOSE_TIME]) > now_ms:
            return rows[-1], rows[:-1]
        return None, rows

    @staticmethod
    def _buy_ratio(candle: list) -> float:
        """Share of volume that came from aggressive buyers. 0.5 == balanced."""
        quote = float(candle[K_QUOTE_VOL])
        if quote <= 0:
            return 0.5
        return float(candle[K_TAKER_BUY_QUOTE]) / quote

    @staticmethod
    def _pooled_buy_ratio(candles: List[list]) -> float:
        total = sum(float(c[K_QUOTE_VOL]) for c in candles)
        if total <= 0:
            return 0.5
        return sum(float(c[K_TAKER_BUY_QUOTE]) for c in candles) / total

    @staticmethod
    def _ema(values: List[float], period: int) -> float:
        if not values:
            return 0.0
        k = 2.0 / (period + 1.0)
        out = values[0]
        for v in values[1:]:
            out = v * k + out * (1 - k)
        return out

    # ------------------------------------------------------------------ fetch

    async def fetch_active_futures_pairs(self, session: aiohttp.ClientSession) -> List[str]:
        try:
            async with session.get(self.exchange_info_url, timeout=self.request_timeout) as response:
                if response.status != 200:
                    logger.error(f"exchangeInfo returned HTTP {response.status}")
                    return []
                data = await response.json()
        except Exception as e:
            # Logged explicitly: a dead API must be distinguishable from a
            # quiet market.
            logger.error(f"exchangeInfo fetch failed: {e}")
            return []

        symbols = data.get("symbols", []) if isinstance(data, dict) else []
        pairs = [
            s["symbol"] for s in symbols
            if isinstance(s, dict) and str(s.get("symbol", "")).upper().endswith("USDT")
        ]
        if not pairs:
            logger.error(f"exchangeInfo parsed but yielded 0 pairs. keys={list(data)[:8]}")
        return pairs

    # --------------------------------------------------------------- analysis

    async def fetch_symbol_metrics(self, session: aiohttp.ClientSession, symbol: str) -> Optional[Dict]:
        try:
            url = f"{self.klines_url}?symbol={symbol}&interval={self.interval}&limit={self.candle_limit}"
            async with session.get(url, timeout=self.request_timeout) as resp:
                if resp.status != 200:
                    return None
                rows = self._normalize_klines(await resp.json())

            if len(rows) < 60:
                return None

            live, closed = self._split_live(rows)
            if len(closed) < 55:
                return None

            conf = closed[-1]                      # last fully closed candle
            history = closed[-51:-1]               # the 50 candles before it
            if len(history) < 40:
                return None

            conf_open = float(conf[K_OPEN])
            conf_high = float(conf[K_HIGH])
            conf_low = float(conf[K_LOW])
            conf_close = float(conf[K_CLOSE])
            conf_quote_vol = float(conf[K_QUOTE_VOL])
            conf_trades = float(conf[K_TRADES])

            current_price = float(live[K_CLOSE]) if live is not None else conf_close
            if current_price <= 0:
                return None

            avg_quote_vol = sum(float(c[K_QUOTE_VOL]) for c in history) / len(history)
            if avg_quote_vol <= 0:
                return None

            # Liquidity floor, in USDT per hour (four 15m candles).
            if avg_quote_vol * 4 < self.min_volume_usd:
                return None

            velocity = conf_quote_vol / avg_quote_vol
            avg_trades = sum(float(c[K_TRADES]) for c in history) / len(history)
            trade_expansion = conf_trades / avg_trades if avg_trades > 0 else 1.0

            # Are we early, or is this already gone?
            window_low = min(float(c[K_LOW]) for c in closed[-self.extension_lookback_candles:])
            extension_pct = (current_price - window_low) / window_low * 100.0
            if extension_pct > self.max_extension_pct:
                return None

            signal = (
                self._detect_breakout(closed, conf, conf_open, conf_high, conf_low,
                                      conf_close, velocity, trade_expansion)
                or self._detect_ramp(closed, current_price, velocity, trade_expansion)
            )
            if signal is None:
                return None

            score, signal_type, sl_anchor, notes = signal
            score = min(score, 100.0)

            if score < self.min_score:
                return None

            sl_price = self._smart_round(sl_anchor * 0.995)
            if current_price <= sl_price:
                return None

            risk = current_price - sl_price
            sl_percent = risk / current_price * 100.0
            if sl_percent > self.max_sl_percent or sl_percent < self.min_sl_percent:
                return None

            tp1 = self._smart_round(current_price + risk * self.tp1_r)
            tp2 = self._smart_round(current_price + risk * self.tp2_r)

            # Leverage that makes the stop the thing which closes the trade,
            # rather than liquidation. Derived, never assumed.
            safe_leverage = max(1, int(self.risk_fraction_of_margin / (sl_percent / 100.0)))

            return {
                "symbol": symbol.replace("USDT", ""),
                "pair": symbol,
                "chain": "WEEX Futures",
                "signal_type": signal_type,
                "side": "LONG",
                "price_usd": current_price,
                "sl_price": sl_price,
                "sl_percent": round(sl_percent, 2),
                "tp1_price": tp1,
                "tp2_price": tp2,
                "tp_price": tp2,
                "tp1_pct": round((tp1 - current_price) / current_price * 100.0, 2),
                "tp2_pct": round((tp2 - current_price) / current_price * 100.0, 2),
                "confluence_score": round(score, 1),
                "micro_velocity": round(velocity, 2),
                "buy_pressure": round(self._buy_ratio(conf), 3),
                "buy_pressure_8": round(self._pooled_buy_ratio(closed[-8:]), 3),
                "trade_expansion": round(trade_expansion, 2),
                "extension_pct": round(extension_pct, 2),
                "suggested_leverage": safe_leverage,
                "notes": notes,
                # Dedupe key -- one signal per symbol per candle, not one per scan.
                "candle_id": f"{symbol}:{int(conf[K_OPEN_TIME])}",
                "dex_url": "https://www.weex.com",
            }

        except asyncio.TimeoutError:
            # str(asyncio.TimeoutError()) is empty, so name it explicitly.
            logger.debug(f"Timed out fetching {symbol} (>{self.request_timeout.total}s)")
            return None
        except Exception as e:
            logger.error(f"Pipeline error on {symbol}: {type(e).__name__}: {e}")
            return None

    # ------------------------------------------------- detectors (delegated)

    def _detect_breakout(self, closed, conf, o, h, l, c, velocity, trade_expansion):
        return strategy.detect_breakout(
            closed, conf, o, h, l, c, velocity, trade_expansion,
            params={
                "lookback": self.breakout_lookback,
                "velocity_min": self.breakout_velocity,
                "buy_ratio_min": self.breakout_buy_ratio,
            },
        )

    def _detect_ramp(self, closed, current_price, velocity, trade_expansion):
        return strategy.detect_ramp(
            closed, current_price, velocity, trade_expansion,
            params={
                "window": self.ramp_window,
                "buy_ratio_min": self.ramp_buy_ratio,
                "volume_expansion_min": self.ramp_volume_expansion,
                "min_velocity": self.ramp_min_velocity,
            },
        )

    # -------------------------------------------------------------- scanning

    async def scan_market(self) -> List[Dict]:
        connector = aiohttp.TCPConnector(limit=40)
        async with aiohttp.ClientSession(connector=connector) as session:
            symbols = await self.fetch_active_futures_pairs(session)
            if not symbols:
                logger.error("Scan aborted: 0 tradable pairs returned.")
                return []

            logger.info(f"Scanning {len(symbols)} pairs...")
            semaphore = asyncio.Semaphore(15)

            async def bound_fetch(sym: str):
                async with semaphore:
                    return await self.fetch_symbol_metrics(session, sym)

            results = await asyncio.gather(
                *(bound_fetch(s) for s in symbols), return_exceptions=True
            )

        hits = [r for r in results if isinstance(r, dict)]
        hits.sort(key=lambda d: d["confluence_score"], reverse=True)
        return hits
