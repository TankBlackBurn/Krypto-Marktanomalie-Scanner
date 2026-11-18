import ccxt.async_support as ccxt
import logging
import asyncio
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("WeexExecutor")

class WeexExecutor:
    def __init__(self, config: dict):
        creds = config.get("api_credentials", {})
        bot_settings = config.get("bot_settings", {})
        
        self.enabled = bot_settings.get("auto_execute_trades", False)
        self.margin_usd = float(bot_settings.get("margin_per_trade_usd", 2.0))
        self.target_leverage = int(bot_settings.get("leverage", 75))
        self.max_open_trades = int(bot_settings.get("max_open_trades", 16))
        
        self.exchange = ccxt.weex({
            'apiKey': creds.get("weex_api_key", ""),
            'secret': creds.get("weex_secret_key", ""),
            'password': creds.get("weex_api_passphrase", ""),
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })

    async def execute_scalp_trade(self, anomaly: dict) -> bool:
        if not self.enabled: 
            return False

        symbol = f"{anomaly['symbol']}/USDT:USDT"
        side = anomaly.get('side', 'LONG').upper()
        entry_price = float(anomaly['price_usd'])
        raw_sl_price = float(anomaly['sl_price'])

        try:
            # 1. Ensure market structures are loaded
            if not self.exchange.markets:
                await self.exchange.load_markets()

            # 2. Check Active Positions with retry
            active_positions = []
            for attempt in range(2):
                try:
                    positions = await self.exchange.fetch_positions()
                    active_positions = [p for p in positions if float(p.get('contracts', 0) or 0) > 0]
                    break
                except Exception as pos_err:
                    logger.debug(f"Position check attempt {attempt + 1} failed: {pos_err}")
                    await asyncio.sleep(0.5)

            # Guard 1: Position limits & Duplicate check
            if len(active_positions) >= self.max_open_trades:
                logger.info(f"⏸️ MAX TRADES REACHED ({len(active_positions)}/{self.max_open_trades}). Skipping {symbol}.")
                return False

            if any(p['symbol'] == symbol for p in active_positions):
                logger.info(f"⏭️ Active position already open for {symbol}. Skipping.")
                return False

            # Guard 2: Free Margin Balance Check
            try:
                balance = await self.exchange.fetch_balance()
                free_usdt = float(balance.get('USDT', {}).get('free', 0.0))
                if free_usdt < self.margin_usd:
                    logger.warning(f"🛑 INSUFFICIENT MARGIN (${free_usdt:.2f} free < ${self.margin_usd:.2f}). Skipping {symbol}.")
                    return False
            except Exception as bal_err:
                logger.debug(f"Balance check notice: {bal_err}")

            # 3. Leverage: take the LOWEST of three ceilings.
            market_info = self.exchange.market(symbol)

            raw_max = market_info.get('limits', {}).get('leverage', {}).get('max')
            # .get('max', default) returns None when the key exists but is null,
            # and min(75, None) raises TypeError -- swallowed by the outer
            # handler, so this used to kill the whole trade silently.
            max_pair_leverage = float(raw_max) if raw_max else float(self.target_leverage)

            # The pipeline sizes leverage so the STOP is what closes the trade
            # rather than liquidation: risk_fraction_of_margin / stop_distance.
            # Live example: the HEI alert carried a 4.73% stop and suggested 5x.
            # At 75x, liquidation sits near a 1.2% adverse move -- the stop
            # could never have been reached, turning a planned -1R into a full
            # loss of margin.
            suggested = anomaly.get('suggested_leverage')
            candidates = [float(self.target_leverage), max_pair_leverage]
            if suggested:
                candidates.append(float(suggested))
            safe_leverage = max(1, int(min(candidates)))

            if suggested and float(suggested) < float(self.target_leverage):
                logger.info(
                    f"Leverage capped at {safe_leverage}x for {symbol} "
                    f"(config wants {self.target_leverage}x, but a "
                    f"{anomaly.get('sl_percent', '?')}% stop needs <={suggested}x "
                    f"to stay reachable before liquidation)."
                )
            
            try:
                await self.exchange.set_leverage(safe_leverage, symbol)
                await asyncio.sleep(0.1)
                await self.exchange.set_margin_mode('isolated', symbol)
            except Exception as lev_err:
                logger.debug(f"Leverage/Margin setup notice for {symbol}: {lev_err}")

            # 4. Precision Sizing & Minimum Contract Checks
            notional_size = self.margin_usd * safe_leverage
            raw_amount = notional_size / entry_price
            amount = float(self.exchange.amount_to_precision(symbol, raw_amount))
            
            # Format Stop Loss Price to match exchange precision
            sl_price = float(self.exchange.price_to_precision(symbol, raw_sl_price))

            min_amount = float(market_info.get('limits', {}).get('amount', {}).get('min', 0.0) or 0.0)
            if amount < min_amount:
                logger.warning(f"⚠️ Order size ({amount}) below min contract limit ({min_amount}) for {symbol}. Skipping.")
                return False

            order_side = 'buy' if side == 'LONG' else 'sell'
            close_side = 'sell' if side == 'LONG' else 'buy'

            logger.info(f"🚀 Sniper Firing: {side} {symbol} | Size: {amount} (~${notional_size:.2f} @ {safe_leverage}x)")

            # 5. STEP 1: Market Order Entry
            entry_order = await self.exchange.create_order(symbol, 'market', order_side, amount)
            filled_amount = float(entry_order.get('filled') or amount)
            logger.info(f"✅ Position Opened! ID: {entry_order.get('id')}")

            await asyncio.sleep(0.4)

            # 6. STEP 2: Stop-Loss Placement with Retry
            sl_placed = False
            for sl_attempt in range(2):
                try:
                    await self.exchange.create_order(symbol, 'market', close_side, filled_amount, None, {
                        'stopLossPrice': sl_price, 
                        'reduceOnly': True
                    })
                    sl_placed = True
                    logger.info(f"✅ Stop-Loss anchored at ${sl_price}! Upside uncapped for moonbag.")
                    break
                except Exception as sl_err:
                    logger.warning(f"⚠️ SL attempt {sl_attempt + 1} failed for {symbol}: {sl_err}")
                    await asyncio.sleep(0.5)

            # Fail-safe Emergency Close if SL cannot be anchored
            if not sl_placed:
                logger.error(f"🚨 CRITICAL: Failed to anchor Stop-Loss on {symbol}. Executing EMERGENCY MARKET CLOSE!")
                await self.exchange.create_order(symbol, 'market', close_side, filled_amount, None, {'reduceOnly': True})
                return False

            return True
            
        except Exception as e:
            logger.error(f"❌ Executor Error on {symbol}: {e}")
            return False

    async def close(self):
        if self.exchange: 
            await self.exchange.close()