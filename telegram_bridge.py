import aiohttp
import logging
from typing import Dict, Any

logger = logging.getLogger("TelegramBridge")


class TelegramBridge:
    """
    Rewritten against the keys data_pipeline.py actually returns.

    The previous version read tp1_pct, tp2_pct, tp3_price and tp3_pct, none of
    which the pipeline produced. That raised KeyError -- not AttributeError --
    so main.py's `except AttributeError` did not catch it, the exception escaped
    the scan loop, and the process exited. The bot died on its first real signal.
    """

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = str(chat_id)
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _send_raw_message(self, text: str) -> bool:
        if not self.bot_token or not self.chat_id:
            logger.warning("Telegram credentials missing; message dropped.")
            return False
        try:
            session = await self._get_session()
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            async with session.post(self.api_url, json=payload) as resp:
                if resp.status != 200:
                    # The old version returned True regardless, so malformed
                    # HTML failed silently and looked like a delivered message.
                    body = await resp.text()
                    logger.error(f"Telegram API {resp.status}: {body[:200]}")
                    return False
                return True
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            return False

    async def send_sniper_signal(self, a: Dict[str, Any], ai_note: str, executed: bool) -> bool:
        symbol = a.get("symbol", "?")
        sig_type = a.get("signal_type", "")

        if "ACCUMULATION" in sig_type:
            header = "📈 ACCUMULATION RAMP"
        elif "BREAKOUT" in sig_type:
            header = "🎯 SNIPER BREAKOUT"
        else:
            header = "🎯 SNIPER SIGNAL"

        score = a.get("confluence_score", 0)
        lev = a.get("suggested_leverage", 1)

        if executed:
            banner = (
                "🟢 <b>TRADE OPENED ON WEEX</b>\n"
                "<i>Position active, stop anchored, monitor running.</i>"
            )
        else:
            banner = (
                "🔵 <b>MANUAL SIGNAL — NOT AUTO-EXECUTED</b>\n"
                f"<i>{a.get('skip_reason', 'Auto-execution disabled or pair unsupported.')}</i>"
            )

        message = (
            f"<b>{header}: ${symbol}</b>\n\n"
            f"{banner}\n\n"
            f"📍 <b>Entry:</b> <code>${a.get('price_usd')}</code>\n"
            f"⭐ <b>Confluence:</b> <b>{score}/100</b>\n"
            f"📊 <b>Volume:</b> {a.get('micro_velocity', 0)}x | "
            f"<b>Taker buys:</b> {a.get('buy_pressure', 0) * 100:.0f}%\n"
            f"🔁 <b>Trade count:</b> {a.get('trade_expansion', 0)}x | "
            f"<b>Off 24h low:</b> +{a.get('extension_pct', 0)}%\n\n"
            f"🎯 <b>Targets</b>\n"
            f"• TP1: <code>${a.get('tp1_price')}</code> (+{a.get('tp1_pct', 0)}%)\n"
            f"• TP2: <code>${a.get('tp2_price')}</code> (+{a.get('tp2_pct', 0)}%)\n\n"
            f"🛑 <b>Stop:</b> <code>${a.get('sl_price')}</code> "
            f"(-{a.get('sl_percent', 0)}%)\n"
            f"⚖️ <b>Max leverage for this stop:</b> {lev}x\n\n"
            f"🔍 <i>{a.get('notes', '')}</i>\n\n"
            f"💡 <b>Assessment:</b>\n<i>{ai_note}</i>"
        )
        return await self._send_raw_message(message)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()