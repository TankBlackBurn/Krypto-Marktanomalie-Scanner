"""
ai_analyst.py -- Claude API second opinion on a signal.

Two modes, set via bot_settings.ai_mode in config.json:

  "off"       No API calls at all. Confluence score decides everything.
  "advisory"  (default) Claude annotates the Telegram alert with a read,
              but never blocks a trade. Execution is decided by the
              confluence score alone.
  "gate"      Claude can veto. Only use this once signals are actually
              firing -- a gate can only ever reduce signal count.

A caveat worth keeping in front of you: Claude sees exactly the numbers
this file sends it. It has no market feed, no order book, no social data,
no knowledge of the coin beyond its ticker. It is a sanity check on the
numbers, not an independent source of information. That was the core
problem with the old Gemini gate -- it was asked to "assess" data that
the pipeline had already scored, and it added nothing.

Where it does earn its keep: catching internally inconsistent setups
(huge volume but flat price, stop wider than the move it's chasing) and
giving you a plain-language reason to skim in Telegram.
"""

import asyncio
import json
import logging
from typing import Dict, Optional

logger = logging.getLogger("AIAnalyst")

try:
    from anthropic import AsyncAnthropic
    _SDK_AVAILABLE = True
except ImportError:
    _SDK_AVAILABLE = False


SYSTEM_PROMPT = (
    "You are a risk reviewer for a crypto futures signal bot. You receive "
    "precomputed metrics for one long setup and have no other information -- "
    "do not speculate about news, the project, or sentiment.\n\n"
    "Check whether the numbers are internally coherent and flag anything that "
    "undermines the setup. If nothing is wrong, say so briefly rather than "
    "inventing concerns.\n\n"
    "Respond ONLY with raw JSON, no markdown fences:\n"
    '{"verdict": "OK" | "CAUTION" | "AVOID", '
    '"concern": "<one sentence, or empty string if none>"}'
)


class ClaudeAnalyst:
    def __init__(self, config: dict):
        creds = config.get("api_credentials", {})
        settings = config.get("bot_settings", {})

        self.mode = str(settings.get("ai_mode", "advisory")).lower()
        self.model = settings.get("ai_model", "claude-haiku-4-5")
        self.timeout = float(settings.get("ai_timeout_seconds", 20))
        self.api_key = creds.get("anthropic_api_key", "")

        self.client: Optional["AsyncAnthropic"] = None
        self.available = False

        if self.mode == "off":
            logger.info("AI analyst disabled (ai_mode: off).")
            return

        if not _SDK_AVAILABLE:
            logger.warning("anthropic package not installed -- run: pip install anthropic")
            return

        if not self.api_key:
            logger.warning("No anthropic_api_key in config.json -- AI analyst inactive.")
            return

        self.client = AsyncAnthropic(api_key=self.api_key)
        self.available = True
        logger.info(f"AI analyst active | model={self.model} | mode={self.mode}")

    @property
    def can_block(self) -> bool:
        """Only gate mode is allowed to stop a trade."""
        return self.mode == "gate" and self.available

    async def review(self, anomaly: Dict) -> Dict:
        """
        Returns {"verdict", "concern", "note"}.

        Never raises. On any failure it returns a neutral OK so that an API
        problem cannot silently kill your signal flow -- which is exactly what
        the old Gemini code did by defaulting to REJECT on every exception.
        """
        if not self.available:
            return {"verdict": "OK", "concern": "", "note": "AI review skipped."}

        facts = {
            "symbol": anomaly.get("symbol"),
            "signal_type": anomaly.get("signal_type"),
            "confluence_score": anomaly.get("confluence_score"),
            "entry_price": anomaly.get("price_usd"),
            "stop_pct": anomaly.get("sl_percent"),
            "tp1_pct": anomaly.get("tp1_pct"),
            "tp2_pct": anomaly.get("tp2_pct"),
            "volume_vs_50_candle_avg": anomaly.get("micro_velocity"),
            "taker_buy_ratio_signal_candle": anomaly.get("buy_pressure"),
            "taker_buy_ratio_last_8_candles": anomaly.get("buy_pressure_8"),
            "trade_count_expansion": anomaly.get("trade_expansion"),
            "pct_above_24h_low": anomaly.get("extension_pct"),
        }

        prompt = (
            "Review this long setup on WEEX 15m futures:\n\n"
            f"{json.dumps(facts, indent=2)}\n\n"
            "Reference points: taker buy ratio 0.50 is balanced flow; volume "
            "multiple 1.0 is average. Respond with the JSON object only."
        )

        try:
            resp = await asyncio.wait_for(
                self.client.messages.create(
                    model=self.model,
                    max_tokens=200,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": prompt}],
                ),
                timeout=self.timeout,
            )

            text = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text")
            text = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            parsed = json.loads(text)

            verdict = str(parsed.get("verdict", "OK")).upper()
            if verdict not in ("OK", "CAUTION", "AVOID"):
                verdict = "OK"
            concern = str(parsed.get("concern", ""))[:250]

            note = f"Claude: {verdict}" + (f" — {concern}" if concern else "")
            return {"verdict": verdict, "concern": concern, "note": note}

        except asyncio.TimeoutError:
            logger.warning(f"AI review timed out for {anomaly.get('symbol')}")
            return {"verdict": "OK", "concern": "", "note": "AI review timed out."}
        except Exception as e:
            # Logged loudly and visibly, then waved through. An API outage
            # should never look like "no setups today".
            logger.error(f"AI review failed for {anomaly.get('symbol')}: {e}")
            return {"verdict": "OK", "concern": "", "note": f"AI review unavailable ({type(e).__name__})."}

    async def close(self):
        if self.client is not None:
            try:
                await self.client.close()
            except Exception:
                pass