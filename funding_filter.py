"""
funding_filter.py -- exclude longs on coins with deeply negative funding.

RATIONALE
Of the hypotheses run through the research harness (see research/funding_test.py),
funding-rate positioning was the one whose effect survived its own error bars:
perpetuals with deeply negative funding showed a negative mean forward return
and no upside outliers. The filter exists to keep the bot out of those
instruments -- it is an exclusion rule, not a way of finding pumps. The exact
threshold and sample statistics are omitted from this portfolio edition.

It touches only a small fraction of signals.

MODES (bot_settings.funding_mode)
  "off"       no API calls, no filtering
  "advisory"  records the funding rate on every signal and annotates the
              alert, but never blocks. Use this to gather data first.
  "block"     (default) excludes the signal. Blocked signals are still
              written to the log so their outcomes can be checked later --
              otherwise you can never find out whether blocking helped.

    python funding_filter.py BTCUSDT COWUSDT     standalone check
"""

import asyncio
import logging
import sys
import time
from typing import Dict, Optional, Tuple

import aiohttp

logger = logging.getLogger("FundingFilter")

FUNDING_URL = "https://api-contract.weex.com/capi/v3/market/fundingRate"


class FundingFilter:
    def __init__(self, config: dict):
        s = (config or {}).get("bot_settings", {})

        self.mode = str(s.get("funding_mode", "block")).lower()
        # Fraction, e.g. -0.002 == -0.20%. Placeholder default.
        self.threshold = float(s.get("funding_block_below", -0.0020))
        # Funding settles every ~4h, so a 30-minute cache is conservative
        # and keeps well clear of the 500-per-10-minute rate limit.
        self.cache_ttl = float(s.get("funding_cache_seconds", 1800))
        self.timeout = float(s.get("funding_timeout_seconds", 8))

        self._cache: Dict[str, Tuple[float, Optional[float]]] = {}
        self._session: Optional[aiohttp.ClientSession] = None

        # Crude rate-limit guard. If the bot ever starts firing far more
        # signals than expected, stop calling rather than get throttled
        # mid-scan and have every check fail at once.
        self._calls = []
        self.max_calls_per_10min = int(s.get("funding_max_calls_per_10min", 200))

        if self.mode == "off":
            logger.info("Funding filter disabled (funding_mode: off).")
        else:
            logger.info(
                f"Funding filter {self.mode} | blocking longs below "
                f"{self.threshold*100:.3f}% | cache {self.cache_ttl/60:.0f}min")

    @property
    def enabled(self) -> bool:
        return self.mode in ("advisory", "block")

    @property
    def can_block(self) -> bool:
        return self.mode == "block"

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    def _rate_ok(self) -> bool:
        now = time.time()
        self._calls = [t for t in self._calls if now - t < 600]
        return len(self._calls) < self.max_calls_per_10min

    async def get_rate(self, symbol: str) -> Optional[float]:
        """Latest funding rate as a fraction, or None if unavailable."""
        if not self.enabled:
            return None

        now = time.time()
        hit = self._cache.get(symbol)
        if hit and (now - hit[0]) < self.cache_ttl:
            return hit[1]

        if not self._rate_ok():
            logger.warning("Funding rate-limit guard tripped; skipping check.")
            return None

        try:
            session = await self._get_session()
            self._calls.append(now)
            async with session.get(
                FUNDING_URL, params={"symbol": symbol},
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as r:
                if r.status != 200:
                    logger.debug(f"Funding HTTP {r.status} for {symbol}")
                    self._cache[symbol] = (now, None)
                    return None
                body = await r.json(content_type=None)
        except Exception as e:
            logger.debug(f"Funding fetch failed for {symbol}: {e}")
            self._cache[symbol] = (now, None)
            return None

        if isinstance(body, dict):
            for k in ("data", "result", "list"):
                if isinstance(body.get(k), list):
                    body = body[k]
                    break
        if not isinstance(body, list) or not body:
            self._cache[symbol] = (now, None)
            return None

        # Take the newest entry explicitly rather than trusting list order.
        newest, newest_t = None, -1
        for e in body:
            if not isinstance(e, dict):
                continue
            try:
                t = int(e["fundingTime"])
                if t > newest_t:
                    newest_t, newest = t, float(e["fundingRate"])
            except (KeyError, TypeError, ValueError):
                continue

        self._cache[symbol] = (now, newest)
        return newest

    async def check(self, symbol: str) -> Tuple[bool, Optional[float], str]:
        """
        Returns (allowed, rate, note).

        Fails OPEN. A funding endpoint outage must never look like "no
        setups today" -- that is exactly how the old Gemini gate silently
        killed every signal for days.
        """
        if not self.enabled:
            return True, None, ""

        rate = await self.get_rate(symbol)
        if rate is None:
            return True, None, "funding unavailable (not blocking)"

        pct = rate * 100.0
        if rate <= self.threshold:
            note = (f"funding {pct:+.4f}% is at or below the "
                    f"{self.threshold*100:+.3f}% exclusion threshold")
            return (not self.can_block), rate, note

        return True, rate, f"funding {pct:+.4f}%"

    async def close(self):
        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception:
                pass


async def _standalone(symbols):
    f = FundingFilter({"bot_settings": {"funding_mode": "block"}})
    print(f"{'symbol':<16}{'funding':>12}   verdict")
    print("-" * 62)
    for s in symbols:
        s = s.upper()
        if not s.endswith("USDT"):
            s += "USDT"
        allowed, rate, note = await f.check(s)
        shown = f"{rate*100:+.4f}%" if rate is not None else "n/a"
        print(f"{s:<16}{shown:>12}   {'ALLOW' if allowed else 'BLOCK'}  {note}")
    await f.close()


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    asyncio.run(_standalone(args or ["BTCUSDT", "COWUSDT", "ACEUSDT"]))