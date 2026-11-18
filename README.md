# Crypto Market Anomaly Scanner

![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)
![asyncio](https://img.shields.io/badge/async-aiohttp-green.svg)
![Status](https://img.shields.io/badge/status-prototype-orange.svg)
![License](https://img.shields.io/badge/license-all%20rights%20reserved-lightgrey.svg)

An asynchronous Python system that scans every USDT perpetual on the WEEX futures exchange for volume and order-flow anomalies, reviews each candidate with an LLM, filters on funding rates, and delivers real-time Telegram alerts. It optionally executes trades, and it ships with a research harness that tests whether its own ideas have any edge.

> **Portfolio edition.** The proprietary signal logic (detection rules, thresholds, scoring weights, level/structure analysis, LLM prompts and tuned parameters) has been removed. `strategy_example.py` is a simple placeholder with the same interface so the whole pipeline runs end to end. Every numeric default in this repo is a neutral placeholder, not a tuned value. Nothing here is financial advice, and no claim of profitability is made.

## How it works

```
        WEEX REST API
             |
   data_pipeline.py  --- async scan of all pairs (aiohttp, semaphore-bounded)
        |   liquidity floor -> extension filter -> strategy -> stop / targets / leverage
        v
   strategy_example.py   (placeholder detector; production logic is private)
        |
        v
   main.py  --- candle-id dedupe + persisted per-symbol cooldown
        |-- funding_filter.py   exclude longs on deeply negative funding
        |-- ai_analyst.py       Claude second opinion (off / advisory / gate)
        |-- executor.py         optional order placement via ccxt
        |-- trade_monitor.py    partial exits, trailing stop, outcome recording
        |-- telegram_bridge.py  real-time alerts
        v
   SQLite ledgers (db/)  <-- api_server.py (read-only FastAPI window for a phone)
```

## Tech stack

Python 3.9+, `asyncio`, `aiohttp`, `ccxt`, Anthropic API, `FastAPI`/`uvicorn`, SQLite, Telegram Bot API.

## Engineering highlights

Most of these came from real bugs found while running the system:

- **Exchange payload quirks.** WEEX returns candles newest-first with the still-forming candle appended. The pipeline sorts by open time and separates the live candle so no detector ever reads stale or unfinished data.
- **Bounded concurrency.** Hundreds of pairs are fetched concurrently behind a semaphore, with explicit timeout handling so a dead API is distinguishable from a quiet market.
- **Fail-open dependencies.** The LLM review and the funding filter never block the signal flow when their API fails. An earlier gate defaulted to "reject" on any exception and silently suppressed every signal.
- **Duplicate-signal control.** A setup re-firing on one price move was being counted as many independent trades. Signals are now de-duplicated by candle id and a per-symbol cooldown that is reloaded from SQLite on restart, and reporting tools de-duplicate retroactively.
- **Silent persistence failure.** `CREATE TABLE IF NOT EXISTS` does not add columns to an existing table, so signals were being sent but not logged. Schema is now migrated explicitly at startup and failures are logged loudly.
- **Execution safety.** Leverage is derived from stop distance so the stop, not liquidation, closes a losing trade. If a stop cannot be placed after entry, the position is closed immediately. If a replacement stop fails after a partial exit, an alert is sent.
- **Read-only dashboard.** `api_server.py` opens databases with SQLite's `mode=ro` (enforced by the driver), uses constant-time token comparison, and never serves credentials.
- **Config actually used.** Backtests once ran on hardcoded defaults because the pipeline was constructed with an empty config. The active config is now printed at the top of every run.

## Research and validation

The `research/` folder is a harness for testing hypotheses against base rates, with the aim of rejecting ideas rather than confirming them:

| Script | Question it answers |
|---|---|
| `backtest.py` | Replay the detectors over recent history with fees and slippage modelled in R |
| `rolling_backtest.py` | Accumulate signals across days into a deduplicated log, with confidence bands |
| `missed_pumps.py` | What were the biggest movers, and which filter rejected each? |
| `pattern_test.py` | Does a candle pattern beat the base rate (lift and mean forward return)? |
| `funding_test.py` | Does funding-rate positioning predict forward returns? |
| `depth_collector.py`, `depth_test.py` | Does order-book thinness predict moves? (collected forward; no history endpoint) |
| `rsi_indicators.py`, `rsi_test.py` | Ultimate RSI and divergence, entered only at pivot confirmation to avoid look-ahead |
| `dedup_report.py`, `self_correction.py` | Forward-test review with deduplication and honest error bars |

Principles used throughout: measure lift against the base rate of all candles, judge on mean forward return rather than best-case gains, include costs, avoid look-ahead, and treat small samples as provisional. Most hypotheses tested did not clear that bar; funding was the exception and became a filter. `signal_tracker.py` resolves live signals into outcomes so the system is judged on data no parameter was fitted to.

## Repository layout

```
main.py               orchestrator
data_pipeline.py      async scanner and signal builder
strategy_example.py   placeholder detector (production logic omitted)
executor.py           order execution via ccxt (disabled by default)
trade_monitor.py      position management
ai_analyst.py         LLM review
funding_filter.py     funding-rate exclusion
telegram_bridge.py    alerts
signal_tracker.py     forward-test outcome tracking
api_server.py         read-only API for a mobile dashboard
research/             backtests and hypothesis tests
tools/                exchange API diagnostics
```

## Running it

```bash
pip install -r requirements.txt
cp config.example.json config.json     # add your own keys; config.json is git-ignored
python main.py
```

Auto-execution is off by default (`auto_execute_trades: false`); the bot sends signals only. Research scripts run from the repo root, for example:

```bash
python -m research.backtest
python -m research.funding_test --limit 300
python signal_tracker.py
```

## Status

Prototype. Not currently running. Provided for portfolio review only; see `LICENSE`.
