# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Chinese A-share investment analysis tool. Consists of a Flask REST API (`app.py`) that wraps several independent data-fetching modules, each targeting a different analytical domain.

## Running the Server

```bash
python app.py
```

Runs on `http://127.0.0.1:5000`. No build step required.

## Running a Single Fetcher Standalone

Each fetcher can be run independently:

```bash
python fetch_macro.py
python fetch_finance.py
python fetch_market.py
python fetch_sentiment.py
python fetch_space.py [--mode daily|weekly]
```

Each writes its result to `./output/<name>_snapshot.json`.

## Dependencies

Key packages: `flask`, `akshare`, `yfinance`, `requests`, `pandas`, `numpy`, `scipy`, `beautifulsoup4`.

## Architecture

### Data Flow

```
fetch_*.py  →  run() -> Dict  →  app.py  →  JSON API response
                    ↓
             output/*.json (disk cache)
```

### Fetcher Modules

All fetchers expose a single `run() -> Dict` function and write their own output files.

| Module | Domain | Output |
|--------|--------|--------|
| `fetch_macro.py` | Global macro (US yields, DXY, commodities via Yahoo Finance; A-share indices via akshare; FOMC dates) | `macro_snapshot.json` |
| `fetch_finance.py` | Company financials & valuation (akshare, uses SOCKS5 proxy patch) | `finance_snapshot.json` |
| `fetch_market.py` | Stock/index price data, β regression, relative PE percentile for stock 001287 中电港 vs 000300 沪深300 | `market_snapshot.json` |
| `fetch_sentiment.py` | A-share market sentiment: breadth (涨跌比), turnover percentile, margin financing (融资), QVIX; persists rolling history files | `sentiment_snapshot.json`, `sentiment_turnover_history.json`, `sentiment_breadth_history.json` |
| `fetch_space.py` | Space industry news scraping: launch events, procurement announcements, listed-company filings, funding news | `space_snapshot.json` |

### API Endpoints (`app.py`)

Each domain has two endpoints:
- `GET /xxx` — always fetches fresh data
- `GET /xxx/cached` — returns disk cache if within TTL, otherwise fetches fresh

Cache TTLs: macro=6h, space=12h, finance=6h, market=6h.

`fetch_sentiment` was added recently (commit `17f66bb`) but is **not yet wired into `app.py`** — it has no route yet.

### Proxy / SSL Handling

Each fetcher handles networking differently — this is intentional and module-specific:
- `fetch_finance.py`: monkey-patches `requests.api.request` to force SOCKS5 proxy (`127.0.0.1:7890`)
- `fetch_market.py`: monkey-patches `Session.send` to set `trust_env=False` (bypasses system HTTP proxy)
- `fetch_sentiment.py`: clears all proxy environment variables at import time
- `fetch_macro.py`: uses standard requests with retry adapter
- All modules disable SSL verification for `csindex.com.cn` via `ssl._create_default_https_context`

### Output Directory

`./output/` is git-ignored. All snapshot JSON files and history files land here at runtime.

### Snapshot Schema Convention

Every snapshot dict includes a `meta.generated_at` ISO timestamp field used by `app.py` for cache age calculation.
