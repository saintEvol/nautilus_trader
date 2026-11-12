# Predict

## Status: Alpha (data only)

The Predict adapter (`PREDICT` venue, [predict.fun](https://predict.fun) on
BNB Mainnet) currently covers the **public data** surface. Execution
(order signing, JWT auth, positions) is not wired.

## Overview

- Markets resolve on-chain; every market outcome is a `BinaryOption`
  instrument with the native symbol `<condition_id>-<onChainId>` and the
  chain-true 18-decimal `USDT@BSC` collateral currency.
- The venue streams only the Yes-axis book (`predictOrderbook/{marketId}`):
  each full frame is published as two `OrderBookDeltas` batches — the Yes
  view verbatim and the exact No complement (`1 − p` at the market's
  decimal precision; inexact complements are rejected loudly, never
  rounded). Quotes derive from the same frames; neither leg ever
  synthesizes a zero-price level.
- Subscriptions are reference-counted per market: the Yes/No legs and the
  quote/book/status consumers of one market share one wire subscription,
  released by the last owner.
- Public trades come from a bounded incremental REST poll
  (`GET /v1/orders/matches`) with an overlap window, stable
  `(settlementId, transactionHash)` dedup and a high-water mark — a page
  budget exhaustion never advances the watermark.
- Resolution (`predictMarketStatus` RESOLVED with wei payouts `0/0.5/1`)
  publishes `InstrumentClose` per outcome leg; trading-status changes map
  to `InstrumentStatus`.
- The WebSocket client owns reconnection: heartbeats are echoed verbatim,
  retained topics are re-subscribed on every new generation, and the data
  client refetches REST state (nothing is replayed across the gap).
- The HTTP client walks cursors with structured completion; a page budget
  running out never masquerades as source exhaustion.

## Authentication

A predict.fun **data API key** (`x-api-key`) is required for the REST
endpoints and the WS handshake. It carries no trading authority. The
adapter consumes it only from explicit `PredictDataClientConfig` — there
is no environment-variable fallback.

## Configuration

```python
from nautilus_trader.adapters.predict import PredictDataClientConfig
# (Python bindings ship when the execution surface lands; the Rust config
#  is available now.)
```

`PredictDataClientConfig` (bon builder):

| Field | Default | Notes |
| --- | --- | --- |
| `api_key` | — (required) | data API key |
| `environment` | Mainnet | selects base URLs (Testnet available) |
| `base_url_http` / `base_url_ws` | per environment | overrides |
| `http_timeout_secs` / `ws_timeout_secs` | 60 / 30 | |
| `instrument_ids` | — | bootstrap instruments; both legs resolve via the market context |
| `trades_poll_interval_secs` | 5 | public-trades poll cadence |
| `trades_poll_overlap_ms` | 30 000 | overlap window re-scanned per poll |
| `trades_poll_max_pages` | 4 | per-poll page budget |
| `discovery_max_pages` | 100 | catalog walk budget |

## Unsupported

Bars (derive from trades at the platform layer), mark/index prices,
funding rates, option greeks, book depth subscriptions, historical quotes,
and anything requiring trading credentials (P5 work).

## Live-sample follow-throughs

Field semantics the official documents leave open are registered, not
guessed: the `matches` sub-fill identity and fee values, the exact
`RateLimit-Reset` unit, and status-logs retention depth require live
samples to pin.
