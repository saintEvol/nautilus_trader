//! Nautilus adapter for predict.fun (POLYMARKET_PREDICT integration).
//!
//! Data-side scope (P3): HTTP client over the public data API (catalog,
//! books/quotes, matches, chance timeseries, status logs), an instrument
//! provider producing one `BinaryOption` per market outcome, a WebSocket
//! client for the `predictOrderbook`/`predictTradingStatus`/
//! `predictMarketStatus` topics (heartbeat echo, reconnect resubscribe,
//! reference-counted per-market subscriptions), full-frame Yes→No dual
//! projection published as batched order-book deltas, REST trades polling
//! with overlap/dedup/watermark, and resolution → `InstrumentStatus` /
//! `InstrumentClose`.
//!
//! Honesty boundaries (until live samples exist): the EIP-712 order-signing
//! contract is pinned offline (`signing`, golden vector from the official
//! SDK); the trading REST transport (JWT auth, order placement/removal/
//! query) and the pure amount mathematics are wired (`http::trading`,
//! `execution::amounts`); the nautilus `ExecutionClient` shell and
//! position operations remain P5 work. The data API requires an
//! `x-api-key` which is user key
//! material — it is only ever consumed from explicit configuration.

pub mod book;
pub(crate) mod common;
pub mod config;
pub mod data;
pub mod dtos;
pub mod execution;
pub mod factories;
pub mod http;
pub mod providers;
pub mod signing;
pub mod websocket;

pub use common::consts::{PREDICT, PREDICT_CLIENT_ID, PREDICT_VENUE, collateral_currency};
pub use common::enums::{
    PredictEnvironment, PredictFeeType, PredictMarketStatus, PredictMarketStatusFilter,
    PredictMetric, PredictOutcomeSide, PredictOutcomeStatus, PredictQuoteType,
    PredictRoundStatusFilter, PredictTimeseriesResolution, PredictTradingStatus,
};
pub use common::parse::{
    outcome_instrument_id, outcome_symbol, parse_instrument_id, parse_outcome_symbol,
};
pub use http::client::{ChanceWalk, MatchesWalk, PredictHttpClient, WalkEnd};
pub use http::Pages;
pub use http::error::PredictHttpError;
pub use http::models::{
    PredictBookLevelDto, PredictCategoryDto, PredictMarketDto, PredictMarketQuoteDto,
    PredictOrderbookDto, PredictOutcomeDto, PredictSeriesDto,
};
pub use providers::PredictInstrumentProvider;
pub use signing::{
    PredictChain, PredictSigningContext, bnb_mainnet, domain_separator, order_struct_hash,
    order_typed_data_hash, order_type_hash, signing_context,
};

pub use dtos::{
    PredictChancePoint, PredictChanceSeriesEnvelope, PredictMarketStatusLogs, PredictMatchData,
    PredictMatchOrder, PredictMatchOutcome, PredictMatchesEnvelope, PredictStatusLog,
    PredictStatusLogsEnvelope,
};
