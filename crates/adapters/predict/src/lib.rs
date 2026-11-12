//! Nautilus adapter for predict.fun (POLYMARKET_PREDICT §6.2 skeleton).
//!
//! Current state (honest): P0 contract pinning — documented wire shapes of
//! the three public data endpoints this integration consumes (matches,
//! chance timeseries, status logs) as typed DTOs with offline fixtures from
//! the official OpenAPI schema, plus the EIP-712 order-signing typed-data
//! contract (domain/Order structure/four exchange contracts) pinned from the
//! official Python SDK with its cross-verified golden hash vector. No HTTP
//! client, key handling, or execution is wired yet (P3); the live API
//! requires an `x-api-key` which is user key material this repo does not hold.
//!
//! Value-unit caveats registered per the plan's own honesty rules:
//! - `series[].{x,y}` numeric semantics: the docs show decimals (0.1) but do
//!   NOT confirm the chance unit (0–1 probability vs percent) or which of
//!   x/y is time — live-sample confirmation is a P0 follow-through requiring
//!   key material; until then the timeseries DTO carries both verbatim.
//! - matches `amountFilled`/`price`/fee amounts are 18-decimal wei strings
//!   per the docs (`1 USDT = 10^18`); exact per-sub-fill identity
//!   (settlementId + order hash + maker array position) is modeled but its
//!   uniqueness remains to be confirmed against live samples.

pub mod dtos;
pub mod signing;

pub use dtos::{
    PredictChancePoint, PredictChanceSeriesEnvelope, PredictMarketStatusLogs, PredictMatchData,
    PredictMatchOrder, PredictMatchOutcome, PredictMatchesEnvelope, PredictPaginationMeta,
    PredictStatusLog, PredictStatusLogsEnvelope,
};
