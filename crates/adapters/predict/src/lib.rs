//! Nautilus adapter for predict.fun (POLYMARKET_PREDICT §6.2 skeleton).
//!
//! Current state (honest): P0 contract pinning only — the documented wire
//! shapes of the two public data endpoints this integration consumes
//! (matches, chance timeseries), captured as typed DTOs with offline
//! fixtures derived from the official OpenAPI schema. No client, signing,
//! or execution is wired yet (P3); the live API requires an `x-api-key`
//! which is user key material this repo does not hold.
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

pub use dtos::{
    PredictChancePoint, PredictChanceSeriesEnvelope, PredictMatchData, PredictMatchOrder,
    PredictMatchOutcome, PredictMatchesEnvelope, PredictPaginationMeta,
};
