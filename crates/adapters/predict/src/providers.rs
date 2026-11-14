//! Instrument provider for the Predict adapter: each market outcome becomes
//! one [`BinaryOption`] instrument.
//!
//! Symbols are `<condition_id>-<onChainId>.PREDICT`; the HTTP market id,
//! neg-risk/yield-bearing flags, fee schedule, trading status and precision
//! ride the instrument `info` metadata (they select the signing contract
//! and the projection axis downstream — never guessable from the symbol).

use std::sync::Arc;

use nautilus_core::UnixNanos;
use nautilus_model::enums::AssetClass;
use nautilus_model::identifiers::{InstrumentId, Symbol};
use nautilus_model::instruments::BinaryOption;
use nautilus_model::types::{Price, Quantity};
use nautilus_core::{Params, datetime::try_datetime_to_unix_nanos};
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::common::consts::{MIN_SIZE_INCREMENT, SIZE_PRECISION, collateral_currency};
use crate::common::parse::outcome_instrument_id;
use crate::http::client::PredictHttpClient;
use crate::http::models::{PredictMarketDto, PredictOutcomeDto};

/// Loads market definitions and serves instrument requests. Definitions are
/// always fetched fresh from REST (mirrors the Kalshi provider — no private
/// cache; the data client keeps its own working context).
#[derive(Debug)]
pub struct PredictInstrumentProvider {
    http: Arc<PredictHttpClient>,
}

impl PredictInstrumentProvider {
    /// Creates a provider over the shared HTTP client.
    #[must_use]
    pub fn new(http: Arc<PredictHttpClient>) -> Self {
        Self { http }
    }

    /// Loads every open market page (bounded by `max_pages`) and builds one
    /// instrument per outcome. A page budget running out is a loud error —
    /// truncated discovery must never look complete.
    ///
    /// # Errors
    /// See [`crate::http::client::PredictHttpClient`].
    pub async fn load_instruments(
        &self,
        max_pages: u32,
    ) -> Result<Vec<BinaryOption>, crate::http::error::PredictHttpError> {
        let pages = self
            .http
            .get_markets(Some(crate::common::enums::PredictMarketStatusFilter::Open), max_pages)
            .await?;
        if !pages.complete {
            return Err(crate::http::error::PredictHttpError::Pagination(format!(
                "instrument discovery page budget exhausted after {max_pages} pages"
            )));
        }
        Ok(pages
            .records
            .iter()
            .flat_map(instruments_for_market)
            .collect())
    }

    /// Serves a single instrument request by fetching the market fresh.
    /// The `instrument_id` symbol carries `(condition_id, on_chain_id)`.
    ///
    /// # Errors
    /// Returns an error when the instrument is not a PREDICT symbol or the
    /// market cannot be found.
    pub async fn load_instrument(
        &self,
        instrument_id: &InstrumentId,
        ts_init: UnixNanos,
    ) -> anyhow::Result<BinaryOption> {
        let (condition_id, on_chain_id) = crate::common::parse::parse_instrument_id(instrument_id)
            .map_err(|e| anyhow::anyhow!(e))?;
        let market = self.find_market_by_condition(condition_id).await?;
        let outcome = market
            .outcomes
            .iter()
            .find(|outcome| outcome.on_chain_id == on_chain_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "market {condition_id} has no outcome {on_chain_id} for {instrument_id}"
                )
            })?;
        parse_instrument(&market, outcome, ts_init)
    }

    /// Finds a market by its on-chain condition id.
    ///
    /// The catalog has no condition-id lookup endpoint, so open markets are
    /// walked once per call; `marketId` is the only direct key. The data
    /// client's context map avoids this in steady state.
    ///
    /// # Errors
    /// See [`crate::http::client::PredictHttpClient`].
    async fn find_market_by_condition(
        &self,
        condition_id: &str,
    ) -> Result<PredictMarketDto, crate::http::error::PredictHttpError> {
        let pages = self.http.get_markets(None, 50).await?;
        if !pages.complete {
            return Err(crate::http::error::PredictHttpError::Pagination(format!(
                "market lookup by condition {condition_id} hit the page budget"
            )));
        }
        pages
            .records
            .into_iter()
            .find(|market| market.condition_id == condition_id)
            .ok_or_else(|| {
                crate::http::error::PredictHttpError::Pagination(format!(
                    "no market with condition {condition_id}"
                ))
            })
    }
}

/// Builds one instrument per buildable outcome of a market; outcomes that
/// fail validation are skipped with a warning (venue data must never
/// panic a discovery walk).
pub fn instruments_for_market(market: &PredictMarketDto) -> Vec<BinaryOption> {
    market
        .outcomes
        .iter()
        .filter_map(|outcome| {
            match parse_instrument(market, outcome, UnixNanos::default()) {
                Ok(instrument) => Some(instrument),
                Err(e) => {
                    log::warn!(
                        "skipping outcome {} of market {}: {e}",
                        outcome.on_chain_id,
                        market.id
                    );
                    None
                }
            }
        })
        .collect()
}

/// Converts one market outcome into a [`BinaryOption`].
///
/// # Errors
///
/// Returns an error on an invalid market (unknown precision, missing
/// activation/expiration semantics, or an outcome that cannot form a
/// native symbol).
pub fn parse_instrument(
    market: &PredictMarketDto,
    outcome: &PredictOutcomeDto,
    ts_init: UnixNanos,
) -> anyhow::Result<BinaryOption> {
    anyhow::ensure!(
        market.decimal_precision > 0 && market.decimal_precision <= 18,
        "market {} has invalid decimalPrecision {}",
        market.id,
        market.decimal_precision
    );
    let price_precision = u8::try_from(market.decimal_precision)?;
    let instrument_id = outcome_instrument_id(&market.condition_id, &outcome.on_chain_id);

    let price_increment =
        Price::from_decimal_dp(decimal_pow10(price_precision), price_precision)?;
    let size_increment =
        Quantity::from_decimal_dp(Decimal::from_str_exact(MIN_SIZE_INCREMENT)?, SIZE_PRECISION)?;
    let currency = collateral_currency();

    let info = Params::new(
        [
            ("market_id".to_string(), serde_json::json!(market.id)),
            (
                "condition_id".to_string(),
                serde_json::json!(market.condition_id),
            ),
            ("on_chain_id".to_string(), serde_json::json!(outcome.on_chain_id)),
            ("index_set".to_string(), serde_json::json!(outcome.index_set)),
            ("is_neg_risk".to_string(), serde_json::json!(market.is_neg_risk)),
            (
                "is_yield_bearing".to_string(),
                serde_json::json!(market.is_yield_bearing),
            ),
            ("fee_rate_bps".to_string(), serde_json::json!(market.fee_rate_bps)),
            ("maker_fee_bps".to_string(), serde_json::json!(0)),
            (
                "trading_status".to_string(),
                serde_json::json!(market.trading_status),
            ),
            ("market_status".to_string(), serde_json::json!(market.status)),
            (
                "category_slug".to_string(),
                serde_json::json!(market.category_slug),
            ),
            (
                "series_slug".to_string(),
                match &market.series_slug {
                    Some(slug) => serde_json::json!(slug),
                    None => serde_json::Value::Null,
                },
            ),
            (
                "question".to_string(),
                serde_json::json!(market.question),
            ),
            (
                "outcome_name".to_string(),
                serde_json::json!(outcome.name),
            ),
        ]
        .into(),
    );

    // Markets have no separate activation instant in the catalog; creation
    // time anchors the instrument start and expiry stays unset (resolved
    // markets close via resolution events, not a timestamped expiry — same
    // shape as the Polymarket BinaryOption instruments).
    let created: jiff::Timestamp = market
        .created_at
        .parse()
        .map_err(|e| anyhow::anyhow!("market {} createdAt {:?}: {e}", market.id, market.created_at))?;
    let activation_ns = try_datetime_to_unix_nanos(created)?;

    Ok(BinaryOption::builder()
        .instrument_id(instrument_id)
        .raw_symbol(Symbol::new(format!(
            "{}-{}",
            market.condition_id, outcome.on_chain_id
        )))
        .asset_class(AssetClass::Alternative)
        .currency(currency)
        .activation_ns(activation_ns)
        .expiration_ns(UnixNanos::from(u64::MAX))
        .price_precision(price_precision)
        .size_precision(SIZE_PRECISION)
        .price_increment(price_increment)
        .size_increment(size_increment)
        .min_quantity(size_increment)
        .event_id(Ustr::from(&market.condition_id))
        .outcome(Ustr::from(&outcome.name))
        .description(Ustr::from(&market.title))
        .info(info)
        .ts_event(activation_ns)
        .ts_init(ts_init)
        .build()?)
}

/// `10^-precision` as an exact decimal.
fn decimal_pow10(precision: u8) -> Decimal {
    Decimal::new(1, u32::from(precision))
}

pub(crate) fn request_bounds(
    start: Option<jiff::Timestamp>,
    end: Option<jiff::Timestamp>,
) -> anyhow::Result<(Option<UnixNanos>, Option<UnixNanos>)> {
    let start = start
        .map(try_datetime_to_unix_nanos)
        .transpose()
        .map_err(|e| anyhow::anyhow!("invalid request start bound: {e}"))?;
    let end = end
        .map(try_datetime_to_unix_nanos)
        .transpose()
        .map_err(|e| anyhow::anyhow!("invalid request end bound: {e}"))?;
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market() -> PredictMarketDto {
        serde_json::from_str(r#"{
            "id": 4242,
            "imageUrl": "img",
            "title": "Example",
            "question": "Will it happen?",
            "description": "desc",
            "tradingStatus": "OPEN",
            "status": "REGISTERED",
            "isVisible": true,
            "isNegRisk": true,
            "isYieldBearing": true,
            "feeRateBps": 100,
            "makerFeeBps": 0,
            "oracleQuestionId": "0xo",
            "conditionId": "0xcond",
            "resolverAddress": "0xr",
            "outcomes": [
                { "name": "Yes", "indexSet": 1, "onChainId": "12345" },
                { "name": "No", "indexSet": 2, "onChainId": "67890" }
            ],
            "spreadThreshold": 0.01,
            "shareThreshold": 5,
            "isBoosted": false,
            "polymarketConditionIds": [],
            "categorySlug": "example",
            "createdAt": "2026-10-01T00:00:00Z",
            "decimalPrecision": 3,
            "marketVariant": "DEFAULT"
        }"#).unwrap()
    }

    #[test]
    fn builds_one_binary_option_per_outcome() {
        let instruments = instruments_for_market(&market());
        assert_eq!(instruments.len(), 2);

        let yes = &instruments[0];
        assert_eq!(yes.id.to_string(), "0xcond-12345.PREDICT");
        assert_eq!(yes.price_precision, 3);
        assert_eq!(yes.size_precision, SIZE_PRECISION);
        assert_eq!(yes.outcome, Some(Ustr::from("Yes")));
        // Chain-true currency: 18-decimal USDT@BSC, not the built-in USDT.
        assert_eq!(yes.currency.precision, 18);
        assert_eq!(yes.currency.code.as_str(), "USDT@BSC");
        // Contract-selection facts survive into metadata.
        assert_eq!(
            yes.info
                .as_ref()
                .and_then(|info| info.get("market_id"))
                .cloned(),
            Some(serde_json::json!(4242))
        );
        assert_eq!(
            yes.info
                .as_ref()
                .and_then(|info| info.get("is_neg_risk"))
                .cloned(),
            Some(serde_json::json!(true))
        );

        let no = &instruments[1];
        assert_eq!(no.id.to_string(), "0xcond-67890.PREDICT");
        assert_eq!(no.outcome, Some(Ustr::from("No")));
    }

    #[test]
    fn invalid_precision_is_rejected_loudly() {
        let mut market = market();
        market.decimal_precision = 0;
        let error = parse_instrument(&market, &market.outcomes[0], UnixNanos::default())
            .expect_err("precision 0 must be rejected");
        assert!(error.to_string().contains("decimalPrecision"), "{error}");
    }
}
