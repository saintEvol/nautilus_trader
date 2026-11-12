//! Documented wire DTOs for the predict.fun public data endpoints
//! (POLYMARKET_PREDICT P0 contract pinning; §7.2 source table).
//!
//! Sources (official OpenAPI, 2026-11-12):
//! - `GET /v1/orders/matches` — dev.predict.fun/api/orders/get-order-match-events
//! - `GET /v1/markets/{id}/timeseries?metric=chance` —
//!   dev.predict.fun/api/markets/get-market-timeseries

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Matches envelope: `{ success, cursor, data }` (cursor feeds `after`).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictMatchesEnvelope {
    pub success: bool,
    /// Pagination cursor — pass back as the `after` query parameter.
    #[serde(default)]
    pub cursor: Option<String>,
    pub data: Vec<PredictMatchData>,
}

/// Chance timeseries envelope: `{ success, cursor, data: {resolution, series} }`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictChanceSeriesEnvelope {
    pub success: bool,
    #[serde(default)]
    pub cursor: Option<String>,
    pub data: PredictChanceSeries,
}

/// The `data` object of the timeseries response.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictChanceSeries {
    /// Actual sampling granularity of the returned points (`1m|5m|1h|1d|1w|1M`)
    /// — the server's answer, not the requested one.
    pub resolution: String,
    pub series: Vec<PredictChancePoint>,
}

/// One timeseries point. The documented schema names the fields `x` and `y`
/// with numeric values and does NOT state which axis is time or the chance
/// unit — both carry verbatim decimals until a live sample fixes them.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PredictChancePoint {
    pub x: Decimal,
    pub y: Decimal,
}

/// One match row: an entire executed trade (taker + makers) — NOT one
/// sub-fill. Sub-fill identity composes
/// `(settlementId, order.hash, makers[].hash)` and remains to be confirmed
/// against live samples (documented maker-filter behavior returns whole
/// transactions).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictMatchData {
    /// The taker order of this trade.
    pub taker: PredictMatchOrder,
    /// Every maker order settled in the same transaction.
    pub makers: Vec<PredictMatchOrder>,
    /// Taker fill amount (18-decimal wei string; `1 USDT = 10^18`).
    #[serde(rename = "amountFilled", deserialize_with = "de_decimal")]
    pub amount_filled: Decimal,
    /// Executed price (18-decimal wei string).
    #[serde(rename = "priceExecuted", deserialize_with = "de_decimal")]
    pub price_executed: Decimal,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "settlementId")]
    pub settlement_id: String,
    /// Execution timestamp (rows sort by this, descending).
    #[serde(rename = "executedAt")]
    pub executed_at: String,
}

/// One order side of a match (taker or maker).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictMatchOrder {
    /// `"Ask" | "Bid"`.
    #[serde(rename = "quoteType")]
    pub quote_type: String,
    /// Order amount (18-decimal wei string).
    #[serde(rename = "amount", deserialize_with = "de_decimal")]
    pub amount: Decimal,
    /// Order price (18-decimal wei string).
    #[serde(rename = "price", deserialize_with = "de_decimal")]
    pub price: Decimal,
    pub outcome: PredictMatchOutcome,
    /// Signer address.
    pub signer: String,
    /// Order hash — sub-fill identity component.
    pub hash: String,
    /// Actual fee charged on this leg: amount + asset type
    /// (`"COLLATERAL"` — the fee-asset fact the plan's P0 asks to pin).
    pub fee: PredictMatchFee,
}

/// Fee as reported on a match leg.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictMatchFee {
    /// Fee amount (18-decimal wei string).
    #[serde(deserialize_with = "de_decimal")]
    pub amount: Decimal,
    /// e.g. `"COLLATERAL"` — fee asset semantics.
    #[serde(rename = "type")]
    pub fee_type: String,
}

/// The outcome object carried on each match leg.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictMatchOutcome {
    pub name: String,
    /// On-chain outcome id (symbol component: `<condition_id>-<onChainId>`).
    #[serde(rename = "onChainId", deserialize_with = "de_string_or_u64")]
    pub on_chain_id: String,
    #[serde(rename = "bestBid", default)]
    pub best_bid: Option<Decimal>,
    #[serde(rename = "bestAsk", default)]
    pub best_ask: Option<Decimal>,
}

/// Pagination facts pinned from the docs (not a wire object).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictPaginationMeta {
    /// Matches page size: 1–150, default 50.
    pub matches_first_max: u32,
    /// Timeseries window: `from` must precede `to` by at most this many days.
    pub timeseries_max_window_days: u32,
    /// Timeseries page size: 1–250 points, default 50.
    pub timeseries_limit_max: u32,
}

pub const PREDICT_PAGINATION: PredictPaginationMeta = PredictPaginationMeta {
    matches_first_max: 150,
    timeseries_max_window_days: 365,
    timeseries_limit_max: 250,
};

fn de_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    Decimal::from_str_exact(&raw).map_err(serde::de::Error::custom)
}

fn de_string_or_u64<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => Ok(s),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        other => Err(serde::de::Error::custom(format!(
            "onChainId must be string or number, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    //! Offline fixtures derived from the OFFICIAL OpenAPI schema (not live
    //! captures — the API requires an `x-api-key` this repo does not hold).
    //! Field names/shapes are pinned; VALUE units (chance axis, wei
    //! normalization) stay registered as live-sample follow-throughs.

    use super::*;

    #[test]
    fn matches_row_parses_documented_shape() {
        let json = r#"{
            "success": true,
            "cursor": "NEXT",
            "data": [
                {
                    "taker": {
                        "quoteType": "Ask",
                        "amount": "1000000000000000000",
                        "price": "500000000000000000",
                        "outcome": {
                            "name": "Yes",
                            "onChainId": "12345",
                            "bestBid": "0.49",
                            "bestAsk": "0.51"
                        },
                        "signer": "0xabc",
                        "hash": "0xorder-taker",
                        "fee": { "amount": "10000000000000000", "type": "COLLATERAL" }
                    },
                    "makers": [
                        {
                            "quoteType": "Bid",
                            "amount": "1000000000000000000",
                            "price": "500000000000000000",
                            "outcome": { "name": "Yes", "onChainId": "12345" },
                            "signer": "0xdef",
                            "hash": "0xorder-maker-1",
                            "fee": { "amount": "0", "type": "COLLATERAL" }
                        }
                    ],
                    "amountFilled": "1000000000000000000",
                    "priceExecuted": "500000000000000000",
                    "transactionHash": "0xtx",
                    "settlementId": "0xsettle",
                    "executedAt": "2026-11-01T00:00:00Z"
                }
            ]
        }"#;
        let envelope: PredictMatchesEnvelope = serde_json::from_str(json).unwrap();
        assert!(envelope.success);
        assert_eq!(envelope.cursor.as_deref(), Some("NEXT"));

        let m = &envelope.data[0];
        // Whole-trade semantics: taker + makers ride one row.
        assert_eq!(m.makers.len(), 1);
        assert_eq!(m.taker.hash, "0xorder-taker");
        assert_eq!(m.makers[0].hash, "0xorder-maker-1");
        // Wei strings decode to exact decimals (no float transit).
        assert_eq!(m.amount_filled, Decimal::new(1_000_000_000_000_000_000, 0));
        assert_eq!(m.taker.fee.amount, Decimal::new(10_000_000_000_000_000, 0));
        assert_eq!(m.taker.fee.fee_type, "COLLATERAL");
        assert_eq!(m.taker.outcome.on_chain_id, "12345");
        // Numeric onChainId also accepted (schema allows both).
        let numeric: PredictMatchOutcome =
            serde_json::from_str(r#"{"name":"No","onChainId":999}"#).unwrap();
        assert_eq!(numeric.on_chain_id, "999");
    }

    #[test]
    fn chance_series_parses_documented_shape() {
        let json = r#"{
            "success": true,
            "cursor": null,
            "data": {
                "resolution": "1m",
                "series": [ { "x": 1730000000, "y": 0.52 }, { "x": 1730000060, "y": 0.53 } ]
            }
        }"#;
        let envelope: PredictChanceSeriesEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(envelope.data.resolution, "1m");
        assert_eq!(envelope.data.series.len(), 2);
        assert_eq!(envelope.data.series[1].y, Decimal::new(53, 2));
        assert_eq!(envelope.data.series[0].x, Decimal::new(1_730_000_000, 0));
    }

    #[test]
    fn pagination_bounds_pinned() {
        assert_eq!(PREDICT_PAGINATION.matches_first_max, 150);
        assert_eq!(PREDICT_PAGINATION.timeseries_max_window_days, 365);
        assert_eq!(PREDICT_PAGINATION.timeseries_limit_max, 250);
    }
}
