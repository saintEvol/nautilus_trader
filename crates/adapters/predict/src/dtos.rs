//! Documented wire DTOs for the predict.fun public data endpoints
//! (POLYMARKET_PREDICT P0 contract pinning; §7.2 source table).
//!
//! Sources (official OpenAPI, 2026-11-12):
//! - `GET /v1/orders/matches` — dev.predict.fun/api/orders/get-order-match-events
//! - `GET /v1/markets/{id}/timeseries?metric=chance` —
//!   dev.predict.fun/api/markets/get-market-timeseries

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::common::enums::{PredictFeeType, PredictQuoteType};
use crate::http::models::PredictMarketDto;

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
/// transactions). The row carries the full market object it belongs to
/// (required by the current official schema — the P0 pin omitted it).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictMatchData {
    /// The market the trade belongs to (outcome routing lives here).
    pub market: PredictMarketDto,
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
    /// The order's side.
    #[serde(rename = "quoteType")]
    pub quote_type: PredictQuoteType,
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
    /// (`COLLATERAL` or `SHARES` — the fee-asset fact the plan's P0 asks to
    /// pin).
    pub fee: PredictMatchFee,
}

/// Fee as reported on a match leg.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictMatchFee {
    /// Fee amount (18-decimal wei string).
    #[serde(deserialize_with = "de_decimal")]
    pub amount: Decimal,
    /// Fee asset semantics.
    #[serde(rename = "type")]
    pub fee_type: PredictFeeType,
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

/// Status-logs envelope: `GET /v1/markets/status-logs?ids=...` (documented
/// schema, 2026-11-12). Auth: `x-api-key`. No payout field exists — resolution
/// rides `proposedOutcomeId/Label` + `resolutionProvider`; the
/// "logs ≠ full history" boundary (log presence vs retention depth) stays a
/// live-sample follow-through.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictStatusLogsEnvelope {
    pub success: bool,
    pub data: Vec<PredictMarketStatusLogs>,
}

/// Per-market log block.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictMarketStatusLogs {
    #[serde(rename = "marketId")]
    pub market_id: u64,
    pub logs: Vec<PredictStatusLog>,
}

/// One status-log row (chain event attestation: transactionHash + logIndex).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictStatusLog {
    /// e.g. `"REGISTERED"` — lifecycle words pinned by live samples.
    pub status: String,
    pub timestamp: String,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "logIndex")]
    pub log_index: u64,
    #[serde(rename = "proposedOutcomeId", default)]
    pub proposed_outcome_id: Option<u64>,
    #[serde(rename = "proposedOutcomeLabel", default)]
    pub proposed_outcome_label: Option<String>,
    #[serde(rename = "resolutionProvider", default)]
    pub resolution_provider: Option<String>,
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
                    "market": {
                        "id": 4242,
                        "imageUrl": "https://example/img.png",
                        "title": "Example market",
                        "question": "Will it happen?",
                        "description": "Resolves YES if it happens",
                        "tradingStatus": "OPEN",
                        "status": "REGISTERED",
                        "isVisible": true,
                        "isNegRisk": false,
                        "isYieldBearing": false,
                        "feeRateBps": 100,
                        "makerFeeBps": 0,
                        "oracleQuestionId": "0xoracle",
                        "conditionId": "0xcond",
                        "resolverAddress": "0xresolver",
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
                        "marketVariant": "DEFAULT",
                        "rewards": { "schedule": [] }
                    },
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
        // The embedded market carries outcome routing.
        assert_eq!(m.market.id, 4242);
        assert_eq!(m.market.condition_id, "0xcond");
        assert_eq!(m.market.outcomes[0].on_chain_id, "12345");
        // Wei strings decode to exact decimals (no float transit).
        assert_eq!(m.amount_filled, Decimal::new(1_000_000_000_000_000_000, 0));
        assert_eq!(m.taker.fee.amount, Decimal::new(10_000_000_000_000_000, 0));
        assert_eq!(m.taker.fee.fee_type, crate::common::enums::PredictFeeType::Collateral);
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
    fn status_logs_parses_documented_shape() {
        let json = r#"{
            "success": true,
            "data": [
                {
                    "marketId": 1234,
                    "logs": [
                        {
                            "status": "REGISTERED",
                            "timestamp": "2026-10-01T00:00:00Z",
                            "transactionHash": "0xtx1",
                            "logIndex": 5,
                            "proposedOutcomeId": null,
                            "proposedOutcomeLabel": null,
                            "resolutionProvider": null
                        },
                        {
                            "status": "RESOLVED",
                            "timestamp": "2026-10-05T00:00:00Z",
                            "transactionHash": "0xtx2",
                            "logIndex": 9,
                            "proposedOutcomeId": 1,
                            "proposedOutcomeLabel": "Yes",
                            "resolutionProvider": "PREDICT_DOT_FUN"
                        }
                    ]
                }
            ]
        }"#;
        let envelope: PredictStatusLogsEnvelope = serde_json::from_str(json).unwrap();
        let market = &envelope.data[0];
        assert_eq!(market.market_id, 1234);
        assert_eq!(market.logs.len(), 2);
        assert_eq!(market.logs[0].status, "REGISTERED");
        assert_eq!(market.logs[0].log_index, 5);
        assert_eq!(market.logs[1].proposed_outcome_id, Some(1));
        assert_eq!(
            market.logs[1].resolution_provider.as_deref(),
            Some("PREDICT_DOT_FUN")
        );
    }

}
