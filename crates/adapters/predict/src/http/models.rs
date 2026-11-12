//! Wire DTOs for the Predict REST API (official OpenAPI `rest.json`,
//! 2026-11-12).
//!
//! Every response parses per its native schema: `success` gates are checked
//! at decode time by the client, envelopes carry the pagination cursor, and
//! values ride exact decimals — prices are Yes-axis probabilities in
//! `[0, 1]`, amounts are 18-decimal wei strings where `1 unit = 10^18`.

use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::common::enums::{
    PredictMarketStatus, PredictMarketStatusFilter, PredictOutcomeStatus, PredictQuoteType,
    PredictTradingStatus,
};

/// Generic paged envelope: `{ success, cursor?, data }` (markets, categories,
/// series rounds, matches share this shape).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictPage<T> {
    pub success: bool,
    #[serde(default)]
    pub cursor: Option<String>,
    pub data: T,
}

/// One outcome of a market (`name`, `indexSet`, `onChainId` are required).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictOutcomeDto {
    pub name: String,
    #[serde(rename = "indexSet")]
    pub index_set: i32,
    /// On-chain outcome token id (decimal bigint string) — symbol component.
    #[serde(rename = "onChainId")]
    pub on_chain_id: String,
    #[serde(default)]
    pub status: Option<PredictOutcomeStatus>,
}

/// A market as returned by the catalog endpoints. Field names are pinned
/// per the OpenAPI `Market` schema; deprecated top-of-book quote fields
/// (`bestBid`/`bestAsk` on outcomes) are not consumed and not decoded.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictMarketDto {
    pub id: i64,
    #[serde(rename = "imageUrl")]
    pub image_url: String,
    pub title: String,
    pub question: String,
    pub description: String,
    #[serde(rename = "tradingStatus")]
    pub trading_status: PredictTradingStatus,
    pub status: PredictMarketStatus,
    #[serde(rename = "isVisible")]
    pub is_visible: bool,
    #[serde(rename = "isNegRisk")]
    pub is_neg_risk: bool,
    #[serde(rename = "isYieldBearing")]
    pub is_yield_bearing: bool,
    /// Taker fee in basis points charged on each matched trade (makers are
    /// never charged — `makerFeeBps` is documented as always 0 and is not
    /// consumed).
    #[serde(rename = "feeRateBps")]
    pub fee_rate_bps: i32,
    #[serde(rename = "oracleQuestionId")]
    pub oracle_question_id: String,
    /// On-chain condition id (0x hex) — symbol component.
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    #[serde(rename = "resolverAddress")]
    pub resolver_address: String,
    pub outcomes: Vec<PredictOutcomeDto>,
    /// The winning outcome once `status` is `RESOLVED`.
    #[serde(default)]
    pub resolution: Option<PredictOutcomeDto>,
    #[serde(default)]
    #[serde(rename = "questionIndex")]
    pub question_index: Option<i32>,
    #[serde(rename = "spreadThreshold")]
    pub spread_threshold: f64,
    #[serde(rename = "shareThreshold")]
    pub share_threshold: i32,
    #[serde(rename = "isBoosted")]
    pub is_boosted: bool,
    /// Cross-venue reference material only — never a confirmed relation.
    #[serde(default)]
    #[serde(rename = "polymarketConditionIds")]
    pub polymarket_condition_ids: Vec<String>,
    #[serde(default)]
    #[serde(rename = "kalshiMarketTicker")]
    pub kalshi_market_ticker: Option<String>,
    #[serde(rename = "categorySlug")]
    pub category_slug: String,
    #[serde(default)]
    #[serde(rename = "seriesSlug")]
    pub series_slug: Option<String>,
    /// ISO-8601 creation timestamp.
    #[serde(rename = "createdAt")]
    pub created_at: String,
    /// Price decimal precision of the Yes axis (tick size is 10^-precision).
    #[serde(rename = "decimalPrecision")]
    pub decimal_precision: i32,
    /// Market template classification, carried verbatim (open-ended enum).
    #[serde(rename = "marketVariant")]
    pub market_variant: String,
}

/// A category (`CategoryWithStats` without the embedded `markets`/`stats`
/// blocks, which this integration does not consume).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictCategoryDto {
    pub id: i64,
    pub slug: String,
    pub title: String,
    pub description: String,
    #[serde(rename = "imageUrl")]
    pub image_url: String,
    #[serde(rename = "isNegRisk")]
    pub is_neg_risk: bool,
    #[serde(rename = "isYieldBearing")]
    pub is_yield_bearing: bool,
    #[serde(rename = "marketVariant")]
    pub market_variant: String,
    pub status: PredictMarketStatus,
}

/// A recurring series (`GET /series/{slug}`).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictSeriesDto {
    pub slug: String,
    pub symbol: String,
    #[serde(rename = "marketVariant")]
    pub market_variant: String,
    #[serde(rename = "intervalSeconds")]
    pub interval_seconds: i32,
}

/// `[price, shares]` level as sent by the order book endpoints (JSON
/// numbers on the Yes axis).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
pub struct PredictBookLevelDto(
    /// Yes price in `[0, 1]`.
    pub Decimal,
    /// Size in shares.
    pub Decimal,
);

impl PredictBookLevelDto {
    /// The level price (Yes axis).
    #[must_use]
    pub const fn price(self) -> Decimal {
        self.0
    }

    /// The level size in shares.
    #[must_use]
    pub const fn size(self) -> Decimal {
        self.1
    }
}

/// `{ price, size }` level as sent by the quote endpoints.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
pub struct PredictQuoteLevelDto {
    pub price: Decimal,
    pub size: Decimal,
}

/// Full order book of one market (Yes axis; best bid first in `bids`, best
/// ask first in `asks`). `GET /markets/{id}/orderbook` and each entry of
/// `GET /markets/orderbooks`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictOrderbookDto {
    #[serde(rename = "marketId")]
    pub market_id: i64,
    /// When the book last changed (epoch ms) — use it to discard frames
    /// arriving out of order.
    #[serde(rename = "updateTimestampMs")]
    pub update_timestamp_ms: i64,
    /// Lowest ask first.
    pub asks: Vec<PredictBookLevelDto>,
    /// Highest bid first.
    pub bids: Vec<PredictBookLevelDto>,
    #[serde(default)]
    pub last_order_settled: Option<PredictSettledOrderDto>,
}

/// The latest settled order summary carried on books/quotes.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictSettledOrderDto {
    pub id: String,
    /// Yes price with 2 decimals.
    pub price: String,
    pub kind: String,
    #[serde(rename = "marketId")]
    pub market_id: i64,
    pub side: PredictQuoteType,
    pub outcome: String,
}

/// Top of a market's book (Yes axis): `GET /markets/{id}/quote` and each
/// entry of `GET /markets/quotes`. Sides are absent when empty.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictMarketQuoteDto {
    #[serde(rename = "marketId")]
    pub market_id: i64,
    #[serde(rename = "updateTimestampMs")]
    pub update_timestamp_ms: i64,
    #[serde(default, rename = "bestBid")]
    pub best_bid: Option<PredictQuoteLevelDto>,
    #[serde(default, rename = "bestAsk")]
    pub best_ask: Option<PredictQuoteLevelDto>,
}

/// Structured error body (`ErrorResponse`): every error path produces it.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictErrorDto {
    pub success: bool,
    /// HTTP status code repeated in the body.
    pub code: u16,
    /// Stable machine-readable error code (e.g. `unauthorized`,
    /// `invalid_cursor`, `too_many_requests`).
    pub error: String,
    pub message: String,
    pub timestamp: String,
}

impl fmt::Display for PredictErrorDto {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}): {}", self.error, self.code, self.message)
    }
}

/// Markets list query (`GET /markets`).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PredictMarketsQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<PredictMarketStatusFilter>,
}

/// Matches query (`GET /orders/matches`). Time filters are exclusive unix
/// millisecond bounds; results sort by `executedAt` descending.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PredictMatchesQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "marketId"
    )]
    pub market_id: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "executedAfter"
    )]
    pub executed_after_ms: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "executedBefore"
    )]
    pub executed_before_ms: Option<i64>,
}

/// Chance timeseries query (`GET /markets/{id}/timeseries?metric=chance`).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictChanceQuery {
    pub metric: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
    /// Window start in unix seconds (inclusive).
    pub from: i64,
    /// Window end in unix seconds (inclusive; defaults to now server-side).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
}

#[cfg(test)]
mod tests {
    //! Offline fixtures derived from the official OpenAPI schema and the
    //! documented example payloads (book/quote semantics pages).

    use super::*;

    #[test]
    fn orderbook_parses_documented_example() {
        // Documented example from the "Prices and order book" concept page
        // (decimal JSON numbers on the Yes axis).
        let json = r#"{
            "marketId": 1,
            "updateTimestampMs": 1727910141000,
            "asks": [[0.492, 30192.26], [0.493, 20003]],
            "bids": [[0.491, 303518.1], [0.49, 1365.44]]
        }"#;
        let book: PredictOrderbookDto = serde_json::from_str(json).unwrap();
        assert_eq!(book.market_id, 1);
        assert_eq!(book.update_timestamp_ms, 1_727_910_141_000);
        assert_eq!(book.asks.len(), 2);
        assert_eq!(book.asks[0].price(), Decimal::new(492, 3));
        assert_eq!(book.asks[0].size(), Decimal::new(3_019_226, 2));
        assert_eq!(book.bids[0].price(), Decimal::new(491, 3));
        // lastOrderSettled absent on the REST shape.
        assert!(book.last_order_settled.is_none());
    }

    #[test]
    fn quote_parses_with_absent_sides() {
        let json = r#"{
            "marketId": 7,
            "updateTimestampMs": 1727910141000,
            "bestBid": { "price": 0.491, "size": 303518.1 }
        }"#;
        let quote: PredictMarketQuoteDto = serde_json::from_str(json).unwrap();
        assert_eq!(quote.best_bid.map(|l| l.price), Some(Decimal::new(491, 3)));
        assert!(quote.best_ask.is_none(), "empty side stays absent");
    }

    #[test]
    fn market_parses_documented_required_fields() {
        let json = r#"{
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
        }"#;
        let market: PredictMarketDto = serde_json::from_str(json).unwrap();
        assert_eq!(market.id, 4242);
        assert_eq!(market.condition_id, "0xcond");
        assert_eq!(market.outcomes.len(), 2);
        assert_eq!(market.outcomes[0].on_chain_id, "12345");
        assert_eq!(market.decimal_precision, 3);
        assert_eq!(market.fee_rate_bps, 100);
        assert_eq!(market.status, PredictMarketStatus::Registered);
        assert_eq!(market.trading_status, PredictTradingStatus::Open);
        assert!(market.resolution.is_none());
    }

    #[test]
    fn error_body_parses() {
        let json = r#"{
            "success": false,
            "code": 401,
            "error": "unauthorized",
            "message": "Missing API key",
            "timestamp": "2026-11-12T00:00:00Z"
        }"#;
        let error: PredictErrorDto = serde_json::from_str(json).unwrap();
        assert_eq!(error.code, 401);
        assert_eq!(error.error, "unauthorized");
    }

    #[test]
    fn queries_serialize_documented_parameter_names() {
        let query = PredictMatchesQuery {
            first: Some(150),
            after: Some("CURSOR".to_string()),
            market_id: Some(7),
            executed_after_ms: Some(1_700_000_000_000),
            executed_before_ms: None,
        };
        let params: Vec<(String, String)> = serde_urlencoded::to_string(&query)
            .unwrap()
            .split('&')
            .filter(|s| !s.is_empty())
            .map(|pair| {
                let (k, v) = pair.split_once('=').unwrap();
                (k.to_string(), v.to_string())
            })
            .collect();
        assert!(params.contains(&("marketId".to_string(), "7".to_string())));
        assert!(params.contains(&("executedAfter".to_string(), "1700000000000".to_string())));
        assert!(!params.iter().any(|(k, _)| k == "executedBefore"));
    }
}
