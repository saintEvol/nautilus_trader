//! Polymarket historical market-level HTTP reads: sampled price history,
//! certified resolutions and per-market open interest
//! (POLYMARKET_PREDICT §5.1/§6.1/§7.2).
//!
//! Every DTO mirrors the documented v2 schema exactly; numeric values are
//! carried as exact `Decimal` (wire JSON numbers or numeric strings both
//! accepted, no float transit). Field semantics that matter downstream:
//! - `PricePoint.resolution_seconds` is the **actual** sampling granularity
//!   (0 = exact tick, else bucket width) — never the requested one.
//! - Resolution `payouts` are per-share micro-USDC integers
//!   (`[outcome0, outcome1]`, full win = 1_000_000).
//! - Open interest is priced gross USDC **per market** (never splittable
//!   across outcome legs); the `GLOBAL` row answers the parameterless form.

use nautilus_network::http::Method;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::common::parse::deserialize_decimal_from_json;
use crate::http::{
    data_api::PolymarketDataApiHttpClient,
    error::{Error, Result, decode_response},
    models::DataApiPage,
};

/// One sampled price point from `GET /v2/prices-history`.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct PricePoint {
    /// The observation's own time (epoch seconds) — never the time that was
    /// asked for.
    pub timestamp: i64,
    /// Price in [0, 1], exact decimal (wire is a JSON number).
    #[serde(deserialize_with = "deserialize_decimal_from_json")]
    pub price: Decimal,
    /// Actual sampling granularity: `0` for an exact tick, else the bucket
    /// width; the price observes `[timestamp, timestamp + resolution_seconds)`.
    pub resolution_seconds: i64,
}

/// A resolution row from `GET /v2/resolutions`.
///
/// Rows are keyed by `question_id` or `condition_id` (exactly one present).
/// `payouts` (per-outcome micro-USDC per share) and `resolved_at` are present
/// on resolved condition-keyed rows; unknown stays `None` — never guessed.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ResolutionRow {
    pub status: String,
    #[serde(default)]
    pub extended_review: bool,
    #[serde(default)]
    pub was_disputed: bool,
    #[serde(default)]
    pub new_version_q: bool,
    #[serde(default)]
    pub transaction_hash: String,
    #[serde(default)]
    pub log_index: String,
    /// Epoch seconds on question-keyed rows; RFC3339 UTC on condition-keyed
    /// rows.
    pub last_update_timestamp: String,
    pub condition_id: Option<String>,
    pub question_id: Option<String>,
    /// Per-outcome payouts in micro-USDC per share (`[outcome0, outcome1]`).
    #[serde(default)]
    pub payouts: Option<Vec<i64>>,
    /// RFC3339 UTC resolution time on resolved rows.
    #[serde(default)]
    pub resolved_at: Option<String>,
    #[serde(default)]
    pub resolved_block: Option<i64>,
    #[serde(default)]
    pub expected_settlement_time: Option<String>,
    #[serde(default)]
    pub settlement_time_basis: Option<String>,
    #[serde(default)]
    pub market_type: Option<String>,
    #[serde(default)]
    pub reporter: Option<String>,
    #[serde(default)]
    pub resolution_source: Option<String>,
}

/// A per-market open-interest row from `GET /v2/oi`.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OpenInterestRow {
    /// Condition id the row answers for; `GLOBAL` on the parameterless form.
    pub condition_id: String,
    /// Priced gross open interest in USDC (`0.0` when nothing is held).
    #[serde(deserialize_with = "deserialize_decimal_from_json")]
    pub value: Decimal,
}

/// Window form for `GET /v2/prices-history` — exactly one per request.
#[derive(Clone, Debug, PartialEq)]
pub enum PricesHistoryWindow {
    /// `start` (inclusive, epoch seconds, capped 15 days back when alone)
    /// with optional `end` (exclusive; requires `start`).
    Bounded { start: i64, end: Option<i64> },
    /// Relative window: `max` | `all` | `1m` | `1w` | `1d` | `6h` | `1h`
    /// (matched case-sensitively).
    Interval(String),
    /// Point-in-time inclusive read; cannot combine with a window.
    AsOf(i64),
}

impl PolymarketDataApiHttpClient {
    /// Fetches one page of an outcome token's sampled price history
    /// (`GET /v2/prices-history`).
    ///
    /// Returns the page and the continuation cursor (`None` on the last
    /// page). Callers walking to exhaustion must consume `next_cursor` — a
    /// short page never proves completeness; only a `null` cursor does.
    pub async fn get_prices_history_page(
        &self,
        token_id: &str,
        window: &PricesHistoryWindow,
        bucket_seconds: Option<i64>,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<DataApiPage<PricePoint>> {
        let mut params = vec![("token_id".to_string(), token_id.to_string())];
        match window {
            PricesHistoryWindow::Bounded { start, end } => {
                params.push(("start".to_string(), start.to_string()));
                if let Some(end) = end {
                    params.push(("end".to_string(), end.to_string()));
                }
            }
            PricesHistoryWindow::Interval(interval) => {
                params.push(("interval".to_string(), interval.clone()));
            }
            PricesHistoryWindow::AsOf(ts) => {
                params.push(("as_of".to_string(), ts.to_string()));
            }
        }
        if let Some(bucket) = bucket_seconds {
            params.push(("bucket_seconds".to_string(), bucket.to_string()));
        }
        if let Some(limit) = limit {
            params.push(("limit".to_string(), limit.to_string()));
        }
        if let Some(cursor) = cursor {
            params.push(("cursor".to_string(), cursor.to_string()));
        }
        let url = format!("{}{PATH_PRICES_HISTORY}", self.base_url());
        let response = self
            .http_client()
            .request_with_params(Method::GET, url, Some(&params), None, None, None, None)
            .await
            .map_err(Error::from_http_client)?;
        decode_response(&response)
    }

    /// Fetches certified resolution rows (`GET /v2/resolutions`) for up to 20
    /// condition ids.
    ///
    /// Non-paginated; a documented miss is an empty list, never an error.
    /// Rows without `payouts`/`resolved_at` are unresolved — callers keep
    /// them unknown.
    pub async fn get_resolutions(&self, condition_ids: &[&str]) -> Result<Vec<ResolutionRow>> {
        if condition_ids.is_empty() || condition_ids.len() > 20 {
            return Err(Error::decode(format!(
                "resolutions selector requires 1..=20 condition ids, got {}",
                condition_ids.len()
            )));
        }
        let params = vec![(
            "condition".to_string(),
            condition_ids.join(","),
        )];
        let url = format!("{}{PATH_RESOLUTIONS}", self.base_url());
        let response = self
            .http_client()
            .request_with_params(Method::GET, url, Some(&params), None, None, None, None)
            .await
            .map_err(Error::from_http_client)?;
        decode_response(&response).map(|page: ResolutionEnvelope| page.data)
    }

    /// Fetches priced gross open interest per market (`GET /v2/oi`) for up to
    /// 20 condition ids (omit all for the `GLOBAL` row).
    pub async fn get_open_interest(
        &self,
        condition_ids: &[&str],
    ) -> Result<Vec<OpenInterestRow>> {
        if condition_ids.len() > 20 {
            return Err(Error::decode(format!(
                "open interest selector accepts at most 20 condition ids, got {}",
                condition_ids.len()
            )));
        }
        let mut params = Vec::new();
        if !condition_ids.is_empty() {
            params.push(("condition".to_string(), condition_ids.join(",")));
        }
        let url = format!("{}{PATH_OI}", self.base_url());
        let response = self
            .http_client()
            .request_with_params(Method::GET, url, Some(&params), None, None, None, None)
            .await
            .map_err(Error::from_http_client)?;
        decode_response(&response).map(|envelope: OpenInterestEnvelope| envelope.data)
    }
}

const PATH_PRICES_HISTORY: &str = "/v2/prices-history";
const PATH_RESOLUTIONS: &str = "/v2/resolutions";
const PATH_OI: &str = "/v2/oi";

/// `{ "data": [ ...rows ] }` envelope (no pagination) for resolutions.
#[derive(Clone, Debug, Deserialize)]
struct ResolutionEnvelope {
    #[serde(default)]
    data: Vec<ResolutionRow>,
}

/// `{ "data": [ ...rows ] }` envelope (no pagination) for open interest.
#[derive(Clone, Debug, Deserialize)]
struct OpenInterestEnvelope {
    #[serde(default)]
    data: Vec<OpenInterestRow>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Offline fixtures pinning the documented wire shapes (field names and
    /// exact numeric forms). Real captured samples replace these anchors as
    /// they arrive (§11.1 external data interpretation).
    #[test]
    fn price_point_parses_documented_shape() {
        let json = r#"{"timestamp": 1735000000, "price": 0.4712, "resolution_seconds": 60}"#;
        let p: PricePoint = serde_json::from_str(json).unwrap();
        assert_eq!(p.timestamp, 1_735_000_000);
        assert_eq!(p.price, Decimal::new(4712, 4));
        assert_eq!(p.resolution_seconds, 60);
    }

    #[test]
    fn price_point_accepts_numeric_string_price() {
        let json = r#"{"timestamp": 1, "price": "0.5", "resolution_seconds": 0}"#;
        let p: PricePoint = serde_json::from_str(json).unwrap();
        assert_eq!(p.price, Decimal::new(5, 1));
        assert_eq!(p.resolution_seconds, 0, "0 = exact tick");
    }

    #[test]
    fn price_history_page_parses_envelope_with_cursor() {
        let json = r#"{
            "data": [
                {"timestamp": 100, "price": 0.25, "resolution_seconds": 0},
                {"timestamp": 160, "price": 0.75, "resolution_seconds": 60}
            ],
            "pagination": {"has_more": true, "next_cursor": "abc"}
        }"#;
        let page: DataApiPage<PricePoint> = serde_json::from_str(json).unwrap();
        assert_eq!(page.data.len(), 2);
        assert_eq!(page.data[1].price, Decimal::new(75, 2));
        assert_eq!(page.pagination.next_cursor.as_deref(), Some("abc"));
        assert!(page.pagination.has_more);
    }

    #[test]
    fn resolution_row_parses_certified_fractional_payouts() {
        let json = r#"{
            "status": "resolved",
            "extended_review": false,
            "was_disputed": false,
            "new_version_q": false,
            "transaction_hash": "0xabc",
            "log_index": "12",
            "last_update_timestamp": "2026-01-01T00:00:00Z",
            "condition_id": "0xcond",
            "payouts": [500000, 500000],
            "resolved_at": "2026-01-01T00:00:00Z",
            "resolved_block": 12345,
            "resolution_source": "derived"
        }"#;
        let r: ResolutionRow = serde_json::from_str(json).unwrap();
        assert_eq!(r.payouts.as_deref(), Some(&[500_000i64, 500_000i64][..]));
        assert_eq!(r.resolved_at.as_deref(), Some("2026-01-01T00:00:00Z"));
        assert_eq!(r.condition_id.as_deref(), Some("0xcond"));
        assert!(r.question_id.is_none(), "condition-keyed row");
    }

    #[test]
    fn unresolved_row_keeps_payouts_unknown() {
        let json = r#"{
            "status": "proposed",
            "extended_review": false,
            "was_disputed": false,
            "new_version_q": false,
            "transaction_hash": "",
            "log_index": "",
            "last_update_timestamp": "1000",
            "question_id": "0xq"
        }"#;
        let r: ResolutionRow = serde_json::from_str(json).unwrap();
        assert!(r.payouts.is_none());
        assert!(r.resolved_at.is_none());
        assert!(r.condition_id.is_none(), "question-keyed row");
    }

    #[test]
    fn open_interest_row_parses() {
        let json = r#"{ "data": [ { "condition_id": "0xcond", "value": 1234567.89 } ] }"#;
        // Production path: the envelope itself deserializes (the client decodes
        // the `{ "data": [...] }` object, not a bare array).
        let envelope: OpenInterestEnvelope = serde_json::from_str(json).unwrap();
        let rows = envelope.data;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].condition_id, "0xcond");
        assert_eq!(rows[0].value, Decimal::new(123_456_789, 2));
    }
}
