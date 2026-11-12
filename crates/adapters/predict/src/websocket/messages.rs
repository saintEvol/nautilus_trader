//! Wire types for the Predict WebSocket API (official AsyncAPI
//! `websocket.json`, 2026-11-12).
//!
//! Protocol shape: the client sends requests (`subscribe` / `unsubscribe`
//! with a self-chosen `requestId`, or a `heartbeat` echo); the server
//! answers each request with a response carrying the same `requestId`, and
//! everything else arrives as a topic message. Every 15 seconds the server
//! sends a `heartbeat` topic message whose `data` must be echoed back
//! verbatim before the next one, or the connection is closed.
//!
//! Reconnects do not replay messages and do not carry subscriptions over:
//! the client must resubscribe every topic and refetch REST state.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::common::consts::{
    TOPIC_CATEGORY_CHANGED, TOPIC_HEARTBEAT, TOPIC_MARKET_CHANGED, TOPIC_MARKET_STATUS,
    TOPIC_ORDERBOOK, TOPIC_SYSTEM, TOPIC_TOP_OF_BOOK, TOPIC_TRADING_STATUS, TOPIC_WALLET_EVENTS,
};
use crate::common::enums::{
    PredictMarketStatus, PredictOrderKind, PredictOutcomeSide, PredictQuoteType,
    PredictTradingStatus,
};

/// A client → server request.
#[derive(Clone, Debug, Serialize)]
pub struct PredictWsRequest {
    pub method: &'static str,
    #[serde(rename = "requestId")]
    pub request_id: i64,
    pub params: Vec<String>,
}

impl PredictWsRequest {
    /// Builds a subscribe request for exactly one topic (the protocol reads
    /// only the first entry).
    #[must_use]
    pub fn subscribe(request_id: i64, topic: &str) -> Self {
        Self {
            method: "subscribe",
            request_id,
            params: vec![topic.to_string()],
        }
    }

    /// Builds an unsubscribe request for exactly one topic.
    #[must_use]
    pub fn unsubscribe(request_id: i64, topic: &str) -> Self {
        Self {
            method: "unsubscribe",
            request_id,
            params: vec![topic.to_string()],
        }
    }
}

/// The heartbeat echo a client must send back unchanged.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct PredictHeartbeatReply {
    pub method: &'static str,
    pub data: i64,
}

impl PredictHeartbeatReply {
    #[must_use]
    pub const fn new(server_time_ms: i64) -> Self {
        Self {
            method: "heartbeat",
            data: server_time_ms,
        }
    }
}

/// The error body on a failed request response.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictWsErrorBody {
    pub code: String,
    #[serde(default)]
    pub message: Option<String>,
}

/// A `[price, shares]` level inside a WS order book frame (JSON numbers on
/// the Yes axis).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
pub struct PredictWsLevel(
    /// Yes price in `[0, 1]`.
    pub Decimal,
    /// Size in shares.
    pub Decimal,
);

/// The full order book frame (`predictOrderbook/{marketId}`): every message
/// is the whole book — never an incremental delta.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictWsOrderbook {
    pub version: i64,
    #[serde(rename = "marketId")]
    pub market_id: i64,
    #[serde(rename = "updateTimestampMs")]
    pub update_timestamp_ms: i64,
    #[serde(rename = "orderCount")]
    pub order_count: i64,
    /// Lowest ask first.
    pub asks: Vec<PredictWsLevel>,
    /// Highest bid first.
    pub bids: Vec<PredictWsLevel>,
    #[serde(rename = "lastOrderSettled")]
    pub last_order_settled: Option<PredictWsSettledOrder>,
    /// In-flight settlements per price level (`[price, usdt_value]`, Yes
    /// axis) — already-matched liquidity, kept for observability only.
    #[serde(rename = "settlementsPending")]
    pub settlements_pending: PredictWsPendingSettlements,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PredictWsPendingSettlements {
    #[serde(default)]
    pub asks: Vec<[Decimal; 2]>,
    #[serde(default)]
    pub bids: Vec<[Decimal; 2]>,
}

/// A settled-order summary carried on book frames and top-of-book updates.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictWsSettledOrder {
    pub id: String,
    /// Yes price with 2 decimals.
    pub price: String,
    pub kind: PredictOrderKind,
    #[serde(rename = "marketId")]
    pub market_id: i64,
    pub side: PredictQuoteType,
    pub outcome: PredictOutcomeSide,
}

/// Top-of-book update (`predictTopOfBook/{marketId}`): sent only when
/// something at the top changes.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictWsTopOfBook {
    #[serde(rename = "marketId")]
    pub market_id: i64,
    #[serde(rename = "bestBid")]
    pub best_bid: Option<PredictWsLevel>,
    #[serde(rename = "bestAsk")]
    pub best_ask: Option<PredictWsLevel>,
    #[serde(rename = "lastOrderSettled")]
    pub last_order_settled: Option<PredictWsSettledOrder>,
}

/// Trading-status update (`predictTradingStatus/{marketId}`).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictWsTradingStatus {
    #[serde(rename = "tsMs")]
    pub ts_ms: i64,
    #[serde(rename = "marketId")]
    pub market_id: i64,
    #[serde(rename = "tradingStatus")]
    pub trading_status: PredictTradingStatus,
}

/// One resolved outcome of a market-status update: the final price in wei
/// where `10^18` is 1. The documented enum is exactly `0`, `0.5`, `1`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictWsResolvedOutcome {
    pub label: String,
    #[serde(rename = "indexSet")]
    pub index_set: i64,
    pub price: Decimal,
    #[serde(rename = "onChainId")]
    pub on_chain_id: String,
}

impl PredictWsResolvedOutcome {
    /// The payout as an exact decimal in `[0, 1]` (wei ÷ 10^18).
    #[must_use]
    pub fn payout(&self) -> Decimal {
        self.price / Decimal::new(1_000_000_000_000_000_000, 0)
    }
}

/// Market-status update (`predictMarketStatus/{marketId}`); `RESOLVED`
/// carries the final outcome prices.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictWsMarketStatus {
    #[serde(rename = "tsMs")]
    pub ts_ms: i64,
    #[serde(rename = "marketId")]
    pub market_id: i64,
    pub status: PredictMarketStatus,
    #[serde(default, rename = "marketOutcomes")]
    pub market_outcomes: Vec<PredictWsResolvedOutcome>,
}

/// Market-changed notice (`predictMarketChanged/{marketId}`): refetch the
/// market (or apply `patch` — patches are variant-specific and are not
/// consumed here; the refetch path is always valid).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictWsMarketChanged {
    #[serde(rename = "tsMs")]
    pub ts_ms: i64,
    #[serde(rename = "marketId")]
    pub market_id: i64,
    #[serde(rename = "patchKind")]
    pub patch_kind: Option<String>,
}

/// Reconnect notice (arrives unasked on the `system` topic before the
/// server shuts down).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictWsReconnectNotice {
    pub event: String,
    pub reason: String,
    #[serde(rename = "graceMs")]
    pub grace_ms: i64,
}

/// Every server → client frame, decoded.
#[derive(Clone, Debug)]
pub enum PredictWsMessage {
    /// The answer to a subscribe/unsubscribe request.
    Response {
        request_id: i64,
        success: bool,
        error: Option<PredictWsErrorBody>,
    },
    /// Server heartbeat: echo it back before the next one.
    Heartbeat { server_time_ms: i64 },
    /// The server is draining: reconnect and resubscribe within `grace_ms`.
    ReconnectNotice(PredictWsReconnectNotice),
    /// A complete order book frame.
    Orderbook(PredictWsOrderbook),
    TopOfBook(PredictWsTopOfBook),
    TradingStatus(PredictWsTradingStatus),
    MarketStatus(PredictWsMarketStatus),
    MarketChanged(PredictWsMarketChanged),
    /// A recognized-but-unconsumed topic (wallet events are P5 execution
    /// work) or an unknown topic — surfaced, never silently dropped.
    Other { topic: String, data: Value },
}

/// Builds the wire topic string for a market id.
#[must_use]
pub fn market_topic(prefix: &str, market_id: i64) -> String {
    format!("{prefix}/{market_id}")
}

/// Splits a topic string into `(prefix, parameter)`.
///
/// # Errors
///
/// Returns an error for topics without a `/` separator.
pub fn split_topic(topic: &str) -> Result<(&str, &str), PredictWsDecodeError> {
    let (prefix, param) = topic
        .split_once('/')
        .ok_or_else(|| PredictWsDecodeError::Protocol(format!("topic {topic:?} has no /")))?;
    Ok((prefix, param))
}

/// Decodes one server frame.
///
/// # Errors
///
/// Returns a [`PredictWsDecodeError`] when the frame is not valid protocol
/// JSON or a known topic payload fails its schema.
pub fn decode_frame(bytes: &[u8]) -> Result<PredictWsMessage, PredictWsDecodeError> {
    #[derive(Deserialize)]
    struct Wire {
        #[serde(rename = "type")]
        kind: String,
        #[serde(default, rename = "requestId")]
        request_id: Option<i64>,
        #[serde(default)]
        success: Option<bool>,
        #[serde(default)]
        error: Option<PredictWsErrorBody>,
        #[serde(default)]
        topic: Option<String>,
        #[serde(default)]
        data: Option<Value>,
    }
    let wire: Wire = serde_json::from_slice(bytes)
        .map_err(|e| PredictWsDecodeError::Parse(format!("frame is not protocol JSON: {e}")))?;
    match wire.kind.as_str() {
        "R" => Ok(PredictWsMessage::Response {
            request_id: wire.request_id.unwrap_or(-1),
            success: wire.success.unwrap_or(false),
            error: wire.error,
        }),
        "M" => {
            let topic = wire
                .topic
                .ok_or_else(|| PredictWsDecodeError::Protocol("topic message without topic".into()))?;
            let data = wire.data.unwrap_or(Value::Null);
            decode_topic_message(&topic, data)
        }
        other => Err(PredictWsDecodeError::Protocol(format!(
            "unknown frame type {other:?}"
        ))),
    }
}

fn decode_topic_message(topic: &str, data: Value) -> Result<PredictWsMessage, PredictWsDecodeError> {
    // Server-reserved topics carry no parameter.
    match topic {
        TOPIC_HEARTBEAT => {
            let server_time_ms = data
                .as_i64()
                .ok_or_else(|| PredictWsDecodeError::Parse("heartbeat data is not an integer".into()))?;
            return Ok(PredictWsMessage::Heartbeat { server_time_ms });
        }
        TOPIC_SYSTEM => {
            let notice: PredictWsReconnectNotice = serde_json::from_value(data)
                .map_err(|e| PredictWsDecodeError::Parse(format!("reconnect notice: {e}")))?;
            return Ok(PredictWsMessage::ReconnectNotice(notice));
        }
        _ => {}
    }
    let (prefix, _param) = split_topic(topic)?;
    match prefix {
        TOPIC_ORDERBOOK => {
            let book: PredictWsOrderbook = serde_json::from_value(data)
                .map_err(|e| PredictWsDecodeError::Parse(format!("orderbook frame: {e}")))?;
            Ok(PredictWsMessage::Orderbook(book))
        }
        TOPIC_TOP_OF_BOOK => {
            let top: PredictWsTopOfBook = serde_json::from_value(data)
                .map_err(|e| PredictWsDecodeError::Parse(format!("top-of-book frame: {e}")))?;
            Ok(PredictWsMessage::TopOfBook(top))
        }
        TOPIC_TRADING_STATUS => {
            let status: PredictWsTradingStatus = serde_json::from_value(data)
                .map_err(|e| PredictWsDecodeError::Parse(format!("trading status: {e}")))?;
            Ok(PredictWsMessage::TradingStatus(status))
        }
        TOPIC_MARKET_STATUS => {
            let status: PredictWsMarketStatus = serde_json::from_value(data)
                .map_err(|e| PredictWsDecodeError::Parse(format!("market status: {e}")))?;
            Ok(PredictWsMessage::MarketStatus(status))
        }
        TOPIC_MARKET_CHANGED => {
            let changed: PredictWsMarketChanged = serde_json::from_value(data)
                .map_err(|e| PredictWsDecodeError::Parse(format!("market changed: {e}")))?;
            Ok(PredictWsMessage::MarketChanged(changed))
        }
        TOPIC_CATEGORY_CHANGED | TOPIC_WALLET_EVENTS => Ok(PredictWsMessage::Other {
            topic: topic.to_string(),
            data,
        }),
        _ => Ok(PredictWsMessage::Other {
            topic: topic.to_string(),
            data,
        }),
    }
}

/// Decode failure for one WS frame.
#[derive(Debug, thiserror::Error)]
pub enum PredictWsDecodeError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("protocol error: {0}")]
    Protocol(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_request_serializes() {
        let request = PredictWsRequest::subscribe(7, "predictOrderbook/123");
        assert_eq!(
            serde_json::to_string(&request).unwrap(),
            r#"{"method":"subscribe","requestId":7,"params":["predictOrderbook/123"]}"#
        );
        let request = PredictWsRequest::unsubscribe(8, "predictTopOfBook/1");
        assert_eq!(
            serde_json::to_string(&request).unwrap(),
            r#"{"method":"unsubscribe","requestId":8,"params":["predictTopOfBook/1"]}"#
        );
    }

    #[test]
    fn heartbeat_echo_serializes_server_value_verbatim() {
        let reply = PredictHeartbeatReply::new(1_738_000_000_000);
        assert_eq!(
            serde_json::to_string(&reply).unwrap(),
            r#"{"method":"heartbeat","data":1738000000000}"#
        );
    }

    #[test]
    fn response_frame_decodes() {
        let message = decode_frame(
            br#"{"type":"R","requestId":7,"success":true}"#,
        )
        .unwrap();
        match message {
            PredictWsMessage::Response {
                request_id,
                success,
                error,
            } => {
                assert_eq!(request_id, 7);
                assert!(success);
                assert!(error.is_none());
            }
            other => panic!("expected Response, got {other:?}"),
        }
        let failure = decode_frame(
            br#"{"type":"R","requestId":-1,"success":false,"error":{"code":"invalid_json"}}"#,
        )
        .unwrap();
        match failure {
            PredictWsMessage::Response { error, .. } => {
                assert_eq!(error.expect("error body").code, "invalid_json");
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[test]
    fn orderbook_frame_decodes() {
        let message = decode_frame(
            br#"{"type":"M","topic":"predictOrderbook/123","data":{
                "version":1,
                "marketId":123,
                "updateTimestampMs":1727910141000,
                "orderCount":42,
                "asks":[[0.492,30192.26]],
                "bids":[[0.491,303518.1]],
                "lastOrderSettled":null,
                "settlementsPending":{"asks":[[0.49,100.5]],"bids":[]}
            }}"#,
        )
        .unwrap();
        match message {
            PredictWsMessage::Orderbook(book) => {
                assert_eq!(book.market_id, 123);
                assert_eq!(book.update_timestamp_ms, 1_727_910_141_000);
                assert_eq!(book.asks[0].0, Decimal::new(492, 3));
                assert_eq!(book.settlements_pending.asks[0][1], Decimal::new(1_005, 1));
                assert!(book.last_order_settled.is_none());
            }
            other => panic!("expected Orderbook, got {other:?}"),
        }
    }

    #[test]
    fn heartbeat_and_reconnect_notice_decode() {
        let message = decode_frame(
            br#"{"type":"M","topic":"heartbeat","data":1738000000000}"#,
        )
        .unwrap();
        match message {
            PredictWsMessage::Heartbeat { server_time_ms } => assert_eq!(server_time_ms, 1_738_000_000_000),
            other => panic!("expected Heartbeat, got {other:?}"),
        }
        let notice = decode_frame(
            br#"{"type":"M","topic":"system","data":{"event":"reconnect","reason":"server_shutdown","graceMs":5000}}"#,
        )
        .unwrap();
        match notice {
            PredictWsMessage::ReconnectNotice(n) => {
                assert_eq!(n.grace_ms, 5_000);
                assert_eq!(n.reason, "server_shutdown");
            }
            other => panic!("expected ReconnectNotice, got {other:?}"),
        }
    }

    #[test]
    fn market_status_resolved_carries_wei_payouts() {
        let message = decode_frame(
            br#"{"type":"M","topic":"predictMarketStatus/9","data":{
                "kind":"marketStatusUpdate",
                "tsMs":1738000000000,
                "marketId":9,
                "status":"RESOLVED",
                "marketOutcomes":[
                    {"label":"Yes","indexSet":1,"price":"500000000000000000","onChainId":"12345"},
                    {"label":"No","indexSet":2,"price":"500000000000000000","onChainId":"67890"}
                ]
            }}"#,
        )
        .unwrap();
        match message {
            PredictWsMessage::MarketStatus(status) => {
                assert_eq!(status.status, PredictMarketStatus::Resolved);
                assert_eq!(status.market_outcomes.len(), 2);
                assert_eq!(status.market_outcomes[0].payout(), Decimal::new(5, 1));
                assert_eq!(status.market_outcomes[0].on_chain_id, "12345");
            }
            other => panic!("expected MarketStatus, got {other:?}"),
        }
    }

    #[test]
    fn trading_status_decodes() {
        let message = decode_frame(
            br#"{"type":"M","topic":"predictTradingStatus/9","data":{
                "kind":"tradingStatusUpdate","tsMs":1738000000000,"marketId":9,
                "tradingStatus":"CANCEL_ONLY"
            }}"#,
        )
        .unwrap();
        match message {
            PredictWsMessage::TradingStatus(status) => {
                assert_eq!(status.trading_status, PredictTradingStatus::CancelOnly);
            }
            other => panic!("expected TradingStatus, got {other:?}"),
        }
    }

    #[test]
    fn unknown_topic_is_surfaced_not_dropped() {
        let message = decode_frame(
            br#"{"type":"M","topic":"predictWalletEvents/eyJhbGciOi","data":{"type":"orderAccepted"}}"#,
        )
        .unwrap();
        match message {
            PredictWsMessage::Other { topic, .. } => {
                assert!(topic.starts_with("predictWalletEvents/"));
            }
            other => panic!("expected Other, got {other:?}"),
        }
    }

    #[test]
    fn topic_helpers() {
        assert_eq!(market_topic(TOPIC_ORDERBOOK, 5), "predictOrderbook/5");
        let (prefix, param) = split_topic("predictOrderbook/123").unwrap();
        assert_eq!(prefix, TOPIC_ORDERBOOK);
        assert_eq!(param, "123");
        assert!(split_topic("no-separator").is_err());
    }
}
