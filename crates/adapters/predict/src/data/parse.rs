//! Pure conversions from Predict wire objects into nautilus data types.
//!
//! All conversions are exact or loud: prices must land on the instrument
//! precision without rounding, and unit semantics are pinned from the
//! venue's own contract (the EIP-712 order structure maker/taker amounts:
//! ASK orders are sized in shares, BID orders in collateral).

use nautilus_core::UnixNanos;
use nautilus_model::data::{InstrumentClose, InstrumentStatus, TradeTick};
use nautilus_model::enums::{AggressorSide, InstrumentCloseType, MarketStatusAction};
use nautilus_model::identifiers::{InstrumentId, TradeId};
use nautilus_model::instruments::BinaryOption;
use nautilus_model::types::{Price, Quantity};
use rust_decimal::Decimal;

use crate::common::enums::{PredictMarketStatus, PredictQuoteType, PredictTradingStatus};
use crate::dtos::PredictMatchData;
use crate::websocket::messages::PredictWsResolvedOutcome;

fn wei() -> Decimal {
    Decimal::new(1_000_000_000_000_000_000, 0)
}

/// Builds the trade id from the full dedup identity: rows may share a
/// settlement with different transactions — the combined id stays unique
/// wherever the identity is.
pub(super) fn trade_id(settlement_id: &str, transaction_hash: &str) -> TradeId {
    let tail = &transaction_hash[transaction_hash.len().saturating_sub(8)..];
    TradeId::new(ustr::Ustr::from(format!("{settlement_id}-{tail}").as_str()))
}

/// Converts a taker quote side into the aggressor side of the trade.
#[must_use]
pub(super) fn aggressor_side(quote_type: PredictQuoteType) -> AggressorSide {
    match quote_type {
        // An Ask taker sells the outcome; a Bid taker buys it.
        PredictQuoteType::Ask => AggressorSide::Sell,
        PredictQuoteType::Bid => AggressorSide::Buy,
    }
}

/// Parses an `executedAt` ISO-8601 timestamp into unix milliseconds.
///
/// # Errors
///
/// Returns an error when the string is not a parseable RFC 3339 timestamp.
pub(super) fn executed_at_ms(executed_at: &str) -> anyhow::Result<i64> {
    let timestamp: jiff::Timestamp = executed_at
        .parse()
        .map_err(|e| anyhow::anyhow!("executedAt {executed_at:?}: {e}"))?;
    Ok(timestamp.as_millisecond())
}

/// Converts one match row into a `TradeTick` for the traded outcome's
/// instrument (the taker is the aggressor; the same economic trade never
/// double-counts — maker legs are part of this row, not extra trades).
///
/// Unit semantics: `priceExecuted` is 18-decimal wei (`10^18 = 1.0`); the
/// taker `amountFilled` rides the taker order's unit — shares for an Ask,
/// collateral for a Bid (per the venue's EIP-712 order structure, where a
/// sell order's maker amount is shares and a buy order's maker amount is
/// collateral). A Bid's collateral amount converts to shares only when the
/// division is exact at the share precision — an inexact conversion is a
/// loud gap (dropped with a warning), never a rounded publish.
///
/// # Errors
///
/// Returns an error when timestamps, prices or precision gates fail.
pub(super) fn match_to_trade_tick(
    row: &PredictMatchData,
    instrument: &BinaryOption,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<TradeTick>> {
    let price_decimal = row.price_executed / wei();
    let size_decimal = match row.taker.quote_type {
        PredictQuoteType::Ask => row.amount_filled / wei(),
        PredictQuoteType::Bid => {
            let collateral = row.amount_filled / wei();
            if price_decimal.is_zero() {
                anyhow::bail!("match {} executed at a zero price", row.settlement_id);
            }
            let shares = collateral / price_decimal;
            // Exactness gate at the venue's share precision (3 decimals).
            let scale = Decimal::from(1_000u32);
            if (shares * scale).fract() != Decimal::ZERO {
                log::warn!(
                    "match {} collateral {collateral} does not divide exactly at price {price_decimal}; trade dropped until units are live-confirmed",
                    row.settlement_id
                );
                return Ok(None);
            }
            shares
        }
    };

    let ts_event = crate::common::parse::unix_ms_to_nanos(executed_at_ms(&row.executed_at)?);
    Ok(Some(TradeTick::new(
        instrument.id,
        Price::from_decimal_dp(price_decimal, instrument.price_precision)?,
        Quantity::from_decimal_dp(size_decimal, instrument.size_precision)?,
        aggressor_side(row.taker.quote_type),
        trade_id(&row.settlement_id, &row.transaction_hash),
        ts_event,
        ts_init,
    )))
}

/// Maps a trading-status value onto the shared status action vocabulary.
///
/// `MATCHING_NOT_ENABLED` maps to quoting-but-not-trading; `CANCEL_ONLY`
/// to a pause with its reason. These two middle states are best-fit
/// mappings of the venue vocabulary, not documented nautilus semantics.
#[must_use]
pub(super) fn trading_status_to_action(status: PredictTradingStatus) -> (MarketStatusAction, Option<&'static str>) {
    match status {
        PredictTradingStatus::Open => (MarketStatusAction::Trading, None),
        PredictTradingStatus::MatchingNotEnabled => (MarketStatusAction::Quoting, Some("MATCHING_NOT_ENABLED")),
        PredictTradingStatus::CancelOnly => (MarketStatusAction::Pause, Some("CANCEL_ONLY")),
        PredictTradingStatus::Closed => (MarketStatusAction::Close, Some("CLOSED")),
    }
}

/// Builds one `InstrumentStatus` for a trading-status transition.
#[must_use]
pub(super) fn trading_status_event(
    instrument_id: InstrumentId,
    status: PredictTradingStatus,
    ts_ms: i64,
    ts_init: UnixNanos,
) -> InstrumentStatus {
    let (action, reason) = trading_status_to_action(status);
    let is_trading = action == MarketStatusAction::Trading;
    InstrumentStatus::new(
        instrument_id,
        action,
        crate::common::parse::unix_ms_to_nanos(ts_ms),
        ts_init,
        reason.map(ustr::Ustr::from),
        None,
        Some(is_trading),
        None,
        None,
    )
}

/// Builds one `InstrumentStatus` for a market-lifecycle transition.
#[must_use]
pub(super) fn market_status_event(
    instrument_id: InstrumentId,
    status: PredictMarketStatus,
    ts_ms: i64,
    ts_init: UnixNanos,
) -> Option<InstrumentStatus> {
    let (action, reason) = match status {
        PredictMarketStatus::Paused => (MarketStatusAction::Pause, Some("PAUSED")),
        PredictMarketStatus::Unpaused => (MarketStatusAction::Trading, Some("UNPAUSED")),
        PredictMarketStatus::PriceDisputed => (MarketStatusAction::Pause, Some("PRICE_DISPUTED")),
        PredictMarketStatus::Resolved => (MarketStatusAction::Close, Some("RESOLVED")),
        PredictMarketStatus::Registered
        | PredictMarketStatus::PriceProposed
        | PredictMarketStatus::Removed => return None,
    };
    let is_trading = action == MarketStatusAction::Trading;
    Some(InstrumentStatus::new(
        instrument_id,
        action,
        crate::common::parse::unix_ms_to_nanos(ts_ms),
        ts_init,
        reason.map(ustr::Ustr::from),
        None,
        Some(is_trading),
        None,
        None,
    ))
}

/// Builds the `InstrumentClose` for one resolved outcome: the final price
/// is the payout (wei ÷ 10^18 — the documented enum is exactly 0 / 0.5 /
/// 1), never inferred from last trades or chance series.
///
/// # Errors
///
/// Returns an error when the payout does not land exactly on the
/// instrument precision.
pub(super) fn resolved_outcome_close(
    instrument_id: InstrumentId,
    outcome: &PredictWsResolvedOutcome,
    price_precision: u8,
    ts_ms: i64,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentClose> {
    let payout = outcome.payout();
    anyhow::ensure!(
        payout >= Decimal::ZERO && payout <= Decimal::ONE,
        "resolved outcome {} payout {payout} outside [0,1]",
        outcome.on_chain_id
    );
    Ok(InstrumentClose::new(
        instrument_id,
        Price::from_decimal_dp(payout, price_precision)?,
        InstrumentCloseType::ContractExpired,
        crate::common::parse::unix_ms_to_nanos(ts_ms),
        ts_init,
    ))
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;
    use rust_decimal::Decimal;

    use super::*;

    fn instrument() -> BinaryOption {
        crate::providers::parse_instrument(
            &serde_json::from_str(r#"{
                "id": 42, "imageUrl": "i", "title": "T", "question": "Q?", "description": "d",
                "tradingStatus": "OPEN", "status": "REGISTERED", "isVisible": true,
                "isNegRisk": false, "isYieldBearing": false, "feeRateBps": 0, "makerFeeBps": 0,
                "oracleQuestionId": "o", "conditionId": "0xcond", "resolverAddress": "r",
                "outcomes": [{"name": "Yes", "indexSet": 1, "onChainId": "1"}],
                "spreadThreshold": 0.01, "shareThreshold": 1, "isBoosted": false,
                "polymarketConditionIds": [], "categorySlug": "c",
                "createdAt": "2026-10-01T00:00:00Z", "decimalPrecision": 3, "marketVariant": "DEFAULT"
            }"#).unwrap(),
            &serde_json::from_str(r#"{"name": "Yes", "indexSet": 1, "onChainId": "1"}"#).unwrap(),
            UnixNanos::default(),
        )
        .unwrap()
    }

    fn row(quote_type: PredictQuoteType, amount_wei: &str, price_wei: &str) -> PredictMatchData {
        let market_json = r#"{
            "id": 42, "imageUrl": "i", "title": "T", "question": "Q?", "description": "d",
            "tradingStatus": "OPEN", "status": "REGISTERED", "isVisible": true,
            "isNegRisk": false, "isYieldBearing": false, "feeRateBps": 0, "makerFeeBps": 0,
            "oracleQuestionId": "o", "conditionId": "0xcond", "resolverAddress": "r",
            "outcomes": [{"name": "Yes", "indexSet": 1, "onChainId": "1"}],
            "spreadThreshold": 0.01, "shareThreshold": 1, "isBoosted": false,
            "polymarketConditionIds": [], "categorySlug": "c",
            "createdAt": "2026-10-01T00:00:00Z", "decimalPrecision": 3, "marketVariant": "DEFAULT"
        }"#;
        let quote = match quote_type {
            PredictQuoteType::Ask => "Ask",
            PredictQuoteType::Bid => "Bid",
        };
        serde_json::from_str(&format!(
            r#"{{
                "market": {market_json},
                "taker": {{
                    "quoteType": "{quote}",
                    "amount": "{amount_wei}",
                    "price": "{price_wei}",
                    "outcome": {{"name": "Yes", "indexSet": 1, "onChainId": "1"}},
                    "signer": "0xabc", "hash": "0xorder",
                    "fee": {{"amount": "0", "type": "COLLATERAL"}}
                }},
                "makers": [],
                "amountFilled": "{amount_wei}",
                "priceExecuted": "{price_wei}",
                "transactionHash": "0xtx",
                "settlementId": "0xsettle",
                "executedAt": "2026-11-01T00:00:30Z"
            }}"#
        ))
        .unwrap()
    }

    #[test]
    fn ask_amount_is_shares_verbatim() {
        // 1.0 share at 0.5: amountFilled 10^18 wei, price 0.5×10^18.
        let tick = match_to_trade_tick(
            &row(PredictQuoteType::Ask, "1000000000000000000", "500000000000000000"),
            &instrument(),
            UnixNanos::default(),
        )
        .unwrap()
        .expect("exact ask converts");
        assert_eq!(tick.price.to_string(), "0.500");
        assert_eq!(tick.size.to_string(), "1.000");
        assert_eq!(tick.aggressor_side, nautilus_model::enums::AggressorSide::Sell);
        assert_eq!(tick.trade_id.to_string(), "0xsettle-0xtx");
        assert_eq!(tick.ts_event.as_u64(), 1_793_491_230_000_000_000);
    }

    #[test]
    fn bid_collateral_converts_exactly_or_drops_loudly() {
        // BUY 0.5 USDT at 0.5 → exactly 1 share.
        let tick = match_to_trade_tick(
            &row(PredictQuoteType::Bid, "500000000000000000", "500000000000000000"),
            &instrument(),
            UnixNanos::default(),
        )
        .unwrap()
        .expect("exact division converts");
        assert_eq!(tick.size.to_string(), "1.000");
        assert_eq!(tick.aggressor_side, nautilus_model::enums::AggressorSide::Buy);

        // BUY 1 USDT at 0.3 → 3.333... shares: inexact → dropped, not rounded.
        let dropped = match_to_trade_tick(
            &row(PredictQuoteType::Bid, "1000000000000000000", "300000000000000000"),
            &instrument(),
            UnixNanos::default(),
        )
        .unwrap();
        assert!(dropped.is_none());
    }

    #[test]
    fn trading_status_maps_to_actions() {
        let status = trading_status_event(
            InstrumentId::from("0xcond-1.PREDICT"),
            PredictTradingStatus::CancelOnly,
            1_738_000_000_000,
            UnixNanos::default(),
        );
        assert_eq!(status.action, MarketStatusAction::Pause);
        assert_eq!(status.reason, Some(ustr::Ustr::from("CANCEL_ONLY")));
        assert_eq!(status.is_trading, Some(false));

        let open = trading_status_event(
            InstrumentId::from("0xcond-1.PREDICT"),
            PredictTradingStatus::Open,
            0,
            UnixNanos::default(),
        );
        assert_eq!(open.action, MarketStatusAction::Trading);
        assert_eq!(open.is_trading, Some(true));
    }

    #[test]
    fn market_lifecycle_maps_only_trading_relevant_states() {
        let id = InstrumentId::from("0xcond-1.PREDICT");
        assert!(market_status_event(id, PredictMarketStatus::Registered, 1, UnixNanos::default()).is_none());
        assert!(market_status_event(id, PredictMarketStatus::PriceProposed, 1, UnixNanos::default()).is_none());
        let paused = market_status_event(id, PredictMarketStatus::Paused, 1, UnixNanos::default()).unwrap();
        assert_eq!(paused.action, MarketStatusAction::Pause);
    }

    #[test]
    fn resolved_outcome_close_carries_exact_payout() {
        let outcome = PredictWsResolvedOutcome {
            label: "Yes".to_string(),
            index_set: 1,
            price: Decimal::from_i128_with_scale(500_000_000_000_000_000, 0),
            on_chain_id: "1".to_string(),
        };
        let close = resolved_outcome_close(
            InstrumentId::from("0xcond-1.PREDICT"),
            &outcome,
            3,
            1_738_000_000_000,
            UnixNanos::default(),
        )
        .unwrap();
        assert_eq!(close.close_price.to_string(), "0.500");
        assert_eq!(close.instrument_id.to_string(), "0xcond-1.PREDICT");
    }
}
