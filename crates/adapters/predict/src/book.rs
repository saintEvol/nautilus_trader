//! Full-frame Yes→No dual projection for Predict order books (§5.3).
//!
//! The venue streams only the Yes-axis book (the outcome whose `indexSet`
//! is 1). One frame produces TWO `OrderBookDeltas` batches — the Yes view
//! verbatim and the No complement (`No bid = 1 − Yes ask`, sizes
//! unchanged, computed exactly at the market's precision via the model's
//! `project_no_book`) — sharing the frame's timestamps: the projections of
//! one published frame must never be mistaken for two observations.
//!
//! Both batches use the CLEAR + ADD(F_SNAPSHOT) + F_LAST shape: a whole
//! frame replaces the book, is never an incremental delta, and the venue
//! has no sequence numbers (sequence stays 0 — a local batch identity at
//! most, never a pretended venue sequence).

use nautilus_core::UnixNanos;
use rust_decimal::Decimal;
use nautilus_model::data::{BookOrder, OrderBookDelta, OrderBookDeltas, QuoteTick};
use nautilus_model::enums::{BookAction, OrderSide, RecordFlag};
use nautilus_model::instruments::BinaryOption;
use nautilus_model::types::{Price, Quantity};
use nautilus_model::prediction::{BookLevel, project_no_book};

/// One projected frame: both instrument views + derived top-of-book quotes.
#[derive(Debug, Clone)]
pub struct ProjectedBook {
    /// The Yes-axis (pricing instrument) batch.
    pub yes: OrderBookDeltas,
    /// The No (complement) batch sharing the frame identity.
    pub no: OrderBookDeltas,
    /// Top-of-book quote for the Yes leg (absent when a side is empty —
    /// never a synthetic zero-price quote).
    pub yes_quote: Option<QuoteTick>,
    /// Top-of-book quote for the No leg.
    pub no_quote: Option<QuoteTick>,
}

/// Projects one full Yes-axis frame into the two instrument views.
///
/// # Errors
///
/// Returns an error when a price cannot be represented exactly at the
/// instrument precision, the Yes frame is internally inconsistent, or the
/// No complement is inexact at the market precision (a rounded complement
/// misprices — loud rejection, no rounding).
pub fn project_book_frame(
    yes_instrument: &BinaryOption,
    no_instrument: &BinaryOption,
    yes_bids: &[BookLevel],
    yes_asks: &[BookLevel],
    update_ts_ms: i64,
    ts_init: UnixNanos,
) -> anyhow::Result<ProjectedBook> {
    let precision = yes_instrument.price_precision;
    let ts_event = crate::common::parse::unix_ms_to_nanos(update_ts_ms);

    let yes = snapshot_batch(yes_instrument, yes_bids, yes_asks, ts_event, ts_init)?;
    let (no_bids, no_asks) = project_no_book(yes_bids, yes_asks, u32::from(precision))
        .map_err(|e| anyhow::anyhow!("no-side projection of market {}: {e}", yes_instrument.id))?;
    let no = snapshot_batch(no_instrument, &no_bids, &no_asks, ts_event, ts_init)?;

    Ok(ProjectedBook {
        yes,
        no,
        yes_quote: derive_quote(yes_instrument, yes_bids, yes_asks, ts_event, ts_init)?,
        no_quote: derive_quote(no_instrument, &no_bids, &no_asks, ts_event, ts_init)?,
    })
}

/// Builds one whole-frame batch: CLEAR anchor, one ADD per level
/// (F_SNAPSHOT), F_LAST on the final delta. Sequence 0 — the venue sends
/// none and 0 must never be read as a venue sequence.
fn snapshot_batch(
    instrument: &BinaryOption,
    bids: &[BookLevel],
    asks: &[BookLevel],
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
    let mut deltas = Vec::with_capacity(1 + bids.len() + asks.len());
    deltas.push(OrderBookDelta::clear(
        instrument.id,
        0,
        ts_event,
        ts_init,
    ));
    let scale = 10u64
        .checked_pow(u32::from(instrument.size_precision))
        .map_or(Decimal::MAX, Decimal::from);
    let mut push_level = |side: OrderSide, level: &BookLevel, flags: u8| -> anyhow::Result<()> {
        // Sizes carry verbatim — an inexact fit at the share precision is a
        // loud rejection, never a rounded publish.
        anyhow::ensure!(
            (level.1 * scale).fract() == Decimal::ZERO,
            "{} size {} is not exact at precision {}",
            instrument.id,
            level.1,
            instrument.size_precision
        );
        let price = Price::from_decimal_dp(level.0, instrument.price_precision)?;
        let size = Quantity::from_decimal_dp(level.1, instrument.size_precision)?;
        deltas.push(OrderBookDelta::new(
            instrument.id,
            BookAction::Add,
            BookOrder::new(side, price, size, 0),
            flags,
            0,
            ts_event,
            ts_init,
        ));
        Ok(())
    };
    for level in bids {
        push_level(OrderSide::Buy, level, RecordFlag::F_SNAPSHOT as u8)?;
    }
    for level in asks {
        push_level(OrderSide::Sell, level, RecordFlag::F_SNAPSHOT as u8)?;
    }
    let flags = RecordFlag::F_SNAPSHOT as u8 | RecordFlag::F_LAST as u8;
    if let Some(delta) = deltas.last_mut() {
        delta.flags |= flags;
    }
    Ok(OrderBookDeltas::new(instrument.id, deltas))
}

/// Derives the top-of-book quote from a full level list (best bid is the
/// max price, best ask the min — inputs arrive best-first but ordering is
/// not trusted).
fn derive_quote(
    instrument: &BinaryOption,
    bids: &[BookLevel],
    asks: &[BookLevel],
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<QuoteTick>> {
    let Some((bid_price, bid_size)) = bids.iter().max_by_key(|level| level.0) else {
        return Ok(None);
    };
    let Some((ask_price, ask_size)) = asks.iter().min_by_key(|level| level.0) else {
        return Ok(None);
    };
    anyhow::ensure!(
        bid_price < ask_price,
        "crossed or locked top of book for {}: bid {bid_price} ask {ask_price}",
        instrument.id
    );
    Ok(Some(QuoteTick::new(
        instrument.id,
        Price::from_decimal_dp(*bid_price, instrument.price_precision)?,
        Price::from_decimal_dp(*ask_price, instrument.price_precision)?,
        Quantity::from_decimal_dp(*bid_size, instrument.size_precision)?,
        Quantity::from_decimal_dp(*ask_size, instrument.size_precision)?,
        ts_event,
        ts_init,
    )))
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;
    use rust_decimal::Decimal;

    use super::*;

    fn instrument(on_chain_id: &str) -> BinaryOption {
        let market: crate::http::models::PredictMarketDto = serde_json::from_str(
            r#"{
                "id": 42, "imageUrl": "i", "title": "T", "question": "Q?", "description": "d",
                "tradingStatus": "OPEN", "status": "REGISTERED", "isVisible": true,
                "isNegRisk": false, "isYieldBearing": false, "feeRateBps": 0, "makerFeeBps": 0,
                "oracleQuestionId": "o", "conditionId": "0xcond", "resolverAddress": "r",
                "outcomes": [{"name": "X", "indexSet": 1, "onChainId": "1"}],
                "spreadThreshold": 0.01, "shareThreshold": 1, "isBoosted": false,
                "polymarketConditionIds": [], "categorySlug": "c",
                "createdAt": "2026-10-01T00:00:00Z", "decimalPrecision": 3, "marketVariant": "DEFAULT"
            }"#,
        )
        .unwrap();
        let outcome = crate::http::models::PredictOutcomeDto {
            name: "X".to_string(),
            index_set: 1,
            on_chain_id: on_chain_id.to_string(),
            status: None,
        };
        crate::providers::parse_instrument(&market, &outcome, UnixNanos::default()).unwrap()
    }

    fn level(price: &str, size: &str) -> BookLevel {
        (Decimal::from_str_exact(price).unwrap(), Decimal::from_str_exact(size).unwrap())
    }

    #[test]
    fn frame_projects_two_complete_batches() {
        let yes = instrument("1");
        let no = instrument("2");
        let bids = [level("0.491", "100"), level("0.49", "50")];
        let asks = [level("0.492", "80")];

        let projected = project_book_frame(&yes, &no, &bids, &asks, 1_727_910_141_000, UnixNanos::from(2_000_000_000))
            .unwrap();

        // Yes batch: Clear + 2 bids + 1 ask, F_SNAPSHOT|F_LAST on the last.
        assert_eq!(projected.yes.deltas.len(), 4);
        assert_eq!(projected.yes.deltas[0].action, BookAction::Clear);
        assert_eq!(projected.yes.deltas[1].order.price.to_string(), "0.491");
        assert!(RecordFlag::F_LAST.matches(projected.yes.deltas[3].flags));
        // Sequence 0 everywhere — no pretended venue sequence.
        assert!(projected.yes.deltas.iter().all(|d| d.sequence == 0));
        // Shared frame identity.
        assert_eq!(projected.yes.ts_event, projected.no.ts_event);
        assert_eq!(projected.yes.ts_event.as_u64(), 1_727_910_141_000_000_000);

        // No batch: complement levels, sizes unchanged.
        assert_eq!(projected.no.deltas.len(), 4);
        let no_ask = &projected.no.deltas[3]; // last = first yes bid complemented = ask side
        assert_eq!(
            no_ask.order.price.to_string(),
            "0.510",
            "No ask = 1 − best Yes bid"
        );

        // Quotes: yes top 0.491/0.492; no top 0.508/0.509.
        let yes_quote = projected.yes_quote.as_ref().unwrap();
        assert_eq!(yes_quote.bid_price.to_string(), "0.491");
        let no_quote = projected.no_quote.as_ref().unwrap();
        assert_eq!(no_quote.bid_price.to_string(), "0.508");
        assert_eq!(no_quote.ask_price.to_string(), "0.509");
        // Instrument identity separation.
        assert_eq!(
            yes_quote.instrument_id,
            InstrumentId::from("0xcond-1.PREDICT")
        );
        assert_eq!(
            no_quote.instrument_id,
            InstrumentId::from("0xcond-2.PREDICT")
        );
    }

    #[test]
    fn empty_side_stays_empty_and_quote_absent() {
        let yes = instrument("1");
        let no = instrument("2");
        let bids = [level("0.4", "10")];

        let projected = project_book_frame(&yes, &no, &bids, &[], 1_000, UnixNanos::from(2_000_000_000)).unwrap();

        // Yes batch = Clear + 1 bid; No batch = Clear + 1 ask (0.6); no
        // zero-price synthetic levels anywhere.
        assert_eq!(projected.yes.deltas.len(), 2);
        assert_eq!(projected.no.deltas.len(), 2);
        assert_eq!(projected.no.deltas[1].order.price.to_string(), "0.600");
        assert!(projected.yes_quote.is_none(), "missing ask → no quote");
        assert!(projected.no_quote.is_none(), "missing bid → no quote");
    }

    #[test]
    fn inexact_complement_is_loud() {
        let yes = instrument("1");
        let no = instrument("2");
        // 1 − 0.3335 at precision 3 would round: rejected.
        let bids = [level("0.3335", "10")];
        let error = project_book_frame(&yes, &no, &bids, &[], 1_000, UnixNanos::from(2_000_000_000))
            .expect_err("inexact complement must be rejected");
        assert!(error.to_string().contains("not exact"), "{error}");
    }

    #[test]
    fn crossed_frame_is_rejected() {
        let yes = instrument("1");
        let no = instrument("2");
        let bids = [level("0.5", "10")];
        let asks = [level("0.4", "10")];
        let error = project_book_frame(&yes, &no, &bids, &asks, 1_000, UnixNanos::from(2_000_000_000))
            .expect_err("crossed book must be rejected");
        assert!(error.to_string().contains("crossed"), "{error}");
    }
}
