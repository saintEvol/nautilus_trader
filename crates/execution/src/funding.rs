// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Shared funding settlement semantics for perpetual swap venues.
//!
//! The backtest exchange and the sandbox (paper) venue drive funding settlement
//! from different clocks, yet must apply the same settlement rules. This module
//! owns those rules as pure computations: funding boundary determination and
//! per-position settlement entries (payment direction, currency validation,
//! overflow protection). Balance application, account state broadcasting, and
//! event publication are side effects performed by each caller along its own
//! path.

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::FundingRateUpdate,
    identifiers::{AccountId, InstrumentId, PositionId},
    position::Position,
    types::{Money, Price},
};
use rust_decimal::Decimal;

/// A single instrument's settlement computation result (one boundary × one instrument).
///
/// # Naming
///
/// Deliberately distinct from the domain event type
/// [`FundingSettlement`](nautilus_model::events::FundingSettlement): this type is
/// a pure computation result only, and outbound events always reuse the model's
/// existing event type.
#[derive(Debug, Clone)]
pub struct FundingSettlementResult {
    /// The instrument the settlement was computed for.
    pub instrument_id: InstrumentId,
    /// The account holding the settled positions (taken from the first position).
    pub account_id: AccountId,
    /// The boundary timestamp of the settlement.
    pub ts_event: UnixNanos,
    /// The funding rate applied for the settlement.
    pub rate: Decimal,
    /// The price used to value open positions (mark price preferred, top-of-book
    /// midpoint fallback; resolved and passed in by the caller).
    pub settlement_price: Price,
    /// Per-position settlement entries.
    pub entries: Vec<FundingEntry>,
}

impl FundingSettlementResult {
    /// Returns the aggregate account adjustment: the sum of all entry amounts.
    ///
    /// [`compute_settlement`] enforces a single settlement currency across entries
    /// and validates that this fold cannot overflow, so callers may treat a `None`
    /// as a defensive branch only.
    #[must_use]
    pub fn total_adjustment(&self) -> Option<Money> {
        let mut entries = self.entries.iter();
        let mut total = entries.next()?.amount;
        for entry in entries {
            total = total.checked_add(entry.amount)?;
        }
        Some(total)
    }
}

/// A per-position funding settlement entry.
#[derive(Debug, Clone)]
pub struct FundingEntry {
    /// The position the entry was computed for.
    pub position_id: PositionId,
    /// The signed quantity of the position at settlement (positive long, negative short).
    pub signed_qty: f64,
    /// The position notional valued at the settlement price.
    pub notional: Money,
    /// The signed funding amount: payment direction is included (long pays,
    /// short receives).
    pub amount: Money,
}

/// Returns whether the funding rate update's event timestamp lies on an interval
/// funding boundary, per the settlement rule shared by the backtest exchange and
/// the sandbox venue.
#[must_use]
pub fn is_interval_funding_boundary(funding_rate: &FundingRateUpdate) -> bool {
    let Some(interval_mins) = funding_rate.interval else {
        return false;
    };
    let interval_ns = u64::from(interval_mins) * 60 * 1_000_000_000;
    interval_ns > 0 && funding_rate.ts_event.as_u64().is_multiple_of(interval_ns)
}

/// Returns the settlement boundary for the funding rate update: the explicit next
/// funding time when provided, otherwise the event timestamp when it lies on an
/// interval boundary.
#[must_use]
pub fn funding_boundary(funding_rate: &FundingRateUpdate) -> Option<UnixNanos> {
    funding_rate
        .next_funding_ns
        .or_else(|| is_interval_funding_boundary(funding_rate).then_some(funding_rate.ts_event))
}

/// Computes funding settlement entries for the open positions of one instrument at
/// one boundary — a pure function with no account side effects.
///
/// Preserves the settlement semantics of the backtest exchange exactly:
/// - payment direction: long pays, short receives (`notional × rate × side`);
/// - the settlement currency must be uniform across positions and must match each
///   position's realized PnL currency;
/// - all arithmetic is checked (notional valuation, funding amount, realized PnL
///   addition, aggregate adjustment) and errors rather than saturating.
///
/// Balance application, account state broadcasting, and event publication are
/// performed by the caller (the backtest exchange or the sandbox venue).
///
/// # Errors
///
/// Returns an error if `positions` is empty, settlement currencies differ across
/// positions, a position cannot be valued at `settlement_price`, the funding
/// amount or the aggregate adjustment overflows decimal or [`Money`] bounds, or a
/// position's realized PnL currency differs from the funding currency or would
/// overflow.
pub fn compute_settlement(
    rate: &FundingRateUpdate,
    boundary: UnixNanos,
    settlement_price: Price,
    positions: &[Position],
) -> anyhow::Result<FundingSettlementResult> {
    let Some(first) = positions.first() else {
        anyhow::bail!("no open positions to settle for {}", rate.instrument_id);
    };
    let account_id = first.account_id;
    let settlement_currency = first.settlement_currency;

    let mut entries = Vec::with_capacity(positions.len());

    for position in positions {
        if position.settlement_currency != settlement_currency {
            anyhow::bail!(
                "position settlement currencies differ for {} (position {})",
                rate.instrument_id,
                position.id
            );
        }

        let notional = position.try_notional_value(settlement_price).map_err(|e| {
            anyhow::anyhow!("invalid notional value for position {}: {e}", position.id)
        })?;
        let side = if position.signed_qty > 0.0 {
            -Decimal::ONE
        } else {
            Decimal::ONE
        };
        let Some(amount) = notional
            .as_decimal()
            .checked_mul(rate.rate)
            .and_then(|value| value.checked_mul(side))
        else {
            anyhow::bail!("funding amount overflow for position {}", position.id);
        };
        let amount = Money::from_decimal(amount, notional.currency).map_err(|e| {
            anyhow::anyhow!("invalid funding amount for position {}: {e}", position.id)
        })?;

        if amount.currency != settlement_currency {
            anyhow::bail!(
                "settlement currency {settlement_currency} differs from funding currency {} for position {}",
                amount.currency,
                position.id
            );
        }

        if let Some(realized) = position.realized_pnl {
            if realized.currency != amount.currency {
                anyhow::bail!(
                    "realized PnL currency {} differs from funding currency {} for position {}",
                    realized.currency,
                    amount.currency,
                    position.id
                );
            }

            if realized.checked_add(amount).is_none() {
                anyhow::bail!("realized PnL overflow for position {}", position.id);
            }
        }

        entries.push(FundingEntry {
            position_id: position.id,
            signed_qty: position.signed_qty,
            notional,
            amount,
        });
    }

    let result = FundingSettlementResult {
        instrument_id: rate.instrument_id,
        account_id,
        ts_event: boundary,
        rate: rate.rate,
        settlement_price,
        entries,
    };

    if result.total_adjustment().is_none() {
        anyhow::bail!(
            "aggregate account adjustment overflow for {}",
            rate.instrument_id
        );
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{OrderSide, OrderType},
        identifiers::{AccountId, TradeId},
        instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
        orders::{builder::OrderTestBuilder, stubs::TestOrderEventStubs},
        types::{Currency, Money, Price, Quantity},
    };

    use super::*;

    /// Builds an open position fixture on the given instrument, mirroring the
    /// backtest exchange test form (all offline literal fixtures).
    fn position_fixture(
        instrument: &InstrumentAny,
        side: OrderSide,
        quantity: &str,
        account_id: AccountId,
    ) -> Position {
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .side(side)
            .quantity(Quantity::from(quantity))
            .build();
        let fill = TestOrderEventStubs::filled(
            &order,
            instrument,
            Some(TradeId::from("T-001")),
            None,
            Some(Price::from("1000.00")),
            Some(Quantity::from(quantity)),
            None,
            Some(Money::from("0 USDT")),
            Some(UnixNanos::from(1)),
            Some(account_id),
        );
        Position::new(instrument, fill.into())
    }

    fn funding_rate_fixture(rate: Decimal) -> FundingRateUpdate {
        FundingRateUpdate::new(
            InstrumentId::from("ETHUSDT-PERP.BINANCE"),
            rate,
            None,
            None,
            UnixNanos::from(1),
            UnixNanos::from(1),
        )
    }

    const TEST_ACCOUNT: &str = "BINANCE-001";

    #[test]
    fn test_compute_settlement_long_pays() {
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let account_id = AccountId::from(TEST_ACCOUNT);
        let positions = [position_fixture(
            &instrument,
            OrderSide::Buy,
            "1.000",
            account_id,
        )];
        let boundary = UnixNanos::from(1_000);
        let rate = funding_rate_fixture(Decimal::from_str_exact("0.001").unwrap());

        let result =
            compute_settlement(&rate, boundary, Price::from("1000.00"), &positions).unwrap();

        assert_eq!(result.instrument_id, instrument.id());
        assert_eq!(result.account_id, account_id);
        assert_eq!(result.ts_event, boundary);
        assert_eq!(result.rate, Decimal::from_str_exact("0.001").unwrap());
        assert_eq!(result.settlement_price, Price::from("1000.00"));
        let [entry] = result.entries.as_slice() else {
            panic!("expected one entry");
        };
        assert_eq!(entry.position_id, positions[0].id);
        assert_eq!(entry.signed_qty, 1.0);
        assert_eq!(entry.notional, Money::from("1000 USDT"));
        // Long pays: notional 1000 × rate 0.001 = 1 USDT debit.
        assert_eq!(entry.amount, Money::from("-1 USDT"));
        assert_eq!(result.total_adjustment(), Some(Money::from("-1 USDT")));
    }

    #[test]
    fn test_compute_settlement_short_receives() {
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let positions = [position_fixture(
            &instrument,
            OrderSide::Sell,
            "2.000",
            AccountId::from(TEST_ACCOUNT),
        )];
        let rate = funding_rate_fixture(Decimal::from_str_exact("0.001").unwrap());

        let result = compute_settlement(
            &rate,
            UnixNanos::from(1_000),
            Price::from("1000.00"),
            &positions,
        )
        .unwrap();

        let [entry] = result.entries.as_slice() else {
            panic!("expected one entry");
        };
        assert_eq!(entry.signed_qty, -2.0);
        assert_eq!(entry.notional, Money::from("2000 USDT"));
        // Short receives: notional 2000 × rate 0.001 = 2 USDT credit.
        assert_eq!(entry.amount, Money::from("2 USDT"));
        assert_eq!(result.total_adjustment(), Some(Money::from("2 USDT")));
    }

    #[test]
    fn test_compute_settlement_uses_caller_settlement_price() {
        // The caller resolves the settlement price (mark price preferred,
        // top-of-book midpoint fallback); the computation must value positions
        // exactly at the price passed in.
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let positions = [position_fixture(
            &instrument,
            OrderSide::Buy,
            "1.000",
            AccountId::from(TEST_ACCOUNT),
        )];
        let rate = funding_rate_fixture(Decimal::from_str_exact("0.001").unwrap());

        let result = compute_settlement(
            &rate,
            UnixNanos::from(1_000),
            Price::from("2500.00"),
            &positions,
        )
        .unwrap();

        let [entry] = result.entries.as_slice() else {
            panic!("expected one entry");
        };
        assert_eq!(entry.notional, Money::from("2500 USDT"));
        assert_eq!(entry.amount, Money::from("-2.5 USDT"));
    }

    #[test]
    fn test_compute_settlement_sums_mixed_direction_positions() {
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let long = position_fixture(
            &instrument,
            OrderSide::Buy,
            "1.000",
            AccountId::from(TEST_ACCOUNT),
        );
        let short = position_fixture(
            &instrument,
            OrderSide::Sell,
            "2.000",
            AccountId::from(TEST_ACCOUNT),
        );
        let positions = [long, short];
        let rate = funding_rate_fixture(Decimal::from_str_exact("0.001").unwrap());

        let result = compute_settlement(
            &rate,
            UnixNanos::from(1_000),
            Price::from("1000.00"),
            &positions,
        )
        .unwrap();

        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.entries[0].amount, Money::from("-1 USDT"));
        assert_eq!(result.entries[1].amount, Money::from("2 USDT"));
        assert_eq!(result.total_adjustment(), Some(Money::from("1 USDT")));
    }

    #[test]
    fn test_compute_settlement_rejects_mixed_settlement_currencies() {
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let mut foreign = position_fixture(
            &instrument,
            OrderSide::Buy,
            "1.000",
            AccountId::from(TEST_ACCOUNT),
        );
        foreign.settlement_currency = Currency::from("USD");
        let positions = [
            position_fixture(
                &instrument,
                OrderSide::Buy,
                "1.000",
                AccountId::from(TEST_ACCOUNT),
            ),
            foreign,
        ];
        let rate = funding_rate_fixture(Decimal::from_str_exact("0.001").unwrap());

        let error = compute_settlement(
            &rate,
            UnixNanos::from(1_000),
            Price::from("1000.00"),
            &positions,
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("settlement currencies differ"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_compute_settlement_rejects_empty_positions() {
        let rate = funding_rate_fixture(Decimal::from_str_exact("0.001").unwrap());

        let error = compute_settlement(&rate, UnixNanos::from(1_000), Price::from("1000.00"), &[])
            .unwrap_err();

        assert!(
            error.to_string().contains("no open positions"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_compute_settlement_rate_overflow_errors() {
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let positions = [position_fixture(
            &instrument,
            OrderSide::Buy,
            "1.000",
            AccountId::from(TEST_ACCOUNT),
        )];
        let rate = funding_rate_fixture(Decimal::MAX);

        let error = compute_settlement(
            &rate,
            UnixNanos::from(1_000),
            Price::from("1000000.00"),
            &positions,
        )
        .unwrap_err();

        // 1_000_000 USDT notional × Decimal::MAX rate overflows decimal bounds.
        assert!(
            error.to_string().contains("overflow"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_is_interval_funding_boundary_aligned() {
        // 480 minutes = 28_800_000_000_000 ns; an exact multiple is a boundary.
        let aligned = FundingRateUpdate::new(
            InstrumentId::from("ETHUSDT-PERP.BINANCE"),
            Decimal::ONE,
            Some(480),
            None,
            UnixNanos::from(28_800_000_000_000),
            UnixNanos::from(1),
        );

        assert!(is_interval_funding_boundary(&aligned));
    }

    #[test]
    fn test_is_interval_funding_boundary_misaligned() {
        let misaligned = FundingRateUpdate::new(
            InstrumentId::from("ETHUSDT-PERP.BINANCE"),
            Decimal::ONE,
            Some(480),
            None,
            UnixNanos::from(28_800_000_000_001),
            UnixNanos::from(1),
        );

        assert!(!is_interval_funding_boundary(&misaligned));
    }

    #[test]
    fn test_is_interval_funding_boundary_no_interval_or_zero() {
        let no_interval = FundingRateUpdate::new(
            InstrumentId::from("ETHUSDT-PERP.BINANCE"),
            Decimal::ONE,
            None,
            None,
            UnixNanos::from(3_600_000_000_000),
            UnixNanos::from(1),
        );
        let zero_interval = FundingRateUpdate::new(
            InstrumentId::from("ETHUSDT-PERP.BINANCE"),
            Decimal::ONE,
            Some(0),
            None,
            UnixNanos::from(0),
            UnixNanos::from(1),
        );

        assert!(!is_interval_funding_boundary(&no_interval));
        // A zero interval can never define a boundary (division guard).
        assert!(!is_interval_funding_boundary(&zero_interval));
    }

    #[test]
    fn test_funding_boundary_prefers_next_funding_ns() {
        let explicit = FundingRateUpdate::new(
            InstrumentId::from("ETHUSDT-PERP.BINANCE"),
            Decimal::ONE,
            Some(480),
            Some(UnixNanos::from(123)),
            // Event timestamp is NOT interval-aligned: only the explicit next
            // funding time can provide the boundary.
            UnixNanos::from(999),
            UnixNanos::from(1),
        );

        assert_eq!(funding_boundary(&explicit), Some(UnixNanos::from(123)));
    }

    #[test]
    fn test_funding_boundary_falls_back_to_interval_alignment() {
        let aligned = FundingRateUpdate::new(
            InstrumentId::from("ETHUSDT-PERP.BINANCE"),
            Decimal::ONE,
            Some(480),
            None,
            UnixNanos::from(28_800_000_000_000),
            UnixNanos::from(1),
        );
        let misaligned = FundingRateUpdate::new(
            InstrumentId::from("ETHUSDT-PERP.BINANCE"),
            Decimal::ONE,
            Some(480),
            None,
            UnixNanos::from(28_800_000_000_001),
            UnixNanos::from(1),
        );

        assert_eq!(
            funding_boundary(&aligned),
            Some(UnixNanos::from(28_800_000_000_000))
        );
        assert_eq!(funding_boundary(&misaligned), None);
    }
}
