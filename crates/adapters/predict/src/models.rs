//! Predict simulation models (§6.2 component list, §8.3 fees).
//!
//! [`PredictFeeModel`] consumes the pure fee arithmetic in
//! `nautilus_model::prediction::fees` (`rate/10000 × min(p, 1−p) × shares`):
//! taker BUY fees are share-denominated (deducted from the received outcome
//! shares via `FeeModel::get_commission_shares` — the fill stays gross and
//! the net position differs), taker SELL fees are collateral `Money`, maker
//! fills pay nothing (`makerFeeBps` is zero in the catalog). One charging
//! contract: the BUY side returns a zero collateral commission — the same
//! fee is never charged both in shares and in `Money`.
//!
//! The rate rides the instrument `info` (`fee_rate_bps` from the market
//! catalog) exactly as the signing flags do; an absent rate is a missing
//! fact, never a silent zero fee.

use nautilus_core::Params;
use nautilus_execution::models::fee::FeeModel;
use nautilus_model::{
    enums::{LiquiditySide, OrderSide},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    prediction::{predict_taker_fee, predict_taker_fee_buy},
    types::{Money, Price, Quantity},
};
use rust_decimal::Decimal;

/// Predict fee model for binary-option backtests and sandbox execution
/// (`POLYMARKET_PREDICT` §6.2).
#[derive(Debug, Clone, Copy, Default)]
pub struct PredictFeeModel;

impl PredictFeeModel {
    /// Resolves the market's taker fee rate (basis points) from the
    /// instrument catalog metadata.
    ///
    /// # Errors
    ///
    /// Returns an error for non-binary instruments and for instruments whose
    /// catalog entry carries no `fee_rate_bps` — a missing rate is never
    /// treated as a known zero fee.
    fn taker_fee_rate_bps(instrument: &InstrumentAny) -> anyhow::Result<u64> {
        let InstrumentAny::BinaryOption(binary) = instrument else {
            anyhow::bail!("PredictFeeModel requires a binary option instrument");
        };

        let rate = binary
            .info
            .as_ref()
            .and_then(|info: &Params| info.get("fee_rate_bps"))
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Predict instrument {} carries no fee_rate_bps; refusing to assume zero fees",
                    binary.id
                )
            })?;

        anyhow::ensure!(rate >= 0, "fee_rate_bps negative ({rate}) — catalog corruption");
        Ok(rate as u64)
    }

    fn liquidity_side(order: &OrderAny) -> anyhow::Result<LiquiditySide> {
        match order.liquidity_side() {
            Some(LiquiditySide::Maker) => Ok(LiquiditySide::Maker),
            Some(LiquiditySide::Taker) => Ok(LiquiditySide::Taker),
            Some(LiquiditySide::NoLiquiditySide) | None => {
                Err(anyhow::anyhow!("Liquidity side not set"))
            }
        }
    }

    /// The probability price on the open interval `(0, 1)` — no silent
    /// clamping at the extremes.
    fn checked_price(fill_px: Price) -> anyhow::Result<Decimal> {
        let price = fill_px.as_decimal();
        if price <= Decimal::ZERO || price >= Decimal::ONE {
            anyhow::bail!("PredictFeeModel requires a fill price in (0, 1)");
        }
        Ok(price)
    }
}

impl FeeModel for PredictFeeModel {
    fn get_commission(
        &self,
        order: &OrderAny,
        fill_quantity: Quantity,
        fill_px: Price,
        instrument: &InstrumentAny,
    ) -> anyhow::Result<Money> {
        let fee_rate_bps = Self::taker_fee_rate_bps(instrument)?;
        let quote = instrument.quote_currency();

        if Self::liquidity_side(order)? == LiquiditySide::Maker {
            // makerFeeBps is zero in the catalog — maker fills pay nothing.
            return Ok(Money::zero(quote));
        }

        if order.order_side() == OrderSide::Buy {
            // §6.3 B single charging contract: the taker BUY fee is deducted
            // from the received shares (get_commission_shares), never also
            // charged as collateral on the same fill.
            return Ok(Money::zero(quote));
        }

        // Taker SELL: the fee is paid from the received collateral.
        let price = Self::checked_price(fill_px)?;
        let fee = predict_taker_fee(price, fill_quantity.as_decimal(), fee_rate_bps)
            .ok_or_else(|| anyhow::anyhow!("taker fee arithmetic overflow"))?;
        Money::from_decimal(fee, quote).map_err(Into::into)
    }

    fn get_commission_shares(
        &self,
        order: &OrderAny,
        fill_quantity: Quantity,
        fill_px: Price,
        instrument: &InstrumentAny,
    ) -> anyhow::Result<Option<Quantity>> {
        let fee_rate_bps = Self::taker_fee_rate_bps(instrument)?;

        if Self::liquidity_side(order)? != LiquiditySide::Taker
            || order.order_side() != OrderSide::Buy
        {
            return Ok(None);
        }

        let price = Self::checked_price(fill_px)?;
        let (net_shares, _fee) = predict_taker_fee_buy(
            price,
            fill_quantity.as_decimal(),
            fee_rate_bps,
            u32::from(instrument.size_precision()),
        )
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Predict taker BUY fee exceeds the purchased shares ({} bps at {})",
                fee_rate_bps,
                fill_px
            )
        })?;

        let fee_shares = fill_quantity.as_decimal() - net_shares;
        if fee_shares.is_zero() {
            return Ok(None);
        }
        Ok(Some(Quantity::from_decimal_dp(
            fee_shares,
            instrument.size_precision(),
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::PredictFeeModel;
    use crate::common::consts::SIZE_PRECISION;
    use nautilus_core::{Params, UnixNanos};
    use nautilus_execution::models::fee::FeeModel;
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide, OrderType, AssetClass},
        instruments::{Instrument, InstrumentAny},
        orders::{OrderAny, builder::OrderTestBuilder, stubs::TestOrderStubs},
        types::{Price, Quantity},
    };
    use rust_decimal::Decimal;

    fn instrument(fee_rate_bps: i64) -> InstrumentAny {
        let info = Params::new(
            [(
                "fee_rate_bps".to_string(),
                serde_json::json!(fee_rate_bps),
            )]
            .into(),
        );
        InstrumentAny::BinaryOption(
            nautilus_model::instruments::BinaryOption::builder()
                .instrument_id(nautilus_model::identifiers::InstrumentId::from(
                    "0xcond-7.PREDICT",
                ))
                .raw_symbol(nautilus_model::identifiers::Symbol::new("0xcond-7"))
                .asset_class(AssetClass::Alternative)
                .currency(crate::collateral_currency())
                .activation_ns(UnixNanos::from(1))
                .expiration_ns(UnixNanos::from(u64::MAX))
                .price_precision(2)
                .size_precision(SIZE_PRECISION)
                .price_increment(Price::from("0.01"))
                .size_increment(Quantity::from("0.001"))
                .outcome(ustr::Ustr::from("Yes"))
                .info(info)
                .ts_event(UnixNanos::default())
                .ts_init(UnixNanos::default())
                .build()
                .unwrap(),
        )
    }

    fn order(instrument: &InstrumentAny, side: OrderSide, liquidity: LiquiditySide) -> OrderAny {
        let limit = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument.id())
            .side(side)
            .price(Price::from("0.50"))
            .quantity(Quantity::from("100"))
            .build();
        TestOrderStubs::make_filled_order(&limit, instrument, liquidity)
    }

    /// §8.3 arithmetic anchor through the model: p=0.5, 100 shares, 200bps.
    /// BUY pays 50 collateral, receives 98 shares (fee 1 ⇒ 2 shares at 0.5);
    /// SELL receives 49 collateral. Order volume stays 100 shares and the
    /// BUY fee is never also charged in `Money`.
    #[test]
    fn section_8_3_anchor_buy_charges_shares_sell_charges_collateral() {
        let instrument = instrument(200);
        let buy = order(&instrument, OrderSide::Buy, LiquiditySide::Taker);
        let sell = order(&instrument, OrderSide::Sell, LiquiditySide::Taker);

        // BUY: zero collateral commission, 2 shares deducted.
        let commission = PredictFeeModel
            .get_commission(&buy, Quantity::from("100"), Price::from("0.5"), &instrument)
            .unwrap();
        assert_eq!(commission.as_decimal(), Decimal::ZERO);
        assert_eq!(commission.currency, crate::collateral_currency());
        let fee_shares = PredictFeeModel
            .get_commission_shares(&buy, Quantity::from("100"), Price::from("0.5"), &instrument)
            .unwrap()
            .expect("taker BUY deducts fee shares");
        assert_eq!(fee_shares, Quantity::from("2"));

        // SELL: 1 collateral unit, no share deduction.
        let commission = PredictFeeModel
            .get_commission(&sell, Quantity::from("100"), Price::from("0.5"), &instrument)
            .unwrap();
        assert_eq!(commission.currency, crate::collateral_currency());
        assert_eq!(commission.as_decimal(), Decimal::ONE);
        assert!(
            PredictFeeModel
                .get_commission_shares(&sell, Quantity::from("100"), Price::from("0.5"), &instrument)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn maker_pays_nothing_on_either_side() {
        let instrument = instrument(200);
        let maker_buy = order(&instrument, OrderSide::Buy, LiquiditySide::Maker);

        let commission = PredictFeeModel
            .get_commission(&maker_buy, Quantity::from("100"), Price::from("0.5"), &instrument)
            .unwrap();
        assert_eq!(commission.as_decimal(), Decimal::ZERO);
        assert_eq!(commission.currency, crate::collateral_currency());
        assert!(
            PredictFeeModel
                .get_commission_shares(&maker_buy, Quantity::from("100"), Price::from("0.5"), &instrument)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn zero_rate_yields_no_deduction() {
        let instrument = instrument(0);
        let buy = order(&instrument, OrderSide::Buy, LiquiditySide::Taker);

        assert!(
            PredictFeeModel
                .get_commission_shares(&buy, Quantity::from("100"), Price::from("0.5"), &instrument)
                .unwrap()
                .is_none(),
            "an explicit zero rate charges nothing (not a missing-rate zero)"
        );
    }

    #[test]
    fn missing_rate_fails_loudly() {
        let instrument = InstrumentAny::BinaryOption(
            nautilus_model::instruments::BinaryOption::builder()
                .instrument_id(nautilus_model::identifiers::InstrumentId::from(
                    "0xcond-8.PREDICT",
                ))
                .raw_symbol(nautilus_model::identifiers::Symbol::new("0xcond-8"))
                .asset_class(AssetClass::Alternative)
                .currency(crate::collateral_currency())
                .activation_ns(UnixNanos::from(1))
                .expiration_ns(UnixNanos::from(u64::MAX))
                .price_precision(2)
                .size_precision(SIZE_PRECISION)
                .price_increment(Price::from("0.01"))
                .size_increment(Quantity::from("0.001"))
                .outcome(ustr::Ustr::from("Yes"))
                .ts_event(UnixNanos::default())
                .ts_init(UnixNanos::default())
                .build()
                .unwrap(),
        );
        let buy = order(&instrument, OrderSide::Buy, LiquiditySide::Taker);

        let error = PredictFeeModel
            .get_commission(&buy, Quantity::from("100"), Price::from("0.5"), &instrument)
            .unwrap_err();
        assert!(error.to_string().contains("no fee_rate_bps"), "{error:#}");
    }

    #[test]
    fn non_binary_instrument_fails_loudly() {
        let audusd = InstrumentAny::CurrencyPair(
            nautilus_model::instruments::stubs::audusd_sim(),
        );
        let buy = order(&audusd, OrderSide::Buy, LiquiditySide::Taker);

        let error = PredictFeeModel
            .get_commission(&buy, Quantity::from("100"), Price::from("0.5"), &audusd)
            .unwrap_err();
        assert!(
            error.to_string().contains("requires a binary option instrument"),
            "{error:#}"
        );
    }

    #[test]
    fn price_outside_open_interval_fails_loudly() {
        let instrument = instrument(200);
        let buy = order(&instrument, OrderSide::Buy, LiquiditySide::Taker);

        let error = PredictFeeModel
            .get_commission_shares(&buy, Quantity::from("100"), Price::from("1.0"), &instrument)
            .unwrap_err();
        assert!(
            error.to_string().contains("fill price in (0, 1)"),
            "{error:#}"
        );
    }

    #[test]
    fn fee_exceeding_purchase_fails_loudly() {
        let instrument = instrument(20_000);
        let buy = order(&instrument, OrderSide::Buy, LiquiditySide::Taker);

        let error = PredictFeeModel
            .get_commission_shares(&buy, Quantity::from("100"), Price::from("0.5"), &instrument)
            .unwrap_err();
        assert!(
            error.to_string().contains("exceeds the purchased shares"),
            "{error:#}"
        );
    }

    #[test]
    fn share_deduction_rounds_up_at_instrument_precision() {
        // p=0.7, 1 share, 300bps: exact fee shares 0.012857142… ⇒ ceil at
        // size precision 3 ⇒ 0.013 deducted (never undercharge).
        let instrument = instrument(300);
        let buy = order(&instrument, OrderSide::Buy, LiquiditySide::Taker);

        let fee_shares = PredictFeeModel
            .get_commission_shares(&buy, Quantity::from("1"), Price::from("0.7"), &instrument)
            .unwrap()
            .expect("non-zero fee deducts shares");
        assert_eq!(fee_shares, Quantity::from("0.013"));
        assert!(fee_shares.as_decimal() > Decimal::new(9, 3) / Decimal::from(7));
    }
}
