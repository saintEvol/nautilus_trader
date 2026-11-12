use rust_decimal::Decimal;

/// Predict taker fee in collateral units:
/// `rate/10000 × min(p, 1−p) × shares`.
///
/// BUY pays the fee from received outcome shares (share-deducted); SELL pays
/// from received collateral. `fee_rate_bps` on a signed order is an upper
/// bound — live fills consume the venue-reported actual fee; this pure form
/// fixes the simulated track (§8.3 arithmetic anchor: p=0.5, 100 shares,
/// 200bps ⇒ fee 1 collateral unit).
pub fn predict_taker_fee(price: Decimal, shares: Decimal, fee_rate_bps: u64) -> Option<Decimal> {
    if price <= Decimal::ZERO || price >= Decimal::ONE {
        return None; // probability price outside (0,1): no silent clamping
    }
    let one = Decimal::ONE;
    let min_edge = one - price;
    let edge = if price < min_edge { price } else { min_edge };
    Some(edge * Decimal::from(fee_rate_bps) / Decimal::from(10_000) * shares)
}

/// BUY leg outcome: gross shares are paid for in collateral; the fee is
/// deducted from the received shares, converted at the fill price.
///
/// Share deduction rounds **up** (ceil at `share_precision` decimals): the
/// simulated track never undercharges the fee; live consumes reported
/// quantities. Gross filled quantity stays `shares` — net inventory is the
/// received amount (§6.3 B: gross fill, net position, fee each keep their own
/// semantics, no double charging).
pub fn predict_taker_fee_buy(
    price: Decimal,
    shares: Decimal,
    fee_rate_bps: u64,
    share_precision: u32,
) -> Option<(Decimal, Decimal)> {
    let fee = predict_taker_fee(price, shares, fee_rate_bps)?;
    let fee_shares_exact = fee / price;
    // ceil at precision: multiply up, ceiling, scale back — deterministic
    // decimal ceiling without float transit.
    let scale = Decimal::from(10u64.pow(share_precision));
    let scaled = fee_shares_exact.checked_mul(scale)?;
    let ceil_units = scaled.ceil();
    let fee_shares = ceil_units.checked_div(scale)?;
    if fee_shares > shares {
        return None; // fee exceeding the entire purchase: refuse, don't clamp
    }
    Some((shares - fee_shares, fee))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn d(x: i64) -> Decimal {
        Decimal::from(x)
    }

    /// §8.3 arithmetic anchor: p=0.5, 100 shares, 200bps.
    /// BUY pays 50 collateral, receives 98 shares (fee 1 ⇒ 2 shares at 0.5);
    /// SELL receives 49 collateral. Order volume stays 100 shares.
    #[test]
    fn predict_fee_anchor() {
        let fee = predict_taker_fee(Decimal::new(5, 1), d(100), 200).unwrap();
        assert_eq!(fee, d(1));
        // BUY: net shares 98, fee 1 collateral.
        let (net, fee) =
            predict_taker_fee_buy(Decimal::new(5, 1), d(100), 200, 6).unwrap();
        assert_eq!(net, d(98));
        assert_eq!(fee, d(1));
        // SELL: proceeds 50 − fee 1 = 49 collateral.
        let proceeds = Decimal::new(5, 1) * d(100) - fee;
        assert_eq!(proceeds, d(49));
    }

    #[test]
    fn fee_vanishes_at_extremes_and_rejects_out_of_range() {
        // At p→0 or p→1 the min(p,1−p) term makes the fee 0 only inside the
        // open interval; outside (0,1) the price is rejected outright.
        assert!(predict_taker_fee(Decimal::ZERO, d(100), 200).is_none());
        assert!(predict_taker_fee(Decimal::ONE, d(100), 200).is_none());
    }

    /// Share deduction rounds up at the instrument share precision — the
    /// simulated track never undercharges.
    #[test]
    fn share_deduction_rounds_up() {
        // p=0.7, 1 share, 300bps: fee = 0.3 × 0.03 = 0.009 collateral.
        let price = Decimal::new(7, 1);
        let fee = predict_taker_fee(price, d(1), 300).unwrap();
        assert_eq!(fee, Decimal::new(9, 3));
        // 0.009 / 0.7 = 0.012857142… shares; ceil at 6dp ⇒ 0.012858 deducted.
        let (net, _) = predict_taker_fee_buy(price, d(1), 300, 6).unwrap();
        let deducted = d(1) - net;
        assert_eq!(deducted, Decimal::from(12_858) / Decimal::from(1_000_000));
        assert!(deducted >= fee / price, "deduction covers the exact fee");
    }

    #[test]
    fn fee_larger_than_purchase_refused() {
        // 9900bps at p=0.5 on 1 share: fee 0.495 ⇒ deduct 0.99 shares, net 0.01.
        let (net, _) = predict_taker_fee_buy(Decimal::new(5, 1), d(1), 9_900, 6).unwrap();
        assert_eq!(net, Decimal::new(1, 2));
        // A rate whose deduction would exceed the whole purchase: refused.
        assert!(predict_taker_fee_buy(Decimal::new(5, 1), d(1), 20_000, 6).is_none());
    }
}
