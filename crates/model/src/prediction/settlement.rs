use rust_decimal::Decimal;

use super::payout::Payout;

/// Terminal settlement accounting for one outcome position
/// (POLYMARKET_PREDICT §6.3 A / §9.3): close the economic position at the
/// certified payout without fabricating an order or fill.
///
/// `quantity` shares held at `avg_cost_open` collateral/share settle at
/// `payout` collateral/share:
/// - cash proceeds = quantity × payout (exact),
/// - realized PnL = quantity × (payout − avg_cost_open),
/// - the full cost basis is consumed (position closes; nothing pending).
///
/// Conservation (the property the offline proofs assert):
/// `proceeds == basis_consumed + realized_pnl`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutcomeSettlement {
    /// Collateral received (before any optional redemption-availability
    /// modeling; redeemability is a run-level assumption, never baked here).
    pub proceeds: Decimal,
    /// Cost basis consumed by the settlement.
    pub basis_consumed: Decimal,
    /// Realized PnL of the settlement (may be negative).
    pub realized_pnl: Decimal,
}

pub fn settle_outcome_position(
    quantity: Decimal,
    avg_cost_open: Decimal,
    payout: Payout,
) -> Option<OutcomeSettlement> {
    if quantity < Decimal::ZERO || avg_cost_open < Decimal::ZERO {
        return None;
    }
    let price = payout.to_decimal()?;
    let proceeds = quantity.checked_mul(price)?;
    let basis_consumed = quantity.checked_mul(avg_cost_open)?;
    let realized_pnl = proceeds.checked_sub(basis_consumed)?;
    Some(OutcomeSettlement {
        proceeds,
        basis_consumed,
        realized_pnl,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{PAYOUT_HALF, PAYOUT_LOSE, PAYOUT_WIN};
    use super::*;
    use rust_decimal::Decimal;

    fn d(x: i64) -> Decimal {
        Decimal::from(x)
    }

    /// §11.1 P0 conservation proofs.
    #[test]
    fn half_payout_settlement_conserves() {
        // 100 shares @ 0.30 settle at 0.5: proceeds 50, basis 30, PnL +20.
        let s = settle_outcome_position(d(100), Decimal::new(3, 1), PAYOUT_HALF).unwrap();
        assert_eq!(s.proceeds, d(50));
        assert_eq!(s.basis_consumed, d(30));
        assert_eq!(s.realized_pnl, d(20));
        assert_eq!(s.proceeds, s.basis_consumed + s.realized_pnl);
    }

    #[test]
    fn win_and_lose_settlements_conserves() {
        // Winner: 98 shares @ 0.49 settle at 1: proceeds 98, basis 48.02, PnL 49.98.
        let s = settle_outcome_position(
            d(98),
            Decimal::new(49, 2),
            PAYOUT_WIN,
        )
        .unwrap();
        assert_eq!(s.proceeds, d(98));
        assert_eq!(s.realized_pnl, Decimal::new(4998, 2));
        assert_eq!(s.proceeds, s.basis_consumed + s.realized_pnl);

        // Loser: proceeds 0, full basis realized as loss.
        let s = settle_outcome_position(d(98), Decimal::new(49, 2), PAYOUT_LOSE).unwrap();
        assert_eq!(s.proceeds, Decimal::ZERO);
        assert_eq!(s.realized_pnl, -Decimal::new(4802, 2));
        assert_eq!(s.proceeds, s.basis_consumed + s.realized_pnl);
    }

    #[test]
    fn fractional_payout_and_unknown_rejected() {
        // Basis-point fraction 3333/10000 on 100 shares @ 0.25.
        let p = Payout::new(3_333, 10_000).unwrap();
        let s = settle_outcome_position(d(100), Decimal::new(25, 2), p).unwrap();
        assert_eq!(s.proceeds, Decimal::new(3333, 2));
        assert_eq!(s.proceeds, s.basis_consumed + s.realized_pnl);

        // Non-terminating payout stays unknown — never a rounded settlement.
        assert!(settle_outcome_position(d(100), Decimal::ONE, Payout::new(1, 3).unwrap()).is_none());
        // Negative inputs rejected.
        assert!(settle_outcome_position(Decimal::ONE, Decimal::new(-1, 0), PAYOUT_WIN).is_none());
    }
}
