use rust_decimal::Decimal;

/// Split accounting (collateral → complete set of outcome tokens).
///
/// `sets` complete sets are minted for `sets × 1` collateral, one token of
/// each outcome per set. Cost allocation is the deterministic disclosed rule:
/// the per-set collateral cost is distributed **equally across the outcome
/// legs** (no market price is manufactured for missing legs — §6.3 C).
#[derive(Debug, Clone, PartialEq)]
pub struct SplitLegAccounting {
    /// Token quantity received per leg (= `sets`).
    pub quantity: Decimal,
    /// Cost basis allocated to this leg.
    pub cost_allocated: Decimal,
    /// Per-share cost basis on this leg.
    pub cost_per_share: Decimal,
}

/// Validate and account a split of `sets` complete sets across `legs` outcomes.
///
/// Errors when: sets ≤ 0, legs = 0, or collateral ≤ 0. Conservation is
/// structural: the last leg absorbs any decimal remainder of the equal
/// allocation, so the legs always sum to the exact collateral spent.
pub fn split_leg_accounting(
    sets: Decimal,
    legs: usize,
    collateral_spent: Decimal,
) -> Result<Vec<SplitLegAccounting>, String> {
    if sets <= Decimal::ZERO {
        return Err(format!("split sets must be positive, got {sets}"));
    }
    if legs == 0 {
        return Err("split requires at least one outcome leg".into());
    }
    if collateral_spent <= Decimal::ZERO {
        return Err(format!("split collateral must be positive, got {collateral_spent}"));
    }
    // Equal allocation: collateral_spent / legs, with the final leg absorbing
    // the decimal remainder so the legs sum to the exact input.
    let per_leg = collateral_spent / Decimal::from(legs);
    let mut out = Vec::with_capacity(legs);
    let mut allocated = Decimal::ZERO;
    for i in 0..legs {
        let cost = if i + 1 == legs {
            collateral_spent - allocated
        } else {
            per_leg
        };
        allocated += cost;
        out.push(SplitLegAccounting {
            quantity: sets,
            cost_allocated: cost,
            cost_per_share: cost / sets,
        });
    }
    debug_assert_eq!(allocated, collateral_spent);
    Ok(out)
}

/// Merge accounting (complete set of outcome tokens → collateral).
///
/// Merging `sets` of every leg releases `sets × 1` collateral. The released
/// cash is attributed **equally across the legs** (deterministic disclosed
/// rule mirroring the equal split allocation — no market price manufactured).
/// Realized PnL per leg = attributed cash share − leg basis removed;
/// conservation across the batch:
/// `cash_released == Σ (leg_basis_removed + leg_realized_pnl)`.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeLegAccounting {
    /// Quantity consumed from this leg (= `sets`).
    pub quantity: Decimal,
    /// Cost basis removed from this leg.
    pub basis_removed: Decimal,
    /// Released cash attributed to this leg.
    pub cash_attributed: Decimal,
    /// Realized PnL attributed to this leg (cash share − basis).
    pub realized_pnl: Decimal,
}

pub fn merge_leg_accounting(
    sets: Decimal,
    leg_costs_per_share: &[Decimal],
) -> Result<(Decimal, Vec<MergeLegAccounting>), String> {
    if sets <= Decimal::ZERO {
        return Err(format!("merge sets must be positive, got {sets}"));
    }
    if leg_costs_per_share.is_empty() {
        return Err("merge requires at least one outcome leg".into());
    }
    let cash_released = sets; // 1 collateral per complete set
    let legs = leg_costs_per_share.len();
    let per_leg_cash_exact = cash_released / Decimal::from(legs);
    let mut out = Vec::with_capacity(legs);
    let mut attributed = Decimal::ZERO;
    for (i, cps) in leg_costs_per_share.iter().enumerate() {
        if *cps < Decimal::ZERO {
            return Err(format!("merge leg {i} cost per share negative: {cps}"));
        }
        // Equal attribution with the final leg absorbing the decimal
        // remainder so the attributed cash sums to the exact release.
        let cash_share = if i + 1 == legs {
            cash_released - attributed
        } else {
            per_leg_cash_exact
        };
        attributed += cash_share;
        let basis_removed = sets * cps;
        out.push(MergeLegAccounting {
            quantity: sets,
            basis_removed,
            cash_attributed: cash_share,
            realized_pnl: cash_share - basis_removed,
        });
    }
    debug_assert_eq!(attributed, cash_released);
    Ok((cash_released, out))
}

/// Convert accounting (neg-risk): `quantity` of token A converts 1:1 into
/// token B of the same market.
///
/// Cost-preserving: the source leg's per-share basis transfers to the target
/// leg — no PnL event, no market price manufactured. Conservation: basis
/// removed from A == basis added to B.
#[derive(Debug, Clone, PartialEq)]
pub struct ConvertAccounting {
    /// Quantity leaving the source leg.
    pub quantity: Decimal,
    /// Per-share cost basis transferred.
    pub cost_per_share: Decimal,
    /// Total basis moved from source to target.
    pub basis_transferred: Decimal,
}

pub fn convert_tokens(
    quantity: Decimal,
    source_cost_per_share: Decimal,
) -> Result<ConvertAccounting, String> {
    if quantity <= Decimal::ZERO {
        return Err(format!("convert quantity must be positive, got {quantity}"));
    }
    if source_cost_per_share < Decimal::ZERO {
        return Err(format!("convert source basis negative: {source_cost_per_share}"));
    }
    Ok(ConvertAccounting {
        quantity,
        cost_per_share: source_cost_per_share,
        basis_transferred: quantity * source_cost_per_share,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn d(x: i64) -> Decimal {
        Decimal::from(x)
    }

    /// §11.1 P0 conservation proofs — split.
    #[test]
    fn split_conserves_cash_and_cost() {
        // Split 100 collateral into 100 binary sets: 100 YES + 100 NO,
        // each leg basis 50 (0.5/share).
        let legs = split_leg_accounting(d(100), 2, d(100)).unwrap();
        assert_eq!(legs.len(), 2);
        for l in &legs {
            assert_eq!(l.quantity, d(100));
            assert_eq!(l.cost_allocated, d(50));
            assert_eq!(l.cost_per_share, Decimal::new(5, 1));
        }
        let total: Decimal = legs.iter().map(|l| l.cost_allocated).sum();
        assert_eq!(total, d(100), "allocated basis == collateral spent");
    }

    /// Non-divisible cost: remainder lands on the last leg, total conserved.
    #[test]
    fn split_remainder_consigned_to_last_leg() {
        // 1 collateral across 3 legs: 0.333… is non-terminating — Decimal
        // division rounds; the last leg absorbs the difference so the sum is
        // exactly the input.
        let legs = split_leg_accounting(d(1), 3, d(1)).unwrap();
        let total: Decimal = legs.iter().map(|l| l.cost_allocated).sum();
        assert_eq!(total, d(1), "remainder conserved, never dropped");
    }

    /// §11.1 P0 conservation proofs — merge.
    #[test]
    fn merge_conserves_cash_basis_and_pnl() {
        // Merge 100 sets of YES @0.5 basis + NO @0.5 basis: releases 100
        // collateral; each leg basis 50 removed; each leg PnL +50.
        let (cash, legs) =
            merge_leg_accounting(d(100), &[Decimal::new(5, 1), Decimal::new(5, 1)]).unwrap();
        assert_eq!(cash, d(100));
        let basis: Decimal = legs.iter().map(|l| l.basis_removed).sum();
        let pnl: Decimal = legs.iter().map(|l| l.realized_pnl).sum();
        assert_eq!(basis, d(100));
        assert_eq!(pnl, Decimal::ZERO, "split@1 + merge@1 is PnL-neutral");
        assert_eq!(cash, basis + pnl);

        // Asymmetric basis (bought legs separately at 0.2 / 0.7): 10
        // collateral attributed 5/5; bases 2/7 → pnl +3/−2.
        let (cash, legs) =
            merge_leg_accounting(d(10), &[Decimal::new(2, 1), Decimal::new(7, 1)]).unwrap();
        let basis: Decimal = legs.iter().map(|l| l.basis_removed).sum();
        let pnl: Decimal = legs.iter().map(|l| l.realized_pnl).sum();
        assert_eq!(legs[0].cash_attributed, d(5));
        assert_eq!(legs[1].cash_attributed, d(5));
        assert_eq!(legs[0].realized_pnl, d(3));
        assert_eq!(legs[1].realized_pnl, d(-2));
        assert_eq!(cash, d(10));
        assert_eq!(basis, d(9));
        assert_eq!(pnl, d(1));
        assert_eq!(cash, basis + pnl);
    }

    /// §11.1 P0 conservation proofs — convert (cost-preserving, no PnL).
    #[test]
    fn convert_preserves_basis_without_pnl_event() {
        let c = convert_tokens(d(25), Decimal::new(4, 1)).unwrap();
        assert_eq!(c.basis_transferred, d(10));
        assert_eq!(c.basis_transferred, c.quantity * c.cost_per_share);
    }

    #[test]
    fn invalid_operations_rejected() {
        assert!(split_leg_accounting(Decimal::ZERO, 2, d(1)).is_err());
        assert!(split_leg_accounting(d(1), 0, d(1)).is_err());
        assert!(merge_leg_accounting(d(1), &[]).is_err());
        assert!(convert_tokens(Decimal::ZERO, Decimal::ONE).is_err());
        assert!(convert_tokens(d(1), Decimal::new(-1, 0)).is_err());
    }
}
