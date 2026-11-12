use rust_decimal::Decimal;

/// Certified resolution payout for an outcome token, as an exact rational
/// `numerator / denominator` of collateral per share (Polymarket CTF reports
/// `payoutNumerators / payoutDenominator`; Predict reports 0 / 0.5 / 1).
///
/// The fraction is kept exact — chart endpoints and `end_date` are never
/// sources for payouts, and unknown or conflicting resolutions stay unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Payout {
    pub numerator: u64,
    pub denominator: u64,
}

pub const PAYOUT_WIN: Payout = Payout {
    numerator: 1,
    denominator: 1,
};
pub const PAYOUT_LOSE: Payout = Payout {
    numerator: 0,
    denominator: 1,
};
pub const PAYOUT_HALF: Payout = Payout {
    numerator: 1,
    denominator: 2,
};

impl Payout {
    /// Creates the canonical (reduced) payout fraction; `None` for a zero
    /// denominator or a payout above 1.
    pub fn new(numerator: u64, denominator: u64) -> Option<Self> {
        if denominator == 0 || numerator > denominator {
            return None;
        }
        let g = gcd(numerator, denominator);
        Some(Self {
            numerator: numerator / g,
            denominator: denominator / g,
        })
    }

    /// Exact conversion to a finite decimal settlement value.
    ///
    /// Lossy divisions are rejected (never rounded into a settlement price):
    /// a fraction terminates in decimal only when the reduced denominator has
    /// no prime factors other than 2 and 5 — anything else stays unknown.
    pub fn to_decimal(self) -> Option<Decimal> {
        if !Self::terminates(self.numerator, self.denominator) {
            return None;
        }
        Decimal::from(self.numerator).checked_div(Decimal::from(self.denominator))
    }

    /// Collateral per share for `quantity` shares (exact; `None` when the
    /// payout is not finitely expressible).
    pub fn proceeds(self, quantity: Decimal) -> Option<Decimal> {
        Some(quantity * self.to_decimal()?)
    }

    /// Whether `numerator / denominator` has a finite decimal expansion
    /// (reduced denominator of form 2^a · 5^b).
    fn terminates(numerator: u64, denominator: u64) -> bool {
        let g = gcd(numerator, denominator);
        let mut d = denominator / g;
        while d % 2 == 0 {
            d /= 2;
        }
        while d % 5 == 0 {
            d /= 5;
        }
        d == 1
    }
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 { a } else { gcd(b, a % b) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn known_payouts_are_exact() {
        assert_eq!(PAYOUT_WIN.to_decimal(), Some(Decimal::ONE));
        assert_eq!(PAYOUT_LOSE.to_decimal(), Some(Decimal::ZERO));
        assert_eq!(PAYOUT_HALF.to_decimal(), Some(Decimal::new(5, 1)));
        // Polymarket fractional form (basis-point denominator).
        let thirdish = Payout::new(3_333, 10_000).unwrap();
        assert_eq!(thirdish.to_decimal(), Some(Decimal::new(3333, 4)));
    }

    #[test]
    fn nonterminating_payout_stays_unknown() {
        // 1/3 does not terminate — never rounded into a settlement price.
        assert_eq!(Payout::new(1, 3).unwrap().to_decimal(), None);
        assert_eq!(Payout::new(1, 3).unwrap().proceeds(Decimal::ONE), None);
    }

    #[test]
    fn invalid_payouts_rejected() {
        assert!(Payout::new(2, 1).is_none());
        assert!(Payout::new(1, 0).is_none());
    }

    #[test]
    fn proceeds_scale_exactly() {
        // 0.5 payout on 100 shares = 50 collateral, exactly.
        let p = PAYOUT_HALF.proceeds(Decimal::from(100));
        assert_eq!(p, Some(Decimal::from(50)));
    }
}
