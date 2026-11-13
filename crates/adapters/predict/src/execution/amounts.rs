//! Predict order amount mathematics (POLYMARKET_PREDICT §8.2 数量合同).
//!
//! Chain units are integer wei strings (10^18 = 1 unit); decimal strings
//! like `"0.4"` are rejected by the venue. Buys: `makerAmount` = USDT paid,
//! `takerAmount` = shares received; sells reversed. Limit amounts must
//! match the price exactly; market orders size from a freshly fetched book
//! within the caller's explicit slippage. All functions are pure and
//! offline-testable; nothing here signs or sends.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

/// 1 unit in wei (USDT and shares share the 18-decimals ERC20 scale).
pub fn wei_per_unit() -> Decimal {
    Decimal::from_i128_with_scale(1_000_000_000_000_000_000, 0)
}

/// A priced share amount after book sizing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SizedOrder {
    /// USDT the maker pays (buy) or receives (sell), wei.
    pub maker_amount_wei: i128,
    /// Shares the maker receives (buy) or sells, wei.
    pub taker_amount_wei: i128,
    /// Worst price actually consumed (for disclosure/receipts).
    pub worst_price: Decimal,
}

fn to_wei(units: Decimal) -> Option<i128> {
    let wei = units * wei_per_unit();
    if wei.fract() != Decimal::ZERO {
        return None; // sub-wei remainder cannot be represented on-chain
    }
    wei.trunc().to_i128()
}

/// LIMIT exactness: amounts must satisfy `taker = shares` and
/// `maker = shares × price` with no rounding — a price that does not divide
/// the notional into whole wei is rejected, never silently rounded
/// (venue: `create_order_limit_amounts_mismatch`).
///
/// # Errors
/// Non-positive price/shares, price outside (0,1), or a sub-wei remainder.
pub fn limit_amounts(
    side: PredictOrderSide,
    shares: Decimal,
    price: Decimal,
) -> anyhow::Result<SizedOrder> {
    anyhow::ensure!(
        price > Decimal::ZERO && price < Decimal::ONE,
        "limit price {price} outside (0,1)"
    );
    anyhow::ensure!(shares > Decimal::ZERO, "limit shares must be positive");
    let taker = to_wei(shares).ok_or_else(|| anyhow::anyhow!("shares {shares} has sub-wei remainder"))?;
    let notional = shares * price;
    let maker = to_wei(notional)
        .ok_or_else(|| anyhow::anyhow!("notional {notional} has sub-wei remainder at price {price}"))?;
    Ok(match side {
        PredictOrderSide::Buy => SizedOrder {
            maker_amount_wei: maker,
            taker_amount_wei: taker,
            worst_price: price,
        },
        PredictOrderSide::Sell => SizedOrder {
            maker_amount_wei: taker,
            taker_amount_wei: maker,
            worst_price: price,
        },
    })
}

/// MARKET sizing from a fresh book (place-orders page: amounts come from
/// walking the book; a market order never rests).
///
/// - Buy by shares (`quantityWei`): walk asks until filled or book ends —
///   a short book is a loud error (the venue would cancel unfilled
///   remainder; the caller decides whether to retry at a new book).
/// - Buy by value (`valueWei`, the most USDT to spend — buys only): walk
///   asks while budget remains; a dust budget that buys nothing is loud.
/// - Sell by shares: walk bids symmetrically.
///
/// Slippage is applied the SDK way (`isMinAmountOut` = fewer shares for the
/// spend; otherwise spend grows): the returned amounts are the **planned**
/// book-exact amounts and the slippage-adjusted bound the venue enforces.
///
/// # Errors
/// Empty/short book, non-positive sizing, or sub-wei remainders.
pub fn market_amounts(
    side: PredictOrderSide,
    book: &[(Decimal, Decimal)],
    sizing: MarketSizing,
) -> anyhow::Result<SizedOrder> {
    let levels = match side {
        PredictOrderSide::Buy => book, // asks: lowest first
        PredictOrderSide::Sell => book, // bids: highest first
    };
    anyhow::ensure!(!levels.is_empty(), "market order sized against an empty book");
    let mut spent = Decimal::ZERO;
    let mut shares = Decimal::ZERO;
    let mut worst = None;
    match sizing {
        MarketSizing::Shares(target) => {
            anyhow::ensure!(target > Decimal::ZERO, "market shares must be positive");
            let mut remaining = target;
            for (price, size) in levels {
                if remaining <= Decimal::ZERO {
                    break;
                }
                let take = remaining.min(*size);
                spent += take * price;
                shares += take;
                remaining -= take;
                worst = Some(*price);
            }
            anyhow::ensure!(
                remaining <= Decimal::ZERO,
                "book too shallow: {target} shares requested, {} available",
                target - remaining
            );
        }
        MarketSizing::Value(budget) => {
            anyhow::ensure!(budget > Decimal::ZERO, "market budget must be positive");
            let mut remaining = budget;
            for (price, size) in levels {
                if remaining <= Decimal::ZERO {
                    break;
                }
                let affordable = remaining / price;
                let take = affordable.min(*size);
                spent += take * price;
                shares += take;
                remaining -= take * price;
                worst = Some(*price);
            }
            anyhow::ensure!(
                shares > Decimal::ZERO,
                "budget {budget} buys nothing at the given book"
            );
        }
    }
    let maker = to_wei(spent).ok_or_else(|| anyhow::anyhow!("market spend {spent} has sub-wei remainder"))?;
    let taker = to_wei(shares).ok_or_else(|| anyhow::anyhow!("market shares {shares} has sub-wei remainder"))?;
    Ok(match side {
        PredictOrderSide::Buy => SizedOrder {
            maker_amount_wei: maker,
            taker_amount_wei: taker,
            worst_price: worst.expect("non-empty book guarantees a level"),
        },
        PredictOrderSide::Sell => SizedOrder {
            maker_amount_wei: taker,
            taker_amount_wei: maker,
            worst_price: worst.expect("non-empty book guarantees a level"),
        },
    })
}

/// The slippage-adjusted bound the venue enforces on a sized market order
/// (place-orders page). `slippage_bps` is the caller's explicit value
/// (0–10000). With `min_amount_out` the buy receives fewer shares
/// (takerAmount drops); without it the buy spends more (makerAmount grows);
/// a sell's takerAmount drops. Sub-wei remainders floor toward the safe
/// side (never promising more than the bound).
#[must_use]
pub fn apply_slippage(
    side: PredictOrderSide,
    sized: &SizedOrder,
    min_amount_out: bool,
    slippage_bps: u32,
) -> SlippageBound {
    let factor = Decimal::from(slippage_bps) / Decimal::from(10_000);
    match (side, min_amount_out) {
        (PredictOrderSide::Buy, true) => {
            let taker = Decimal::from(sized.taker_amount_wei) * (Decimal::ONE - factor);
            SlippageBound {
                maker_amount_wei: sized.maker_amount_wei,
                taker_amount_wei: taker.trunc().to_i128().unwrap_or(sized.taker_amount_wei),
            }
        }
        (PredictOrderSide::Buy, false) => {
            let maker = Decimal::from(sized.maker_amount_wei) * (Decimal::ONE + factor);
            SlippageBound {
                maker_amount_wei: maker.trunc().to_i128().unwrap_or(sized.maker_amount_wei),
                taker_amount_wei: sized.taker_amount_wei,
            }
        }
        (PredictOrderSide::Sell, _) => {
            let taker = Decimal::from(sized.taker_amount_wei) * (Decimal::ONE - factor);
            SlippageBound {
                maker_amount_wei: sized.maker_amount_wei,
                taker_amount_wei: taker.trunc().to_i128().unwrap_or(sized.taker_amount_wei),
            }
        }
    }
}

/// A venue-enforced slippage bound (wei, whole units).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlippageBound {
    pub maker_amount_wei: i128,
    pub taker_amount_wei: i128,
}

/// Order side (EIP-712 `side`: 0 buy, 1 sell).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredictOrderSide {
    Buy,
    Sell,
}

impl PredictOrderSide {
    #[must_use]
    pub const fn eip712_code(self) -> u8 {
        match self {
            Self::Buy => 0,
            Self::Sell => 1,
        }
    }
}

/// MARKET sizing input (§8.2: buys may size by shares or by max USDT
/// spend; sells size by shares only).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MarketSizing {
    Shares(Decimal),
    Value(Decimal),
}

#[cfg(test)]
mod tests {
    use super::*;
use rust_decimal::Decimal;
    use std::str::FromStr as _;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// 文档算术锚：1 share @ 0.4 → maker 0.4e18 / taker 1e18；方向互换
    /// 按买/卖约定。
    #[test]
    fn limit_amounts_exact_wei() {
        let buy = limit_amounts(PredictOrderSide::Buy, d("1"), d("0.4")).unwrap();
        assert_eq!(buy.maker_amount_wei, 400_000_000_000_000_000);
        assert_eq!(buy.taker_amount_wei, 1_000_000_000_000_000_000);
        let sell = limit_amounts(PredictOrderSide::Sell, d("2.5"), d("0.64")).unwrap();
        assert_eq!(sell.maker_amount_wei, 2_500_000_000_000_000_000);
        assert_eq!(sell.taker_amount_wei, 1_600_000_000_000_000_000);
    }

    /// 价格不整除 notional 到 wei → 响亮拒（venue amounts_mismatch 同款
    /// 语义，绝不静默取整）。
    #[test]
    fn limit_sub_wei_remainder_is_loud() {
        // 18 位小数的价格 × 整数份额 = 恰 18 位 notional（wei 整除）；
        // 份额带尾数 → 19 位 notional = sub-wei 余数，响亮拒。
        assert!(limit_amounts(PredictOrderSide::Buy, d("1"), d("0.333333333333333333")).is_ok());
        let err = limit_amounts(
            PredictOrderSide::Buy,
            d("0.1"),
            d("0.333333333333333333"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("sub-wei"), "{err}");
        assert!(limit_amounts(PredictOrderSide::Buy, d("1"), d("1")).is_err());
        assert!(limit_amounts(PredictOrderSide::Buy, d("0"), d("0.4")).is_err());
    }

    /// 市价按份额走档：两档 0.4×5 + 0.55×3 → 8 份、worst 0.55；浅盘响亮。
    #[test]
    fn market_walks_levels_by_shares() {
        let book = vec![(d("0.4"), d("5")), (d("0.55"), d("3"))];
        let sized =
            market_amounts(PredictOrderSide::Buy, &book, MarketSizing::Shares(d("8"))).unwrap();
        assert_eq!(sized.taker_amount_wei, 8_000_000_000_000_000_000);
        // 5*0.4 + 3*0.55 = 3.65
        assert_eq!(sized.maker_amount_wei, 3_650_000_000_000_000_000);
        assert_eq!(sized.worst_price, d("0.55"));

        let err =
            market_amounts(PredictOrderSide::Buy, &book, MarketSizing::Shares(d("9"))).unwrap_err();
        assert!(err.to_string().contains("too shallow"), "{err}");
    }

    /// 市价按金额（仅买入）：预算 2 USDT 走 0.4 档 5 份的 4/5。
    #[test]
    fn market_walks_levels_by_value() {
        let book = vec![(d("0.4"), d("5")), (d("0.55"), d("3"))];
        let sized =
            market_amounts(PredictOrderSide::Buy, &book, MarketSizing::Value(d("2"))).unwrap();
        assert_eq!(sized.maker_amount_wei, 2_000_000_000_000_000_000);
        assert_eq!(sized.taker_amount_wei, 5_000_000_000_000_000_000);
        // 卖侧按份额走 bid 对称。
        let bids = vec![(d("0.6"), d("10"))];
        let sell =
            market_amounts(PredictOrderSide::Sell, &bids, MarketSizing::Shares(d("2"))).unwrap();
        assert_eq!(sell.maker_amount_wei, 2_000_000_000_000_000_000);
        assert_eq!(sell.taker_amount_wei, 1_200_000_000_000_000_000);
    }

    /// 滑点界（SDK 语义）：min_amount_out 买方 taker 降；否则 maker 升；
    /// 卖方恒 taker 降。全部向安全侧取整。
    #[test]
    fn slippage_bounds_follow_sdk_semantics() {
        let sized = SizedOrder {
            maker_amount_wei: 1_000_000_000_000_000_000,
            taker_amount_wei: 1_000_000_000_000_000_000,
            worst_price: d("1"),
        };
        let min_out = apply_slippage(PredictOrderSide::Buy, &sized, true, 100);
        assert_eq!(min_out.maker_amount_wei, sized.maker_amount_wei);
        assert_eq!(min_out.taker_amount_wei, 990_000_000_000_000_000);
        let grow = apply_slippage(PredictOrderSide::Buy, &sized, false, 100);
        assert_eq!(grow.maker_amount_wei, 1_010_000_000_000_000_000);
        assert_eq!(grow.taker_amount_wei, sized.taker_amount_wei);
        let sell = apply_slippage(PredictOrderSide::Sell, &sized, false, 100);
        assert_eq!(sell.taker_amount_wei, 990_000_000_000_000_000);
    }
}
