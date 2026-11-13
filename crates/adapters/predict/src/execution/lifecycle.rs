//! Predict order lifecycle mapping (POLYMARKET_PREDICT §8.2 状态映射表).
//!
//! Pure decision layer: venue order facts (status + `amountFilled` growth
//! + wallet events) → the terminal/intermediate action the execution
//! client must take. Offline-testable against the documented lifecycle
//! (dev.predict.fun/trading/order-lifecycle):
//!
//! | venue fact | mapping |
//! | --- | --- |
//! | accepted (speed-bump wait) | accepted, actual unfilled qty retained — never a fictitious book entry |
//! | OPEN | resting — no event |
//! | matched, on-chain not settled | pending settlement — **no** final
//! Filled（该行由 submit 管线的钱包事件摄取承载——本纯层输入是订单
//! 记录观测，撮合未结算在该面表现为 amountFilled 未增长） |
//! | settled, amountFilled grows | real sub-fill (idempotent by fill identity) |
//! | MARKET FILLED with amountFilled < amount | record actual fills, then close the unfilled remainder — never a full synthetic fill |
//! | CANCELLED / EXPIRED | fills already recorded stand; remainder cancels/expires |
//! | INVALIDATED / settlement failure | proven fills stand; explicit failure reason; no negative fills from status flips |
//!
//! The mapping never invents quantities: every emitted fill carries the
//! venue's own `amountFilled` delta, and unknown outcomes stay unknown.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr as _;

/// Order status word (closed lexicon — decode of an unknown word is the
/// transport layer's loud error, never a guess here).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredictOrderStatusWord {
    Open,
    Filled,
    Cancelled,
    Expired,
    Invalidated,
}

/// What the execution client should do for one observation of an order.
#[derive(Clone, Debug, PartialEq)]
pub enum LifecycleAction {
    /// Nothing terminal: resting (OPEN) or accepted-but-unfilled.
    Hold,
    /// `amountFilled` grew by this many wei shares since the last
    /// observation — emit a real sub-fill for exactly this delta
    /// (idempotence lives in the caller's fill-identity ledger).
    Fill { delta_shares_wei: i128 },
    /// Terminal: any recorded fills stand; the unfilled remainder is
    /// cancelled (venue cancelled it or the market order's remainder was
    /// cancelled by construction).
    CloseRemainderCancelled,
    /// Terminal: remainder expired (GTD deadline reached).
    CloseRemainderExpired,
    /// Terminal: venue removed the order (cannot be matched/settled any
    /// more, or a settlement failed). Proven fills stand; no negative
    /// fills are fabricated for previously recorded amounts.
    CloseInvalidated { reason: String },
}

/// Maps one order-record observation (status + cumulative filled wei)
/// against the previously observed cumulative fill.
///
/// MARKET FILLED with `amount_filled < amount` yields the delta fill first
/// and a remainder-cancellation conclusion — the caller emits both, never
/// a full-quantity synthetic fill.
#[must_use]
pub fn map_observation(
    status: PredictOrderStatusWord,
    amount_wei: i128,
    amount_filled_wei: i128,
    last_seen_filled_wei: i128,
    invalidation_reason: Option<&str>,
) -> Vec<LifecycleAction> {
    let delta = amount_filled_wei.saturating_sub(last_seen_filled_wei).max(0);
    let mut actions = Vec::new();
    if delta > 0 {
        actions.push(LifecycleAction::Fill {
            delta_shares_wei: delta,
        });
    }
    let fully_filled = amount_filled_wei >= amount_wei;
    match status {
        PredictOrderStatusWord::Open => {
            actions.push(LifecycleAction::Hold);
        }
        PredictOrderStatusWord::Filled => {
            // A market order is FILLED after any fill because the rest is
            // cancelled by construction — the remainder closes only when
            // it is genuinely unfilled.
            if !fully_filled {
                actions.push(LifecycleAction::CloseRemainderCancelled);
            }
        }
        PredictOrderStatusWord::Cancelled => {
            if !fully_filled {
                actions.push(LifecycleAction::CloseRemainderCancelled);
            }
        }
        PredictOrderStatusWord::Expired => {
            if !fully_filled {
                actions.push(LifecycleAction::CloseRemainderExpired);
            }
        }
        PredictOrderStatusWord::Invalidated => {
            actions.push(LifecycleAction::CloseInvalidated {
                reason: invalidation_reason
                    .unwrap_or("venue removed the order (unsettleable)")
                    .to_owned(),
            });
            // Previously recorded fills are NOT reversed — no negative
            // deltas are ever produced here.
        }
    }
    actions
}

/// Converts a cumulative `amountFilled` wei string to i128 (transport
/// keeps strings; this is the numeric boundary).
///
/// # Errors
/// Non-numeric or overflowing text is a loud error, never a clamp.
pub fn parse_wei(text: &str) -> anyhow::Result<i128> {
    let value = Decimal::from_str(text)
        .map_err(|_| anyhow::anyhow!("wei amount {text:?} is not a decimal"))?;
    anyhow::ensure!(
        value.fract() == Decimal::ZERO,
        "wei amount {text:?} carries a fraction"
    );
    value
        .trunc()
        .to_i128()
        .ok_or_else(|| anyhow::anyhow!("wei amount {text:?} overflows i128"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 增量成交：OPEN 期 amountFilled 0→3e18 → 恰一条 3e18 子成交，
    /// 无终局；再观察无增长 → 只有 Hold（幂等由 delta=0 保证）。
    #[test]
    fn open_order_emits_real_fill_deltas_only() {
        let a = map_observation(
            PredictOrderStatusWord::Open,
            10_000_000_000_000_000_000,
            3_000_000_000_000_000_000,
            0,
            None,
        );
        assert_eq!(
            a,
            vec![LifecycleAction::Fill {
                delta_shares_wei: 3_000_000_000_000_000_000
            }, LifecycleAction::Hold]
        );
        let again = map_observation(
            PredictOrderStatusWord::Open,
            10_000_000_000_000_000_000,
            3_000_000_000_000_000_000,
            3_000_000_000_000_000_000,
            None,
        );
        assert_eq!(again, vec![LifecycleAction::Hold]);
    }

    /// MARKET FILLED 但 amountFilled < amount：真实增量成交 + 余量取消
    /// ——绝不伪造全量成交。
    #[test]
    fn partially_filled_market_closes_remainder() {
        let a = map_observation(
            PredictOrderStatusWord::Filled,
            10_000_000_000_000_000_000,
            4_000_000_000_000_000_000,
            1_000_000_000_000_000_000,
            None,
        );
        assert_eq!(
            a,
            vec![
                LifecycleAction::Fill {
                    delta_shares_wei: 3_000_000_000_000_000_000
                },
                LifecycleAction::CloseRemainderCancelled
            ]
        );
    }

    /// CANCELLED/EXPIRED：已记录成交保留，未成交余量收口（两词分开）。
    #[test]
    fn cancelled_and_expired_keep_proven_fills() {
        for (word, expect) in [
            (PredictOrderStatusWord::Cancelled, LifecycleAction::CloseRemainderCancelled),
            (PredictOrderStatusWord::Expired, LifecycleAction::CloseRemainderExpired),
        ] {
            let a = map_observation(word, 10, 4, 4, None);
            assert_eq!(a, vec![expect], "{word:?}");
        }
    }

    /// INVALIDATED/settlement failure：已证成交保留、明确失败原因、
    /// 永不产生负增量。
    #[test]
    fn invalidated_never_fabricates_negative_fills() {
        // amountFilled 回落（所方视图收缩）：delta 饱和到 0，不造负数。
        let a = map_observation(
            PredictOrderStatusWord::Invalidated,
            10_000_000_000_000_000_000,
            2_000_000_000_000_000_000,
            3_000_000_000_000_000_000,
            Some("settlement reverted"),
        );
        assert_eq!(
            a,
            vec![LifecycleAction::CloseInvalidated {
                reason: "settlement reverted".into()
            }]
        );
    }

    /// wei 文本边界：整数过、分数/溢出/非数响亮。
    #[test]
    fn wei_parsing_is_exact_or_loud() {
        assert_eq!(parse_wei("123").unwrap(), 123);
        assert!(parse_wei("1.5").is_err());
        assert!(parse_wei("not-a-number").is_err());
        assert!(parse_wei("170141183460469231731687303715884105728").is_err());
    }
}
