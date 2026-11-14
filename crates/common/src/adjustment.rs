//! Joint cash/inventory adjustment batches (`POLYMARKET_PREDICT` §6.3 C,
//! §8.5(e)) — the shared accounting core for backtest AND sandbox (paper).
//!
//! The batch is the narrow contract for ONE simulated position operation —
//! settlement, split, merge, redeem, convert — combining exact per-currency
//! cash deltas with multi-leg outcome-inventory changes and explicit cost
//! attribution. Callers only PROPOSE a batch; the apply core
//! [`apply_batch_to_state`] validates everything first and applies it
//! atomically: any failing component leaves no partial cash and no partial
//! token change, a repeated `operation_id` is never re-applied, and no fake
//! `OrderFilled` ever initializes an inventory leg.
//!
//! The types and constructors were moved verbatim from
//! `nautilus_backtest::modules::adjustment` (which re-exports them — the
//! backtest API is unchanged); the apply core is the verbatim extraction of
//! `SimulatedExchange::apply_adjustment_batch` so the sandbox execution
//! client can run the exact same semantics on live-paper state.
//!
//! Conservation lives in the pure accounting functions of
//! `nautilus_model::prediction` (split/merge/convert); the batch
//! constructors here turn their outputs into batches so callers cannot
//! hand-write an unbalanced one for those actions.

use std::{cell::RefCell, collections::HashSet, rc::Rc};

use nautilus_core::UUID4;
use nautilus_model::{
    enums::OmsType,
    events::{PositionAdjusted, PositionEvent},
    identifiers::{AccountId, InstrumentId, PositionId, StrategyId, TraderId},
    instruments::{Instrument, InstrumentAny},
    position::Position,
    prediction::{convert_tokens, merge_leg_accounting, split_leg_accounting},
    types::{Money, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::{
    cache::Cache,
    clock::Clock,
    msgbus::{self, switchboard},
};

/// Negates a finite money amount.
///
/// # Panics
///
/// Panics only if the negation of a finite fee exceeds [`Money`] bounds
/// (excluded by Money's own representation).
fn negate(fee: &Money) -> Money {
    Money::from_decimal(-fee.as_decimal(), fee.currency)
        .expect("negation of a finite fee cannot overflow Money bounds")
}

/// The operation a batch represents — exactly the actions a real
/// prediction-market position operation can take.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdjustmentAction {
    /// Collateral → complete outcome sets.
    Split,
    /// Complete outcome sets → collateral.
    Merge,
    /// Redeem resolved outcome shares for collateral.
    Redeem,
    /// Predict neg-risk conversion between outcome tokens.
    Convert,
}

impl AdjustmentAction {
    /// The position-event vocabulary for every action's inventory changes.
    #[must_use]
    pub const fn position_adjustment_type(
        self,
    ) -> nautilus_model::enums::PositionAdjustmentType {
        let _ = self;
        nautilus_model::enums::PositionAdjustmentType::Adjustment
    }
}

/// Why the batch exists — explicit instruction or frozen settlement
/// material. Nothing automatic ever proposes a split/merge/convert.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdjustmentSource {
    /// An explicit strategy instruction (P7 portals route here).
    StrategyInstruction,
    /// Frozen settlement material (e.g. the certified `InstrumentClose`).
    SettlementMaterial { event_id: UUID4 },
}

/// One outcome-inventory leg of a batch: an exact share change and the
/// position's average open price per share AFTER the change (explicit cost
/// attribution — the caller owns the allocation; the conservation
/// constructors below enforce it for split/merge/convert).
#[derive(Clone, Debug, PartialEq)]
pub struct OutcomeAdjustment {
    /// The outcome instrument whose inventory changes.
    pub instrument_id: InstrumentId,
    /// `None` ESTABLISHES a new position (no fake fill); otherwise the
    /// position the change applies to.
    pub position_id: Option<PositionId>,
    /// Signed share change: positive mints/opens, negative spends/closes.
    pub quantity_change: Decimal,
    /// The position's average cost per share after this change.
    pub avg_px_open_after: Decimal,
    /// Realized PnL attributed to this leg (merge cash share − basis
    /// removed; the pure function's allocation).
    pub realized_pnl: Decimal,
}

/// A complete, atomic adjustment proposal.
#[derive(Clone, Debug)]
pub struct SimulationAdjustmentBatch {
    /// Idempotent identity: one real operation, one id. Re-application is
    /// rejected, never silently repeated.
    pub operation_id: UUID4,
    pub trader_id: TraderId,
    pub strategy_id: StrategyId,
    pub account_id: AccountId,
    pub action: AdjustmentAction,
    pub source: AdjustmentSource,
    /// Exact per-currency cash changes (signed). Pending redemption rights
    /// are NOT spendable cash and never appear here as positive deltas.
    pub cash_deltas: Vec<Money>,
    /// Actual fees charged by this operation (per currency, exact).
    pub fees: Vec<Money>,
    /// The inventory legs.
    pub outcome_adjustments: Vec<OutcomeAdjustment>,
}

impl SimulationAdjustmentBatch {
    /// Builds a SPLIT batch from the pure split accounting: conservation is
    /// structural (the pure function's contract) — cash out equals the
    /// collateral spent plus fees, and each leg's cost attribution is the
    /// pure allocation for its assignment slot.
    ///
    /// # Errors
    ///
    /// Returns an error if collateral or fees are negative, or the pure
    /// accounting rejects the inputs.
    #[expect(
        clippy::too_many_arguments,
        reason = "batch construction carries full ownership, pricing and leg context"
    )]
    pub fn from_split(
        operation_id: UUID4,
        trader_id: TraderId,
        strategy_id: StrategyId,
        account_id: AccountId,
        sets: Decimal,
        collateral_spent: Money,
        assignments: &[(InstrumentId, Option<PositionId>)],
        fees: Vec<Money>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            collateral_spent.as_decimal() >= Decimal::ZERO,
            "split collateral spent must be non-negative"
        );
        anyhow::ensure!(
            fees.iter().all(|fee| fee.as_decimal() >= Decimal::ZERO),
            "split fees must be non-negative"
        );
        // On-chain a split spends exactly one collateral per complete set.
        anyhow::ensure!(
            collateral_spent.as_decimal() == sets,
            "split collateral {collateral_spent_as} != sets {sets} (1 collateral per set)",
            collateral_spent_as = collateral_spent.as_decimal()
        );
        anyhow::ensure!(
            assignments
                .iter()
                .all(|(_, position_id)| position_id.is_none()),
            "split legs open NEW positions — assigning to an existing position would silently reprice its basis"
        );
        let accounting = split_leg_accounting(sets, assignments.len(), collateral_spent.as_decimal())
            .map_err(|e| anyhow::anyhow!("split accounting: {e}"))?;
        let outcome_adjustments = assignments
            .iter()
            .zip(accounting.iter())
            .map(|((instrument_id, position_id), computed)| OutcomeAdjustment {
                instrument_id: *instrument_id,
                position_id: *position_id,
                quantity_change: computed.quantity,
                avg_px_open_after: computed.cost_per_share,
                realized_pnl: Decimal::ZERO,
            })
            .collect();
        let cash_delta = Money::from_decimal(
            -collateral_spent.as_decimal(),
            collateral_spent.currency,
        )
        .map_err(|e| anyhow::anyhow!("split cash delta: {e:?}"))?;
        Ok(Self {
            operation_id,
            trader_id,
            strategy_id,
            account_id,
            action: AdjustmentAction::Split,
            source: AdjustmentSource::StrategyInstruction,
            cash_deltas: std::iter::once(cash_delta)
                .chain(fees.iter().map(negate))
                .collect(),
            fees,
            outcome_adjustments,
        })
    }

    /// Builds a MERGE batch from the pure merge accounting: cash back plus
    /// fee spend, inventory consumed per leg, cost conservation structural.
    ///
    /// # Errors
    ///
    /// Returns an error if fees are negative or the pure accounting rejects
    /// the inputs.
    #[expect(
        clippy::too_many_arguments,
        reason = "batch construction carries full ownership, pricing and leg context"
    )]
    pub fn from_merge(
        operation_id: UUID4,
        trader_id: TraderId,
        strategy_id: StrategyId,
        account_id: AccountId,
        sets: Decimal,
        legs: &[(InstrumentId, Option<PositionId>, Decimal)],
        fees: Vec<Money>,
        currency: nautilus_model::types::Currency,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            fees.iter().all(|fee| fee.as_decimal() >= Decimal::ZERO),
            "merge fees must be non-negative"
        );
        let (cash_return, accounting) = merge_leg_accounting(
            sets,
            &legs.iter()
                .map(|(_, _, cost_per_share)| *cost_per_share)
                .collect::<Vec<_>>(),
        )
        .map_err(|e| anyhow::anyhow!("merge accounting: {e}"))?;
        let outcome_adjustments = legs
            .iter()
            .zip(accounting.iter())
            .map(|((instrument_id, position_id, _), computed)| OutcomeAdjustment {
                instrument_id: *instrument_id,
                position_id: *position_id,
                quantity_change: -computed.quantity,
                // Fully consumed legs close their position.
                avg_px_open_after: Decimal::ZERO,
                realized_pnl: computed.realized_pnl,
            })
            .collect();
        let cash_return = Money::from_decimal(cash_return, currency)
            .map_err(|e| anyhow::anyhow!("merge cash return: {e:?}"))?;
        Ok(Self {
            operation_id,
            trader_id,
            strategy_id,
            account_id,
            action: AdjustmentAction::Merge,
            source: AdjustmentSource::StrategyInstruction,
            cash_deltas: std::iter::once(cash_return)
                .chain(fees.iter().map(negate))
                .collect(),
            fees,
            outcome_adjustments,
        })
    }

    /// Builds a REDEEM batch from frozen settlement material: resolved
    /// outcome shares redeemed for collateral at the certified payout.
    ///
    /// # Errors
    ///
    /// Returns an error if the payout is outside `[0, 1]` or fees are
    /// negative.
    #[expect(
        clippy::too_many_arguments,
        reason = "batch construction carries full ownership, settlement and leg context"
    )]
    pub fn from_redeem(
        operation_id: UUID4,
        trader_id: TraderId,
        strategy_id: StrategyId,
        account_id: AccountId,
        legs: &[(InstrumentId, PositionId, Decimal)],
        payout: Decimal,
        settlement_event_id: UUID4,
        currency: nautilus_model::types::Currency,
        fees: Vec<Money>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (Decimal::ZERO..=Decimal::ONE).contains(&payout),
            "redeem payout {payout} outside [0,1]"
        );
        anyhow::ensure!(
            fees.iter().all(|fee| fee.as_decimal() >= Decimal::ZERO),
            "redeem fees must be non-negative"
        );
        let mut cash_total = Decimal::ZERO;
        let outcome_adjustments = legs
            .iter()
            .map(|(instrument_id, position_id, quantity)| {
                cash_total += *quantity * payout;
                OutcomeAdjustment {
                    instrument_id: *instrument_id,
                    position_id: Some(*position_id),
                    quantity_change: -*quantity,
                    avg_px_open_after: Decimal::ZERO,
                    realized_pnl: Decimal::ZERO,
                }
            })
            .collect();
        let cash_return = Money::from_decimal(cash_total, currency)
            .map_err(|e| anyhow::anyhow!("redeem proceeds: {e:?}"))?;
        Ok(Self {
            operation_id,
            trader_id,
            strategy_id,
            account_id,
            action: AdjustmentAction::Redeem,
            source: AdjustmentSource::SettlementMaterial {
                event_id: settlement_event_id,
            },
            cash_deltas: std::iter::once(cash_return)
                .chain(fees.iter().map(negate))
                .collect(),
            fees,
            outcome_adjustments,
        })
    }

    /// Builds a CONVERT batch from the pure convert accounting: basis moves
    /// 1:1 from source to target at the same per-share cost.
    ///
    /// # Errors
    ///
    /// Returns an error if the pure accounting rejects the conversion.
    #[expect(
        clippy::too_many_arguments,
        reason = "batch construction carries full ownership, pricing and leg context"
    )]
    pub fn from_convert(
        operation_id: UUID4,
        trader_id: TraderId,
        strategy_id: StrategyId,
        account_id: AccountId,
        source: InstrumentId,
        source_position: Option<PositionId>,
        source_cost_per_share: Decimal,
        target: InstrumentId,
        quantity: Decimal,
        fees: Vec<Money>,
    ) -> anyhow::Result<Self> {
        let accounting = convert_tokens(quantity, source_cost_per_share)
            .map_err(|e| anyhow::anyhow!("convert accounting: {e}"))?;
        let outcome_adjustments = vec![
            OutcomeAdjustment {
                instrument_id: source,
                position_id: source_position,
                quantity_change: -accounting.quantity,
                avg_px_open_after: source_cost_per_share,
                realized_pnl: Decimal::ZERO,
            },
            OutcomeAdjustment {
                instrument_id: target,
                position_id: None,
                quantity_change: accounting.quantity,
                avg_px_open_after: accounting.cost_per_share,
                realized_pnl: Decimal::ZERO,
            },
        ];
        Ok(Self {
            operation_id,
            trader_id,
            strategy_id,
            account_id,
            action: AdjustmentAction::Convert,
            source: AdjustmentSource::StrategyInstruction,
            cash_deltas: fees.iter().map(negate).collect(),
            fees,
            outcome_adjustments,
        })
    }

    /// Structural validation independent of account state: non-empty,
    /// per-instrument exactness at the given size precision, no duplicate
    /// instruments, decrements reference existing positions.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first violated rule.
    ///
    /// # Panics
    ///
    /// Panics only if a size precision exceeds `u64` scale (excluded by the
    /// instrument's own precision bounds).
    pub fn validate_structure(
        &self,
        size_precision: impl Fn(&InstrumentId) -> Option<u8>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.outcome_adjustments.is_empty() || !self.cash_deltas.is_empty(),
            "batch {} carries no changes",
            self.operation_id
        );
        let mut seen = HashSet::new();
        for leg in &self.outcome_adjustments {
            anyhow::ensure!(
                seen.insert(leg.instrument_id),
                "batch {} repeats instrument {}",
                self.operation_id,
                leg.instrument_id
            );
            anyhow::ensure!(
                leg.quantity_change != Decimal::ZERO,
                "batch {} leg {} carries a zero quantity change",
                self.operation_id,
                leg.instrument_id
            );
            let Some(precision) = size_precision(&leg.instrument_id) else {
                anyhow::bail!(
                    "batch {} references unknown instrument {}",
                    self.operation_id,
                    leg.instrument_id
                );
            };
            let scale = 10u64
                .checked_pow(u32::from(precision))
                .expect("size precision within u64 scale");
            anyhow::ensure!(
                (leg.quantity_change * Decimal::from(scale)).fract() == Decimal::ZERO,
                "batch {} quantity change {} for {} is not exact at precision {precision}",
                self.operation_id,
                leg.quantity_change,
                leg.instrument_id
            );
            if leg.quantity_change < Decimal::ZERO {
                anyhow::ensure!(
                    leg.position_id.is_some(),
                    "batch {} decrements {} without a position reference",
                    self.operation_id,
                    leg.instrument_id
                );
            }
            anyhow::ensure!(
                leg.avg_px_open_after >= Decimal::ZERO,
                "batch {} negative cost attribution for {}",
                self.operation_id,
                leg.instrument_id
            );
        }
        for fee in &self.fees {
            anyhow::ensure!(
                fee.as_decimal() >= Decimal::ZERO,
                "batch {} carries a negative fee",
                self.operation_id
            );
        }
        Ok(())
    }
}

/// Application-phase failure: raised only AFTER validation passed and the
/// idempotence ledger recorded the operation id, so account state may already
/// be partially applied. The backtest maps this onto its callback-error
/// freeze; the sandbox logs it loudly instead of panicking the engine
/// thread.
///
/// Semantic note (deliberate divergence from the pre-extraction template):
/// `SimulatedExchange::apply_adjustment_batch` emitted two near-identical
/// strings per site (a callback-error form `adjustment batch {id} …` and a
/// bail form `adjustment {id} …`). The shared core keeps the callback-error
/// wording for the single error both consumers see — phase semantics (which
/// failures freeze the backtest) are preserved exactly.
#[derive(Debug)]
pub struct AdjustmentApplyFailure(pub String);

impl std::fmt::Display for AdjustmentApplyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for AdjustmentApplyFailure {}

fn adjust_failure(message: String) -> anyhow::Error {
    anyhow::Error::new(AdjustmentApplyFailure(message))
}

/// Applies a joint cash/inventory adjustment batch atomically against
/// exchange-agnostic state (`POLYMARKET_PREDICT` §6.3 C; shared by backtest
/// `SimulatedExchange` and the sandbox execution client since §8.5(e)).
///
/// Everything is validated BEFORE anything changes: instruments exist,
/// quantity changes are exact at each instrument's size precision,
/// decrements reference cached positions with sufficient quantity, and
/// the account can absorb every negative cash delta. Application then
/// runs within this one call — cash via `adjust_cash`, inventory via
/// `PositionAdjusted` on cached positions. A repeated `operation_id` is
/// rejected, never re-applied.
///
/// This is the verbatim extraction of the former
/// `SimulatedExchange::apply_adjustment_batch` body; substitutions are
/// exactly: `self.applied_adjustments` → `applied`, `self.frozen_account`
/// → `frozen`, `self.instruments` → `instruments`, `self.adjust_account`
/// → `adjust_cash`, `self.oms_type` → `oms_type`, `self.cache`/`self.clock`
/// → `cache`/`clock`, and the callback-error freeze → the
/// [`AdjustmentApplyFailure`] marker.
///
/// # Errors
///
/// Returns an error naming the first violated rule; on failure no partial
/// cash change remains from validation rejections (validation precedes
/// application). Application-phase failures are marked
/// [`AdjustmentApplyFailure`].
///
/// # Panics
///
/// Never panics.
#[expect(
    clippy::too_many_arguments,
    reason = "explicit state plumbing keeps the core exchange-agnostic (backtest and sandbox)"
)]
pub fn apply_batch_to_state(
    cache: &Rc<RefCell<Cache>>,
    clock: &Rc<RefCell<dyn Clock>>,
    applied: &mut HashSet<UUID4>,
    instruments: &impl Fn(&InstrumentId) -> Option<InstrumentAny>,
    adjust_cash: &mut impl FnMut(Money) -> bool,
    oms_type: OmsType,
    frozen: bool,
    batch: &SimulationAdjustmentBatch,
) -> anyhow::Result<()> {
    if applied.contains(&batch.operation_id) {
        anyhow::bail!(
            "adjustment operation {} already applied — never re-applied",
            batch.operation_id
        );
    }
    anyhow::ensure!(
        !frozen,
        "adjustment {} rejected: account is frozen",
        batch.operation_id
    );

    // ---- validate everything first (no state touched yet) ----
    let size_precision = |instrument_id: &InstrumentId| -> Option<u8> {
        instruments(instrument_id).map(|instrument| instrument.size_precision())
    };
    batch.validate_structure(size_precision)?;

    // Inventory: existence, instrument and OWNER equality for every
    // positioned leg (any sign), sufficiency for decrements.
    {
        let cache = cache.borrow();
        for leg in &batch.outcome_adjustments {
            let Some(position_id) = leg.position_id else {
                continue;
            };
            let Some(position) = cache.position(&position_id) else {
                anyhow::bail!(
                    "adjustment {} references missing position {position_id}",
                    batch.operation_id
                );
            };
            anyhow::ensure!(
                position.instrument_id == leg.instrument_id,
                "adjustment {} position {position_id} belongs to {}, not {}",
                batch.operation_id,
                position.instrument_id,
                leg.instrument_id
            );
            anyhow::ensure!(
                position.account_id == batch.account_id
                    && position.strategy_id == batch.strategy_id
                    && position.trader_id == batch.trader_id,
                "adjustment {} position {position_id} owned by {}/{}/{}, not the batch owner",
                batch.operation_id,
                position.trader_id,
                position.strategy_id,
                position.account_id
            );
            if leg.quantity_change < Decimal::ZERO {
                anyhow::ensure!(
                    position.quantity.as_decimal() + leg.quantity_change >= Decimal::ZERO,
                    "adjustment {} overdrafts position {position_id}: {} {}",
                    batch.operation_id,
                    position.quantity.as_decimal(),
                    leg.quantity_change
                );
            }
        }
    }

    // Cash sufficiency: deltas aggregate PER CURRENCY (collateral plus
    // fees arrive as separate same-currency deltas).
    {
        let cache = cache.borrow();
        let Some(account) = cache.account(&batch.account_id) else {
            anyhow::bail!(
                "adjustment {} references missing account {}",
                batch.operation_id,
                batch.account_id
            );
        };
        let mut totals: std::collections::HashMap<nautilus_model::types::Currency, Decimal> =
            std::collections::HashMap::new();
        for delta in &batch.cash_deltas {
            *totals.entry(delta.currency).or_default() += delta.as_decimal();
        }
        for (currency, total) in totals {
            if total >= Decimal::ZERO {
                continue;
            }
            let balances = account.balances();
            let Some(balance) = balances.get(&currency) else {
                anyhow::bail!(
                    "adjustment {} draws on missing {currency} balance",
                    batch.operation_id
                );
            };
            anyhow::ensure!(
                balance.free.as_decimal() + total >= Decimal::ZERO,
                "adjustment {} overdrafts {currency} free balance: {} {}",
                batch.operation_id,
                balance.free.as_decimal(),
                total
            );
        }
    }

    // Validation passed — the idempotence ledger records the id now
    // (a rejected batch above never burned it).
    applied.insert(batch.operation_id);

    // ---- apply: cash first (validation guarantees no mid-way
    // failure; an unexpected one aborts loudly with the caller-side
    // freeze rather than leaving partial state hidden).
    for delta in &batch.cash_deltas {
        if !adjust_cash(*delta) {
            return Err(adjust_failure(format!(
                "adjustment batch {} failed applying cash delta {delta}",
                batch.operation_id
            )));
        }
    }

    let ts_init = clock.borrow().timestamp_ns();
    for leg in &batch.outcome_adjustments {
        let Some(position_id) = leg.position_id else {
            // Establish a NEW position with NO fake fill (§6.3 C): the
            // explicit opening constructor sets exact state.
            let instrument = instruments(&leg.instrument_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "adjustment {} references unknown instrument {}",
                    batch.operation_id,
                    leg.instrument_id
                )
            })?;
            let quantity =
                Quantity::from_decimal_dp(leg.quantity_change, instrument.size_precision())
                    .map_err(|e| {
                        adjust_failure(format!(
                            "adjustment batch {} opening quantity failed: {e:?}",
                            batch.operation_id
                        ))
                    })?;
            // Netting engines route fills to
            // `{instrument_id}-{strategy_id}`: minting any other id
            // would orphan the inventory from later fills. An already
            // open netting position absorbs the change instead of
            // splitting into a second position.
            let netting_id = PositionId::from(format!(
                "{}-{}",
                leg.instrument_id, batch.strategy_id
            ));
            let existing_netting = {
                let cache = cache.borrow();
                cache
                    .position(&netting_id)
                    .filter(|position| position.is_open())
                    .map(|position| position.id)
            };
            if let Some(existing) = existing_netting {
                let mut adjustment = PositionAdjusted::new(
                    batch.trader_id,
                    batch.strategy_id,
                    leg.instrument_id,
                    existing,
                    batch.account_id,
                    batch.action.position_adjustment_type(),
                    Some(leg.quantity_change),
                    None,
                    Some(Ustr::from(&format!(
                        "adjustment_batch:{}",
                        batch.operation_id
                    ))),
                    UUID4::new(),
                    ts_init,
                    ts_init,
                );
                // The absorbed tranche carries its allocated basis
                // (§6.3 C): without it the position keeps the old
                // average and the allocation vanishes.
                adjustment.cost_basis_added =
                    Some(leg.quantity_change * leg.avg_px_open_after);
                {
                    let mut cache = cache.borrow_mut();
                    if let Err(e) = cache.update_position_from_adjustment(existing, adjustment) {
                        return Err(adjust_failure(format!(
                            "adjustment batch {} failed updating position {existing}: {e}",
                            batch.operation_id
                        )));
                    }
                }
                let event = PositionEvent::PositionAdjusted(adjustment);
                let topic = switchboard::get_event_position_topic(batch.strategy_id);
                msgbus::publish_position_event(topic, &event);
                continue;
            }
            let position_id = netting_id;
            let position = Position::from_adjustment_opening(
                &instrument,
                batch.trader_id,
                batch.strategy_id,
                batch.account_id,
                position_id,
                quantity,
                leg.avg_px_open_after,
                ts_init,
                ts_init,
            )
            .map_err(|e| {
                adjust_failure(format!(
                    "adjustment batch {} opening position failed: {e:?}",
                    batch.operation_id
                ))
            })?;
            {
                let mut cache = cache.borrow_mut();
                cache
                    .add_position_without_order(&position, oms_type)
                    .map_err(|e| {
                        adjust_failure(format!(
                            "adjustment batch {} failed caching position {position_id}: {e}",
                            batch.operation_id
                        ))
                    })?;
            }
            let adjustment = PositionAdjusted::new(
                batch.trader_id,
                batch.strategy_id,
                leg.instrument_id,
                position_id,
                batch.account_id,
                batch.action.position_adjustment_type(),
                Some(leg.quantity_change),
                None,
                Some(Ustr::from(&format!(
                    "adjustment_batch:{}",
                    batch.operation_id
                ))),
                UUID4::new(),
                ts_init,
                ts_init,
            );
            let event = PositionEvent::PositionAdjusted(adjustment);
            let topic = switchboard::get_event_position_topic(batch.strategy_id);
            msgbus::publish_position_event(topic, &event);
            continue;
        };
        let pnl_change = (!leg.realized_pnl.is_zero()).then(|| {
            let currency = instruments(&leg.instrument_id)
                .map(|instrument| instrument.quote_currency())
                .expect("instrument validated earlier");
            Money::from_decimal(leg.realized_pnl, currency)
                .expect("realized PnL within Money bounds")
        });
        let adjustment = PositionAdjusted::new(
            batch.trader_id,
            batch.strategy_id,
            leg.instrument_id,
            position_id,
            batch.account_id,
            batch.action.position_adjustment_type(),
            Some(leg.quantity_change),
            pnl_change,
            Some(Ustr::from(&format!(
                "adjustment_batch:{}",
                batch.operation_id
            ))),
            UUID4::new(),
            ts_init,
            ts_init,
        );
        {
            let mut cache = cache.borrow_mut();
            if let Err(e) = cache.update_position_from_adjustment(position_id, adjustment) {
                return Err(adjust_failure(format!(
                    "adjustment batch {} failed updating position {position_id}: {e}",
                    batch.operation_id
                )));
            }
        }
        let event = PositionEvent::PositionAdjusted(adjustment);
        let topic = switchboard::get_event_position_topic(batch.strategy_id);
        msgbus::publish_position_event(topic, &event);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_model::identifiers::InstrumentId;
    use nautilus_model::types::Currency;
    use rust_decimal_macros::dec;

    fn ids() -> (TraderId, StrategyId, AccountId) {
        (
            TraderId::from("TESTER-000"),
            StrategyId::from("S-001"),
            AccountId::from("SIM-001"),
        )
    }

    #[test]
    fn split_batch_conserves_cash_and_inventory() {
        let (trader, strategy, account) = ids();
        let yes = InstrumentId::from("0xc-111.PREDICT");
        let no = InstrumentId::from("0xc-222.PREDICT");
        let batch = SimulationAdjustmentBatch::from_split(
            UUID4::new(),
            trader,
            strategy,
            account,
            dec!(5),
            Money::from_decimal(dec!(5), Currency::USD()).unwrap(),
            &[(yes, None), (no, None)],
            vec![],
        )
        .unwrap();
        // Cash out = collateral; both legs at `sets` shares.
        assert_eq!(batch.cash_deltas.len(), 1);
        assert_eq!(batch.cash_deltas[0].as_decimal(), dec!(-5));
        assert_eq!(batch.outcome_adjustments[0].quantity_change, dec!(5));
        assert_eq!(batch.outcome_adjustments[1].quantity_change, dec!(5));
        // Σ(leg qty × per-share cost) == collateral (structural conservation).
        let total: Decimal = batch
            .outcome_adjustments
            .iter()
            .map(|leg| leg.quantity_change * leg.avg_px_open_after)
            .sum();
        assert_eq!(total, dec!(5));
    }

    #[test]
    fn merge_batch_consumes_legs_and_returns_cash() {
        let (trader, strategy, account) = ids();
        let yes = InstrumentId::from("0xc-111.PREDICT");
        let no = InstrumentId::from("0xc-222.PREDICT");
        let batch = SimulationAdjustmentBatch::from_merge(
            UUID4::new(),
            trader,
            strategy,
            account,
            dec!(5),
            &[
                (yes, Some(PositionId::from("P-1")), dec!(0.5)),
                (no, Some(PositionId::from("P-2")), dec!(0.4)),
            ],
            vec![],
            Currency::USD(),
        )
        .unwrap();
        // Cash back = sets (1 collateral per complete set).
        assert_eq!(batch.cash_deltas[0].as_decimal(), dec!(5));
        for leg in &batch.outcome_adjustments {
            assert_eq!(leg.quantity_change, dec!(-5));
            assert!(leg.position_id.is_some());
        }
    }

    #[test]
    fn convert_batch_moves_basis_one_to_one() {
        let (trader, strategy, account) = ids();
        let source = InstrumentId::from("0xc-111.PREDICT");
        let target = InstrumentId::from("0xc-999.PREDICT");
        let batch = SimulationAdjustmentBatch::from_convert(
            UUID4::new(),
            trader,
            strategy,
            account,
            source,
            Some(PositionId::from("P-1")),
            dec!(0.4),
            target,
            dec!(10),
            vec![],
        )
        .unwrap();
        assert_eq!(batch.outcome_adjustments[0].quantity_change, dec!(-10));
        assert_eq!(batch.outcome_adjustments[1].quantity_change, dec!(10));
        // Basis conservation: same per-share cost on both sides.
        assert_eq!(
            batch.outcome_adjustments[0].avg_px_open_after,
            batch.outcome_adjustments[1].avg_px_open_after
        );
    }

    #[test]
    fn structure_validation_rejects_inexact_and_dangling_legs() {
        let (trader, strategy, account) = ids();
        let id = InstrumentId::from("0xc-111.PREDICT");
        let batch = SimulationAdjustmentBatch {
            operation_id: UUID4::new(),
            trader_id: trader,
            strategy_id: strategy,
            account_id: account,
            action: AdjustmentAction::Split,
            source: AdjustmentSource::StrategyInstruction,
            cash_deltas: vec![],
            fees: vec![],
            outcome_adjustments: vec![OutcomeAdjustment {
                instrument_id: id,
                position_id: None,
                quantity_change: dec!(-1.5), // inexact at precision 0
                avg_px_open_after: dec!(0.5),
                realized_pnl: dec!(0),
            }],
        };
        let precision = |_: &InstrumentId| Some(0u8);
        let error = batch.validate_structure(precision).unwrap_err();
        assert!(error.to_string().contains("not exact"), "{error}");

        let mut decrement = batch;
        decrement.outcome_adjustments[0].quantity_change = dec!(-1);
        let error = decrement.validate_structure(precision).unwrap_err();
        assert!(error.to_string().contains("without a position"), "{error}");
    }
}
