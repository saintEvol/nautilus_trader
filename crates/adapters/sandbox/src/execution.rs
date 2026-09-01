// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Sandbox execution client implementation.

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashSet},
    fmt::Debug,
    rc::Rc,
};

use ahash::AHashMap;
use async_trait::async_trait;
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::Clock,
    factories::OrderEventFactory,
    live::try_get_exec_event_sender,
    messages::{
        ExecutionEvent,
        execution::{
            BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder,
            GenerateFillReports, GenerateOrderStatusReport, GenerateOrderStatusReports,
            GeneratePositionStatusReports, ModifyOrder, QueryAccount, QueryOrder, SubmitOrder,
            SubmitOrderList,
        },
    },
    msgbus::{
        self, MStr, MessagingSwitchboard, Pattern, TypedHandler,
        switchboard, typed_handler::ShareableMessageHandler,
    },
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{UUID4, UnixNanos, WeakCell, datetime::NANOSECONDS_IN_SECOND};
use nautilus_execution::{
    client::core::ExecutionClientCore,
    funding,
    matching_engine::OrderMatchingEngine,
    models::{fee::FeeModelHandle, fill::FillModelHandle},
};
use nautilus_model::{
    accounts::AccountAny,
    data::{
        Bar, FundingRateUpdate, InstrumentClose, InstrumentStatus, OrderBookDeltas, QuoteTick,
        TradeTick,
    },
    enums::{OmsType, PositionAdjustmentType},
    events::{
        FundingSettlement, OrderEventAny, PositionAdjusted, PositionEvent,
    },
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    position::Position,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Money, Price},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::config::SandboxExecutionClientConfig;

/// Interval between periodic sweeps that retire expired matching engines with no open position.
///
/// This bounds retained matching-engine and cache state for quote-only instruments that expire
/// without an `InstrumentClose`, expired-order, or `PositionClosed` event to trigger cleanup.
const EXPIRED_ENGINE_SWEEP_INTERVAL_NS: u64 = 60 * NANOSECONDS_IN_SECOND;

/// Inner state for the sandbox execution client.
///
/// This is wrapped in `Rc<RefCell<>>` so message handlers can hold weak references.
struct SandboxInner {
    /// Dynamic clock for matching engines.
    clock: Rc<RefCell<dyn Clock>>,
    /// Reference to the cache.
    cache: Rc<RefCell<Cache>>,
    /// The sandbox configuration.
    config: SandboxExecutionClientConfig,
    /// Matching engines per instrument.
    matching_engines: AHashMap<InstrumentId, OrderMatchingEngine>,
    /// Next raw ID assigned to a matching engine.
    next_engine_raw_id: u32,
    /// Current account balances.
    balances: AHashMap<String, Money>,
    event_handler: Option<Rc<dyn Fn(OrderEventAny)>>,
    /// The client ID (for timer naming).
    client_id: ClientId,
    /// Factory for account state events, built from the same configuration
    /// inputs as the client's (the settlement path lives on the inner state).
    account_state_factory: OrderEventFactory,
    /// Pending funding settlements keyed by (boundary, instrument ID).
    pending_funding: BTreeMap<(UnixNanos, InstrumentId), FundingRateUpdate>,
    /// Funding boundaries already settled (idempotency record).
    settled_funding: HashSet<(UnixNanos, InstrumentId)>,
    /// Wall-clock settlement alert callback; captures a weak self-reference and
    /// is assigned in [`SandboxExecutionClient::new`] once the `Rc` exists.
    funding_timer_callback: TimeEventCallback,
}

fn check_quote_or_drop(context: &str, quote: &QuoteTick, instrument: &InstrumentAny) -> bool {
    if quote_matches_instrument_precision(quote, instrument) {
        return true;
    }

    log::warn!(
        "Dropping {context} for {} due to precision mismatch \
         (bid_px={}, ask_px={}, bid_sz={}, ask_sz={}, expected_price={}, expected_size={})",
        instrument.id(),
        quote.bid_price.precision,
        quote.ask_price.precision,
        quote.bid_size.precision,
        quote.ask_size.precision,
        instrument.price_precision(),
        instrument.size_precision(),
    );
    false
}

fn check_trade_or_drop(context: &str, trade: &TradeTick, instrument: &InstrumentAny) -> bool {
    if trade_matches_instrument_precision(trade, instrument) {
        return true;
    }

    log::warn!(
        "Dropping {context} for {} due to precision mismatch \
         (px={}, sz={}, expected_price={}, expected_size={})",
        instrument.id(),
        trade.price.precision,
        trade.size.precision,
        instrument.price_precision(),
        instrument.size_precision(),
    );
    false
}

fn check_bar_or_drop(context: &str, bar: &Bar, instrument: &InstrumentAny) -> bool {
    if bar_matches_instrument_precision(bar, instrument) {
        return true;
    }

    log::warn!(
        "Dropping {context} for {} due to precision mismatch \
         (open={}, high={}, low={}, close={}, volume={}, expected_price={}, expected_size={})",
        instrument.id(),
        bar.open.precision,
        bar.high.precision,
        bar.low.precision,
        bar.close.precision,
        bar.volume.precision,
        instrument.price_precision(),
        instrument.size_precision(),
    );
    false
}

fn quote_matches_instrument_precision(quote: &QuoteTick, instrument: &InstrumentAny) -> bool {
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    quote.bid_price.precision == price_precision
        && quote.ask_price.precision == price_precision
        && quote.bid_size.precision == size_precision
        && quote.ask_size.precision == size_precision
}

fn trade_matches_instrument_precision(trade: &TradeTick, instrument: &InstrumentAny) -> bool {
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    trade.price.precision == price_precision && trade.size.precision == size_precision
}

fn bar_matches_instrument_precision(bar: &Bar, instrument: &InstrumentAny) -> bool {
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    bar.open.precision == price_precision
        && bar.high.precision == price_precision
        && bar.low.precision == price_precision
        && bar.close.precision == price_precision
        && bar.volume.precision == size_precision
}

impl SandboxInner {
    /// Ensures a matching engine exists for the given instrument.
    fn ensure_matching_engine(&mut self, instrument: &InstrumentAny) {
        let instrument_id = instrument.id();

        if !self.matching_engines.contains_key(&instrument_id) {
            let engine_config = self.config.to_matching_engine_config();
            let fill_model = FillModelHandle::default();
            let fee_model = self
                .config
                .fee_model
                .clone()
                .map(FeeModelHandle::from)
                .unwrap_or_default();
            let raw_id = self.next_engine_raw_id;
            self.next_engine_raw_id = self.next_engine_raw_id.wrapping_add(1);

            let mut engine = OrderMatchingEngine::new(
                instrument.clone(),
                raw_id,
                fill_model,
                fee_model,
                self.config.book_type,
                self.config.oms_type,
                self.config.account_type,
                self.clock.clone(),
                self.cache.clone(),
                engine_config,
            );

            if let Some(handler) = &self.event_handler {
                engine.set_event_handler(handler.clone());
            }

            self.matching_engines.insert(instrument_id, engine);
        }
    }

    /// Processes a quote tick through the matching engine.
    fn process_quote_tick(&mut self, quote: &QuoteTick) {
        let instrument_id = quote.instrument_id;

        // Try to get instrument from cache, create engine if found
        let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
        if let Some(instrument) = instrument {
            if !check_quote_or_drop("quote tick", quote, &instrument) {
                return;
            }

            self.ensure_matching_engine(&instrument);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                engine.process_quote_tick(quote);
            }

            // A fresh top-of-book may unblock a pending funding settlement that
            // was waiting on a settlement price.
            self.settle_funding_due();
        }
    }

    /// Processes a trade tick through the matching engine.
    fn process_trade_tick(&mut self, trade: &TradeTick) {
        if !self.config.trade_execution {
            return;
        }

        let instrument_id = trade.instrument_id;

        let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
        if let Some(instrument) = instrument {
            if !check_trade_or_drop("trade tick", trade, &instrument) {
                return;
            }

            self.ensure_matching_engine(&instrument);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                engine.process_trade_tick(trade);
            }
        }
    }

    /// Processes a bar through the matching engine.
    fn process_bar(&mut self, bar: &Bar) {
        if !self.config.bar_execution {
            return;
        }

        let instrument_id = bar.bar_type.instrument_id();

        let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
        if let Some(instrument) = instrument {
            if !check_bar_or_drop("bar", bar, &instrument) {
                return;
            }

            self.ensure_matching_engine(&instrument);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                engine.process_bar(bar);
            }
        }
    }

    fn process_order_book_deltas(&mut self, deltas: &OrderBookDeltas) {
        let instrument_id = deltas.instrument_id;

        let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
        if let Some(instrument) = instrument {
            self.ensure_matching_engine(&instrument);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id)
                && let Err(e) = engine.process_order_book_deltas(deltas)
            {
                log::error!("Error processing order book deltas: {e}");
            }
        }
    }

    fn process_instrument_status(&mut self, status: &InstrumentStatus) {
        let instrument_id = status.instrument_id;

        if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
            engine.process_status(status.action);
            return;
        }

        let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
        if let Some(instrument) = instrument {
            self.ensure_matching_engine(&instrument);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                engine.process_status(status.action);
            }
        } else {
            log::warn!(
                "Ignoring instrument status for {instrument_id}: instrument missing from cache",
            );
        }
    }

    fn process_instrument_close(&mut self, close: &InstrumentClose) {
        let instrument_id = close.instrument_id;

        // A delayed close belongs to an existing exposure lifecycle. Unlike an
        // instrument status update, it must not recreate execution state from
        // cache after rotation/unsubscribe; pending-settlement ownership stays
        // with the already-initialized matching engine.
        if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
            engine.process_instrument_close(*close);
            self.sync_expired_cleanup(instrument_id);
        } else {
            log::warn!(
                "Ignoring instrument close for {instrument_id}: no existing matching engine",
            );
        }
    }

    fn is_expired_now(&self, instrument_id: InstrumentId) -> bool {
        let Some(engine) = self.matching_engines.get(&instrument_id) else {
            return false;
        };

        let now_ns = self.clock.borrow().timestamp_ns();
        engine
            .instrument
            .expiration_ns()
            .is_some_and(|ns| now_ns >= ns)
    }

    fn has_open_orders(&self, instrument_id: InstrumentId) -> bool {
        self.cache.borrow().has_orders_open(
            Some(&self.config.venue),
            Some(&instrument_id),
            None,
            None,
            None,
        )
    }

    fn sync_expired_cleanup(&mut self, instrument_id: InstrumentId) {
        if !self.is_expired_now(instrument_id) {
            return;
        }

        let has_open_positions = self.cache.borrow().has_positions_open(
            Some(&self.config.venue),
            Some(&instrument_id),
            None,
            None,
            None,
        );

        if has_open_positions {
            return;
        }

        self.matching_engines.remove(&instrument_id);
        self.cache
            .borrow_mut()
            .purge_instrument_skip_order_guard(instrument_id);
    }

    fn sync_expired_cleanup_many(&mut self, instrument_ids: &[InstrumentId]) {
        for &instrument_id in instrument_ids {
            self.sync_expired_cleanup(instrument_id);
        }
    }

    /// Retires matching engines whose instrument has expired with no open position or order.
    ///
    /// This is the periodic trigger for quote-only instruments that create a matching engine from
    /// market data but never reach an `InstrumentClose`, expired-order, or `PositionClosed` event.
    /// It performs no settlement: `sync_expired_cleanup` retains any expired engine that still has
    /// an open position.
    ///
    /// Instruments with open orders are retained too. The event-driven callers of
    /// `sync_expired_cleanup` each terminalize order state through the matching engine first, which
    /// is what `Cache::purge_instrument_skip_order_guard` requires of its callers; this sweep has
    /// no such event, so purging here would orphan a resting order behind a removed engine.
    fn sweep_expired_engines(&mut self) {
        let expired_ids: Vec<InstrumentId> = self
            .matching_engines
            .keys()
            .copied()
            .filter(|instrument_id| {
                self.is_expired_now(*instrument_id) && !self.has_open_orders(*instrument_id)
            })
            .collect();

        self.sync_expired_cleanup_many(&expired_ids);
    }

    /// Processes a funding rate update: queues a settlement at its funding boundary.
    ///
    /// Updates defining no boundary (in-period predictor values) are ignored.
    /// Boundaries already past settle immediately; future boundaries arm the
    /// wall-clock settlement alert.
    fn process_funding_rate(&mut self, funding_rate: &FundingRateUpdate) {
        let Some(boundary) = funding::funding_boundary(funding_rate) else {
            log::debug!(
                "Funding rate update for {} does not define a settlement boundary",
                funding_rate.instrument_id
            );
            return;
        };

        let key = (boundary, funding_rate.instrument_id);
        if self.settled_funding.contains(&key) {
            log::debug!(
                "Funding boundary {boundary} for {} already settled",
                funding_rate.instrument_id
            );
            return;
        }

        self.pending_funding.insert(key, *funding_rate);

        let now = self.clock.borrow().timestamp_ns();
        if boundary <= now {
            self.settle_funding_due();
        } else {
            self.arm_funding_settlement_alert();
        }
    }

    /// Settles every pending funding boundary at or before the current time.
    ///
    /// Settlements that fail (no settlement price, arithmetic overflow) stay
    /// pending and are retried by the next funding rate arrival, the next quote
    /// tick, or the periodic sweep.
    fn settle_funding_due(&mut self) {
        if self.pending_funding.is_empty() {
            return;
        }

        let now = self.clock.borrow().timestamp_ns();
        let due: Vec<(UnixNanos, InstrumentId)> = self
            .pending_funding
            .keys()
            .copied()
            .take_while(|(boundary, _)| *boundary <= now)
            .collect();

        for key in due {
            self.settle_funding_boundary(key);
        }

        self.arm_funding_settlement_alert();
    }

    /// Settles one pending funding boundary.
    ///
    /// Computes entries with the shared funding module, applies balance and
    /// position adjustments, then publishes `FundingSettlement`, per-position
    /// `PositionAdjusted`, and the adjusted `AccountState` — the same outbound
    /// shape as the backtest exchange settlement.
    ///
    /// Failures log and retain the pending entry (never silently drop), with
    /// retries driven by [`Self::settle_funding_due`].
    fn settle_funding_boundary(&mut self, key: (UnixNanos, InstrumentId)) {
        let (boundary, instrument_id) = key;
        if self.settled_funding.contains(&key) {
            self.pending_funding.remove(&key);
            return;
        }
        let Some(funding_rate) = self.pending_funding.get(&key).copied() else {
            return;
        };

        // The matching engine owns the book backing the settlement price
        // fallback; create it from the cached instrument when absent (parity
        // with the backtest exchange's add_instrument fallback).
        if !self.matching_engines.contains_key(&instrument_id) {
            let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
            match instrument {
                Some(instrument) => self.ensure_matching_engine(&instrument),
                None => {
                    log::warn!(
                        "Cannot settle funding for {instrument_id}: no matching engine or instrument"
                    );
                    return;
                }
            }
        }

        let account_id = self.config.account_id;
        let open_positions: Vec<Position> = self
            .cache
            .borrow()
            .positions_open(
                Some(&self.config.venue),
                Some(&instrument_id),
                None,
                Some(&account_id),
                None,
            )
            .into_iter()
            .map(|position| position.cloned())
            .collect();

        if open_positions.is_empty() {
            // Consume the boundary with no exchange of value so later updates
            // for it are ignored (backtest parity).
            self.pending_funding.remove(&key);
            self.settled_funding.insert(key);
            return;
        }

        let Some(settlement_price) = self.funding_settlement_price(instrument_id) else {
            log::warn!(
                "Cannot settle funding for {instrument_id} at boundary {boundary}: \
                 no mark price or top-of-book price; retaining pending settlement"
            );
            return;
        };

        let computed = match funding::compute_settlement(
            &funding_rate,
            boundary,
            settlement_price,
            &open_positions,
        ) {
            Ok(computed) => computed,
            Err(e) => {
                log::error!("Cannot settle funding for {instrument_id}: {e}");
                return;
            }
        };

        // Settling a single instrument enforces a uniform settlement currency,
        // so the aggregate account adjustment is a single signed amount.
        let Some(total_adjustment) = computed.total_adjustment() else {
            log::error!(
                "Cannot settle funding for {instrument_id}: aggregate account adjustment overflow"
            );
            return;
        };

        if !self.config.frozen_account {
            let cache = self.cache.borrow();
            let Some(account) = cache.account(&account_id) else {
                log::error!("Cannot settle funding: no account for {account_id}");
                return;
            };
            let Some(balance) = account.balance(Some(total_adjustment.currency)) else {
                log::error!(
                    "Cannot settle funding: no account balance for currency {}",
                    total_adjustment.currency
                );
                return;
            };

            if balance.total.checked_add(total_adjustment).is_none()
                || balance.free.checked_add(total_adjustment).is_none()
            {
                log::error!(
                    "Cannot settle funding: {} account adjustment exceeds Money bounds",
                    total_adjustment.currency
                );
                return;
            }
        }

        let ts_init = self.clock.borrow().timestamp_ns();
        let settlement = FundingSettlement::new(
            self.config.trader_id,
            instrument_id,
            account_id,
            funding_rate.rate,
            settlement_price,
            open_positions[0].settlement_currency,
            UUID4::new(),
            boundary,
            ts_init,
        );

        let mut adjusted_positions = Vec::with_capacity(computed.entries.len());
        for (original, entry) in open_positions.into_iter().zip(computed.entries.iter()) {
            let mut adjusted = original.clone();
            let adjustment = PositionAdjusted::new(
                settlement.trader_id,
                adjusted.strategy_id,
                adjusted.instrument_id,
                adjusted.id,
                adjusted.account_id,
                PositionAdjustmentType::Funding,
                None,
                Some(entry.amount),
                Some(Ustr::from(&format!(
                    "funding_settlement:{}",
                    settlement.event_id
                ))),
                UUID4::new(),
                settlement.ts_event,
                settlement.ts_init,
            );
            adjusted.apply_adjustment(adjustment);
            adjusted_positions.push((original, adjusted, adjustment));
        }

        {
            let mut cache = self.cache.borrow_mut();

            for (index, (_, adjusted, _)) in adjusted_positions.iter().enumerate() {
                if let Err(e) = cache.update_position(adjusted) {
                    log::error!(
                        "Cannot update position {} after funding settlement: {e}",
                        adjusted.id
                    );

                    for (original, _, _) in adjusted_positions[..index].iter().rev() {
                        if let Err(rollback_error) = cache.update_position(original) {
                            log::error!(
                                "Cannot roll back position {} after failed funding settlement: {rollback_error}",
                                original.id
                            );
                        }
                    }
                    return;
                }
            }
        }

        if !self.config.frozen_account {
            let ts_event = self.clock.borrow().timestamp_ns();
            if !self.adjust_account(total_adjustment, ts_event) {
                let mut cache = self.cache.borrow_mut();
                for (original, _, _) in adjusted_positions.iter().rev() {
                    if let Err(e) = cache.update_position(original) {
                        log::error!(
                            "Cannot roll back position {} after failed account adjustment: {e}",
                            original.id
                        );
                    }
                }
                return;
            }
        }

        self.pending_funding.remove(&key);
        self.settled_funding.insert(key);

        let settlement_topic = switchboard::get_funding_settlement_topic(settlement.instrument_id);
        msgbus::publish_any(settlement_topic, &settlement);

        for (_, _, adjustment) in adjusted_positions {
            let event = PositionEvent::PositionAdjusted(adjustment);
            let topic = switchboard::get_event_position_topic(adjustment.strategy_id);
            msgbus::publish_position_event(topic, &event);
        }
    }

    /// Returns the funding settlement price: the cached mark price preferred,
    /// falling back to the matching engine's top-of-book midpoint.
    fn funding_settlement_price(&self, instrument_id: InstrumentId) -> Option<Price> {
        if let Some(mark_price) = self.cache.borrow().mark_price(&instrument_id) {
            return Some(mark_price.value);
        }

        let engine = self.matching_engines.get(&instrument_id)?;
        let bid = engine.best_bid_price()?;
        let ask = engine.best_ask_price()?;
        let midpoint = (bid.as_decimal() + ask.as_decimal()) / Decimal::from(2);
        Price::from_decimal_dp(midpoint, bid.precision.max(ask.precision)).ok()
    }

    /// Applies a funding adjustment to the account by publishing the adjusted
    /// balance as an `AccountState` to the portfolio, which owns the cached
    /// account (the same path the sandbox uses for all balance updates).
    ///
    /// Returns `false` (and logs) when the account or balance is missing or the
    /// adjustment overflows, in which case callers must roll back position updates.
    fn adjust_account(&self, adjustment: Money, ts_event: UnixNanos) -> bool {
        let account_id = self.config.account_id;

        // Compute the adjusted balance under a scoped cache borrow so the
        // publication below cannot overlap the cache borrow.
        let adjusted = {
            let cache = self.cache.borrow();
            let Some(account) = cache.account(&account_id) else {
                log::error!("Cannot adjust account for funding: no account for {account_id}");
                return false;
            };

            let Some(balance) = account.balance(Some(adjustment.currency)) else {
                log::error!(
                    "Cannot adjust account for funding: no balance for currency {}",
                    adjustment.currency
                );
                return false;
            };

            let mut current_balance = *balance;
            let Some(total) = current_balance.total.checked_add(adjustment) else {
                log::error!(
                    "Cannot adjust account for funding: {} total balance overflow",
                    adjustment.currency
                );
                return false;
            };
            let Some(free) = current_balance.free.checked_add(adjustment) else {
                log::error!(
                    "Cannot adjust account for funding: {} free balance overflow",
                    adjustment.currency
                );
                return false;
            };
            current_balance.total = total;
            current_balance.free = free;

            let margins: Vec<MarginBalance> = match &*account {
                AccountAny::Margin(margin_account) => {
                    margin_account.margins.values().copied().collect()
                }
                _ => Vec::new(),
            };

            Some((current_balance, margins))
        };

        let Some((current_balance, margins)) = adjusted else {
            return false;
        };

        let ts_init = self.clock.borrow().timestamp_ns();
        let state = self.account_state_factory.generate_account_state(
            vec![current_balance],
            margins,
            true,
            ts_event,
            ts_init,
        );
        let endpoint = MessagingSwitchboard::portfolio_update_account();
        msgbus::send_account_state(endpoint, &state);
        true
    }

    /// Arms (or re-arms) the one-shot wall-clock alert at the earliest pending
    /// funding boundary.
    ///
    /// The alert fires through the runner clock's callback dispatch on the
    /// runner task; a nested msgbus dispatch holding the borrow skips and the
    /// periodic sweep retries.
    fn arm_funding_settlement_alert(&self) {
        let Some(next_boundary) = self
            .pending_funding
            .first_key_value()
            .map(|((boundary, _), _)| *boundary)
        else {
            return;
        };

        let name = self.funding_settlement_timer_name();
        let callback = self.funding_timer_callback.clone();
        let mut clock = self.clock.borrow_mut();

        // Never arm into the past: due boundaries are settled by data-driven
        // retries and the periodic sweep instead of a tight alert loop.
        if next_boundary <= clock.timestamp_ns() {
            return;
        }

        // Keep an existing alert armed at or before the next boundary.
        if let Some(armed_at) = clock.next_time_ns(&name)
            && armed_at <= next_boundary
        {
            return;
        }

        if let Err(e) = clock.set_time_alert_ns(&name, next_boundary, Some(callback), None) {
            log::error!("Failed to arm sandbox funding settlement alert: {e}");
        }
    }

    /// Returns the timer name for the wall-clock funding settlement alert.
    fn funding_settlement_timer_name(&self) -> String {
        format!("{}-sandbox-funding-settlement", self.client_id)
    }

    /// Cancels the wall-clock funding settlement alert (no-op when unarmed).
    fn cancel_funding_timer(&self) {
        self.clock
            .borrow_mut()
            .cancel_timer(&self.funding_settlement_timer_name());
    }
}

/// Registered message handlers for later deregistration.
struct RegisteredHandlers {
    deltas_pattern: MStr<Pattern>,
    deltas_handler: TypedHandler<OrderBookDeltas>,
    quote_pattern: MStr<Pattern>,
    quote_handler: TypedHandler<QuoteTick>,
    trade_pattern: MStr<Pattern>,
    trade_handler: TypedHandler<TradeTick>,
    bar_pattern: MStr<Pattern>,
    bar_handler: TypedHandler<Bar>,
    status_pattern: MStr<Pattern>,
    status_handler: ShareableMessageHandler,
    close_pattern: MStr<Pattern>,
    close_handler: ShareableMessageHandler,
    funding_pattern: MStr<Pattern>,
    funding_handler: TypedHandler<FundingRateUpdate>,
    position_pattern: MStr<Pattern>,
    position_handler: TypedHandler<PositionEvent>,
}

/// A sandbox execution client for paper trading against live market data.
///
/// The `SandboxExecutionClient` simulates order execution using the `OrderMatchingEngine`
/// to match orders against market data. This enables strategy testing in real-time
/// without actual order execution on exchanges.
pub struct SandboxExecutionClient {
    /// The core execution client functionality.
    core: RefCell<ExecutionClientCore>,
    /// Factory for generating order events.
    factory: OrderEventFactory,
    /// The sandbox configuration.
    config: SandboxExecutionClientConfig,
    /// Inner state wrapped for handler access.
    inner: Rc<RefCell<SandboxInner>>,
    /// Registered message handlers for cleanup.
    handlers: RefCell<Option<RegisteredHandlers>>,
    /// Reference to the clock.
    clock: Rc<RefCell<dyn Clock>>,
    /// Reference to the cache.
    cache: Rc<RefCell<Cache>>,
}

impl Debug for SandboxExecutionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(SandboxExecutionClient))
            .field("venue", &self.config.venue)
            .field("account_id", &self.core.borrow().account_id)
            .field("connected", &self.core.borrow().is_connected())
            .field(
                "matching_engines",
                &self.inner.borrow().matching_engines.len(),
            )
            .finish()
    }
}

impl SandboxExecutionClient {
    /// Creates a new [`SandboxExecutionClient`] instance.
    #[must_use]
    pub fn new(
        core: ExecutionClientCore,
        config: SandboxExecutionClientConfig,
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
    ) -> Self {
        let mut balances = AHashMap::new();
        for money in &config.starting_balances {
            balances.insert(money.currency.code.to_string(), *money);
        }

        let factory = OrderEventFactory::new(
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );

        let noop_timer_callback: Rc<dyn Fn(TimeEvent)> = Rc::new(|_: TimeEvent| {});
        let inner = Rc::new(RefCell::new(SandboxInner {
            clock: clock.clone(),
            cache: cache.clone(),
            config: config.clone(),
            matching_engines: AHashMap::new(),
            next_engine_raw_id: 0,
            balances,
            event_handler: None,
            client_id: core.client_id,
            account_state_factory: factory.clone(),
            pending_funding: BTreeMap::new(),
            settled_funding: HashSet::new(),
            funding_timer_callback: TimeEventCallback::from(noop_timer_callback),
        }));

        // Two-phase init: the settlement alert callback needs a weak reference
        // to the inner state, which only exists once the `Rc` is constructed.
        {
            let inner_weak = WeakCell::from(Rc::downgrade(&inner));
            let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_event: TimeEvent| {
                let Some(inner_rc) = inner_weak.upgrade() else {
                    return;
                };

                // The alert fires on the runner task, but a nested msgbus
                // dispatch may already hold the borrow; skipping is safe
                // because the periodic sweep and the next funding rate or
                // market data arrival all retry due boundaries.
                if let Ok(mut inner) = inner_rc.try_borrow_mut() {
                    inner.settle_funding_due();
                } else {
                    log::debug!("Skipping sandbox funding settlement due to active borrow");
                }
            });
            inner.borrow_mut().funding_timer_callback = TimeEventCallback::from(callback);
        }

        Self {
            core: RefCell::new(core),
            factory,
            config,
            inner,
            handlers: RefCell::new(None),
            clock,
            cache,
        }
    }

    /// Returns a reference to the configuration.
    #[must_use]
    pub const fn config(&self) -> &SandboxExecutionClientConfig {
        &self.config
    }

    /// Returns the number of active matching engines.
    #[must_use]
    pub fn matching_engine_count(&self) -> usize {
        self.inner.borrow().matching_engines.len()
    }

    fn dispatch_order_event(&self, event: OrderEventAny) {
        if let Some(handler) = &self.inner.borrow().event_handler {
            handler(event);
        } else {
            let endpoint = MessagingSwitchboard::exec_engine_process();
            msgbus::send_order_event(endpoint, event);
        }
    }

    /// Registers message handlers for market data subscriptions.
    ///
    /// This subscribes to order book deltas, quotes, trades, and bars for the
    /// configured venue, routing all received data to the matching engines.
    fn register_message_handlers(&self) {
        if self.handlers.borrow().is_some() {
            log::warn!("Sandbox message handlers already registered");
            return;
        }

        let inner_weak = WeakCell::from(Rc::downgrade(&self.inner));
        let venue = self.config.venue;
        let account_id = self.core.borrow().account_id;

        // Order book deltas handler
        let deltas_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |deltas: &OrderBookDeltas| {
                if deltas.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    inner_rc.borrow_mut().process_order_book_deltas(deltas);
                }
            })
        };

        // Quote tick handler
        let quote_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |quote: &QuoteTick| {
                if quote.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    inner_rc.borrow_mut().process_quote_tick(quote);
                }
            })
        };

        // Trade tick handler
        let trade_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |trade: &TradeTick| {
                if trade.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    inner_rc.borrow_mut().process_trade_tick(trade);
                }
            })
        };

        // Bar handler (topic is data.bars.{bar_type}, filter by venue in handler)
        let bar_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |bar: &Bar| {
                if bar.bar_type.instrument_id().venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    inner_rc.borrow_mut().process_bar(bar);
                }
            })
        };

        let status_handler = {
            let inner = inner_weak.clone();
            ShareableMessageHandler::from_typed(move |status: &InstrumentStatus| {
                if status.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    inner_rc.borrow_mut().process_instrument_status(status);
                }
            })
        };

        let close_handler = {
            let inner = inner_weak.clone();
            ShareableMessageHandler::from_typed(move |close: &InstrumentClose| {
                if close.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    inner_rc.borrow_mut().process_instrument_close(close);
                }
            })
        };

        // Funding rate handler (wall-clock funding settlement)
        let funding_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |funding_rate: &FundingRateUpdate| {
                if funding_rate.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    inner_rc.borrow_mut().process_funding_rate(funding_rate);
                }
            })
        };

        let position_handler = {
            TypedHandler::from(move |event: &PositionEvent| {
                let PositionEvent::PositionClosed(position_closed) = event else {
                    return;
                };

                if position_closed.instrument_id.venue == venue
                    && position_closed.account_id == account_id
                    && let Some(inner_rc) = inner_weak.upgrade()
                {
                    // ExecutionEngine updates the cached position state before publishing
                    // PositionClosed, so this retry observes the post-settlement cache view.
                    if let Ok(mut inner) = inner_rc.try_borrow_mut() {
                        inner.sync_expired_cleanup(position_closed.instrument_id);
                    } else {
                        log::debug!(
                            "Skipping immediate expired cleanup retry for {} due to active sandbox borrow",
                            position_closed.instrument_id,
                        );
                    }
                }
            })
        };

        // Subscribe patterns
        let deltas_pattern: MStr<Pattern> = format!("data.book.deltas.{venue}.*").into();
        let quote_pattern: MStr<Pattern> = format!("data.quotes.{venue}.*").into();
        let trade_pattern: MStr<Pattern> = format!("data.trades.{venue}.*").into();
        let bar_pattern: MStr<Pattern> = "data.bars.*".into();
        let status_pattern: MStr<Pattern> = format!("data.status.{venue}.*").into();
        let close_pattern: MStr<Pattern> = format!("data.close.{venue}.*").into();
        let funding_pattern: MStr<Pattern> = format!("data.funding_rates.{venue}.*").into();
        let position_pattern: MStr<Pattern> = "events.position.*".into();

        msgbus::subscribe_book_deltas(deltas_pattern, deltas_handler.clone(), Some(10));
        msgbus::subscribe_quotes(quote_pattern, quote_handler.clone(), Some(10));
        msgbus::subscribe_trades(trade_pattern, trade_handler.clone(), Some(10));
        msgbus::subscribe_bars(bar_pattern, bar_handler.clone(), Some(10));
        msgbus::subscribe_any(status_pattern, status_handler.clone(), Some(10));
        msgbus::subscribe_instrument_close(close_pattern, close_handler.clone(), Some(10));
        // The funding settlement feature is an explicit opt-out: without the
        // subscription no funding state is queued or scheduled.
        if self.config.funding_settlement {
            msgbus::subscribe_funding_rates(funding_pattern, funding_handler.clone(), Some(10));
        }
        msgbus::subscribe_position_events(position_pattern, position_handler.clone(), Some(10));

        // Store handlers for later deregistration
        *self.handlers.borrow_mut() = Some(RegisteredHandlers {
            deltas_pattern,
            deltas_handler,
            quote_pattern,
            quote_handler,
            trade_pattern,
            trade_handler,
            bar_pattern,
            bar_handler,
            status_pattern,
            status_handler,
            close_pattern,
            close_handler,
            funding_pattern,
            funding_handler,
            position_pattern,
            position_handler,
        });

        log::debug!(
            "Sandbox registered message handlers for venue={}",
            self.config.venue
        );
    }

    /// Deregisters message handlers to stop receiving market data.
    fn deregister_message_handlers(&self) {
        if let Some(handlers) = self.handlers.borrow_mut().take() {
            msgbus::unsubscribe_book_deltas(handlers.deltas_pattern, &handlers.deltas_handler);
            msgbus::unsubscribe_quotes(handlers.quote_pattern, &handlers.quote_handler);
            msgbus::unsubscribe_trades(handlers.trade_pattern, &handlers.trade_handler);
            msgbus::unsubscribe_bars(handlers.bar_pattern, &handlers.bar_handler);
            msgbus::unsubscribe_any(handlers.status_pattern, &handlers.status_handler);
            msgbus::unsubscribe_instrument_close(handlers.close_pattern, &handlers.close_handler);
            // Safe no-op when the funding subscription was disabled at start.
            msgbus::unsubscribe_funding_rates(handlers.funding_pattern, &handlers.funding_handler);
            msgbus::unsubscribe_position_events(
                handlers.position_pattern,
                &handlers.position_handler,
            );

            log::debug!(
                "Sandbox deregistered message handlers for venue={}",
                self.config.venue
            );
        }
    }

    fn expiry_sweep_timer_name(&self) -> String {
        format!("{}-sandbox-expiry-sweep", self.core.borrow().client_id)
    }

    /// Registers the periodic sweep that retires expired matching engines with no open position.
    ///
    /// The sweep is also the bounded retry tick for funding settlements still
    /// pending (e.g. waiting on a settlement price source): no-op when nothing
    /// is due.
    fn register_expiry_sweep_timer(&self) {
        let inner_weak = WeakCell::from(Rc::downgrade(&self.inner));
        let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_event: TimeEvent| {
            let Some(inner_rc) = inner_weak.upgrade() else {
                return;
            };

            // The timer fires on the runner task, but a nested msgbus dispatch may already hold the
            // borrow; skipping is safe because the next interval retries.
            if let Ok(mut inner) = inner_rc.try_borrow_mut() {
                inner.sweep_expired_engines();
                inner.settle_funding_due();
            } else {
                log::debug!("Skipping sandbox expiry sweep due to active borrow");
            }
        });

        let name = self.expiry_sweep_timer_name();

        if let Err(e) = self.clock.borrow_mut().set_timer_ns(
            &name,
            EXPIRED_ENGINE_SWEEP_INTERVAL_NS,
            None,
            None,
            Some(TimeEventCallback::from(callback)),
            None,
            None,
        ) {
            log::error!("Failed to register sandbox expiry sweep timer: {e}");
        }
    }

    /// Cancels the periodic expired-engine sweep timer.
    fn cancel_expiry_sweep_timer(&self) {
        self.clock
            .borrow_mut()
            .cancel_timer(&self.expiry_sweep_timer_name());
    }

    /// Returns current account balances, preferring cache state over starting balances.
    fn get_current_account_balances(&self) -> Vec<AccountBalance> {
        let account_id = self.core.borrow().account_id;
        let cache = self.cache.borrow();

        // Use account from cache if available (updated by fill events)
        if let Some(account) = cache.account(&account_id) {
            return account.balances().into_values().collect();
        }

        // Fall back to starting balances
        self.get_account_balances()
    }

    fn sync_cached_account_config(&self) -> anyhow::Result<()> {
        let Some(mut account) = self.get_account() else {
            return Ok(());
        };

        account.set_calculate_account_state(!self.config.frozen_account);

        if let AccountAny::Margin(margin_account) = &mut account {
            margin_account.set_default_leverage(self.config.default_leverage);
            for (instrument_id, leverage) in &self.config.leverages {
                margin_account.set_leverage(*instrument_id, *leverage);
            }
        }

        self.cache.borrow_mut().update_account(&account)
    }

    /// Processes a quote tick through the matching engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not found in the cache.
    pub fn process_quote_tick(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        let instrument_id = quote.instrument_id;
        let instrument = self.cache.borrow().try_instrument(&instrument_id)?.clone();

        if !check_quote_or_drop("quote tick", quote, &instrument) {
            return Ok(());
        }

        let mut inner = self.inner.borrow_mut();
        inner.ensure_matching_engine(&instrument);
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_quote_tick(quote);
        }
        Ok(())
    }

    /// Processes a trade tick through the matching engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not found in the cache.
    pub fn process_trade_tick(&self, trade: &TradeTick) -> anyhow::Result<()> {
        if !self.config.trade_execution {
            return Ok(());
        }

        let instrument_id = trade.instrument_id;
        let instrument = self.cache.borrow().try_instrument(&instrument_id)?.clone();

        if !check_trade_or_drop("trade tick", trade, &instrument) {
            return Ok(());
        }

        let mut inner = self.inner.borrow_mut();
        inner.ensure_matching_engine(&instrument);
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_trade_tick(trade);
        }
        Ok(())
    }

    /// Processes a bar through the matching engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not found in the cache.
    pub fn process_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        if !self.config.bar_execution {
            return Ok(());
        }

        let instrument_id = bar.bar_type.instrument_id();
        let instrument = self.cache.borrow().try_instrument(&instrument_id)?.clone();

        if !check_bar_or_drop("bar", bar, &instrument) {
            return Ok(());
        }

        let mut inner = self.inner.borrow_mut();
        inner.ensure_matching_engine(&instrument);
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_bar(bar);
        }
        Ok(())
    }

    /// Processes order book deltas through the matching engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not found in the cache.
    pub fn process_order_book_deltas(&self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        let instrument_id = deltas.instrument_id;
        let instrument = self.cache.borrow().try_instrument(&instrument_id)?.clone();

        let mut inner = self.inner.borrow_mut();
        inner.ensure_matching_engine(&instrument);
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_order_book_deltas(deltas)?;
        }
        Ok(())
    }

    /// Resets the sandbox to its initial state.
    pub fn reset(&self) {
        let mut inner = self.inner.borrow_mut();
        for engine in inner.matching_engines.values_mut() {
            engine.reset();
        }

        inner.balances.clear();
        for money in &self.config.starting_balances {
            inner
                .balances
                .insert(money.currency.code.to_string(), *money);
        }

        inner.pending_funding.clear();
        inner.settled_funding.clear();
        inner.cancel_funding_timer();

        log::info!(
            "Sandbox execution client reset: venue={}",
            self.config.venue
        );
    }

    /// Generates account balance entries from current balances.
    fn get_account_balances(&self) -> Vec<AccountBalance> {
        self.inner
            .borrow()
            .balances
            .values()
            .map(|money| AccountBalance::new(*money, Money::zero(money.currency), *money))
            .collect()
    }

    fn get_order(&self, client_order_id: &ClientOrderId) -> anyhow::Result<OrderAny> {
        Ok(self.cache.borrow().try_order_owned(client_order_id)?)
    }
}

#[async_trait(?Send)]
impl ExecutionClient for SandboxExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.borrow().is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.borrow().client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.borrow().account_id
    }

    fn venue(&self) -> Venue {
        self.core.borrow().venue
    }

    fn oms_type(&self) -> OmsType {
        self.config.oms_type
    }

    fn on_instrument(&mut self, instrument: InstrumentAny) {
        let instrument_id = instrument.id();
        let mut inner = self.inner.borrow_mut();
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id)
            && let Err(e) = engine.update_instrument(instrument)
        {
            log::error!("Failed to update instrument {instrument_id} in sandbox engine: {e}");
        }
    }

    fn get_account(&self) -> Option<AccountAny> {
        let account_id = self.core.borrow().account_id;
        self.cache.borrow().account_owned(&account_id)
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        let ts_init = self.clock.borrow().timestamp_ns();
        let state = self
            .factory
            .generate_account_state(balances, margins, reported, ts_event, ts_init);
        let endpoint = MessagingSwitchboard::portfolio_update_account();
        msgbus::send_account_state(endpoint, &state);
        self.sync_cached_account_config()?;
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.borrow().is_started() {
            return Ok(());
        }

        if let Some(sender) = try_get_exec_event_sender() {
            let handler: Rc<dyn Fn(OrderEventAny)> = Rc::new(move |event: OrderEventAny| {
                if let Err(e) = sender.send(ExecutionEvent::Order(event)) {
                    log::warn!("Failed to send order event: {e}");
                }
            });
            let mut inner = self.inner.borrow_mut();
            inner.event_handler = Some(handler.clone());
            for engine in inner.matching_engines.values_mut() {
                engine.set_event_handler(handler.clone());
            }
        }

        // Register message handlers to receive market data
        self.register_message_handlers();
        self.register_expiry_sweep_timer();

        self.core.borrow().set_started();
        let core = self.core.borrow();
        log::info!(
            "Sandbox execution client started: venue={}, account_id={}, oms_type={:?}, account_type={:?}",
            self.config.venue,
            core.account_id,
            self.config.oms_type,
            self.config.account_type,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.borrow().is_stopped() {
            return Ok(());
        }

        // Deregister message handlers to stop receiving data
        self.deregister_message_handlers();
        self.cancel_expiry_sweep_timer();
        self.inner.borrow_mut().cancel_funding_timer();

        self.core.borrow().set_stopped();
        self.core.borrow().set_disconnected();
        log::info!(
            "Sandbox execution client stopped: venue={}",
            self.config.venue
        );
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.borrow().is_connected() {
            return Ok(());
        }

        let balances = self.get_account_balances();
        let ts_event = self.clock.borrow().timestamp_ns();
        self.generate_account_state(balances, vec![], false, ts_event)?;

        self.core.borrow().set_connected();
        log::info!(
            "Sandbox execution client connected: venue={}",
            self.config.venue
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.core.borrow().is_disconnected() {
            return Ok(());
        }

        self.core.borrow().set_disconnected();
        log::info!(
            "Sandbox execution client disconnected: venue={}",
            self.config.venue
        );
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let mut order = self.get_order(&cmd.client_order_id)?;

        if order.is_closed() {
            log::warn!("Cannot submit closed order {}", order.client_order_id());
            return Ok(());
        }

        let ts_init = self.clock.borrow().timestamp_ns();
        let event = self.factory.generate_order_submitted(&order, ts_init);
        self.dispatch_order_event(event);

        let instrument_id = order.instrument_id();
        let instrument = self.cache.borrow().try_instrument(&instrument_id)?.clone();

        let mut inner = self.inner.borrow_mut();
        inner.ensure_matching_engine(&instrument);

        // Update matching engine with latest market data from cache
        let cache = self.cache.borrow();

        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            if let Some(quote) = cache.quote(&instrument_id)
                && check_quote_or_drop("cached quote tick", quote, &instrument)
            {
                engine.process_quote_tick(quote);
            }

            if self.config.trade_execution
                && let Some(trade) = cache.trade(&instrument_id)
                && check_trade_or_drop("cached trade tick", trade, &instrument)
            {
                engine.process_trade_tick(trade);
            }
        }
        drop(cache);

        let account_id = self.core.borrow().account_id;

        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_order(&mut order, account_id);
            inner.sync_expired_cleanup(instrument_id);
        }

        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let ts_init = self.clock.borrow().timestamp_ns();
        let mut cleanup_instrument_ids = Vec::new();

        let orders: Vec<OrderAny> = self
            .cache
            .borrow()
            .orders_for_ids(&cmd.order_list.client_order_ids, &cmd);

        for order in &orders {
            if order.is_closed() {
                log::warn!("Cannot submit closed order {}", order.client_order_id());
                continue;
            }

            let event = self.factory.generate_order_submitted(order, ts_init);
            self.dispatch_order_event(event);
        }

        let account_id = self.core.borrow().account_id;

        for order in &orders {
            if order.is_closed() {
                continue;
            }

            let instrument_id = order.instrument_id();
            if !cleanup_instrument_ids.contains(&instrument_id) {
                cleanup_instrument_ids.push(instrument_id);
            }
            let instrument = self.cache.borrow().instrument(&instrument_id).cloned();

            if let Some(instrument) = instrument {
                let mut inner = self.inner.borrow_mut();
                inner.ensure_matching_engine(&instrument);

                // Update with latest market data
                let cache = self.cache.borrow();

                if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
                    if let Some(quote) = cache.quote(&instrument_id)
                        && check_quote_or_drop("cached quote tick", quote, &instrument)
                    {
                        engine.process_quote_tick(quote);
                    }

                    if self.config.trade_execution
                        && let Some(trade) = cache.trade(&instrument_id)
                        && check_trade_or_drop("cached trade tick", trade, &instrument)
                    {
                        engine.process_trade_tick(trade);
                    }
                }
                drop(cache);

                if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
                    let mut order_clone = order.clone();
                    engine.process_order(&mut order_clone, account_id);
                }
            }
        }

        if !cleanup_instrument_ids.is_empty() {
            self.inner
                .borrow_mut()
                .sync_expired_cleanup_many(&cleanup_instrument_ids);
        }

        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let account_id = self.core.borrow().account_id;

        let mut inner = self.inner.borrow_mut();
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_modify(&cmd, account_id);
        }
        Ok(())
    }

    fn batch_modify_orders(&self, cmd: BatchModifyOrders) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let account_id = self.core.borrow().account_id;

        let mut inner = self.inner.borrow_mut();
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_batch_modify(&cmd, account_id);
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let account_id = self.core.borrow().account_id;

        let mut inner = self.inner.borrow_mut();
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_cancel(&cmd, account_id);
        }
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let account_id = self.core.borrow().account_id;

        let mut inner = self.inner.borrow_mut();
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_cancel_all(&cmd, account_id);
        }
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let account_id = self.core.borrow().account_id;

        let mut inner = self.inner.borrow_mut();
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_batch_cancel(&cmd, account_id);
        }
        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        let balances = self.get_current_account_balances();
        let ts_event = self.clock.borrow().timestamp_ns();
        self.generate_account_state(balances, vec![], false, ts_event)?;
        Ok(())
    }

    fn query_order(&self, _cmd: QueryOrder) -> anyhow::Result<()> {
        // Orders are tracked in the cache, no external query needed for sandbox
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        _cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        // Sandbox orders are tracked internally
        Ok(None)
    }

    async fn generate_order_status_reports(
        &self,
        _cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        // Sandbox orders are tracked internally
        Ok(Vec::new())
    }

    async fn generate_fill_reports(
        &self,
        _cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        // Sandbox fills are tracked internally
        Ok(Vec::new())
    }

    async fn generate_position_status_reports(
        &self,
        _cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        // Sandbox positions are tracked internally
        Ok(Vec::new())
    }

    async fn generate_mass_status(
        &self,
        _lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let core = self.core.borrow();
        let ts_init = self.clock.borrow().timestamp_ns();
        Ok(Some(ExecutionMassStatus::new(
            core.client_id,
            core.account_id,
            core.venue,
            ts_init,
            None,
        )))
    }
}

#[cfg(test)]
mod funding_settlement_tests {
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clients::ExecutionClient,
        clock::{Clock, TestClock},
        msgbus::{
            self, MessageBus, MessagingSwitchboard, TypedHandler,
            typed_handler::ShareableMessageHandler,
        },
    };
    use nautilus_core::UnixNanos;
    use nautilus_execution::client::core::ExecutionClientCore;
    use nautilus_model::{
        data::{FundingRateUpdate, MarkPriceUpdate, QuoteTick},
        enums::{AccountType, BookType, OrderSide, OrderType, PositionAdjustmentType},
        events::{AccountState, FundingSettlement, PositionEvent},
        identifiers::{AccountId, ClientId, InstrumentId, TradeId, TraderId, Venue},
        instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
        orders::{builder::OrderTestBuilder, stubs::TestOrderEventStubs},
        position::Position,
        types::{Currency, Money, Price, Quantity},
    };
    use rust_decimal::Decimal;
    use ustr::Ustr;

    use crate::config::SandboxExecutionClientConfig;

    use super::SandboxExecutionClient;

    const ACCOUNT_ID: &str = "SANDBOX-001";

    /// Captured outbound events for assertions.
    #[derive(Default)]
    struct Captures {
        account_states: RefCell<Vec<AccountState>>,
        settlements: RefCell<Vec<FundingSettlement>>,
        adjustments: RefCell<Vec<PositionEvent>>,
    }

    struct Fixture {
        client: SandboxExecutionClient,
        cache: Rc<RefCell<Cache>>,
        clock: Rc<RefCell<dyn Clock>>,
        instrument: InstrumentAny,
        captures: Rc<Captures>,
    }

    /// Builds an open long position fixture (all offline literal data), mirroring
    /// the shared funding module's test form.
    fn position_fixture(instrument: &InstrumentAny, account_id: AccountId) -> Position {
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from("1.000"))
            .build();
        let fill = TestOrderEventStubs::filled(
            &order,
            instrument,
            Some(TradeId::from("T-001")),
            None,
            Some(Price::from("1000.00")),
            Some(Quantity::from("1.000")),
            None,
            Some(Money::from("0 USDT")),
            Some(UnixNanos::from(1)),
            Some(account_id),
        );
        Position::new(instrument, fill.into())
    }

    fn funding_rate_fixture(
        instrument_id: InstrumentId,
        rate: Decimal,
        interval: Option<u16>,
        next_funding_ns: Option<UnixNanos>,
        ts_event: UnixNanos,
    ) -> FundingRateUpdate {
        FundingRateUpdate::new(instrument_id, rate, interval, next_funding_ns, ts_event, ts_event)
    }

    async fn setup_fixture(with_position: bool, with_mark_price: bool) -> Fixture {
        *msgbus::get_message_bus().borrow_mut() = MessageBus::default();

        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let config = SandboxExecutionClientConfig {
            trader_id: TraderId::from("SANDBOX-001"),
            account_id: AccountId::from(ACCOUNT_ID),
            venue: Venue::new("BINANCE"),
            starting_balances: vec![Money::new(100_000.0, Currency::USDT())],
            base_currency: None,
            oms_type: nautilus_model::enums::OmsType::Netting,
            account_type: AccountType::Margin,
            default_leverage: Decimal::ONE,
            leverages: ahash::AHashMap::new(),
            book_type: BookType::L1_MBP,
            fee_model: None,
            frozen_account: false,
            bar_execution: false,
            trade_execution: false,
            reject_stop_orders: true,
            support_gtd_orders: true,
            support_contingent_orders: true,
            use_position_ids: true,
            use_random_ids: false,
            use_reduce_only: true,
            funding_settlement: true,
        };

        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));

        let core = ExecutionClientCore::new(
            config.trader_id,
            ClientId::new("SANDBOX"),
            config.venue,
            config.oms_type,
            config.account_id,
            config.account_type,
            config.base_currency,
            cache.clone(),
        );

        let client = SandboxExecutionClient::new(core, config, clock.clone(), cache.clone());

        {
            let mut cache_mut = cache.borrow_mut();
            cache_mut.add_instrument(instrument.clone()).unwrap();

            if with_mark_price {
                cache_mut
                    .add_mark_price(MarkPriceUpdate::new(
                        instrument.id(),
                        Price::from("1000.00"),
                        UnixNanos::from(0),
                        UnixNanos::from(0),
                    ))
                    .unwrap();
            }

            if with_position {
                let position = position_fixture(&instrument, AccountId::from(ACCOUNT_ID));
                cache_mut
                    .add_position(&position, nautilus_model::enums::OmsType::Netting)
                    .unwrap();
            }
        }

        // Portfolio simulation: apply published AccountStates to the cache.
        let captures = Rc::new(Captures::default());
        {
            let captures = captures.clone();
            let cache = cache.clone();
            let handler = TypedHandler::from(move |state: &AccountState| {
                captures.account_states.borrow_mut().push(state.clone());
                cache.borrow_mut().update_account_state(state).unwrap();
            });
            msgbus::register_account_state_endpoint(
                MessagingSwitchboard::portfolio_update_account(),
                handler,
            );
        }
        // Octopus adapter simulation: capture outbound FundingSettlements.
        {
            let captures = captures.clone();
            let handler = ShareableMessageHandler::from_typed(move |settlement: &FundingSettlement| {
                captures.settlements.borrow_mut().push(*settlement);
            });
            msgbus::subscribe_any(
                "events.funding_settlements.*".into(),
                handler,
                Some(10),
            );
        }
        // Execution engine simulation: capture outbound PositionAdjusted events.
        {
            let captures = captures.clone();
            let handler = TypedHandler::from(move |event: &PositionEvent| {
                if matches!(event, PositionEvent::PositionAdjusted(_)) {
                    captures.adjustments.borrow_mut().push(event.clone());
                }
            });
            msgbus::subscribe_position_events("events.position.*".into(), handler, Some(10));
        }

        let mut client = client;
        client.connect().await.unwrap();
        client.start().unwrap();

        Fixture {
            client,
            cache,
            clock,
            instrument,
            captures,
        }
    }

    /// Publishes a funding rate update over the message bus so the delivery path
    /// matches production (subscription → handler → scheduler).
    fn deliver(_fixture: &Fixture, funding_rate: &FundingRateUpdate) {
        let topic = nautilus_common::msgbus::switchboard::get_funding_rate_topic(
            funding_rate.instrument_id,
        );
        msgbus::publish_funding_rate(topic, funding_rate);
    }

    fn cache_usdt_balance(fixture: &Fixture) -> Option<Money> {
        let cache = fixture.cache.borrow();
        let account = cache.account(&AccountId::from(ACCOUNT_ID))?;
        account.balance(Some(Currency::USDT())).map(|b| b.total)
    }

    fn inner_pending(fixture: &Fixture) -> BTreeMap<(UnixNanos, InstrumentId), FundingRateUpdate> {
        fixture.client.inner.borrow().pending_funding.clone()
    }

    fn inner_settled_count(fixture: &Fixture) -> usize {
        fixture.client.inner.borrow().settled_funding.len()
    }

    #[tokio::test]
    async fn test_due_boundary_settles_balance_and_publishes_events() {
        let fixture = setup_fixture(/* with_position */ true, /* with_mark_price */ true).await;
        let instrument_id = fixture.instrument.id();
        let boundary = UnixNanos::default(); // TestClock starts at 0: boundary already due.

        deliver(
            &fixture,
            &funding_rate_fixture(
                instrument_id,
                Decimal::from_str_exact("0.001").unwrap(),
                None,
                Some(boundary),
                UnixNanos::from(0),
            ),
        );

        // AccountState flow: initial connect state, then the settlement state.
        let states = fixture.captures.account_states.borrow();
        assert_eq!(states.len(), 2, "expected connect + settlement states");
        let settlement_state = &states[1];
        assert!(settlement_state.is_reported);
        // Long 1 @ 1000 × rate 0.001 pays 1 USDT: 100_000 - 1 = 99_999.
        assert_eq!(
            settlement_state.balances[0].total,
            Money::from("99999 USDT")
        );
        assert_eq!(
            settlement_state.balances[0].free,
            Money::from("99999 USDT")
        );
        drop(states);

        assert_eq!(cache_usdt_balance(&fixture), Some(Money::from("99999 USDT")));

        // Outbound FundingSettlement must match the backtest event shape.
        let settlements = fixture.captures.settlements.borrow();
        let [settlement] = settlements.as_slice() else {
            panic!("expected one FundingSettlement, got {}", settlements.len());
        };
        assert_eq!(settlement.instrument_id, instrument_id);
        assert_eq!(settlement.account_id, AccountId::from(ACCOUNT_ID));
        assert_eq!(settlement.rate, Decimal::from_str_exact("0.001").unwrap());
        assert_eq!(settlement.settlement_price, Price::from("1000.00"));
        assert_eq!(settlement.currency, Currency::USDT());
        assert_eq!(settlement.ts_event, boundary);
        drop(settlements);

        // Outbound PositionAdjusted(Funding) with the per-position amount.
        let adjustments = fixture.captures.adjustments.borrow();
        assert_eq!(adjustments.len(), 1);
        let PositionEvent::PositionAdjusted(adjusted) = &adjustments[0] else {
            panic!("expected PositionAdjusted event");
        };
        assert_eq!(adjusted.adjustment_type, PositionAdjustmentType::Funding);
        assert_eq!(adjusted.pnl_change, Some(Money::from("-1 USDT")));
        assert!(adjusted.reason.is_some());
        drop(adjustments);

        // Position realized PnL accumulated in the cache.
        let positions: Vec<Position> = fixture
            .cache
            .borrow()
            .positions_open(
                Some(&Venue::new("BINANCE")),
                Some(&instrument_id),
                None,
                Some(&AccountId::from(ACCOUNT_ID)),
                None,
            )
            .into_iter()
            .map(|position| position.cloned())
            .collect();
        let [position] = positions.as_slice() else {
            panic!("expected one open position");
        };
        assert_eq!(position.realized_pnl, Some(Money::from("-1 USDT")));

        // Idempotency bookkeeping.
        assert!(inner_pending(&fixture).is_empty());
        assert_eq!(inner_settled_count(&fixture), 1);
    }

    #[tokio::test]
    async fn test_in_period_predictor_and_future_boundary_do_not_settle() {
        let fixture = setup_fixture(/* with_position */ true, /* with_mark_price */ true).await;
        let instrument_id = fixture.instrument.id();

        // In-period predictor value: misaligned with the 480-minute interval and
        // no explicit next funding time — defines no boundary at all.
        deliver(
            &fixture,
            &funding_rate_fixture(
                instrument_id,
                Decimal::from_str_exact("0.001").unwrap(),
                Some(480),
                None,
                UnixNanos::from(28_800_000_000_001),
            ),
        );
        assert!(inner_pending(&fixture).is_empty());
        assert_eq!(fixture.captures.account_states.borrow().len(), 1);
        assert!(fixture.captures.settlements.borrow().is_empty());

        // Future boundary (predicted next funding): queued and scheduled, but
        // nothing settles before the wall clock reaches it.
        let future_boundary = UnixNanos::from(1_000_000_000_000);
        deliver(
            &fixture,
            &funding_rate_fixture(
                instrument_id,
                Decimal::from_str_exact("0.001").unwrap(),
                None,
                Some(future_boundary),
                UnixNanos::from(0),
            ),
        );
        assert_eq!(inner_pending(&fixture).len(), 1);
        assert!(inner_pending(&fixture).contains_key(&(future_boundary, instrument_id)));
        assert_eq!(inner_settled_count(&fixture), 0);
        assert_eq!(fixture.captures.account_states.borrow().len(), 1);
        assert!(fixture.captures.settlements.borrow().is_empty());

        // The wall-clock alert is armed at the future boundary.
        let timer_name = fixture
            .client
            .inner
            .borrow()
            .funding_settlement_timer_name();
        assert!(fixture.clock.borrow().timer_exists(&Ustr::from(&timer_name)));
        assert_eq!(
            fixture.clock.borrow().next_time_ns(&timer_name),
            Some(future_boundary)
        );
    }

    #[tokio::test]
    async fn test_same_boundary_is_idempotent() {
        let fixture = setup_fixture(/* with_position */ true, /* with_mark_price */ true).await;
        let instrument_id = fixture.instrument.id();
        let boundary = UnixNanos::default();

        deliver(
            &fixture,
            &funding_rate_fixture(
                instrument_id,
                Decimal::from_str_exact("0.001").unwrap(),
                None,
                Some(boundary),
                UnixNanos::from(0),
            ),
        );
        assert_eq!(fixture.captures.settlements.borrow().len(), 1);

        // A second update for the same boundary is skipped entirely.
        deliver(
            &fixture,
            &funding_rate_fixture(
                instrument_id,
                Decimal::from_str_exact("0.010").unwrap(),
                None,
                Some(boundary),
                UnixNanos::from(0),
            ),
        );

        assert_eq!(fixture.captures.settlements.borrow().len(), 1);
        assert_eq!(fixture.captures.account_states.borrow().len(), 2);
        assert_eq!(
            fixture.captures.settlements.borrow()[0].rate,
            Decimal::from_str_exact("0.001").unwrap()
        );
        assert_eq!(cache_usdt_balance(&fixture), Some(Money::from("99999 USDT")));
        assert!(inner_pending(&fixture).is_empty());
        assert_eq!(inner_settled_count(&fixture), 1);
    }

    #[tokio::test]
    async fn test_missing_settlement_price_warns_and_retains_pending() {
        let fixture = setup_fixture(/* with_position */ true, /* with_mark_price */ false).await;
        let instrument_id = fixture.instrument.id();
        let boundary = UnixNanos::default();

        // No mark price seeded and no quote processed: the matching engine book
        // is empty, so settlement cannot be priced. Must warn, not panic, and
        // must not drop the pending boundary.
        deliver(
            &fixture,
            &funding_rate_fixture(
                instrument_id,
                Decimal::from_str_exact("0.001").unwrap(),
                None,
                Some(boundary),
                UnixNanos::from(0),
            ),
        );

        assert!(fixture.captures.settlements.borrow().is_empty());
        assert_eq!(fixture.captures.account_states.borrow().len(), 1);
        assert_eq!(inner_pending(&fixture).len(), 1);
        assert_eq!(inner_settled_count(&fixture), 0);

        // Recovery: a quote tick fills the book (BBO midpoint fallback) and the
        // quote-driven retry settles the retained boundary. Published over the
        // message bus so the delivery path matches production.
        let quote = QuoteTick::new(
            instrument_id,
            Price::new(998.0, 2),
            Price::new(1002.0, 2),
            Quantity::new(100.0, 3),
            Quantity::new(100.0, 3),
            UnixNanos::from(2),
            UnixNanos::from(2),
        );
        let quote_topic =
            nautilus_common::msgbus::switchboard::get_quotes_topic(instrument_id);
        msgbus::publish_quote(quote_topic, &quote);

        let settlements = fixture.captures.settlements.borrow();
        let [settlement] = settlements.as_slice() else {
            panic!("expected the retained boundary to settle after recovery");
        };
        // Midpoint of 998/1002 = 1000.00: long 1 pays 1 USDT.
        assert_eq!(settlement.settlement_price, Price::from("1000.00"));
        drop(settlements);
        assert_eq!(cache_usdt_balance(&fixture), Some(Money::from("99999 USDT")));
        assert!(inner_pending(&fixture).is_empty());
        assert_eq!(inner_settled_count(&fixture), 1);
    }
}
