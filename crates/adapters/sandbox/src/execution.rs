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
    collections::{BTreeMap, BinaryHeap, HashSet},
    fmt::Debug,
    rc::Rc,
};

use ahash::{AHashMap, AHashSet};
use async_trait::async_trait;
use nautilus_common::{
    adjustment::SimulationAdjustmentBatch,
    cache::Cache,
    clients::{ExecutionClient, generate_mass_status},
    clock::Clock,
    factories::OrderEventFactory,
    live::try_get_exec_event_sender,
    messages::{
        ExecutionEvent,
        execution::{
            BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder,
            GenerateFillReports, GenerateOrderStatusReport, GenerateOrderStatusReports,
            GeneratePositionStatusReports, ModifyOrder, QueryAccount, QueryOrder, SubmitOrder,
            SubmitOrderList, TradingCommand,
        },
    },
    msgbus::{
        self, MStr, MessagingSwitchboard, Pattern, TypedHandler,
        switchboard, typed_handler::ShareableMessageHandler,
    },
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{DurationNanos, Params, UUID4, UnixNanos, WeakCell};
use nautilus_execution::{
    client::core::ExecutionClientCore,
    funding,
    matching_engine::{
        OrderMatchingEngine,
        inflight::InflightOrders,
        orders::{MatchingOrders, OtoCommand},
    },
    models::{fee::FeeModelHandle, fill::FillModelHandle, latency::LatencyModel},
};
use nautilus_model::{
    accounts::AccountAny,
    data::{
        Bar, FundingRateUpdate, InstrumentClose, InstrumentStatus, OrderBookDeltas,
        OrderBookDepth, QuoteTick, TradeTick,
    },
    enums::{OmsType, PositionAdjustmentType},
    events::{
        FundingSettlement, OrderCancelDenied, OrderEventAny, OrderModifyDenied, OrderRejected,
        PositionAdjusted, PositionEvent,
    },
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TraderId, Venue},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    position::Position,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Money, Price},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::config::SandboxExecutionClientConfig;

// Bounds retained state for quote-only instruments that expire without event-driven cleanup
const EXPIRED_ENGINE_SWEEP_INTERVAL: DurationNanos = DurationNanos::from_mins(1);

/// A sandbox execution client for paper trading against live market data.
///
/// The `SandboxExecutionClient` simulates order execution using the `OrderMatchingEngine`
/// to match orders against market data. This enables strategy testing in real-time
/// without actual order execution on exchanges.
///
/// Order status reports use cached orders with a venue order ID for this account and venue.
/// Full-history requests include retained closed orders.
pub struct SandboxExecutionClient {
    core: RefCell<ExecutionClientCore>,
    factory: OrderEventFactory,
    config: SandboxExecutionClientConfig,
    inner: Rc<RefCell<SandboxInner>>,
    handlers: RefCell<Option<RegisteredHandlers>>,
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
}

struct RegisteredHandlers {
    deltas_pattern: MStr<Pattern>,
    deltas_handler: TypedHandler<OrderBookDeltas>,
    depth_pattern: MStr<Pattern>,
    depth_handler: TypedHandler<OrderBookDepth>,
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
    adjustment_pattern: MStr<Pattern>,
    adjustment_handler: ShareableMessageHandler,
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
    /// Creates a new [`SandboxExecutionClient`] with an explicitly configured fee model.
    ///
    /// The fee model is resolved once and shared across all matching engines for
    /// this account, ensuring model state is shared within the account and isolated
    /// across accounts.
    ///
    /// # Errors
    ///
    /// Returns an error if starting balances repeat a currency or `config.fee_model` is `None`.
    /// A deliberate fee choice, including an explicit zero-fee model, is required.
    pub fn new(
        core: ExecutionClientCore,
        config: SandboxExecutionClientConfig,
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
    ) -> anyhow::Result<Self> {
        config.validate()?;

        let fee_model = config.fee_model.clone().map(FeeModelHandle::from).ok_or_else(|| {
            anyhow::anyhow!(
                "SandboxExecutionClientConfig requires an explicit fee_model, including an explicit zero-fee model"
            )
        })?;

        Self::new_with_fee_model(core, config, clock, cache, fee_model)
    }

    /// Creates a sandbox execution client with a runtime fee model.
    ///
    /// The supplied model takes precedence over `config.fee_model` and is shared
    /// across all matching engines for this client.
    ///
    /// # Errors
    ///
    /// Returns an error if starting balances repeat a currency.
    pub fn new_with_fee_model(
        core: ExecutionClientCore,
        config: SandboxExecutionClientConfig,
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
        fee_model: FeeModelHandle,
    ) -> anyhow::Result<Self> {
        config.validate()?;

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
        let fill_model = config
            .fill_model
            .clone()
            .map(FillModelHandle::from)
            .unwrap_or_default();

        let inner = Rc::new_cyclic(|weak: &std::rc::Weak<RefCell<SandboxInner>>| {
            RefCell::new(SandboxInner {
                clock: clock.clone(),
                cache: cache.clone(),
                config: config.clone(),
                fill_model,
                fee_model,
                matching_engines: AHashMap::new(),
                next_engine_raw_id: 0,
                balances,
                event_handler: None,
                inbound_queue: BinaryHeap::new(),
                inflight_orders: InflightOrders::default(),
                matching_orders: MatchingOrders::default(),
                inbound_seq: 0,
                trader_id: core.trader_id,
                client_id: core.client_id,
                account_id: core.account_id,
                account_state_factory: factory.clone(),
                pending_funding: BTreeMap::new(),
                settled_funding: HashSet::new(),
                applied_adjustments: HashSet::new(),
                funding_timer_callback: TimeEventCallback::from(noop_timer_callback),
                self_weak: WeakCell::from(weak.clone()),
            })
        });

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

        Ok(Self {
            core: RefCell::new(core),
            factory,
            config,
            inner,
            handlers: RefCell::new(None),
            clock,
            cache,
        })
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

    /// Applies a joint cash/inventory adjustment batch atomically
    /// (`POLYMARKET_PREDICT` §8.5(e); see `SandboxInner`'s implementation
    /// for the shared-core semantics). The msgbus subscription registered
    /// at connect routes published batches here automatically; this entry
    /// point serves direct/test callers.
    ///
    /// # Errors
    ///
    /// Returns the shared core's error naming the first violated rule.
    pub fn apply_adjustment_batch(&self, batch: &SimulationAdjustmentBatch) -> anyhow::Result<()> {
        self.inner.borrow_mut().apply_adjustment_batch(batch)
    }

    fn dispatch_order_event(&self, event: OrderEventAny) {
        self.inner.borrow().dispatch_order_event(event);
    }

    fn register_message_handlers(&self) {
        if self.handlers.borrow().is_some() {
            log::warn!("Sandbox message handlers already registered");
            return;
        }

        let inner_weak = WeakCell::from(Rc::downgrade(&self.inner));
        let venue = self.config.venue;

        let deltas_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |deltas: &OrderBookDeltas| {
                if deltas.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    SandboxInner::on_order_book_deltas(&inner_rc, deltas);
                }
            })
        };

        let depth_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |depth: &OrderBookDepth| {
                SandboxInner::on_order_book_depth(&inner, venue, depth);
            })
        };

        let quote_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |quote: &QuoteTick| {
                if quote.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    SandboxInner::on_quote_tick(&inner_rc, quote);
                }
            })
        };

        let trade_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |trade: &TradeTick| {
                if trade.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    SandboxInner::on_trade_tick(&inner_rc, trade);
                }
            })
        };

        // Bar topics include the bar type, so filter by venue in the handler
        let bar_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |bar: &Bar| {
                if bar.bar_type.instrument_id().venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    SandboxInner::on_bar(&inner_rc, bar);
                }
            })
        };

        let status_handler = {
            let inner = inner_weak.clone();
            ShareableMessageHandler::from_typed(move |status: &InstrumentStatus| {
                if status.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    SandboxInner::on_instrument_status(&inner_rc, status);
                }
            })
        };

        let close_handler = {
            let inner = inner_weak.clone();
            ShareableMessageHandler::from_typed(move |close: &InstrumentClose| {
                if close.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    SandboxInner::on_instrument_close(&inner_rc, close);
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

        // Joint cash/inventory adjustment batches (`POLYMARKET_PREDICT`
        // §8.5(e)): strategies publish `SimulationAdjustmentBatch` on the
        // engine thread's msgbus; the shared accounting core (identical to
        // backtest `SimulatedExchange::apply_adjustment_batch`) applies it
        // against this client's account/cache state. Exact-topic
        // subscription keyed by this client's account id. A rejected batch
        // logs loudly — the engine thread never panics on one bad batch.
        let adjustment_handler = {
            let inner = inner_weak.clone();
            ShareableMessageHandler::from_typed(move |batch: &SimulationAdjustmentBatch| {
                if let Some(inner_rc) = inner.upgrade() {
                    if let Err(e) = inner_rc.borrow_mut().apply_adjustment_batch(batch) {
                        log::error!(
                            "Sandbox adjustment batch {} rejected: {e:#}",
                            batch.operation_id
                        );
                    }
                }
            })
        };

        let position_handler = {
            TypedHandler::from(move |event: &PositionEvent| {
                if let Some(inner_rc) = inner_weak.upgrade() {
                    SandboxInner::on_position_event(&inner_rc, event);
                }
            })
        };

        let deltas_pattern: MStr<Pattern> = format!("data.book.deltas.{venue}.*").into();
        let depth_pattern: MStr<Pattern> = format!("data.book.depth.{venue}.*").into();
        let quote_pattern: MStr<Pattern> = format!("data.quotes.{venue}.*").into();
        let trade_pattern: MStr<Pattern> = format!("data.trades.{venue}.*").into();
        let bar_pattern: MStr<Pattern> = "data.bars.*".into();
        let status_pattern: MStr<Pattern> = format!("data.status.{venue}.*").into();
        let close_pattern: MStr<Pattern> = format!("data.close.{venue}.*").into();
        let funding_pattern: MStr<Pattern> = format!("data.funding_rates.{venue}.*").into();
        let position_pattern: MStr<Pattern> = "events.position.*".into();
        let adjustment_pattern: MStr<Pattern> =
            switchboard::get_adjustment_batch_topic(self.config.account_id).into();

        msgbus::subscribe_book_deltas(deltas_pattern, deltas_handler.clone(), Some(10));
        msgbus::subscribe_book_depth(depth_pattern, depth_handler.clone(), Some(10));
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
        msgbus::subscribe_any(adjustment_pattern, adjustment_handler.clone(), Some(10));

        *self.handlers.borrow_mut() = Some(RegisteredHandlers {
            deltas_pattern,
            deltas_handler,
            depth_pattern,
            depth_handler,
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
            adjustment_pattern,
            adjustment_handler,
        });

        log::debug!(
            "Sandbox registered message handlers for venue={}",
            self.config.venue
        );
    }

    fn deregister_message_handlers(&self) {
        if let Some(handlers) = self.handlers.borrow_mut().take() {
            msgbus::unsubscribe_book_deltas(handlers.deltas_pattern, &handlers.deltas_handler);
            msgbus::unsubscribe_book_depth(handlers.depth_pattern, &handlers.depth_handler);
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
            msgbus::unsubscribe_any(handlers.adjustment_pattern, &handlers.adjustment_handler);

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
            EXPIRED_ENGINE_SWEEP_INTERVAL,
            None,
            None,
            Some(TimeEventCallback::from(callback)),
            None,
            None,
        ) {
            log::error!("Failed to register sandbox expiry sweep timer: {e}");
        }
    }

    fn cancel_expiry_sweep_timer(&self) {
        self.clock
            .borrow_mut()
            .cancel_timer(&self.expiry_sweep_timer_name());
    }

    fn cancel_inbound_alert(&self) {
        let client_id = self.core.borrow().client_id;
        self.clock
            .borrow_mut()
            .cancel_timer(&inbound_alert_name(client_id));
    }

    fn get_current_account_balances(&self) -> Vec<AccountBalance> {
        let account_id = self.core.borrow().account_id;
        let cache = self.cache.borrow();

        if let Some(account) = cache.account(&account_id) {
            return account.balances().into_values().collect();
        }

        self.get_account_balances()
    }

    fn sync_cached_account_config(&self) -> anyhow::Result<()> {
        let Some(mut account) = self.get_account() else {
            return Ok(());
        };

        account.set_calculate_account_state(!self.config.frozen_account);

        if let AccountAny::Margin(margin_account) = &mut account {
            if let Some(model) = &self.config.margin_model {
                margin_account.set_margin_model(model.clone().into());
            }

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
        SandboxInner::drain_inbound(&self.inner);

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

        inner.drain_oto_commands();
        Ok(())
    }

    /// Processes a trade tick through the matching engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not found in the cache.
    pub fn process_trade_tick(&self, trade: &TradeTick) -> anyhow::Result<()> {
        SandboxInner::drain_inbound(&self.inner);

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

        inner.drain_oto_commands();
        Ok(())
    }

    /// Processes a bar through the matching engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not found in the cache.
    pub fn process_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        SandboxInner::drain_inbound(&self.inner);

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

        inner.drain_oto_commands();
        Ok(())
    }

    /// Processes order book deltas through the matching engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not found in the cache.
    pub fn process_order_book_deltas(&self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        SandboxInner::drain_inbound(&self.inner);

        let instrument_id = deltas.instrument_id;
        let instrument = self.cache.borrow().try_instrument(&instrument_id)?.clone();

        let mut inner = self.inner.borrow_mut();
        inner.ensure_matching_engine(&instrument);
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_order_book_deltas(deltas)?;
        }

        inner.drain_oto_commands();
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

        inner.clear_inbound_queue();
        self.cancel_inbound_alert();
        inner.pending_funding.clear();
        inner.settled_funding.clear();
        inner.applied_adjustments.clear();
        inner.cancel_funding_timer();

        log::info!(
            "Sandbox execution client reset: venue={}",
            self.config.venue
        );
    }

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

    // Position reports are always empty, so an absent report is not evidence of a flat position
    fn provides_bulk_position_coverage(&self, _instrument_id: InstrumentId) -> bool {
        false
    }

    fn settles_contract_expirations(&self) -> bool {
        true
    }

    fn on_instrument(&mut self, instrument: InstrumentAny) {
        let instrument_id = instrument.id();
        let mut inner = self.inner.borrow_mut();
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id)
            && let Err(e) = engine.update_instrument(instrument)
        {
            log::error!("Failed to update instrument {instrument_id} in sandbox engine: {e}");
        }

        inner.drain_oto_commands();
    }

    fn get_account(&self) -> Option<AccountAny> {
        let account_id = self.core.borrow().account_id;
        self.cache.borrow().account_owned(&account_id)
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        Self::reset(self);
        Ok(())
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        let ts_init = self.clock.borrow().timestamp_ns();
        let state = self
            .factory
            .generate_account_state(balances, margins, reported, ts_event, ts_init, info);
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

        self.inner.borrow_mut().load_open_orders();

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

        self.deregister_message_handlers();
        self.cancel_expiry_sweep_timer();
        self.cancel_inbound_alert();
        self.inner.borrow_mut().cancel_funding_timer();

        // Command outcomes take the execution channel, so the engine stopping
        // this client processes them once its own borrow is released. Without a runner sender they
        // would dispatch synchronously into an engine `ExecutionEngine::stop` still holds mutably,
        // which is unsupported
        {
            let mut inner = self.inner.borrow_mut();
            let discarded = inner.take_inbound_queue();
            inner.refuse_discarded(discarded);
        }

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
        self.generate_account_state(balances, vec![], false, ts_event, None)?;

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
        let order = self.get_order(&cmd.client_order_id)?;

        if order.is_closed() {
            log::warn!("Cannot submit closed order {}", order.client_order_id());
            return Ok(());
        }

        let ts_init = self.clock.borrow().timestamp_ns();
        let event = self.factory.generate_order_submitted(&order, ts_init);
        self.dispatch_order_event(event);

        let mut inner = self.inner.borrow_mut();
        if inner.config.latency_model.is_none() {
            inner.apply_submit_order(&cmd)?;
        } else {
            inner.defer_or_apply(TradingCommand::SubmitOrder(cmd));
        }
        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let ts_init = self.clock.borrow().timestamp_ns();

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

        let mut inner = self.inner.borrow_mut();
        if inner.config.latency_model.is_none() {
            let _ = inner.apply_submit_order_list(&cmd);
        } else {
            inner.defer_or_apply(TradingCommand::SubmitOrderList(cmd));
        }
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.config.latency_model.is_none() {
            inner.apply_modify_order(&cmd);
        } else {
            inner.defer_or_apply(TradingCommand::ModifyOrder(cmd));
        }
        Ok(())
    }

    fn batch_modify_orders(&self, cmd: BatchModifyOrders) -> anyhow::Result<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.config.latency_model.is_none() {
            inner.apply_batch_modify_orders(&cmd);
        } else {
            inner.defer_or_apply(TradingCommand::ModifyOrders(cmd));
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.config.latency_model.is_none() {
            inner.apply_cancel_order(&cmd);
        } else {
            inner.defer_or_apply(TradingCommand::CancelOrder(cmd));
        }
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.config.latency_model.is_none() {
            inner.apply_cancel_all_orders(&cmd);
        } else {
            inner.defer_or_apply(TradingCommand::CancelAllOrders(cmd));
        }
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.config.latency_model.is_none() {
            inner.apply_batch_cancel_orders(&cmd);
        } else {
            inner.defer_or_apply(TradingCommand::CancelOrders(cmd));
        }
        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        let balances = self.get_current_account_balances();
        let ts_event = self.clock.borrow().timestamp_ns();
        self.generate_account_state(balances, vec![], false, ts_event, None)?;
        Ok(())
    }

    fn query_order(&self, _cmd: QueryOrder) -> anyhow::Result<()> {
        // Orders are tracked in the cache, no external query needed for sandbox
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let cache = self.cache.borrow();

        let client_order_id = cmd.client_order_id.or_else(|| {
            cmd.venue_order_id
                .and_then(|id| cache.client_order_id(&id).copied())
        });

        let Some(order) = client_order_id.and_then(|id| cache.order(&id)) else {
            return Ok(None);
        };

        if order.account_id() != Some(self.core.borrow().account_id)
            || order.instrument_id().venue != self.config.venue
            || cmd
                .instrument_id
                .is_some_and(|id| id != order.instrument_id())
            || cmd
                .venue_order_id
                .is_some_and(|id| Some(id) != order.venue_order_id())
        {
            return Ok(None);
        }

        let ts_init = self.clock.borrow().timestamp_ns();
        Ok(order_status_report(&order, ts_init))
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let account_id = self.core.borrow().account_id;
        let ts_init = self.clock.borrow().timestamp_ns();
        let cache = self.cache.borrow();

        let reports = cache
            .orders_refs(
                Some(&self.config.venue),
                cmd.instrument_id.as_ref(),
                None,
                Some(&account_id),
                None,
            )
            .into_iter()
            .filter(|order| !cmd.open_only || order.is_open() || order.is_inflight())
            .filter(|order| {
                !order.is_closed()
                    || (cmd.start.is_none_or(|start| order.ts_last() >= start)
                        && cmd.end.is_none_or(|end| order.ts_last() <= end))
            })
            .filter_map(|order| order_status_report(&order, ts_init))
            .collect();

        Ok(reports)
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
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ts_init = self.clock.borrow().timestamp_ns();
        generate_mass_status(self, lookback_mins, ts_init).await
    }
}

fn order_status_report(order: &OrderAny, ts_init: UnixNanos) -> Option<OrderStatusReport> {
    let mut report = order.to_order_status_report(None)?;
    report.ts_init = ts_init;
    report.venue_position_id = None;

    for event in order.events() {
        if let OrderEventAny::Canceled(event) = event {
            report.cancel_reason = event.reason.map(|reason| reason.to_string());
        }
    }

    Some(report)
}

// Wrapped in `Rc<RefCell<>>` so message handlers can hold weak references
struct SandboxInner {
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
    config: SandboxExecutionClientConfig,
    fill_model: FillModelHandle,
    fee_model: FeeModelHandle,
    matching_engines: AHashMap<InstrumentId, OrderMatchingEngine>,
    next_engine_raw_id: u32,
    balances: AHashMap<String, Money>,
    /// Forwards order events to the runner's execution channel once `start` finds a sender; shared
    /// with every matching engine this client owns.
    event_handler: Option<Rc<dyn Fn(OrderEventAny)>>,
    /// The trader ID (settlement event attribution; config no longer carries it).
    trader_id: TraderId,
    /// The client ID (for timer naming).
    client_id: ClientId,
    /// Factory for account state events, built from the same configuration
    /// inputs as the client's (the settlement path lives on the inner state).
    account_state_factory: OrderEventFactory,
    /// Pending funding settlements keyed by (boundary, InstrumentId).
    pending_funding: BTreeMap<(UnixNanos, InstrumentId), FundingRateUpdate>,
    /// Funding boundaries already settled (idempotency record).
    settled_funding: HashSet<(UnixNanos, InstrumentId)>,
    /// Applied joint-adjustment operation ids (`POLYMARKET_PREDICT`
    /// §6.3 C / §8.5(e) — same idempotency record form as funding): a
    /// repeated `operation_id` is never re-applied. Cleared on reset.
    applied_adjustments: HashSet<UUID4>,
    /// Wall-clock settlement alert callback; captures a weak self-reference and
    /// is assigned in [`SandboxExecutionClient::new`] once the `Rc` exists.
    funding_timer_callback: TimeEventCallback,
    /// Inbound commands deferred by latency, ordered as a min-heap by due time.
    inbound_queue: BinaryHeap<DelayedCommand>,
    inflight_orders: InflightOrders,
    matching_orders: MatchingOrders,
    /// Monotonic sequence providing FIFO tie-breaking for deferred commands sharing a due time, so
    /// no queued command can be overtaken by one enqueued after it.
    inbound_seq: u64,
    account_id: AccountId,
    self_weak: WeakCell<Self>,
}

/// A [`TradingCommand`] deferred by inbound latency, ordered by `due_ns` then `seq` so the
/// `BinaryHeap` behaves as a min-heap for FIFO draining. Equality follows the same key.
#[derive(Debug)]
struct DelayedCommand {
    due_ns: UnixNanos,
    seq: u64,
    command: TradingCommand,
}

impl Ord for DelayedCommand {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .due_ns
            .cmp(&self.due_ns)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for DelayedCommand {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DelayedCommand {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for DelayedCommand {}

impl SandboxInner {
    fn load_open_orders(&mut self) {
        let venue = self.config.venue;
        let account_id = self.account_id;
        let mut open_orders: Vec<(OrderAny, AccountId)> = {
            let cache = self.cache.borrow();
            cache
                .orders_open(Some(&venue), None, None, Some(&account_id), None)
                .into_iter()
                .filter(|order| !order.is_emulated())
                .filter_map(|order| {
                    order
                        .account_id()
                        .map(|account_id| (order.clone(), account_id))
                })
                .collect()
        };

        open_orders.sort_by(|(a, _), (b, _)| {
            a.ts_init()
                .cmp(&b.ts_init())
                .then_with(|| a.client_order_id().cmp(&b.client_order_id()))
        });

        for (order, account_id) in open_orders {
            let instrument_id = order.instrument_id();
            let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
            match instrument {
                Some(instrument) => {
                    self.ensure_matching_engine(&instrument);

                    if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                        engine.restore_open_order(&order, account_id);
                    }
                }
                None => {
                    log::warn!(
                        "No instrument for {instrument_id} to restore open order {}",
                        order.client_order_id()
                    );
                }
            }
        }
    }

    fn ensure_matching_engine(&mut self, instrument: &InstrumentAny) {
        let instrument_id = instrument.id();

        if !self.matching_engines.contains_key(&instrument_id) {
            let engine_config = self.config.to_matching_engine_config();
            let fill_model = self.fill_model.clone();
            let fee_model = self.fee_model.clone();
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

            engine.set_inflight_orders(self.inflight_orders.clone());
            engine.set_matching_orders(self.matching_orders.clone());
            self.matching_engines.insert(instrument_id, engine);
        }
    }

    fn process_quote_tick(&mut self, quote: &QuoteTick) {
        let instrument_id = quote.instrument_id;

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

        self.drain_oto_commands();
    }

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

        self.drain_oto_commands();
    }

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

        self.drain_oto_commands();
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

        self.drain_oto_commands();
    }

    fn process_order_book_depth(&mut self, depth: &OrderBookDepth) {
        let instrument_id = depth.instrument_id;

        let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
        if let Some(instrument) = instrument {
            self.ensure_matching_engine(&instrument);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id)
                && let Err(e) = engine.process_order_book_depth(depth)
            {
                log::error!("Error processing order book depth: {e}");
            }
        }

        self.drain_oto_commands();
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

        self.drain_oto_commands();
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

        if self
            .matching_engines
            .get_mut(&instrument_id)
            .is_some_and(OrderMatchingEngine::has_pending_fills)
        {
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

    // Retain open-order engines until terminal events apply, otherwise retry settlement
    // when a synchronous position callback could not borrow the sandbox.
    fn sweep_expired_engines(&mut self) {
        let expired_ids: Vec<InstrumentId> = self
            .matching_engines
            .keys()
            .copied()
            .filter(|instrument_id| {
                self.is_expired_now(*instrument_id) && !self.has_open_orders(*instrument_id)
            })
            .collect();

        let ts_now = self.clock.borrow().timestamp_ns();

        for instrument_id in expired_ids {
            if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                engine.process_instrument_expiration(ts_now);
            }

            self.sync_expired_cleanup(instrument_id);
        }

        self.drain_oto_commands();
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
            self.trader_id,
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
            if let Err(error) = adjusted.apply_adjustment(adjustment) {
                log::error!(
                    "Cannot apply funding adjustment for position {}: {error}",
                    adjusted.id
                );
                return;
            }
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
            None,
        );
        let endpoint = MessagingSwitchboard::portfolio_update_account();
        msgbus::send_account_state(endpoint, &state);
        true
    }

    /// Applies a joint cash/inventory adjustment batch atomically against
    /// this client's state (`POLYMARKET_PREDICT` §8.5(e)) — the shared core
    /// ([`nautilus_common::adjustment::apply_batch_to_state`], the verbatim
    /// extraction of backtest `SimulatedExchange::apply_adjustment_batch`).
    ///
    /// Instruments resolve from the cache (the sandbox's instrument source
    /// everywhere else); cash moves through the funding-settlement account
    /// path (`adjust_account`); the client's `frozen_account` flag gates
    /// batches exactly as it gates funding cash moves.
    ///
    /// # Errors
    ///
    /// Returns the core's error naming the first violated rule (idempotent
    /// replay, unknown instrument/position/account, insufficient balance —
    /// validation precedes application, so no partial state remains).
    fn apply_adjustment_batch(&mut self, batch: &SimulationAdjustmentBatch) -> anyhow::Result<()> {
        let ts_event = self.clock.borrow().timestamp_ns();
        let cache = Rc::clone(&self.cache);
        let clock = Rc::clone(&self.clock);
        let oms_type = self.config.oms_type;
        let frozen = self.config.frozen_account;
        // Split borrows: `adjust_account` borrows the inner state, so the
        // idempotence ledger moves out for the call.
        let mut applied = std::mem::take(&mut self.applied_adjustments);
        let this: &SandboxInner = self;
        let instruments = |instrument_id: &InstrumentId| {
            this.cache.borrow().instrument(instrument_id).cloned()
        };
        let mut adjust_cash = move |delta: Money| this.adjust_account(delta, ts_event);
        let result = nautilus_common::adjustment::apply_batch_to_state(
            &cache,
            &clock,
            &mut applied,
            &instruments,
            &mut adjust_cash,
            oms_type,
            frozen,
            batch,
        );
        self.applied_adjustments = applied;
        result
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

    /// Routes a deferred [`TradingCommand`] to its venue-side apply helper.
    fn apply_trading_command(&mut self, cmd: &TradingCommand) -> anyhow::Result<()> {
        self.inflight_orders.remove(cmd);

        // Only a deferred command can overtake the submit that would have created the engine, so
        // build it here and let the venue raise the rejection.
        if matches!(
            cmd,
            TradingCommand::ModifyOrder(_)
                | TradingCommand::ModifyOrders(_)
                | TradingCommand::CancelOrder(_)
                | TradingCommand::CancelOrders(_)
        ) && !self.ensure_engine_for(cmd.instrument_id())
        {
            self.refuse_command(cmd, "No matching engine for instrument");
            return Ok(());
        }

        match cmd {
            TradingCommand::SubmitOrder(cmd) => self.apply_submit_order(cmd)?,
            TradingCommand::SubmitOrderList(cmd) => {
                // Only a deferred leg has an `OrderSubmitted` out that nothing else would resolve
                for order in self.apply_submit_order_list(cmd) {
                    self.reject_submit_leg(cmd, &order, "No instrument for order");
                }
            }
            TradingCommand::ModifyOrder(cmd) => self.apply_modify_order(cmd),
            TradingCommand::ModifyOrders(cmd) => self.apply_batch_modify_orders(cmd),
            TradingCommand::CancelOrder(cmd) => self.apply_cancel_order(cmd),
            TradingCommand::CancelOrders(cmd) => self.apply_batch_cancel_orders(cmd),
            TradingCommand::CancelAllOrders(cmd) => self.apply_cancel_all_orders(cmd),
            TradingCommand::QueryOrder(_) | TradingCommand::QueryAccount(_) => {}
        }
        Ok(())
    }

    fn refuse_command(&self, command: &TradingCommand, reason: &str) {
        self.refuse_command_deduped(command, reason, &mut AHashSet::new());
    }

    fn refuse_command_deduped(
        &self,
        command: &TradingCommand,
        reason: &str,
        pending_denied: &mut AHashSet<(ClientOrderId, UUID4)>,
    ) {
        let ts_now = self.clock.borrow().timestamp_ns();
        let account_id = self.account_id;
        let reason = Ustr::from(reason);

        let reject_submit = |trader_id, strategy_id, instrument_id, client_order_id| {
            self.dispatch_order_event(OrderEventAny::Rejected(OrderRejected::new(
                trader_id,
                strategy_id,
                instrument_id,
                client_order_id,
                account_id,
                reason,
                UUID4::new(),
                ts_now,
                ts_now,
                false,
                false,
            )));
        };

        match command {
            TradingCommand::SubmitOrder(cmd) => reject_submit(
                cmd.trader_id,
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
            ),
            TradingCommand::SubmitOrderList(cmd) => {
                // Keyed per leg: the list's `instrument_id` is only representative
                let in_flight: Vec<(ClientOrderId, InstrumentId)> = self
                    .cache
                    .borrow()
                    .orders_for_ids(&cmd.order_list.client_order_ids, cmd)
                    .iter()
                    .filter(|order| !order.is_closed())
                    .map(|order| (order.client_order_id(), order.instrument_id()))
                    .collect();

                for (client_order_id, instrument_id) in in_flight {
                    reject_submit(
                        cmd.trader_id,
                        cmd.strategy_id,
                        instrument_id,
                        client_order_id,
                    );
                }
            }
            TradingCommand::ModifyOrder(cmd) => {
                if pending_denied.insert((cmd.client_order_id, cmd.command_id)) {
                    self.deny_modify(cmd, reason, ts_now);
                }
            }
            TradingCommand::ModifyOrders(cmd) => {
                for modify in &cmd.modifies {
                    if pending_denied.insert((modify.client_order_id, modify.command_id)) {
                        self.deny_modify(modify, reason, ts_now);
                    }
                }
            }
            TradingCommand::CancelOrder(cmd) => {
                if pending_denied.insert((cmd.client_order_id, cmd.command_id)) {
                    self.deny_cancel(cmd, reason, ts_now);
                }
            }
            TradingCommand::CancelOrders(cmd) => {
                for cancel in &cmd.cancels {
                    if pending_denied.insert((cancel.client_order_id, cancel.command_id)) {
                        self.deny_cancel(cancel, reason, ts_now);
                    }
                }
            }
            // Cancel-all carries no individual command IDs to deny
            TradingCommand::CancelAllOrders(_) => {}
            TradingCommand::QueryOrder(_) | TradingCommand::QueryAccount(_) => {}
        }
    }

    fn deny_modify(&self, cmd: &ModifyOrder, reason: Ustr, ts_now: UnixNanos) {
        let mut event = OrderModifyDenied::new(
            cmd.trader_id,
            cmd.strategy_id,
            cmd.instrument_id,
            cmd.client_order_id,
            reason,
            UUID4::new(),
            ts_now,
            ts_now,
            cmd.venue_order_id,
            Some(self.account_id),
        );
        event.causation_id = Some(cmd.command_id);
        self.dispatch_order_event(OrderEventAny::ModifyDenied(event));
    }

    fn deny_cancel(&self, cmd: &CancelOrder, reason: Ustr, ts_now: UnixNanos) {
        let mut event = OrderCancelDenied::new(
            cmd.trader_id,
            cmd.strategy_id,
            cmd.instrument_id,
            cmd.client_order_id,
            reason,
            UUID4::new(),
            ts_now,
            ts_now,
            cmd.venue_order_id,
            Some(self.account_id),
        );
        event.causation_id = Some(cmd.command_id);
        self.dispatch_order_event(OrderEventAny::CancelDenied(event));
    }

    fn dispatch_order_event(&self, event: OrderEventAny) {
        if let Some(handler) = &self.event_handler {
            handler(event);
        } else {
            msgbus::send_order_event(MessagingSwitchboard::exec_engine_process(), event);
        }
    }

    /// Creates the matching engine from the cached instrument when it does not exist yet, so it can
    /// answer for an order it has never seen. Returns whether an engine now exists.
    fn ensure_engine_for(&mut self, instrument_id: InstrumentId) -> bool {
        if !self.matching_engines.contains_key(&instrument_id) {
            let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
            let Some(instrument) = instrument else {
                log::warn!(
                    "Cannot process command for {instrument_id}: instrument missing from cache",
                );
                return false;
            };
            self.ensure_matching_engine(&instrument);
        }

        self.matching_engines.contains_key(&instrument_id)
    }

    fn apply_submit_order(&mut self, cmd: &SubmitOrder) -> anyhow::Result<()> {
        let mut order = self.cache.borrow().try_order_owned(&cmd.client_order_id)?;

        let instrument_id = order.instrument_id();
        let instrument = self.cache.borrow().try_instrument(&instrument_id)?.clone();

        self.ensure_matching_engine(&instrument);

        self.seed_cached_market_data(instrument_id);

        if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
            engine.process_order(&mut order, self.account_id);
            self.sync_expired_cleanup(instrument_id);
        }

        self.drain_oto_commands();
        Ok(())
    }

    /// Applies a submit-order-list command to the matching engines (venue-side), less the per-order
    /// `OrderSubmitted` dispatch kept by the client handler, returning the legs that could not
    /// reach a matching engine.
    fn apply_submit_order_list(&mut self, cmd: &SubmitOrderList) -> Vec<OrderAny> {
        let orders: Vec<OrderAny> = self
            .cache
            .borrow()
            .orders_for_ids(&cmd.order_list.client_order_ids, cmd);

        let mut cleanup_instrument_ids = Vec::new();
        let mut unresolved: Vec<OrderAny> = Vec::new();

        for order in &orders {
            if order.is_closed() {
                continue;
            }

            let instrument_id = order.instrument_id();
            if !cleanup_instrument_ids.contains(&instrument_id) {
                cleanup_instrument_ids.push(instrument_id);
            }
            let instrument = self.cache.borrow().instrument(&instrument_id).cloned();

            let Some(instrument) = instrument else {
                // Skipped per leg rather than failing the whole command: the legs that did reach
                // the venue are live orders.
                unresolved.push(order.clone());
                continue;
            };

            self.ensure_matching_engine(&instrument);

            self.seed_cached_market_data(instrument_id);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                let mut order_clone = order.clone();
                engine.process_order(&mut order_clone, self.account_id);
            }
        }

        if !cleanup_instrument_ids.is_empty() {
            self.sync_expired_cleanup_many(&cleanup_instrument_ids);
        }

        self.drain_oto_commands();
        unresolved
    }

    fn drain_oto_commands(&mut self) {
        while let Some(command) = self.matching_orders.pop_command() {
            let instrument_id = command.instrument_id();
            if self.ensure_engine_for(instrument_id) {
                if matches!(command, OtoCommand::Release { .. }) {
                    self.seed_cached_market_data(instrument_id);
                }

                if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                    engine.process_oto_command(&command);
                }
            }

            if let OtoCommand::Release {
                parent_quantity: Some(quantity),
                parent_order_id,
                parent_instrument_id,
                ..
            } = command
                && let Some(engine) = self.matching_engines.get_mut(&parent_instrument_id)
            {
                engine.process_oto_resize(parent_order_id, quantity);
            }
        }
    }

    /// Rejects a single leg of `cmd` the venue could not accept, leaving its siblings untouched.
    fn reject_submit_leg(&self, cmd: &SubmitOrderList, order: &OrderAny, reason: &str) {
        let ts_now = self.clock.borrow().timestamp_ns();
        self.dispatch_order_event(OrderEventAny::Rejected(OrderRejected::new(
            cmd.trader_id,
            cmd.strategy_id,
            order.instrument_id(),
            order.client_order_id(),
            self.account_id,
            Ustr::from(reason),
            UUID4::new(),
            ts_now,
            ts_now,
            false,
            false,
        )));
    }

    fn seed_cached_market_data(&mut self, instrument_id: InstrumentId) {
        let Some(engine) = self.matching_engines.get_mut(&instrument_id) else {
            return;
        };

        // Cached ticks initialize an empty engine; replay would replenish consumed liquidity
        if self.config.liquidity_consumption
            && (engine.get_book().update_count != 0 || engine.get_core().last.is_some())
        {
            return;
        }

        let (quote, trade) = {
            let cache = self.cache.borrow();
            (
                cache.quote(&instrument_id).copied(),
                cache.trade(&instrument_id).copied(),
            )
        };

        if let Some(quote) = quote
            && check_quote_or_drop("cached quote tick", &quote, &engine.instrument)
        {
            engine.process_quote_tick(&quote);
        }

        if self.config.trade_execution
            && let Some(trade) = trade
            && check_trade_or_drop("cached trade tick", &trade, &engine.instrument)
        {
            engine.process_trade_tick(&trade);
        }
    }

    fn apply_modify_order(&mut self, cmd: &ModifyOrder) {
        let account_id = self.account_id;
        if let Some(engine) = self.matching_engines.get_mut(&cmd.instrument_id) {
            engine.process_modify(cmd, account_id);
        } else {
            let ts_now = self.clock.borrow().timestamp_ns();
            self.deny_modify(cmd, "No matching engine for instrument".into(), ts_now);
        }

        self.drain_oto_commands();
    }

    fn apply_batch_modify_orders(&mut self, cmd: &BatchModifyOrders) {
        let account_id = self.account_id;
        if let Some(engine) = self.matching_engines.get_mut(&cmd.instrument_id) {
            engine.process_batch_modify(cmd, account_id);
        } else {
            let ts_now = self.clock.borrow().timestamp_ns();
            for command in &cmd.modifies {
                self.deny_modify(command, "No matching engine for instrument".into(), ts_now);
            }
        }

        self.drain_oto_commands();
    }

    fn apply_cancel_order(&mut self, cmd: &CancelOrder) {
        let account_id = self.account_id;
        if let Some(engine) = self.matching_engines.get_mut(&cmd.instrument_id) {
            engine.process_cancel(cmd, account_id);
        } else {
            let ts_now = self.clock.borrow().timestamp_ns();
            self.deny_cancel(cmd, "No matching engine for instrument".into(), ts_now);
        }

        self.drain_oto_commands();
    }

    fn apply_cancel_all_orders(&mut self, cmd: &CancelAllOrders) {
        let instrument_id = cmd.instrument_id;
        if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
            engine.process_cancel_all(cmd, self.account_id);
        } else {
            log::debug!("No open orders to cancel for {instrument_id}: no matching engine");
        }

        self.drain_oto_commands();
    }

    fn apply_batch_cancel_orders(&mut self, cmd: &BatchCancelOrders) {
        let account_id = self.account_id;
        if let Some(engine) = self.matching_engines.get_mut(&cmd.instrument_id) {
            engine.process_batch_cancel(cmd, account_id);
        } else {
            let ts_now = self.clock.borrow().timestamp_ns();
            for command in &cmd.cancels {
                self.deny_cancel(command, "No matching engine for instrument".into(), ts_now);
            }
        }

        self.drain_oto_commands();
    }

    /// Enqueues a trading command to be applied after its inbound latency elapses, as backtest
    /// `generate_inflight_command` does, but keyed off arrival rather than `command.ts_init()`.
    fn enqueue(&mut self, command: TradingCommand, now_ns: UnixNanos, due_ns: UnixNanos) {
        let seq = self.inbound_seq;
        self.inbound_seq += 1;

        self.inflight_orders.insert(&command);
        self.inbound_queue.push(DelayedCommand {
            due_ns,
            seq,
            command,
        });

        self.arm_inbound_alert(now_ns);
    }

    /// Defers `command` by its inbound latency leg, or applies it inline when that leg is zero.
    ///
    /// A zero-leg command is still queued behind a head already due, so the two apply in
    /// `(due_ns, seq)` order.
    fn defer_or_apply(&mut self, command: TradingCommand) {
        let now_ns = self.clock.borrow().timestamp_ns();

        let leg_latency = match self.command_leg_latency(&command, now_ns) {
            Ok(latency) => latency,
            Err(e) => {
                let reason = format!("Latency model failed: {e:#}");
                log::error!("{reason}");
                self.refuse_command(&command, &reason);
                return;
            }
        };

        let Some(due_ns) = now_ns.checked_add(leg_latency) else {
            let reason = "Latency arrival timestamp exceeds u64 nanoseconds";
            log::error!("{reason}");
            self.refuse_command(&command, reason);
            return;
        };

        let head_is_due = self
            .inbound_queue
            .peek()
            .is_some_and(|delayed| delayed.due_ns <= now_ns);
        if head_is_due || leg_latency > DurationNanos::ZERO {
            self.enqueue(command, now_ns, due_ns);
            return;
        }

        if let Err(e) = self.apply_trading_command(&command) {
            log::error!("Error applying command: {e}");
            self.refuse_command(&command, "Command could not be applied at the venue");
        }
    }

    /// Returns the inbound latency leg for `command`, or zero when no model is set.
    fn command_leg_latency(
        &self,
        command: &TradingCommand,
        ts_now: UnixNanos,
    ) -> anyhow::Result<DurationNanos> {
        let Some(latency_model) = self.config.latency_model.as_ref() else {
            return Ok(DurationNanos::ZERO);
        };

        match command {
            TradingCommand::SubmitOrder(_) | TradingCommand::SubmitOrderList(_) => {
                latency_model.get_insert_latency(command, ts_now)
            }
            TradingCommand::ModifyOrder(_) | TradingCommand::ModifyOrders(_) => {
                latency_model.get_update_latency(command, ts_now)
            }
            TradingCommand::CancelOrder(_)
            | TradingCommand::CancelOrders(_)
            | TradingCommand::CancelAllOrders(_) => {
                latency_model.get_delete_latency(command, ts_now)
            }
            TradingCommand::QueryOrder(_) | TradingCommand::QueryAccount(_) => {
                Ok(DurationNanos::ZERO)
            }
        }
    }

    fn pop_due(&mut self, now_ns: UnixNanos) -> Option<DelayedCommand> {
        self.inbound_queue
            .peek()
            .is_some_and(|delayed| delayed.due_ns <= now_ns)
            .then(|| self.inbound_queue.pop().expect("peek returned Some"))
    }

    fn on_quote_tick(inner: &Rc<RefCell<Self>>, quote: &QuoteTick) {
        Self::drain_inbound(inner);
        inner.borrow_mut().process_quote_tick(quote);
    }

    fn on_trade_tick(inner: &Rc<RefCell<Self>>, trade: &TradeTick) {
        Self::drain_inbound(inner);
        inner.borrow_mut().process_trade_tick(trade);
    }

    fn on_bar(inner: &Rc<RefCell<Self>>, bar: &Bar) {
        Self::drain_inbound(inner);
        inner.borrow_mut().process_bar(bar);
    }

    fn on_order_book_deltas(inner: &Rc<RefCell<Self>>, deltas: &OrderBookDeltas) {
        Self::drain_inbound(inner);
        inner.borrow_mut().process_order_book_deltas(deltas);
    }

    fn on_order_book_depth(inner: &WeakCell<Self>, venue: Venue, depth: &OrderBookDepth) {
        if depth.instrument_id.venue != venue {
            return;
        }

        let Some(inner_rc) = inner.upgrade() else {
            return;
        };

        Self::drain_inbound(&inner_rc);
        inner_rc.borrow_mut().process_order_book_depth(depth);
    }

    fn on_instrument_status(inner: &Rc<RefCell<Self>>, status: &InstrumentStatus) {
        Self::drain_inbound(inner);
        inner.borrow_mut().process_instrument_status(status);
    }

    fn on_instrument_close(inner: &Rc<RefCell<Self>>, close: &InstrumentClose) {
        Self::drain_inbound(inner);
        inner.borrow_mut().process_instrument_close(close);
    }

    fn on_position_event(inner: &Rc<RefCell<Self>>, event: &PositionEvent) {
        let instrument_id = event.instrument_id();

        let Ok(mut inner) = inner.try_borrow_mut() else {
            log::debug!(
                "Skipping immediate expiration retry for {instrument_id} due to active sandbox borrow",
            );
            return;
        };

        if instrument_id.venue != inner.config.venue || event.account_id() != inner.account_id {
            return;
        }

        // Position events observe the cache after the fill has been applied
        let ts_now = inner.clock.borrow().timestamp_ns();
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_instrument_expiration(ts_now);
        }

        inner.sync_expired_cleanup(instrument_id);
        inner.drain_oto_commands();
    }

    /// Applies every inbound command whose latency has elapsed, in `(due_ns, seq)` order, then
    /// arms the alert for the earliest command still queued.
    ///
    /// Called at the top of each data handler and public `process_*` method, so a command due
    /// before a tick is processed is applied before that tick, and by the alert when no data is
    /// flowing.
    fn drain_inbound(inner: &Rc<RefCell<Self>>) {
        // The alert fires on the runner task, where a nested msgbus dispatch may already hold the
        // borrow; the next data tick or public `process_*` call releases the queue instead.
        let Ok(mut this) = inner.try_borrow_mut() else {
            log::debug!("Skipping sandbox inbound drain due to active borrow");
            return;
        };

        if this.config.latency_model.is_none() {
            return;
        }

        let now_ns = this.clock.borrow().timestamp_ns();

        while let Some(delayed) = this.pop_due(now_ns) {
            if let Err(e) = this.apply_trading_command(&delayed.command) {
                log::error!("Error applying deferred command: {e}");
                this.refuse_command(
                    &delayed.command,
                    "Command could not be applied at the venue",
                );
            }
        }

        this.arm_inbound_alert(now_ns);
    }

    /// Takes every command still deferred by inbound latency, ordered for [`Self::refuse_discarded`]
    /// to unwind last-issued-first.
    fn take_inbound_queue(&mut self) -> Vec<DelayedCommand> {
        self.inflight_orders.clear();

        if self.inbound_queue.is_empty() {
            return Vec::new();
        }

        log::warn!(
            "Discarding {} command(s) still in flight at stop",
            self.inbound_queue.len(),
        );

        // Publish command outcomes before rejecting the submit they were queued behind
        let mut discarded = std::mem::take(&mut self.inbound_queue).into_vec();
        discarded.sort_unstable_by_key(|delayed| std::cmp::Reverse(delayed.seq));
        discarded
    }

    fn refuse_discarded(&self, discarded: Vec<DelayedCommand>) {
        let mut pending_denied = AHashSet::new();

        for delayed in discarded {
            self.refuse_command_deduped(
                &delayed.command,
                "Client stopped before the command was sent",
                &mut pending_denied,
            );
        }
    }

    /// Clears every command still deferred by inbound latency, without rejecting any of them:
    /// `reset` discards all client state, so there is nothing to release.
    fn clear_inbound_queue(&mut self) {
        self.inbound_queue.clear();
        self.inflight_orders.clear();
    }

    /// (Re)arms the `LiveClock` alert for the earliest queued `due_ns` while that is still ahead
    /// of `now_ns`.
    ///
    /// A due time already reached is not armed for: the drain releasing it is already pending on
    /// the runner, and an alert at a time the clock has passed only asks the clock to warn. The
    /// clock reads its own time again when arming, so a leg shorter than that gap can still warn.
    fn arm_inbound_alert(&self, now_ns: UnixNanos) {
        let Some(earliest_due) = self.inbound_queue.peek().map(|delayed| delayed.due_ns) else {
            return;
        };

        if earliest_due <= now_ns {
            return;
        }

        let name = inbound_alert_name(self.client_id);
        let armed_ns = self.clock.borrow().next_time_ns(&name);

        match armed_ns {
            // Already armed no later than the new earliest due, so that alert still wakes the drain
            Some(armed_ns) if armed_ns <= earliest_due => return,
            // Canceling first avoids the warning `replace_existing_timer` would log
            Some(_) => self.clock.borrow_mut().cancel_timer(&name),
            None => {}
        }

        let inner_weak = self.self_weak.clone();
        let alert_name = name.clone();

        let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_event: TimeEvent| {
            let Some(inner_rc) = inner_weak.upgrade() else {
                return;
            };

            // Retire the spent one-shot before the drain's exit path reads it back as still armed
            if let Ok(this) = inner_rc.try_borrow() {
                this.clock.borrow_mut().cancel_timer(&alert_name);
            }

            // The pass arms for whatever it leaves queued
            Self::drain_inbound(&inner_rc);
        });

        if let Err(e) = self.clock.borrow_mut().set_time_alert_ns(
            &name,
            earliest_due,
            Some(TimeEventCallback::from(callback)),
            Some(true),
        ) {
            log::error!("Failed to arm sandbox inbound alert '{name}': {e}");
        }
    }
}

fn inbound_alert_name(client_id: ClientId) -> String {
    format!("{client_id}-sandbox-inbound-alert")
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
#[cfg(test)]
mod funding_settlement_tests {
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clients::ExecutionClient,
        clock::{Clock, VirtualClock},
        msgbus::{
            self, MessageBus, MessagingSwitchboard, TypedHandler,
            typed_handler::ShareableMessageHandler,
        },
    };
    use nautilus_core::UnixNanos;
    use nautilus_execution::{
        client::core::ExecutionClientCore,
        models::fee::{FeeModelAny, MakerTakerFeeModel},
    };
    use nautilus_model::{
        data::{FundingRateUpdate, MarkPriceUpdate, QuoteTick},
        enums::{AccountType, BookType, OrderSide, OrderType, PositionAdjustmentType},
        events::{AccountState, FundingSettlement, PositionEvent},
        identifiers::{AccountId, ClientId, InstrumentId, TradeId, TraderId, Venue},
        instruments::{
            Instrument,
            InstrumentAny,
            stubs::{crypto_future_btcusdt, crypto_perpetual_ethusdt},
        },
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
            account_id: AccountId::from(ACCOUNT_ID),
            venue: Venue::new("BINANCE"),
            starting_balances: vec![Money::new(100_000.0, Currency::USDT())],
            base_currency: None,
            oms_type: nautilus_model::enums::OmsType::Netting,
            account_type: AccountType::Margin,
            default_leverage: Decimal::ONE,
            leverages: ahash::AHashMap::new(),
            book_type: BookType::L1_MBP,
            fee_model: Some(FeeModelAny::MakerTaker(MakerTakerFeeModel::zero())),
            margin_model: None,
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
            fill_model: None,
            latency_model: None,
            queue_position: false,
            liquidity_consumption: false,
            bar_adaptive_high_low_ordering: false,
            use_market_order_acks: false,
            oto_full_trigger: false,
            price_protection_points: 0,
            order_priority_reset_policy: Default::default(),
        };

        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));

        let core = ExecutionClientCore::new(
            TraderId::from("SANDBOX-001"),
            ClientId::new("SANDBOX"),
            config.venue,
            config.oms_type,
            config.account_id,
            config.account_type,
            config.base_currency,
            cache.clone(),
        );

        let client = SandboxExecutionClient::new(core, config, clock.clone(), cache.clone())
            .expect("sandbox client with explicit zero-fee model");

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
        let boundary = UnixNanos::default(); // VirtualClock starts at 0: boundary already due.

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

    // ── Joint adjustment batches (§8.5(e)): msgbus topic delivery → shared
    //    core → cash/inventory effects; idempotence; validation atomicity. ──

    fn adjustment_ids() -> (TraderId, nautilus_model::identifiers::StrategyId, AccountId) {
        (
            TraderId::from("SANDBOX-001"),
            nautilus_model::identifiers::StrategyId::from("S-001"),
            AccountId::from(ACCOUNT_ID),
        )
    }

    fn adjustment_legs(
        fixture: &Fixture,
    ) -> (InstrumentId, InstrumentId) {
        let second = InstrumentAny::CryptoFuture(crypto_future_btcusdt(
            2,
            6,
            Price::from("0.01"),
            Quantity::from("0.000001"),
        ));
        let second_id = second.id();
        fixture.cache.borrow_mut().add_instrument(second).unwrap();
        (fixture.instrument.id(), second_id)
    }

    fn split_batch(
        operation_id: nautilus_core::UUID4,
        sets: Decimal,
    ) -> nautilus_common::adjustment::SimulationAdjustmentBatch {
        let (trader, strategy, account) = adjustment_ids();
        nautilus_common::adjustment::SimulationAdjustmentBatch::from_split(
            operation_id,
            trader,
            strategy,
            account,
            sets,
            Money::from_decimal(sets, Currency::USDT()).unwrap(),
            &[
                (InstrumentId::from("ETHUSDT-PERP.BINANCE"), None),
                (InstrumentId::from("ETHUSDT-123.BINANCE"), None),
            ],
            vec![],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_adjustment_batch_topic_applies_split_cash_and_inventory() {
        let fixture = setup_fixture(/* with_position */ false, /* with_mark_price */ false).await;
        let (first_id, second_id) = adjustment_legs(&fixture);

        // Production delivery path: publish on the account-keyed adjustment
        // topic; the connect-time subscription routes it into the shared core.
        let batch = split_batch(nautilus_core::UUID4::new(), Decimal::from(5));
        let topic = nautilus_common::msgbus::switchboard::get_adjustment_batch_topic(
            AccountId::from(ACCOUNT_ID),
        );
        msgbus::publish_any(topic, &batch);

        // Cash out exactly the collateral.
        assert_eq!(
            cache_usdt_balance(&fixture),
            Some(Money::from("99995 USDT"))
        );
        // Both legs established at the netting ids with exact quantity/basis
        // (5 collateral over 2 legs of 5 shares → 0.5 per share).
        let strategy_id = adjustment_ids().1;
        for (instrument_id, expected_qty) in [
            (first_id, Quantity::from("5.000")),
            (second_id, Quantity::from("5.000000")),
        ] {
            let netting_id = nautilus_model::identifiers::PositionId::from(format!(
                "{instrument_id}-{strategy_id}"
            ));
            let position = fixture
                .cache
                .borrow()
                .position(&netting_id)
                .map(|position| position.clone())
                .expect("netting position established without a fake fill");
            assert_eq!(position.quantity, expected_qty);
            assert_eq!(position.avg_px_open, Decimal::from_str_exact("0.5").unwrap());
        }

        // A second split absorbs into the open netting positions and carries
        // its allocated basis via `cost_basis_added` (§6.3 C).
        let absorb = split_batch(nautilus_core::UUID4::new(), Decimal::from(3));
        let topic = nautilus_common::msgbus::switchboard::get_adjustment_batch_topic(
            AccountId::from(ACCOUNT_ID),
        );
        msgbus::publish_any(topic, &absorb);
        assert_eq!(
            cache_usdt_balance(&fixture),
            Some(Money::from("99992 USDT"))
        );
        {
            let cache = fixture.cache.borrow();
            let netting_id = nautilus_model::identifiers::PositionId::from(format!(
                "{first_id}-{strategy_id}"
            ));
            let position = cache.position(&netting_id).expect("absorbed netting leg");
            assert_eq!(position.quantity, Quantity::from("8.000"));
        }
        let adjustments = fixture.captures.adjustments.borrow();
        let last = adjustments.last().expect("PositionAdjusted published");
        let PositionEvent::PositionAdjusted(last) = last else {
            panic!("expected PositionAdjusted");
        };
        // 3 collateral over 2 legs → 1.5 allocated basis on this leg.
        assert_eq!(last.cost_basis_added, Some(Decimal::from_str_exact("1.5").unwrap()));
    }

    #[tokio::test]
    async fn test_adjustment_batch_replay_rejected_state_unchanged() {
        let fixture = setup_fixture(/* with_position */ false, /* with_mark_price */ false).await;
        let _ = adjustment_legs(&fixture);

        let operation_id = nautilus_core::UUID4::new();
        let batch = split_batch(operation_id, Decimal::from(5));
        fixture.client.apply_adjustment_batch(&batch).unwrap();
        assert_eq!(
            cache_usdt_balance(&fixture),
            Some(Money::from("99995 USDT"))
        );

        // Same operation_id replayed: rejected, never re-applied.
        let err = fixture.client.apply_adjustment_batch(&batch).unwrap_err();
        assert!(err.to_string().contains("already applied"), "{err}");
        assert_eq!(
            cache_usdt_balance(&fixture),
            Some(Money::from("99995 USDT"))
        );
    }

    #[tokio::test]
    async fn test_adjustment_batch_insufficient_cash_rejected_atomically() {
        let fixture = setup_fixture(/* with_position */ false, /* with_mark_price */ false).await;
        let _ = adjustment_legs(&fixture);

        // 200,000 sets against a 100,000 USDT balance: validation precedes
        // application — no partial cash, no inventory legs.
        let batch = split_batch(nautilus_core::UUID4::new(), Decimal::from(200_000));
        let err = fixture.client.apply_adjustment_batch(&batch).unwrap_err();
        assert!(err.to_string().contains("overdrafts"), "{err}");
        assert_eq!(
            cache_usdt_balance(&fixture),
            Some(Money::from("100000 USDT"))
        );
        let cache = fixture.cache.borrow();
        let positions = cache.positions_open(None, None, None, None, None);
        assert!(
            positions.is_empty(),
            "rejected batch must leave no inventory legs"
        );
    }
}
