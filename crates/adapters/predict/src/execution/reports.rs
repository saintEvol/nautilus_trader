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

//! Reconciliation reports for durable local submission identities.

use alloy_primitives::B256;
use nautilus_common::messages::execution::{
    GenerateFillReports, GenerateOrderStatusReport, GenerateOrderStatusReports,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::OrderStatus,
    events::{OrderAccepted, OrderEventAny, OrderFilled, OrderSubmitted},
    identifiers::{InstrumentId, VenueOrderId},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport},
    types::{Money, Quantity},
};
use rust_decimal::Decimal;

use super::{
    amounts::wei_per_unit, client::PredictExecutionClient, fill_evidence::PredictFillEvidence,
    lifecycle::parse_wei, orders::TrackedOrder,
};
use crate::{
    common::consts::{PREDICT_VENUE, collateral_currency},
    http::{
        error::PredictHttpError,
        trading::{PredictOrderRecord, PredictOrderStatus},
    },
};

impl PredictExecutionClient {
    fn local_orders(&self, instrument: Option<&InstrumentId>) -> Vec<OrderAny> {
        self.core
            .cache()
            .orders(Some(&PREDICT_VENUE), instrument, None, None, None)
            .into_iter()
            .filter(|order| {
                order.trader_id() == self.core.trader_id
                    && order
                        .account_id()
                        .is_none_or(|account| account == self.core.account_id)
            })
            .map(|order| order.cloned())
            .collect()
    }

    async fn signed_hash(&self, order: &OrderAny) -> anyhow::Result<Option<B256>> {
        match &self.submission_store {
            Some(store) => {
                store
                    .load(
                        self.core.account_id,
                        order.client_order_id(),
                        order.ts_init(),
                    )
                    .await
            }
            None => Ok(None),
        }
    }

    pub(super) async fn recover_missing_orders(&self) -> anyhow::Result<Vec<OrderAny>> {
        let Some(store) = &self.submission_store else {
            return Ok(Vec::new());
        };
        let submissions = store
            .recoverable(self.core.account_id, self.core.trader_id)
            .await?;
        let cache = self.core.cache();
        let mut recovered = Vec::new();
        let mut ids = std::collections::HashSet::new();
        for submission in submissions {
            let initialized = submission.order.ok_or_else(|| {
                anyhow::anyhow!("recoverable Predict submission lacks order initialization")
            })?;
            anyhow::ensure!(
                submission.account_id == self.core.account_id
                    && initialized.trader_id == self.core.trader_id
                    && initialized.instrument_id.venue == *PREDICT_VENUE
                    && initialized.client_order_id == submission.client_order_id
                    && initialized.ts_init == submission.initialized_at
                    && ids.insert(initialized.client_order_id),
                "invalid or ambiguous durable Predict order identity"
            );
            if let Some(order) = cache.order(&initialized.client_order_id) {
                anyhow::ensure!(
                    order.init_event() == &initialized,
                    "Predict cache order conflicts with durable initialization"
                );
                if order.status() != OrderStatus::Initialized {
                    continue;
                }
            }
            let mut fills = Vec::new();
            let positions = cache.positions(
                Some(&PREDICT_VENUE),
                Some(&initialized.instrument_id),
                Some(&initialized.strategy_id),
                Some(&self.core.account_id),
                None,
            );
            for position in positions {
                fills.extend(
                    position
                        .events
                        .iter()
                        .filter(|fill| {
                            fill.client_order_id == initialized.client_order_id
                                && fill.trader_id == initialized.trader_id
                        })
                        .cloned(),
                );
            }
            for snapshot in cache.position_snapshots(None, Some(&self.core.account_id)) {
                if snapshot.instrument_id == initialized.instrument_id
                    && snapshot.strategy_id == initialized.strategy_id
                    && snapshot.trader_id == initialized.trader_id
                {
                    fills.extend(
                        snapshot
                            .events
                            .into_iter()
                            .filter(|fill| fill.client_order_id == initialized.client_order_id),
                    );
                }
            }
            recovered.push(replay_submission(initialized, self.core.account_id, fills)?);
        }
        Ok(recovered)
    }

    pub(super) async fn restore_tracking(&self) -> anyhow::Result<()> {
        for order in self
            .local_orders(None)
            .into_iter()
            .filter(|order| !order.is_closed())
        {
            let Some(hash) = self.signed_hash(&order).await? else {
                continue;
            };
            let filled = order
                .filled_qty()
                .as_decimal()
                .checked_mul(wei_per_unit())
                .ok_or_else(|| {
                    anyhow::anyhow!("Predict cached filled quantity exceeds wei range")
                })?;
            let last_seen_filled_wei: i128 = filled.try_into()?;
            if order.status() == OrderStatus::Initialized {
                self.emitter.emit_order_submitted(&order);
            }
            self.tracked
                .lock()
                .expect("tracked lock")
                .entry(format!("{hash:#x}"))
                .or_insert_with(|| TrackedOrder {
                    venue_order_id: order.venue_order_id(),
                    order,
                    quote: collateral_currency(),
                    last_seen_filled_wei,
                });
        }
        Ok(())
    }

    async fn remote_order(&self, order: &OrderAny) -> anyhow::Result<Option<PredictOrderRecord>> {
        let Some(hash) = self.signed_hash(order).await? else {
            return Ok(None);
        };
        match self.trading.get_order(&format!("{hash:#x}")).await {
            Ok(record) => {
                anyhow::ensure!(
                    record.order.hash.parse::<B256>()? == hash
                        && record.order.maker.parse::<alloy_primitives::Address>()? == self.wallet,
                    "Predict order reply disagrees with durable order identity"
                );
                Ok(Some(record))
            }
            // A signed order can be absent while the remote outcome is unknown.
            Err(PredictHttpError::Http { status: 404, .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn confirmed_fills(
        &self,
        record: &PredictOrderRecord,
    ) -> anyhow::Result<Vec<PredictFillEvidence>> {
        let expected = parse_wei(&record.amount_filled)?;
        if expected == 0 {
            return Ok(Vec::new());
        }
        let fills =
            PredictFillEvidence::load(&self.data_http, &self.chain, self.signer.chain(), record)
                .await?;
        let total = fills
            .iter()
            .try_fold(0_i128, |total, fill| total.checked_add(fill.shares_wei))
            .ok_or_else(|| anyhow::anyhow!("Predict fill quantity overflow"))?;
        anyhow::ensure!(
            total == expected,
            "Predict confirmed fills disagree with venue filled quantity"
        );
        Ok(fills)
    }

    async fn status_report(
        &self,
        order: &OrderAny,
        record: &PredictOrderRecord,
    ) -> anyhow::Result<OrderStatusReport> {
        let quantity = quantity_from_wei(&record.amount)?;
        let filled_qty = quantity_from_wei(&record.amount_filled)?;
        anyhow::ensure!(
            filled_qty <= quantity,
            "Predict filled quantity exceeds venue order quantity"
        );
        let status = match record.status {
            PredictOrderStatus::Open if filled_qty.is_zero() => OrderStatus::Accepted,
            PredictOrderStatus::Open => OrderStatus::PartiallyFilled,
            PredictOrderStatus::Filled if filled_qty == quantity => OrderStatus::Filled,
            PredictOrderStatus::Filled
            | PredictOrderStatus::Cancelled
            | PredictOrderStatus::Invalidated => OrderStatus::Canceled,
            PredictOrderStatus::Expired => OrderStatus::Expired,
        };
        let now = self.clock.get_time_ns();
        let mut report = OrderStatusReport::new(
            self.core.account_id,
            order.instrument_id(),
            Some(order.client_order_id()),
            VenueOrderId::new(&record.id),
            Some(order.order_side()),
            order.order_type(),
            order.time_in_force(),
            status,
            quantity,
            filled_qty,
            order.ts_init(),
            now,
            now,
            None,
        );
        if filled_qty.non_zero() {
            let fills = self.confirmed_fills(record).await?;
            let notional: Decimal = fills
                .iter()
                .map(|fill| fill.quantity.as_decimal() * fill.price.as_decimal())
                .sum();
            report.avg_px = Some(notional / filled_qty.as_decimal());
        }
        report.price = order.price();
        report.expire_time = order.expire_time();
        report.post_only = order.is_post_only();
        report.reduce_only = order.is_reduce_only();
        Ok(report)
    }

    pub(super) async fn single_order_report(
        &self,
        request: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        if request.client_order_id.is_none() && request.venue_order_id.is_none() {
            return Ok(None);
        }
        for order in self.local_orders(request.instrument_id.as_ref()) {
            if request
                .client_order_id
                .is_some_and(|id| id != order.client_order_id())
            {
                continue;
            }
            if let Some(record) = self.remote_order(&order).await?
                && request
                    .venue_order_id
                    .is_none_or(|id| id.as_str() == record.id)
            {
                return self.status_report(&order, &record).await.map(Some);
            }
        }
        Ok(None)
    }

    pub(super) async fn order_reports(
        &self,
        request: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let mut reports = Vec::new();
        for order in self.local_orders(request.instrument_id.as_ref()) {
            if !in_window(order.ts_init(), request.start, request.end)
                || request.open_only && order.is_closed()
            {
                continue;
            }
            if let Some(record) = self.remote_order(&order).await?
                && (!request.open_only || record.status == PredictOrderStatus::Open)
            {
                reports.push(self.status_report(&order, &record).await?);
            }
        }
        Ok(reports)
    }

    pub(super) async fn fill_reports(
        &self,
        request: &GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let mut reports = Vec::new();
        for order in self.local_orders(request.instrument_id.as_ref()) {
            let Some(record) = self.remote_order(&order).await? else {
                continue;
            };
            let venue_id = VenueOrderId::new(&record.id);
            if request.venue_order_id.is_some_and(|id| id != venue_id) {
                continue;
            }
            for fill in self.confirmed_fills(&record).await? {
                if !in_window(fill.ts_event, request.start, request.end) {
                    continue;
                }
                let mut report = FillReport::new(
                    self.core.account_id,
                    order.instrument_id(),
                    venue_id,
                    fill.trade_id,
                    order.order_side(),
                    fill.quantity,
                    fill.price,
                    fill.commission
                        .unwrap_or_else(|| Money::zero(collateral_currency())),
                    fill.liquidity,
                    Some(order.client_order_id()),
                    None,
                    fill.ts_event,
                    self.clock.get_time_ns(),
                    None,
                );
                report.commission_shares = fill.commission_shares;
                reports.push(report);
            }
        }
        reports.sort_by_key(|report| report.ts_event);
        Ok(reports)
    }
}

fn replay_submission(
    initialized: nautilus_model::events::OrderInitialized,
    account: nautilus_model::identifiers::AccountId,
    mut fills: Vec<OrderFilled>,
) -> anyhow::Result<OrderAny> {
    let submitted = OrderSubmitted::new(
        initialized.trader_id,
        initialized.strategy_id,
        initialized.instrument_id,
        initialized.client_order_id,
        account,
        UUID4::new(),
        initialized.ts_init,
        initialized.ts_init,
    );
    let mut events = vec![
        OrderEventAny::Initialized(initialized),
        OrderEventAny::Submitted(submitted),
    ];
    // Restore fills retained by the position without delivering them a second time to accounting
    fills.sort_by_key(|fill| (fill.ts_event, fill.ts_init, fill.trade_id));
    if let Some(first) = fills.first() {
        events.push(OrderEventAny::Accepted(OrderAccepted::new(
            submitted.trader_id,
            submitted.strategy_id,
            submitted.instrument_id,
            submitted.client_order_id,
            first.venue_order_id,
            account,
            UUID4::new(),
            first.ts_event,
            first.ts_init,
            true,
        )));
    }
    let mut seen = std::collections::HashSet::new();
    events.extend(
        fills
            .into_iter()
            .filter(|fill| seen.insert(fill.trade_id))
            .map(OrderEventAny::Filled),
    );
    Ok(OrderAny::from_events(events)?)
}

fn quantity_from_wei(value: &str) -> anyhow::Result<Quantity> {
    Ok(Quantity::from_decimal(
        Decimal::try_from(parse_wei(value)?)? / wei_per_unit(),
    )?)
}

fn in_window(ts: UnixNanos, start: Option<UnixNanos>, end: Option<UnixNanos>) -> bool {
    start.is_none_or(|start| ts >= start) && end.is_none_or(|end| ts <= end)
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide, OrderType, TimeInForce},
        orders::builder::OrderTestBuilder,
        types::Price,
    };

    #[test]
    fn predict_recovery_retains_initialization_and_previously_accounted_fills() {
        let original = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id("0xcondition-42.PREDICT".into())
            .side(OrderSide::Buy)
            .quantity(Quantity::from("3.000"))
            .price(Price::from("0.500"))
            .time_in_force(TimeInForce::Gtc)
            .build();
        let initialized = original.init_event().clone();
        let account = "PREDICT-001".into();
        let recovered = replay_submission(initialized.clone(), account, vec![]).unwrap();
        assert_eq!(recovered.init_event(), &initialized);
        assert_eq!(recovered.status(), OrderStatus::Submitted);
        assert_eq!(recovered.account_id(), Some(account));

        let fill = OrderFilled::new(
            recovered.trader_id(),
            recovered.strategy_id(),
            recovered.instrument_id(),
            recovered.client_order_id(),
            VenueOrderId::new("venue-1"),
            account,
            "trade-1".into(),
            recovered.order_side(),
            recovered.order_type(),
            Quantity::from("1.000"),
            Price::from("0.500"),
            collateral_currency(),
            LiquiditySide::Taker,
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(1),
            true,
            None,
            None,
            None,
        );
        let recovered =
            replay_submission(initialized.clone(), account, vec![fill.clone(), fill]).unwrap();
        assert_eq!(recovered.init_event(), &initialized);
        assert_eq!(recovered.status(), OrderStatus::PartiallyFilled);
        assert_eq!(recovered.filled_qty(), Quantity::from("1.000"));
        assert_eq!(recovered.trade_ids().len(), 1);
    }
}
