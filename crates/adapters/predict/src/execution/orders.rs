//! Submit pipeline + wallet-event ingestion for the Predict execution
//! client (POLYMARKET_PREDICT §8.2 wiring of the contract layers).
//!
//! Submit: cache order → instrument facts (info Params: on_chain_id /
//! market_id / variant / fee bps / precision) → §8.2 quantity contract
//! (LIMIT exact wei; MARKET walks a fresh book — complement legs consume
//! the mirrored Yes-axis book) → EIP-712 signed request → submit-once
//! placement. Definitive 4xx → rejected; timeout/transport/5xx → the
//! order stays open (`retain_unresolved_submissions`) until venue
//! evidence.
//!
//! Wallet events: one session task consumes the WS stream; on every
//! connection generation the wallet topic is re-subscribed with a fresh
//! JWT; order-affecting events trigger an order query + the pure
//! lifecycle mapping (`execution::lifecycle`) — fills are the venue's own
//! `amountFilled` deltas, terminal statuses close remainders, and no
//! quantity is ever invented.
//!
//! Registered gap (honest): account-state generation needs an on-chain
//! USDT read (no REST balance endpoint) — not wired; `connect` warns.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alloy_primitives::U256;

use crate::websocket::PredictWebSocketClient;
use nautilus_core::time::AtomicTime;
use nautilus_live::ExecutionEventEmitter;
use nautilus_live::task::TaskGroup;
use nautilus_model::enums::{LiquiditySide, OrderSide, OrderType};
use nautilus_common::messages::execution::{CancelOrder, SubmitOrder};
use nautilus_model::identifiers::{InstrumentId, TradeId, VenueOrderId};
use nautilus_model::instruments::InstrumentAny;
use nautilus_model::orders::Order as _;
use nautilus_model::types::{Price, Quantity};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::execution::amounts::{
    MarketSizing, PredictOrderSide, SizedOrder, limit_amounts, market_amounts,
};
use crate::execution::client::PredictExecutionClient;
use crate::execution::lifecycle::{
    LifecycleAction, PredictOrderStatusWord, map_observation, parse_wei,
};
use crate::execution::request::{
    Expiration, MarketVariant, PlacementToggles, PredictRequestSigner, build_signed_request,
};
use crate::http::trading::{PlaceOrderSettings, PredictOrderStrategy, PredictTradingClient};
use crate::websocket::messages::{
    WALLET_EVENT_ORDER_CANCELLED, WALLET_EVENT_ORDER_EXPIRED, WALLET_EVENT_ORDER_NOT_ACCEPTED,
    WALLET_EVENT_ORDER_TX_FAILED, WALLET_EVENT_ORDER_TX_SUCCESS,
};

/// One tracked live order (wallet-event reconciliation state).
#[derive(Clone)]
pub(super) struct TrackedOrder {
    pub(super) order: nautilus_model::orders::OrderAny,
    pub(super) venue_order_id: VenueOrderId,
    pub(super) order_hash: String,
    pub(super) market_id: i64,
    /// Token-axis side (venue code after complement mirroring).
    pub(super) side: PredictOrderSide,
    pub(super) quote: nautilus_model::types::Currency,
    pub(super) amount_wei: i128,
    pub(super) last_seen_filled_wei: i128,
    /// Token-axis price (wei) the request was sized/signed at.
    pub(super) price_per_share_wei: i128,
}

impl PredictExecutionClient {
    pub(super) fn emitter(&self) -> &ExecutionEventEmitter {
        &self.emitter
    }

    pub(super) fn clock(&self) -> &'static AtomicTime {
        self.clock
    }

    pub(super) fn trading_client(&self) -> &Arc<PredictTradingClient> {
        &self.trading
    }

    pub(super) fn pending_tasks(&self) -> &TaskGroup {
        &self.pending_tasks
    }

    /// Submit pipeline (called on the engine thread; the async work runs on
    /// the pending-task group).
    pub(super) fn submit_pipeline(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;
        if order.is_closed() {
            log::warn!("Cannot submit closed order {}", order.client_order_id());
            return Ok(());
        }
        match order.order_type() {
            OrderType::Limit | OrderType::Market => {}
            other => {
                self.emitter.emit_order_denied(
                    &order,
                    &format!("predict.fun does not support {other:?} orders"),
                );
                return Ok(());
            }
        }
        let facts = self.instrument_facts(&order.instrument_id());
        let facts = match facts {
            Ok(f) => f,
            Err(e) => {
                self.emitter
                    .emit_order_denied(&order, &format!("instrument facts: {e}"));
                return Ok(());
            }
        };
        let emitter = self.emitter.clone();
        let trading = self.trading.clone();
        let data_http = self.data_http.clone();
        let tracked = self.tracked.clone();
        let clock = self.clock;
        let spawned = self.pending_tasks.spawn(async move {
            if let Err(e) = run_submit(order, facts, emitter, trading, data_http, tracked, clock)
                .await
            {
                log::error!("predict submit pipeline failed: {e:?}");
            }
        });
        if spawned.is_err() {
            log::warn!("predict submit skipped after shutdown began");
        }
        Ok(())
    }

    /// Cancel by venue order id (removal-lock hits stay retryable — logged,
    /// the caller may re-issue; noop means already gone).
    pub(super) fn cancel_pipeline(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let venue_order_id = cmd
            .venue_order_id
            .clone()
            .or_else(|| self.tracked_venue_order_id(&cmd.client_order_id.to_string()));
        let Some(venue_order_id) = venue_order_id else {
            anyhow::bail!(
                "cannot cancel {}: no venue order id (order not yet accepted)",
                cmd.client_order_id
            );
        };
        let trading = self.trading.clone();
        let id = venue_order_id.as_ref().to_string();
        self.pending_tasks
            .spawn(async move {
                match trading.remove_orders(&[id.clone()]).await {
                    Ok(outcome) if outcome.removed.contains(&id) => {
                        log::info!("predict order {id} removed");
                    }
                    Ok(outcome) if outcome.rejected.contains(&id) => {
                        log::warn!(
                            "predict order {id} removal rejected (removal-lock) — retry after lock expiry"
                        );
                    }
                    Ok(outcome) => log::info!("predict order {id} removal noop: {outcome:?}"),
                    Err(e) => log::error!("predict order {id} removal failed: {e}"),
                }
            })
            .map_err(|e| anyhow::anyhow!("shutdown began: {e}"))?;
        Ok(())
    }

    fn tracked_venue_order_id(&self, client_order_id: &str) -> Option<VenueOrderId> {
        self.tracked
            .lock()
            .expect("tracked lock is not poisoned")
            .values()
            .find(|t| t.order.client_order_id().as_ref() == client_order_id)
            .map(|t| t.venue_order_id.clone())
    }
}

/// Instrument facts resolved from the cached BinaryOption's info Params.
pub(super) struct InstrumentFacts {
    pub(super) market_id: i64,
    pub(super) token_id: String,
    /// Complement leg = the non-primary outcome token (Yes-axis book needs
    /// mirroring for market sizing).
    pub(super) complement_leg: bool,
    pub(super) variant: MarketVariant,
    pub(super) fee_rate_bps: u64,
    pub(super) price_precision: u8,
    pub(super) signer: Arc<PredictRequestSigner>,
}

impl PredictExecutionClient {
    fn instrument_facts(&self, instrument_id: &InstrumentId) -> anyhow::Result<InstrumentFacts> {
        let cache = self.core.cache();
        let instrument = cache
            .instrument(instrument_id)
            .ok_or_else(|| anyhow::anyhow!("instrument {instrument_id} not in cache"))?;
        let InstrumentAny::BinaryOption(binary) = instrument else {
            anyhow::bail!("instrument {instrument_id} is not a BinaryOption");
        };
        let info = binary.info.as_ref();
        let get = |key: &str| info.and_then(|p| p.get(key));
        let string_of = |key: &str| -> anyhow::Result<String> {
            get(key)
                .and_then(|v| v.as_str().map(str::to_owned))
                .ok_or_else(|| anyhow::anyhow!("instrument info missing {key}"))
        };
        let market_id = get("market_id")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow::anyhow!("instrument info missing market_id"))?;
        let is_neg_risk = get("is_neg_risk").and_then(|v| v.as_bool()).unwrap_or(false);
        let is_yield_bearing =
            get("is_yield_bearing").and_then(|v| v.as_bool()).unwrap_or(false);
        let index_set = get("index_set")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow::anyhow!("instrument info missing index_set"))?;
        let fee_rate_bps =
            get("fee_rate_bps").and_then(|v| v.as_i64()).unwrap_or(0).max(0) as u64;
        Ok(InstrumentFacts {
            market_id,
            token_id: string_of("on_chain_id")?,
            // index_set bit 0 = primary outcome; the other leg consumes the
            // mirrored book.
            complement_leg: (index_set & 1) == 0,
            variant: MarketVariant {
                is_neg_risk,
                is_yield_bearing,
            },
            fee_rate_bps,
            price_precision: binary.price_precision,
            signer: self.signer.clone(),
        })
    }
}

/// The async submit body (runs on the pending-task group).
async fn run_submit(
    order: nautilus_model::orders::OrderAny,
    facts: InstrumentFacts,
    emitter: ExecutionEventEmitter,
    trading: Arc<PredictTradingClient>,
    data_http: std::sync::Arc<crate::http::client::PredictHttpClient>,
    tracked: Arc<Mutex<HashMap<String, TrackedOrder>>>,
    clock: &'static AtomicTime,
) -> anyhow::Result<()> {
    use nautilus_model::instruments::Instrument as _;
    // §8.2 quantity contract: quote-quantity is MARKET BUY only; sells and
    // limits size in shares.
    let is_market = order.order_type() == OrderType::Market;
    let quote_quantity = order.is_quote_quantity();
    if quote_quantity && (!is_market || order.order_side() == OrderSide::Sell) {
        emitter.emit_order_denied(
            &order,
            "quote quantity is MARKET BUY only (Predict quantity contract)",
        );
        return Ok(());
    }

    let qty = order.quantity();
    let side = match order.order_side() {
        OrderSide::Buy => PredictOrderSide::Buy,
        OrderSide::Sell => PredictOrderSide::Sell,
        other => {
            emitter.emit_order_denied(&order, &format!("unsupported side {other:?}"));
            return Ok(());
        }
    };

    let sized = if is_market {
        let book_dto = data_http
            .get_orderbook(facts.market_id)
            .await
            .map_err(|e| anyhow::anyhow!("orderbook fetch for market {}: {e}", facts.market_id))?;
        // Yes-axis levels → token-axis (complement legs consume the mirror:
        // buy-other @ p = sell-yes @ 1−p ⇒ other-asks = (1−yes_bid, size)，
        // other-bids = (1−yes_ask, size)，按价格序重排)。
        let levels: Vec<(Decimal, Decimal)> = {
            let yes_levels: Vec<(Decimal, Decimal)> = match side {
                PredictOrderSide::Buy => book_dto
                    .asks
                    .iter()
                    .map(|l| (l.price(), l.size()))
                    .collect(),
                PredictOrderSide::Sell => book_dto
                    .bids
                    .iter()
                    .map(|l| (l.price(), l.size()))
                    .collect(),
            };
            if facts.complement_leg {
                let mut mirrored: Vec<(Decimal, Decimal)> = yes_levels
                    .into_iter()
                    .map(|(p, s)| (Decimal::ONE - p, s))
                    .collect();
                match side {
                    // buy-other consumes other-asks ascending; mirror of yes
                    // bids is descending → reverse.
                    PredictOrderSide::Buy => mirrored.reverse(),
                    // sell-other consumes other-bids descending; mirror of
                    // yes asks is ascending → reverse.
                    PredictOrderSide::Sell => mirrored.reverse(),
                }
                mirrored
            } else {
                yes_levels
            }
        };
        let sizing = if quote_quantity {
            MarketSizing::Value(qty.as_decimal())
        } else {
            MarketSizing::Shares(qty.as_decimal())
        };
        market_amounts(side, &levels, sizing)?
    } else {
        let price = order
            .price()
            .ok_or_else(|| anyhow::anyhow!("limit order without price"))?;
        limit_amounts(side, qty.as_decimal(), price.as_decimal())?
    };

    let price_per_share_wei = sized_px_wei(&sized, side);
    // 份额轴（venue amount 语义按 outcome token 数量；fill delta = 份额）：
    // buy 的 taker 是份额，sell 的 maker 是份额。
    let shares_wei = match side {
        PredictOrderSide::Buy => sized.taker_amount_wei,
        PredictOrderSide::Sell => sized.maker_amount_wei,
    };
    let now_unix = u64::try_from(clock.get_time_ns().as_u64() / 1_000_000_000)?;
    let expiration = if is_market {
        // SDK 语义：市价单构建后 5 分钟过期。
        Expiration::Gtd {
            unix_seconds: now_unix.saturating_add(5 * 60),
        }
    } else {
        match order.time_in_force() {
            nautilus_model::enums::TimeInForce::Gtd => {
                let exp_ns = order.expire_time().map(|t| t.as_u64()).unwrap_or(0);
                anyhow::ensure!(
                    exp_ns > 0,
                    "GTD limit order carries no expire_time"
                );
                Expiration::Gtd {
                    unix_seconds: u64::try_from(exp_ns / 1_000_000_000)?,
                }
            }
            _ => Expiration::Gtc,
        }
    };

    emitter.emit_order_submitted(&order);
    let (wire, settings) = build_signed_request(
        &facts.signer,
        crate::signing::PredictChain::BnbMainnet,
        facts.variant,
        U256::from_str_radix(&facts.token_id, 10)
            .map_err(|e| anyhow::anyhow!("token id {:?}: {e}", facts.token_id))?,
        side,
        &sized,
        price_per_share_wei,
        U256::from(clock.get_time_ns().as_u64()), // salt：ns 时戳（秒级会同刻撞 salt）
        expiration,
        now_unix,
        facts.fee_rate_bps,
        PlacementToggles {
            strategy: if is_market {
                PredictOrderStrategy::Market
            } else {
                PredictOrderStrategy::Limit
            },
            ..Default::default()
        },
    )
    .map_err(anyhow::Error::msg)?;

    match trading.place_order(&wire, &settings).await {
        Ok(placed) => {
            emitter.emit_order_accepted(
                &order,
                VenueOrderId::new(placed.order_id.clone()),
                clock.get_time_ns(),
            );
            tracked
                .lock()
                .expect("tracked lock is not poisoned")
                .insert(
                    placed.order_id.clone(),
                    TrackedOrder {
                        order,
                        venue_order_id: VenueOrderId::new(placed.order_id),
                        order_hash: placed.order_hash,
                        market_id: facts.market_id,
                        side,
                        quote: crate::common::consts::collateral_currency(),
                        amount_wei: shares_wei,
                        last_seen_filled_wei: 0,
                        price_per_share_wei,
                    },
                );
            Ok(())
        }
        Err(e) => {
            // 定性：4xx 是所方已处理的明确拒绝；超时/传输/5xx = 结局
            // unknown（retain_unresolved——订单保持 open 直到证据）。
            if matches!(
                &e,
                crate::http::error::PredictHttpError::Http { status, .. } if (400..500).contains(status)
            ) {
                emitter.emit_order_rejected(&order, &e.to_string(), clock.get_time_ns(), false);
                Ok(())
            } else {
                log::warn!(
                    "predict order submission outcome unknown (kept open for venue evidence): {e}"
                );
                Ok(())
            }
        }
    }
}

/// Token-axis price (wei) of a sized order (buy: maker/taker; sell:
/// taker/maker — both axes are shares-per-quote consistent).
fn sized_px_wei(sized: &SizedOrder, side: PredictOrderSide) -> i128 {
    let px = if side == PredictOrderSide::Buy {
        Decimal::from(sized.maker_amount_wei) / Decimal::from(sized.taker_amount_wei)
    } else {
        Decimal::from(sized.taker_amount_wei) / Decimal::from(sized.maker_amount_wei)
    };
    (px * crate::execution::amounts::wei_per_unit())
        .trunc()
        .to_i128()
        .unwrap_or(0)
}

/// Wallet-event ingestion loop (session task): per-generation JWT
/// resubscribe + order-query lifecycle reconciliation.
pub(super) async fn run_wallet_events(
    client: PredictWebSocketClient,
    trading: Arc<PredictTradingClient>,
    mut events: tokio::sync::mpsc::UnboundedReceiver<crate::websocket::client::PredictWsEvent>,
    emitter: ExecutionEventEmitter,
    tracked: Arc<Mutex<HashMap<String, TrackedOrder>>>,
    mut subscription: crate::websocket::user::WalletSubscription,
    clock: &'static AtomicTime,
) {
    loop {
        let Some(event) = events.recv().await else { break };
        match event {
            crate::websocket::client::PredictWsEvent::Ready { .. } => {
                if let Err(e) = subscription
                    .refresh(&client, async { trading.wallet_jwt().await.map_err(anyhow::Error::new) })
                    .await
                {
                    log::error!("predict wallet subscription refresh failed (honest gap): {e:#}");
                }
            }
            crate::websocket::client::PredictWsEvent::Message {
                message:
                    crate::websocket::messages::PredictWsMessage::WalletEvent(wallet_event),
                ..
            } => {
                if let Err(e) = handle_wallet_event(&wallet_event, &trading, &emitter, &tracked, clock).await {
                    log::warn!("predict wallet event handling failed: {e:#}");
                }
            }
            crate::websocket::client::PredictWsEvent::Error { message, .. } => {
                log::warn!("predict wallet stream error: {message}");
            }
            _ => {}
        }
    }
    log::info!("predict wallet-event session task exited");
}

async fn handle_wallet_event(
    event: &crate::websocket::messages::PredictWalletEventDto,
    trading: &Arc<PredictTradingClient>,
    emitter: &ExecutionEventEmitter,
    tracked: &Arc<Mutex<HashMap<String, TrackedOrder>>>,
    clock: &'static AtomicTime,
) -> anyhow::Result<()> {
    // 只对已跟踪订单推进（外来钱包事件——同 JWT 属同一钱包，但未跟踪
    // 订单不是本 client 的执行面）。
    let (order_id, mut entry) = {
        let tracked = tracked
            .lock()
            .expect("tracked lock is not poisoned");
        let Some(entry) = tracked
            .values()
            .find(|t| t.order_hash == event.order_hash)
            .cloned()
        else {
            return Ok(());
        };
        (entry.venue_order_id.as_ref().to_string(), entry)
    };
    // 任意已跟踪订单的钱包事件都触发一次订单查询 + 生命周期映射——
    // 终局/增量判定以所方订单记录为真源（事件只是触发器，不作为事实）。
    let record = match trading.get_order(&event.order_hash).await {
        Ok(record) => record,
        Err(e) => {
            log::debug!("predict order query for {} failed (will retry on next event): {e}", event.order_hash);
            return Ok(());
        }
    };
    let status = match record.status {
        crate::http::trading::PredictOrderStatus::Open => PredictOrderStatusWord::Open,
        crate::http::trading::PredictOrderStatus::Filled => PredictOrderStatusWord::Filled,
        crate::http::trading::PredictOrderStatus::Cancelled => PredictOrderStatusWord::Cancelled,
        crate::http::trading::PredictOrderStatus::Expired => PredictOrderStatusWord::Expired,
        crate::http::trading::PredictOrderStatus::Invalidated => PredictOrderStatusWord::Invalidated,
    };
    let amount = parse_wei(&record.amount)?;
    let filled = parse_wei(&record.amount_filled)?;
    let actions = map_observation(status, amount, filled, entry.last_seen_filled_wei, None);
    entry.last_seen_filled_wei = filled;
    let share_unit = crate::execution::amounts::wei_per_unit();
    let mut remove = false;
    for action in actions {
        match action {
            LifecycleAction::Hold => {}
            LifecycleAction::Fill { delta_shares_wei } => {
                let qty_decimal = Decimal::from(delta_shares_wei) / share_unit;
                let qty = Quantity::from_decimal(qty_decimal)?;
                let px_decimal = Decimal::from(entry.price_per_share_wei) / share_unit;
                let px = Price::from_decimal(px_decimal)?;
                emitter.emit_order_filled(
                    &entry.order,
                    entry.venue_order_id.clone(),
                    None,
                    TradeId::new(format!("{}-{}", entry.order_hash, entry.last_seen_filled_wei)),
                    qty,
                    px,
                    entry.quote,
                    None, // 费用面：venue fill 无佣金事实（feeRateBps 是 maker 计费率，逐 fill 费用额未披露——不编造）
                    LiquiditySide::NoLiquiditySide,
                    clock.get_time_ns(),
                );
            }
            LifecycleAction::CloseRemainderCancelled => {
                emitter.emit_order_canceled(
                    &entry.order,
                    Some(entry.venue_order_id.clone()),
                    clock.get_time_ns(),
                );
                remove = true;
            }
            LifecycleAction::CloseRemainderExpired => {
                emitter.emit_order_expired(
                    &entry.order,
                    Some(entry.venue_order_id.clone()),
                    clock.get_time_ns(),
                );
                remove = true;
            }
            LifecycleAction::CloseInvalidated { reason } => {
                emitter.emit_order_rejected(
                    &entry.order,
                    &format!("venue invalidated: {reason}"),
                    clock.get_time_ns(),
                    false,
                );
                remove = true;
            }
        }
    }
    if remove {
        tracked
            .lock()
            .expect("tracked lock is not poisoned")
        .remove(&order_id);
    } else {
        // 仍在途（Open/增量成交）——回写水位。
        tracked
            .lock()
            .expect("tracked lock is not poisoned")
        .insert(order_id, entry);
    }
    Ok(())
}
