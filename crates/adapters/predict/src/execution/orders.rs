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
use nautilus_model::enums::{OrderSide, OrderType};
use nautilus_common::messages::execution::{CancelOrder, SubmitOrder};
use nautilus_model::identifiers::{InstrumentId, VenueOrderId};
use nautilus_model::instruments::InstrumentAny;
use nautilus_model::orders::Order as _;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::execution::amounts::{
    MarketSizing, PredictOrderSide, SizedOrder, limit_amounts, market_amounts,
};
use crate::execution::client::PredictExecutionClient;
use super::{chain_account::ChainAccountReader, fill_evidence::PredictFillEvidence};
use crate::execution::lifecycle::{
    LifecycleAction, PredictOrderStatusWord, map_observation, parse_wei,
};
use crate::execution::request::{
    Expiration, MarketVariant, PlacementToggles, PredictRequestSigner, build_signed_request,
};
use crate::http::trading::{PredictOrderStrategy, PredictTradingClient};

/// One tracked live order (wallet-event reconciliation state).
#[derive(Clone)]
pub(super) struct TrackedOrder {
    pub(super) order: nautilus_model::orders::OrderAny,
    pub(super) venue_order_id: Option<VenueOrderId>,
    pub(super) quote: nautilus_model::types::Currency,
    pub(super) last_seen_filled_wei: i128,
}

impl PredictExecutionClient {
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
        let settings = match placement_settings(&order, cmd.params.as_ref()) {
            Ok(settings) => settings,
            Err(e) => {
                self.emitter.emit_order_denied(&order, &e.to_string());
                return Ok(());
            }
        };
        let facts = self.instrument_facts(&order.instrument_id());
        let facts = match facts {
            Ok(f) => f,
            Err(e) => {
                self.emitter
                    .emit_order_denied(&order, &format!("instrument facts: {e}"));
                return Ok(());
            }
        };
        let Some(store) = self.submission_store.clone() else {
            self.emitter.emit_order_denied(&order, "Predict submission requires a durable order store");
            return Ok(());
        };
        let account = self.core.account_id;
        let emitter = self.emitter.clone();
        let trading = self.trading.clone();
        let data_http = self.data_http.clone();
        let chain = self.chain.clone();
        let tracked = self.tracked.clone();
        let clock = self.clock;
        let order_backup = order.clone();
        let spawned = self.pending_tasks.spawn(async move {
            let reject_emitter = emitter.clone();
            if let Err(e) = run_submit(order, facts, settings, emitter, trading, data_http, chain, tracked, clock, store, account)
                .await
            {
                // pre-send 失败（走档拉取/合同校验/签名）= 确定性未发送
                //（submit-once 歧义从 POST 才开始）——安全 emit rejected，
                // 订单不留悬死态。
                log::error!("predict submit pipeline failed (pre-send, definitive): {e:?}");
                reject_emitter.emit_order_rejected(&order_backup, &e.to_string(), clock.get_time_ns(), false);
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
            .and_then(|t| t.venue_order_id)
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
        // 签名载荷承重字段：错 false = 错 EIP-712 domain（neg-risk 选错
        // 合约）——缺登记响亮拒，不默认。
        let required_bool = |key: &str| -> anyhow::Result<bool> {
            get(key)
                .and_then(|v| v.as_bool())
                .ok_or_else(|| anyhow::anyhow!("instrument info missing {key} (EIP-712 domain)"))
        };
        let is_neg_risk = required_bool("is_neg_risk")?;
        let is_yield_bearing = required_bool("is_yield_bearing")?;
        let index_set = get("index_set")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow::anyhow!("instrument info missing index_set"))?;
        let fee_rate_bps_raw = get("fee_rate_bps")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow::anyhow!("instrument info missing fee_rate_bps"))?;
        anyhow::ensure!(
            fee_rate_bps_raw >= 0,
            "instrument info fee_rate_bps negative ({fee_rate_bps_raw})——目录脏数据"
        );
        let fee_rate_bps = fee_rate_bps_raw as u64;
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
            signer: self.signer.clone(),
        })
    }
}

fn placement_settings(
    order: &nautilus_model::orders::OrderAny,
    params: Option<&nautilus_core::Params>,
) -> anyhow::Result<PlacementToggles> {
    use nautilus_model::enums::TimeInForce;

    let is_market = order.order_type() == OrderType::Market;
    let tif = order.time_in_force();
    let slippage = params.and_then(|params| params.get("slippage_bps"));
    if is_market {
        anyhow::ensure!(!order.is_post_only(), "Predict post-only requires a limit order");
        anyhow::ensure!(
            matches!(tif, TimeInForce::Gtc | TimeInForce::Ioc | TimeInForce::Fok),
            "unsupported Predict market time in force {tif:?}"
        );
        let slippage = slippage.map(|value| {
            value.as_u64()
                .filter(|bps| *bps <= 10_000)
                .ok_or_else(|| anyhow::anyhow!("Predict slippage_bps must be an integer in 0..=10000"))
        }).transpose()?;
        Ok(PlacementToggles {
            strategy: PredictOrderStrategy::Market,
            is_fill_or_kill: Some(tif == TimeInForce::Fok),
            slippage_bps: slippage.map(|bps| bps.to_string()),
            is_min_amount_out: slippage.map(|_| order.is_quote_quantity()),
            ..Default::default()
        })
    } else {
        anyhow::ensure!(slippage.is_none(), "Predict slippage requires a market order");
        anyhow::ensure!(
            matches!(tif, TimeInForce::Gtc | TimeInForce::Gtd),
            "unsupported Predict limit time in force {tif:?}"
        );
        Ok(PlacementToggles {
            is_post_only: Some(order.is_post_only()),
            ..Default::default()
        })
    }
}

/// The async submit body (runs on the pending-task group).
#[expect(clippy::too_many_arguments)]
async fn run_submit(
    order: nautilus_model::orders::OrderAny,
    facts: InstrumentFacts,
    settings: PlacementToggles,
    emitter: ExecutionEventEmitter,
    trading: Arc<PredictTradingClient>,
    data_http: std::sync::Arc<crate::http::client::PredictHttpClient>,
    chain: Arc<ChainAccountReader>,
    tracked: Arc<Mutex<HashMap<String, TrackedOrder>>>,
    clock: &'static AtomicTime,
    store: Arc<dyn super::submission_store::PredictSubmissionStore>,
    account: nautilus_model::identifiers::AccountId,
) -> anyhow::Result<()> {
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
    // OrderSide 是 Buy/Sell 二值闭集（已入 cache 的订单必带方向）。
    let side = match order.order_side() {
        OrderSide::Buy => PredictOrderSide::Buy,
        OrderSide::Sell => PredictOrderSide::Sell,
    };

    let mut sized = if is_market {
        let book_dto = data_http
            .get_orderbook(facts.market_id)
            .await
            .map_err(|e| anyhow::anyhow!("orderbook fetch for market {}: {e}", facts.market_id))?;
        // Yes-axis levels → token-axis (complement legs consume the mirror:
        // buy-other @ p = sell-yes @ 1−p ⇒ other-asks = (1−yes_bid, size)，
        // other-bids = (1−yes_ask, size)，按价格序重排)。
        let levels = ladder_source(&book_dto, side, facts.complement_leg);
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
    if let Some(bps) = settings.slippage_bps.as_deref() {
        let bound = crate::execution::amounts::apply_slippage(
            side,
            &sized,
            settings.is_min_amount_out.unwrap_or(false),
            bps.parse()?,
        );
        sized.maker_amount_wei = bound.maker_amount_wei;
        sized.taker_amount_wei = bound.taker_amount_wei;
    }
    anyhow::ensure!(sized.maker_amount_wei > 0 && sized.taker_amount_wei > 0,
        "Predict slippage produces a zero order amount");
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

    let (wire, settings) = build_signed_request(
        &facts.signer,
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
        settings,
    )
    .map_err(anyhow::Error::msg)?;

    let submission = super::submission_store::PredictSubmission {
        account_id: account,
        client_order_id: order.client_order_id(),
        initialized_at: order.ts_init(),
        order_hash: wire.hash.parse()?,
        order: Some(order.init_event().clone()),
    };
    if !store.reserve(&submission).await? {
        log::warn!("Predict order {} already has a durable submission", order.client_order_id());
        return Ok(());
    }
    emitter.emit_order_submitted(&order);
    let hash = wire.hash.clone();
    tracked.lock().expect("tracked lock").insert(hash.clone(), TrackedOrder {
        order: order.clone(),
        venue_order_id: None,
        quote: crate::common::consts::collateral_currency(),
        last_seen_filled_wei: 0,
    });
    match trading.place_order(&wire, &settings).await {
        Ok(placed) => {
            if !placed.order_hash.eq_ignore_ascii_case(&hash) {
                log::error!("Predict returned a hash different from the signed order");
                return Ok(());
            }
            let mut orders = tracked.lock().expect("tracked lock");
            if let Some(entry) = orders.get_mut(&hash)
                && entry.venue_order_id.is_none()
            {
                let id = VenueOrderId::new(placed.order_id);
                emitter.emit_order_accepted(&order, id, clock.get_time_ns());
                entry.venue_order_id = Some(id);
            }
        }
        Err(e) if matches!(
            &e,
            crate::http::error::PredictHttpError::Http { status, .. }
                if (400..500).contains(status) && !matches!(*status, 408 | 409 | 429)
        ) => {
            let mut orders = tracked.lock().expect("tracked lock");
            if orders.get(&hash).is_some_and(|entry| entry.venue_order_id.is_none()) {
                orders.remove(&hash);
                emitter.emit_order_rejected(&order, &e.to_string(), clock.get_time_ns(), false);
            }
            return Ok(());
        }
        Err(e) => log::warn!("Predict order submission remains unknown for {hash}: {e}"),
    }
    if let Err(e) = reconcile_order(
        &hash, &trading, &data_http, &chain, facts.signer.chain(), &emitter, &tracked, clock,
    ).await {
        log::warn!("Predict post-submit reconciliation pending for {hash}: {e:#}");
    }
    Ok(())
}

/// 走档输入选择（BLOCKER-1 原缺陷位）：主腿 buy 吃 yes-asks / sell
/// 吃 yes-bids；补腿取对侧并镜像 1−p（buy-other 吃 other-asks ≡
/// yes-bids——BUY YES@p ≡ SELL NO@1−p；sell-other 同理）。镜像后消费
/// 序天然保持（bids 降序 → 升序；asks 升序 → 降序）——不反序。
#[must_use]
pub fn ladder_source(
    book: &crate::http::models::PredictOrderbookDto,
    side: PredictOrderSide,
    complement_leg: bool,
) -> Vec<(Decimal, Decimal)> {
    let (primary, complement): (&[_], &[_]) = match side {
        PredictOrderSide::Buy => (&book.asks, &book.bids),
        PredictOrderSide::Sell => (&book.bids, &book.asks),
    };
    let source = if complement_leg { complement } else { primary };
    let levels: Vec<(Decimal, Decimal)> =
        source.iter().map(|l| (l.price(), l.size())).collect();
    if complement_leg {
        mirror_yes_levels(&levels)
    } else {
        levels
    }
}

/// Yes 轴档位 → 补腿 token 轴档位（价格镜像 1−p，数量原样）。输入按
/// best-first（DTO 合同），输出的消费序天然保持——映射自身不排序。
#[must_use]
pub fn mirror_yes_levels(yes_levels: &[(Decimal, Decimal)]) -> Vec<(Decimal, Decimal)> {
    yes_levels
        .iter()
        .map(|(p, s)| (Decimal::ONE - p, *s))
        .collect()
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
    http: Arc<crate::http::client::PredictHttpClient>,
    chain: Arc<ChainAccountReader>,
    network: crate::signing::PredictChain,
    mut events: tokio::sync::mpsc::UnboundedReceiver<crate::websocket::client::PredictWsEvent>,
    emitter: ExecutionEventEmitter,
    tracked: Arc<Mutex<HashMap<String, TrackedOrder>>>,
    mut subscription: crate::websocket::user::WalletSubscription,
    clock: &'static AtomicTime,
) {
    let mut poll = tokio::time::interval(std::time::Duration::from_secs(15));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let event = tokio::select! {
            event = events.recv() => event,
            _ = poll.tick() => {
                let hashes: Vec<_> = tracked.lock().expect("tracked lock")
                    .keys().cloned().collect();
                for hash in hashes {
                    if let Err(e) = reconcile_order(&hash, &trading, &http, &chain, network,
                        &emitter, &tracked, clock).await
                    {
                        log::warn!("Predict order reconciliation pending for {hash}: {e:#}");
                    }
                }
                continue;
            }
        };
        let Some(event) = event else { break };
        match event {
            crate::websocket::client::PredictWsEvent::Ready { .. } => {
                if let Err(e) = subscription
                    .refresh(&client, async { trading.wallet_jwt().await.map_err(anyhow::Error::new) })
                    .await
                {
                    log::error!("predict wallet subscription refresh failed (honest gap): {e:#}");
                }
                poll.reset_immediately();
            }
            crate::websocket::client::PredictWsEvent::Message {
                message:
                    crate::websocket::messages::PredictWsMessage::WalletEvent(wallet_event),
                ..
            } => {
                if let Err(e) = reconcile_order(&wallet_event.order_hash, &trading, &http, &chain, network, &emitter, &tracked, clock).await {
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

/// 一次认领的结果（发射面板快照 + 摘除决定）。
pub(super) struct Claimed {
    pub actions: Vec<LifecycleAction>,
    pub remove: bool,
    pub quote: nautilus_model::types::Currency,
    pub order: nautilus_model::orders::OrderAny,
}

/// **认领协议**（只在持锁临界区内调用——双消费者面）：按**当前**条目
/// 水位计算动作、max 推进水位当场写回、决定终局摘除。并发第二路调用
/// 时当前水位已推进 → delta=0 → 无 Fill；条目已摘除 → None。
/// `remove` 语义：满额终局，或任何 Close 类动作。
#[must_use]
pub(super) fn claim_observation(
    tracked: &mut HashMap<String, TrackedOrder>,
    order_id: &str,
    status: PredictOrderStatusWord,
    amount: i128,
    filled: i128,
) -> Option<Claimed> {
    let entry = tracked.get_mut(order_id)?;
    let actions = map_observation(status, amount, filled, entry.last_seen_filled_wei, None);
    // 水位永不回退（收缩→恢复不补发——TradeId 恒唯一）。
    entry.last_seen_filled_wei = entry.last_seen_filled_wei.max(filled);
    let remove = filled >= amount && status != PredictOrderStatusWord::Open
        || actions
            .iter()
            .any(|a| !matches!(a, LifecycleAction::Hold | LifecycleAction::Fill { .. }));
    Some(Claimed {
        actions,
        remove,
        quote: entry.quote,
        order: entry.order.clone(),
    })
}

#[expect(clippy::too_many_arguments)]
async fn reconcile_order(
    hash: &str,
    trading: &PredictTradingClient,
    http: &crate::http::client::PredictHttpClient,
    chain: &ChainAccountReader,
    network: crate::signing::PredictChain,
    emitter: &ExecutionEventEmitter,
    tracked: &Mutex<HashMap<String, TrackedOrder>>,
    clock: &'static AtomicTime,
) -> anyhow::Result<()> {
    let Some(previous) = tracked.lock().expect("tracked lock")
        .get(hash).map(|order| order.last_seen_filled_wei)
    else {
        return Ok(());
    };
    let record = trading.get_order(hash).await?;
    anyhow::ensure!(record.order.hash.eq_ignore_ascii_case(hash), "Predict order query returned another hash");
    let status = match record.status {
        crate::http::trading::PredictOrderStatus::Open => PredictOrderStatusWord::Open,
        crate::http::trading::PredictOrderStatus::Filled => PredictOrderStatusWord::Filled,
        crate::http::trading::PredictOrderStatus::Cancelled => PredictOrderStatusWord::Cancelled,
        crate::http::trading::PredictOrderStatus::Expired => PredictOrderStatusWord::Expired,
        crate::http::trading::PredictOrderStatus::Invalidated => PredictOrderStatusWord::Invalidated,
    };
    let amount = parse_wei(&record.amount)?;
    let filled = parse_wei(&record.amount_filled)?;
    // 观测域校验（认领**前**）：非负 + 数量域——失败即丢弃本观测（下一
    // 事件重试），不进临界区（防止「水位已认领而 fill 转换失败未发射」
    // 的丢失窗口）。
    anyhow::ensure!(
        amount >= 0 && filled >= 0,
        "venue order observation carries negative amounts ({amount}/{filled})——丢弃"
    );
    let evidence = if filled > previous {
        PredictFillEvidence::load(http, chain, network, &record).await?
    } else {
        Vec::new()
    };
    {
        let mut tracked_guard = tracked.lock().expect("tracked lock");
        let Some(entry) = tracked_guard.get_mut(hash) else { return Ok(()) };
        if filled < entry.last_seen_filled_wei {
            return Ok(());
        }
        let venue_order_id = VenueOrderId::new(&record.id);
        if let Some(existing) = entry.venue_order_id {
            anyhow::ensure!(existing == venue_order_id, "Predict venue order identity changed");
        } else {
            emitter.emit_order_accepted(&entry.order, venue_order_id, clock.get_time_ns());
            entry.venue_order_id = Some(venue_order_id);
        }
        let delta = filled - entry.last_seen_filled_wei;
        let mut running = 0_i128;
        let mut new_evidence = Vec::new();
        if delta > 0 {
            for fill in evidence {
                let before = running;
                running = running.checked_add(fill.shares_wei)
                    .ok_or_else(|| anyhow::anyhow!("Predict cumulative fill overflow"))?;
                if running <= entry.last_seen_filled_wei {
                    continue;
                }
                anyhow::ensure!(before >= entry.last_seen_filled_wei,
                    "Predict history disagrees with reconciled fill boundary");
                new_evidence.push(fill);
            }
            anyhow::ensure!(running == filled,
                "Predict reported {filled} shares in wei with only {running} supported by complete confirmed matches");
        }
        let Some(claim) = claim_observation(&mut tracked_guard, hash, status, amount, filled)
        else { return Ok(()) };
        let Claimed { actions, remove, quote, order } = claim;
        for action in actions {
            match action {
                LifecycleAction::Hold => {}
                LifecycleAction::Fill { .. } => {
                    for fill in new_evidence.drain(..) {
                        emitter.emit_order_filled_with_share_commission(
                            &order, venue_order_id, None, fill.trade_id, fill.quantity,
                            fill.price, quote, fill.commission, fill.commission_shares,
                            fill.liquidity, fill.ts_event,
                        );
                    }
                }
                LifecycleAction::CloseRemainderCancelled => {
                    emitter.emit_order_canceled(
                        &order,
                        Some(venue_order_id.clone()),
                        clock.get_time_ns(),
                    );
                }
                LifecycleAction::CloseRemainderExpired => {
                    emitter.emit_order_expired(
                        &order,
                        Some(venue_order_id.clone()),
                        clock.get_time_ns(),
                    );
                }
                LifecycleAction::CloseInvalidated { reason } => {
                    emitter.emit_order_rejected(
                        &order,
                        &format!("venue invalidated: {reason}"),
                        clock.get_time_ns(),
                        false,
                    );
                }
            }
        }
        if remove {
            tracked_guard.remove(hash);
        }
    }
    Ok(())
}

#[cfg(test)]
mod mirror_tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr as _;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// 选侧组合锚（原缺陷位）：buy-补腿 → yes-bids；sell-补腿 →
    /// yes-asks；主腿 buy → asks / sell → bids；补腿镜像后消费序保持。
    #[test]
    fn ladder_source_selects_correct_side_per_leg() {
        use crate::http::models::{PredictBookLevelDto, PredictOrderbookDto};
        let book = PredictOrderbookDto {
            market_id: 1,
            update_timestamp_ms: 0,
            asks: vec![
                PredictBookLevelDto(d("0.62"), d("50")),
                PredictBookLevelDto(d("0.63"), d("200")),
            ],
            bids: vec![PredictBookLevelDto(d("0.58"), d("100"))],
            last_order_settled: None,
        };
        assert_eq!(
            ladder_source(&book, PredictOrderSide::Buy, true),
            vec![(d("0.42"), d("100"))],
            "buy-补腿吃镜像 bids"
        );
        assert_eq!(
            ladder_source(&book, PredictOrderSide::Sell, true),
            vec![(d("0.38"), d("50")), (d("0.37"), d("200"))],
            "sell-补腿吃镜像 asks（降序保持）"
        );
        assert_eq!(
            ladder_source(&book, PredictOrderSide::Buy, false),
            vec![(d("0.62"), d("50")), (d("0.63"), d("200"))]
        );
        assert_eq!(
            ladder_source(&book, PredictOrderSide::Sell, false),
            vec![(d("0.58"), d("100"))]
        );
    }

    /// BLOCKER-1 数值锚：yes bids [(0.58,100)]、asks [(0.62,50),(0.63,200)]
    /// ——真实 NO 可买档只有 [(0.42,100)]（镜像 bids，升序保持）；NO 卖
    /// 档 = 镜像 asks = [(0.38,50),(0.37,200)]（降序保持）。
    #[test]
    fn mirror_maps_sides_with_order_preserved() {
        let bids = vec![(d("0.58"), d("100"))];
        let mirrored = mirror_yes_levels(&bids);
        assert_eq!(mirrored, vec![(d("0.42"), d("100"))]);

        let asks = vec![(d("0.62"), d("50")), (d("0.63"), d("200"))];
        let mirrored = mirror_yes_levels(&asks);
        // 源升序 → 镜像后降序（0.38 > 0.37）——best-first 天然保持，不反序。
        assert_eq!(
            mirrored,
            vec![(d("0.38"), d("50")), (d("0.37"), d("200"))]
        );
        // 价格全在 (0,1)。
        assert!(mirrored.iter().all(|(p, _)| *p > Decimal::ZERO && *p < Decimal::ONE));
    }
}

#[cfg(test)]
mod claim_tests {
    use super::*;

    fn test_order() -> nautilus_model::orders::OrderAny {
        use nautilus_model::instruments::{Instrument as _, stubs::crypto_perpetual_ethusdt};
        use nautilus_model::orders::builder::OrderTestBuilder;
        let instrument =
            nautilus_model::instruments::InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let order = OrderTestBuilder::new(nautilus_model::enums::OrderType::Market)
            .instrument_id(instrument.id())
            .side(nautilus_model::enums::OrderSide::Buy)
            .quantity(nautilus_model::types::Quantity::from("1.000"))
            .build();
        // claim 协议只消费订单身份字段（client_order_id/hash 面板）——
        // 构造未成交市价单即可（builder 输出直接进 OrderAny）。
        order.into()
    }

    #[rstest::rstest]
    fn market_placement_encodes_fok_and_explicit_slippage() {
        let order: nautilus_model::orders::OrderAny =
            nautilus_model::orders::builder::OrderTestBuilder::new(OrderType::Market)
                .instrument_id("0xcondition-42.PREDICT".into())
                .side(OrderSide::Buy)
                .quantity(nautilus_model::types::Quantity::from("1"))
                .time_in_force(nautilus_model::enums::TimeInForce::Fok)
                .build().into();
        let params = nautilus_core::Params::new([
            ("slippage_bps".to_owned(), serde_json::json!(125))
        ].into_iter().collect());
        let settings = placement_settings(&order, Some(&params)).unwrap();
        assert_eq!(settings.strategy, PredictOrderStrategy::Market);
        assert_eq!(settings.is_fill_or_kill, Some(true));
        assert_eq!(settings.slippage_bps.as_deref(), Some("125"));
        let invalid = nautilus_core::Params::new([
            ("slippage_bps".to_owned(), serde_json::json!(10001))
        ].into_iter().collect());
        assert!(placement_settings(&order, Some(&invalid)).is_err());
    }

    fn tracked_map() -> HashMap<String, TrackedOrder> {
        let mut m = HashMap::new();
        m.insert(
            "PREDICT-o1".to_owned(),
            TrackedOrder {
                order: test_order(),
                venue_order_id: Some(VenueOrderId::new("PREDICT-o1")),
                quote: crate::common::consts::collateral_currency(),
                last_seen_filled_wei: 0,
            },
        );
        m
    }

    /// 认领协议锚（**在缺陷位**——claim_observation 就是锁内运行体）：
    /// 共享 map 上顺序双调 = 持锁交错的全序形态。第一路认领 delta=F；
    /// 第二路同观测 → 当前水位已推进 → delta=0 → 无 Fill 动作（不双发）。
    #[test]
    fn claim_protocol_second_path_emits_nothing() {
        let mut map = tracked_map();
        let amount = 10_000_000_000_000_000_000_i128;
        let filled = 5_000_000_000_000_000_000_i128;
        let first =
            claim_observation(&mut map, "PREDICT-o1", PredictOrderStatusWord::Open, amount, filled)
                .expect("第一路认领");
        assert!(matches!(
            first.actions[0],
            LifecycleAction::Fill { delta_shares_wei } if delta_shares_wei == filled
        ));
        assert!(!first.remove);
        let second =
            claim_observation(&mut map, "PREDICT-o1", PredictOrderStatusWord::Open, amount, filled)
                .expect("第二路仍可见条目");
        assert!(
            second.actions.iter().all(|a| matches!(a, LifecycleAction::Hold)),
            "第二路不得再发射（水位已推进）"
        );
        assert!(!second.remove);
    }

    /// 终局认领摘除后，并发第二路 → None（不复活、不双发终局事件）；
    /// 收缩观测不回退水位、恢复只算新增。
    #[test]
    fn claim_protocol_terminal_and_regression() {
        let mut map = tracked_map();
        let amount = 10_i128;
        let filled = 10_i128;
        let term =
            claim_observation(&mut map, "PREDICT-o1", PredictOrderStatusWord::Filled, amount, filled)
                .expect("终局认领");
        assert!(term.remove, "满额终局摘除");
        // 调用侧在认领临界区内执行摘除（与 handle_wallet_event 同形）。
        map.remove("PREDICT-o1");
        assert!(
            claim_observation(&mut map, "PREDICT-o1", PredictOrderStatusWord::Filled, amount, filled)
                .is_none(),
            "摘除后第二路 None"
        );
        // 收缩→恢复：水位不回退、恢复只算新增。
        let mut map = tracked_map();
        claim_observation(&mut map, "PREDICT-o1", PredictOrderStatusWord::Open, 10, 5).unwrap();
        let shrunk =
            claim_observation(&mut map, "PREDICT-o1", PredictOrderStatusWord::Open, 10, 3).unwrap();
        assert!(shrunk.actions.iter().all(|a| matches!(a, LifecycleAction::Hold)));
        let recovered =
            claim_observation(&mut map, "PREDICT-o1", PredictOrderStatusWord::Open, 10, 7).unwrap();
        assert!(matches!(
            recovered.actions[0],
            LifecycleAction::Fill { delta_shares_wei } if delta_shares_wei == 2
        ));
    }
}
