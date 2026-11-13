//! `PredictExecutionClient` — the nautilus `ExecutionClient` shell for
//! predict.fun (POLYMARKET_PREDICT §8.2 wiring of the contract layers).
//!
//! Composition (all layers committed and offline-tested):
//! - `http::trading::PredictTradingClient` — JWT auth, submit-once order
//!   placement, removal, order query;
//! - `execution::amounts` / `execution::request` — §8.2 sizing + EIP-712
//!   signed request assembly;
//! - `execution::lifecycle` — venue fact → action mapping (never invents
//!   quantities);
//! - `websocket::client` + `websocket::user` — wallet-events stream with
//!   per-generation JWT refresh;
//! - `http::positions` — wallet positions (reconciliation input).
//!
//! Honesty boundaries (loud, not silent):
//! - cash balance reporting is NOT wired (needs an on-chain USDT read via
//!   RPC — no documented REST balance endpoint): `connect` warns and
//!   `generate_account_state` reports positions only until that lands;
//! - venue order modification does not exist: modify is denied loudly;
//! - unknown submit outcomes stay unknown (`retain_unresolved_submissions`
//!   = true) and are reconciled by order hash — never re-signed blindly.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use nautilus_common::clients::ExecutionClient;
use nautilus_core::string::secret::SecretString;
use nautilus_core::UnixNanos;
use nautilus_live::ExecutionClientCore;
use nautilus_model::identifiers::{AccountId, ClientId};
use nautilus_model::types::{AccountBalance, MarginBalance};

use crate::common::consts::{PREDICT_VENUE, collateral_currency};
use crate::http::positions::PredictPositionDto;
use nautilus_common::live::runner::get_exec_event_sender;

use crate::http::trading::PredictTradingClient;

/// Execution configuration (mirror of the data config's credential and
/// endpoint surface plus the trading-only wallet key).
#[derive(Clone, Debug, Default)]
pub struct PredictExecutionConfig {
    pub api_key: Option<SecretString>,
    pub wallet_private_key: Option<SecretString>,
    pub base_url_http: Option<String>,
    pub base_url_ws: Option<String>,
    pub proxy_url: Option<SecretString>,
    pub http_timeout_secs: Option<u64>,
}

impl PredictExecutionConfig {
    /// Validates the material presence loudly (missing key material is a
    /// deployment fact, not a silent zero-credential client).
    ///
    /// # Errors
    /// Missing API key or wallet private key.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.api_key.is_some(),
            "predict execution requires the data API key (x-api-key)"
        );
        anyhow::ensure!(
            self.wallet_private_key.is_some(),
            "predict execution requires the wallet private key (EOA signing)"
        );
        Ok(())
    }
}

/// The execution client proper.
pub struct PredictExecutionClient {
    pub(super) core: ExecutionClientCore,
    pub(super) clock: &'static nautilus_core::time::AtomicTime,
    pub(super) emitter: nautilus_live::ExecutionEventEmitter,
    pub(super) trading: Arc<PredictTradingClient>,
    /// wallet address (JWT signer) for positions reads.
    pub(super) wallet_address: String,
    pub(super) data_http: std::sync::Arc<crate::http::client::PredictHttpClient>,
    pub(super) signer: Arc<crate::execution::request::PredictRequestSigner>,
    pub(super) ws: crate::websocket::client::PredictWebSocketClient,
    /// wallet-event receiver (consumed by the session task in `start`).
    ws_events: Option<
        tokio::sync::mpsc::UnboundedReceiver<
            crate::websocket::client::PredictWsEvent,
        >,
    >,
    session_tasks: nautilus_live::task::TaskGroup,
    pub(super) pending_tasks: nautilus_live::task::TaskGroup,
    /// venue order id → tracked order (wallet-event reconciliation state).
    pub(super) tracked: std::sync::Arc<
        std::sync::Mutex<HashMap<String, crate::execution::orders::TrackedOrder>>,
    >,
}

impl PredictExecutionClient {
    /// Builds the client from explicit configuration (no environment
    /// fallback — credentials are user material and only ever arrive here).
    ///
    /// # Errors
    /// Invalid configuration/key material.
    pub fn new(
        core: ExecutionClientCore,
        config: PredictExecutionConfig,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let api_key = config.api_key.clone().expect("validate checked");
        let wallet_key = config
            .wallet_private_key
            .clone()
            .expect("validate checked");
        let base_http = config
            .base_url_http
            .clone()
            .unwrap_or_else(|| "https://api.predict.fun/v1".to_owned());
        let base_ws = config
            .base_url_ws
            .clone()
            .unwrap_or_else(|| "wss://ws.predict.fun/ws".to_owned());
        let timeout = config.http_timeout_secs.unwrap_or(60);
        let proxy = config
            .proxy_url
            .as_ref()
            .map(|url| nautilus_network::websocket::proxy::ProxyUrl::parse(url.expose_secret()))
            .transpose()?;

        let signer = Arc::new(crate::http::trading::EoaPredictSigner::from_key_hex(
            &wallet_key,
        )?);
        let request_signer = Arc::new(
            crate::execution::request::PredictRequestSigner::from_key_hex(&wallet_key)
                .map_err(anyhow::Error::msg)?,
        );
        let wallet_address = crate::http::trading::PredictMessageSigner::address(signer.as_ref())
            .to_checksum(None);
        let trading = PredictTradingClient::new(
            &base_http,
            &api_key,
            signer,
            timeout,
            proxy.as_ref(),
        )?;
        let data_http = crate::http::client::PredictHttpClient::new(
            &base_http,
            &api_key,
            timeout,
            proxy,
        )?;
        let clock = nautilus_core::time::get_atomic_clock_realtime();
        let emitter = nautilus_live::ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            nautilus_model::enums::AccountType::Cash,
            Some(collateral_currency()),
        );
        let (ws, ws_events) = crate::websocket::client::PredictWebSocketClient::new(
            base_ws,
            &api_key,
            10,
            30,
            config
                .proxy_url
                .as_ref()
                .map(|url| url.expose_secret().to_owned()),
        );
        Ok(Self {
            core,
            clock,
            emitter,
            trading: Arc::new(trading),
            wallet_address,
            data_http: std::sync::Arc::new(data_http),
            signer: request_signer,
            ws,
            ws_events: Some(ws_events),
            session_tasks: nautilus_live::task::TaskGroup::default(),
            pending_tasks: nautilus_live::task::TaskGroup::default(),
            tracked: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        })
    }

    /// factory 期 sender 装接（start() 的 get_exec_event_sender 为权威——
    /// LiveNode 在 client start 前重绑线程 sender；emitter 契约同
    /// Polymarket）。
    pub fn set_event_sender(
        &mut self,
        sender: impl Into<
            nautilus_common::live::sender::EventSender<
                nautilus_common::messages::ExecutionEvent,
            >,
        >,
    ) {
        self.emitter.set_sender(sender);
    }

    /// Reads the wallet's open positions (reconciliation/interest face).
    ///
    /// # Errors
    /// Transport errors.
    pub async fn wallet_positions(&self) -> anyhow::Result<Vec<PredictPositionDto>> {
        let walk = self
            .data_http
            .walk_positions(&self.wallet_address, 10)
            .await
            .map_err(|e| anyhow::anyhow!("positions walk: {e}"))?;
        Ok(walk.rows)
    }

}

#[async_trait(?Send)]
impl ExecutionClient for PredictExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> nautilus_model::identifiers::Venue {
        *PREDICT_VENUE
    }

    fn oms_type(&self) -> nautilus_model::enums::OmsType {
        nautilus_model::enums::OmsType::Netting
    }

    fn get_account(&self) -> Option<nautilus_model::accounts::AccountAny> {
        None // account state generation 未接（connect 注释同面）
    }

    /// Unknown submit outcomes stay unknown — venue evidence is required
    /// before any local closure (the engine registers this requirement).
    fn retain_unresolved_submissions(&self) -> bool {
        true
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }
        self.emitter.set_sender(get_exec_event_sender());
        // 钱包事件摄取会话任务（随代刷新 JWT + 生命周期对账）。
        if let Some(events) = self.ws_events.take() {
            let ws = self.ws.clone();
            let trading = self.trading.clone();
            let emitter = self.emitter.clone();
            let tracked = self.tracked.clone();
            let clock = self.clock;
            let subscription = crate::websocket::user::WalletSubscription::default();
            if let Err(e) = self.session_tasks.spawn(crate::execution::orders::run_wallet_events(
                ws, trading, events, emitter, tracked, subscription, clock,
            )) {
                log::warn!("predict wallet-event task not started (shutdown began): {e}");
            }
        }
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }
        self.session_tasks.begin_shutdown();
        self.pending_tasks.begin_shutdown();
        self.core.set_stopped();
        self.core.set_disconnected();
        Ok(())
    }

    fn generate_account_state(
        &self,
        _balances: Vec<AccountBalance>,
        _margins: Vec<MarginBalance>,
        _reported: bool,
        _ts_event: UnixNanos,
        _info: Option<nautilus_core::params::Params>,
    ) -> anyhow::Result<()> {
        // 脚手架阶段：账户状态发射未接（现金余额需链上 USDT 读）——
        // 响亮拒，不发射半截状态。
        anyhow::bail!(
            "predict account state generation is not wired yet \
             (cash balance read pending; positions face: wallet_positions())"
        )
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        // 现金余额面未接（无 REST 余额端点——链上 USDT 读属后续增量）：
        // 响亮 WARN，不静默；仓位面经 wallet_positions 可查。
        log::warn!(
            "predict execution client: cash balance reporting is NOT wired \
             (on-chain USDT read pending) — account state will carry positions \
             only; order flow is fully wired"
        );
        self.ws.connect().await?;
        self.core.set_connected();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.ws.stop().await;
        self.core.set_disconnected();
        Ok(())
    }

    fn submit_order(&self, cmd: nautilus_common::messages::execution::SubmitOrder) -> anyhow::Result<()> {
        self.submit_pipeline(cmd)
    }

    fn submit_order_list(
        &self,
        cmd: nautilus_common::messages::execution::SubmitOrderList,
    ) -> anyhow::Result<()> {
        for order_init in cmd.order_inits.iter() {
            let submit = nautilus_common::messages::execution::SubmitOrder {
                trader_id: cmd.trader_id,
                client_id: cmd.client_id,
                strategy_id: cmd.strategy_id,
                instrument_id: cmd.instrument_id,
                order_init: order_init.clone(),
                exec_algorithm_id: None,
                position_id: None,
                params: None,
                causation_id: cmd.causation_id,
                correlation_id: cmd.correlation_id,
                client_order_id: order_init.client_order_id,
                command_id: cmd.command_id,
                ts_init: cmd.ts_init,
            };
            self.submit_pipeline(submit)?;
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: nautilus_common::messages::execution::CancelOrder) -> anyhow::Result<()> {
        self.cancel_pipeline(cmd)
    }

    // 改单：venue 无改单语义——trait 默认（响亮拒绝）。账户状态面维持
    // 登记缺口（链上 USDT 读）。submit/cancel + 钱包事件摄取已接
    // （orders.rs——§8.2 管线）。
}
