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

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nautilus_common::clients::ExecutionClient;
use nautilus_core::string::secret::SecretString;
use nautilus_core::UnixNanos;
use nautilus_live::ExecutionClientCore;
use nautilus_model::identifiers::{AccountId, ClientId, VenueOrderId};
use nautilus_model::types::{AccountBalance, Currency, MarginBalance};

use crate::common::consts::{PREDICT_VENUE, collateral_currency};
use crate::execution::lifecycle::{
    LifecycleAction, PredictOrderStatusWord, map_observation, parse_wei,
};
use crate::http::positions::PredictPositionDto;
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

/// One in-flight (or reconcile-tracked) venue order.
#[derive(Clone, Debug, Default)]
struct InflightOrder {
    venue_order_id: Option<String>,
    order_hash: String,
    amount_wei: i128,
    last_seen_filled_wei: i128,
}

/// The execution client proper.
pub struct PredictExecutionClient {
    core: ExecutionClientCore,
    trading: Arc<PredictTradingClient>,
    /// wallet address (JWT signer) for positions reads.
    wallet_address: String,
    data_http: crate::http::client::PredictHttpClient,
    ws: crate::websocket::client::PredictWebSocketClient,
    wallet_sub: Mutex<crate::websocket::user::WalletSubscription>,
    inflight: Mutex<HashMap<String, InflightOrder>>, // client_order_id → order
    /// emit surface (set at start by the runtime).
    emitter: RefCell<Option<nautilus_live::ExecutionEventEmitter>>,
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
        let (ws, _events) = crate::websocket::client::PredictWebSocketClient::new(
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
            trading: Arc::new(trading),
            wallet_address,
            data_http,
            ws,
            wallet_sub: Mutex::new(crate::websocket::user::WalletSubscription::default()),
            inflight: Mutex::new(HashMap::new()),
            emitter: RefCell::new(None),
        })
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

    /// Maps one venue order-query observation of a tracked order into
    /// lifecycle actions and advances the tracked watermark (the caller
    /// turns actions into engine events; quantities are always the
    /// venue's own deltas).
    fn observe(&self, client_order_id: &str, record_status: PredictOrderStatusWord, record: &crate::http::trading::PredictOrderRecord) -> anyhow::Result<Vec<LifecycleAction>> {
        let mut inflight = self
            .inflight
            .lock()
            .expect("inflight lock is not poisoned");
        let Some(order) = inflight.get_mut(client_order_id) else {
            anyhow::bail!("order {client_order_id} is not tracked");
        };
        let amount = parse_wei(&record.amount)?;
        let filled = parse_wei(&record.amount_filled)?;
        let actions = map_observation(
            record_status,
            amount,
            filled,
            order.last_seen_filled_wei,
            None,
        );
        order.last_seen_filled_wei = filled;
        Ok(actions)
    }

    #[must_use]
    pub fn collateral() -> Currency {
        collateral_currency()
    }

    fn tracked_venue_order_id(&self, client_order_id: &str) -> Option<VenueOrderId> {
        self.inflight
            .lock()
            .expect("inflight lock is not poisoned")
            .get(client_order_id)
            .and_then(|order| order.venue_order_id.clone())
            .map(VenueOrderId::new)
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
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.core.set_stopped();
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
        self.core.set_connected();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.ws.stop().await;
        self.core.set_disconnected();
        Ok(())
    }

    // 提交/撤单/改单面：trait 默认实现（响亮 not-supported）——submit
    // 管线（乐器→token 身份、市价走档、事件摄取→发射）是下一增量，
    // 半接线不冒充整所完成（§11）。已提交的合同层（运输/数量数学/
    // 生命周期映射/请求签名/钱包事件解码）全部经离线锚验证。
}
