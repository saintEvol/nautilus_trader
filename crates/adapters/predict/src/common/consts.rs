//! Constants for the Predict adapter.

use std::sync::LazyLock;

use nautilus_model::{
    defi::{currency::TokenCurrency, currency as defi_currency},
    identifiers::{ClientId, Venue},
    types::Currency,
};
use ustr::Ustr;

use super::enums::PredictEnvironment;

/// Venue identifier string.
pub const PREDICT: &str = "PREDICT";

/// Static venue instance.
pub static PREDICT_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(PREDICT)));

/// Static client ID instance.
pub static PREDICT_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(PREDICT)));

/// Public-data API key header (checked once at the WS handshake, on every
/// REST request).
pub(crate) const API_KEY_HEADER: &str = "x-api-key";

/// Default share quantity precision (wallet events report shares with 3
/// decimals; book sizes carry the same granularity).
pub(crate) const SIZE_PRECISION: u8 = 3;

/// Minimum share quantity increment.
pub(crate) const MIN_SIZE_INCREMENT: &str = "0.001";

// WS topics (AsyncAPI `/asyncapi/websocket.json`): `predictOrderbook/{marketId}`
// etc. — subscribe/unsubscribe name exactly one topic each.
pub(crate) const TOPIC_ORDERBOOK: &str = "predictOrderbook";
/// Wallet-events topic prefix (param is the JWT — never logged).
pub(crate) const TOPIC_WALLET_EVENTS: &str = "predictWalletEvents";
pub(crate) const TOPIC_TOP_OF_BOOK: &str = "predictTopOfBook";
pub(crate) const TOPIC_TRADING_STATUS: &str = "predictTradingStatus";
pub(crate) const TOPIC_MARKET_STATUS: &str = "predictMarketStatus";
pub(crate) const TOPIC_MARKET_CHANGED: &str = "predictMarketChanged";

/// Server-pushed topics that cannot be subscribed to.
pub(crate) const TOPIC_HEARTBEAT: &str = "heartbeat";
pub(crate) const TOPIC_SYSTEM: &str = "system";

// Batch/page limits pinned from the OpenAPI descriptions.
/// `GET /categories` page size ceiling.
pub(crate) const CATEGORIES_FIRST_MAX: u32 = 100;
/// `GET /orders/matches` page size ceiling.
pub(crate) const MATCHES_FIRST_MAX: u32 = 150;
/// `GET /markets/orderbooks` batch ceiling.
pub(crate) const ORDERBOOKS_BATCH_MAX: usize = 100;
/// `GET /markets/quotes` batch ceiling.
pub(crate) const QUOTES_BATCH_MAX: usize = 500;
/// `GET /markets/status-logs` batch ceiling.
pub(crate) const STATUS_LOGS_BATCH_MAX: usize = 100;

/// USDT contract on BNB Mainnet (official Python SDK `ADDRESSES_BY_CHAIN_ID`).
pub(crate) const USDT_BNB_ADDRESS: &str = "0x55d398326f99059fF775485246999027B3197955";
/// BNB Mainnet 公共 JSON-RPC（账户状态链上读缺省端点；部署可经
/// PredictExecutionConfig.rpc_url 覆盖——私有端点/代理场景）。
pub const BNB_PUBLIC_RPC: &str = "https://bsc-dataseed.binance.org";

/// BNB Mainnet chain id.
pub(crate) const BNB_MAINNET_CHAIN_ID: u32 = 56;

/// Registers and returns the Predict collateral currency: chain-bound USDT
/// with its real 18-decimal precision (Predict transacts `1 USDT = 10^18`
/// wei — the built-in `USDT` constant carries the 8-decimal Omni definition
/// and must not be silently reused).
///
/// The code renders as `USDT@BSC`; registration is idempotent and conflicting
/// re-definitions are rejected loudly by the registry.
#[must_use]
pub fn collateral_currency() -> Currency {
    const USDT_BNB_CODE: &str = "USDT@BSC";
    let currency = Currency::new(
        USDT_BNB_CODE,
        18,
        0,
        "Tether USD (BNB Smart Chain)",
        nautilus_model::enums::CurrencyType::Crypto,
    );
    // The registry compares the address against its canonical rendering —
    // store exactly what `format!("{address:#x}")` produces.
    let parsed = alloy_primitives::Address::parse_checksummed(USDT_BNB_ADDRESS, None)
        .expect("pinned USDT contract address");
    let address = format!("{parsed:#x}");
    let metadata = TokenCurrency {
        chain_id: BNB_MAINNET_CHAIN_ID,
        address,
        symbol: "USDT".to_string(),
        chain_name: nautilus_model::defi::Blockchain::Bsc,
    };
    defi_currency::register(currency, metadata)
        .unwrap_or_else(|e| panic!("register Predict collateral currency: {e:?}"))
}

/// Returns the default HTTP base URL for the environment.
#[must_use]
pub(crate) fn http_base_url(environment: PredictEnvironment) -> &'static str {
    match environment {
        PredictEnvironment::Mainnet => "https://api.predict.fun/v1",
        PredictEnvironment::Testnet => "https://api-testnet.predict.fun/v1",
    }
}

/// Returns the default WebSocket URL for the environment.
///
/// The AsyncAPI document pins `ws-testnet.predict.fun`; the developer docs'
/// prose still lists testnet as not yet available — the machine-readable
/// contract wins until a live handshake says otherwise.
#[must_use]
pub(crate) fn websocket_url(environment: PredictEnvironment) -> &'static str {
    match environment {
        PredictEnvironment::Mainnet => "wss://ws.predict.fun/ws",
        PredictEnvironment::Testnet => "wss://ws-testnet.predict.fun/ws",
    }
}
