//! Predict trading REST transport (§6.2: JWT wallet auth + order actions).
//!
//! Contract sources (read 2026-11-13, field names verbatim):
//! - `GET /v1/auth/message` — dynamic message to sign (`data.message`).
//! - `POST /v1/auth` — `{signer, signature, message}` → `data.token`
//!   (JWT; documented 24 h validity, no expiry field in the response —
//!   the client decodes the token's `exp` claim when present and falls
//!   back to the documented fixed lifetime otherwise).
//! - `POST /v1/orders` — signed EIP-712 order + placement settings →
//!   `201` `data: {code, orderId, orderHash, removalLockedUntil}`. A `201`
//!   only means initial checks passed (balance/allowance finish
//!   asynchronously) — order outcomes come from wallet events and order
//!   queries, never from this acceptance alone.
//! - `POST /v1/orders/remove` — `{data: {ids: [...]}}` (≤ 100) →
//!   `{removed, noop, rejected}` (removal-lock hits stay retryable).
//! - `GET /v1/orders/{hash}` — order record with `status` and
//!   `amountFilled` (reconciliation source of truth).
//!
//! Disciplines pinned here:
//! - Order placement is submit-once: no transport-level automatic retry
//!   (a retried POST is a *new* submission attempt whose first outcome may
//!   be lost — unknown outcomes belong to reconciliation by `orderHash`,
//!   never to a silent retry).
//! - The JWT is cached and refreshed before expiry; concurrent callers
//!   share one refresh (single mutex-guarded flow). A 401 drops the cached
//!   token and re-authenticates once.
//! - EOA wallets sign the auth message via EIP-191. Predict-account
//!   (smart-wallet) signing uses the SDK's own format and is NOT
//!   implemented — [`PredictMessageSigner`] is the seam where that variant
//!   lands; the gap stays loud, not silently mis-signed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::signers::local::PrivateKeySigner;
use alloy_primitives::Address;
use nautilus_core::string::secret::SecretString;
use nautilus_network::http::{
    HttpClient, HttpClientError, HttpResponse, Method, create_standard_nautilus_headers,
};
use nautilus_network::retry::{RetryConfig, RetryManager, RetryError};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

use super::client::HEADER_RATE_LIMIT_RESET;
use super::error::{PredictHttpError, decode_response};
use crate::common::consts::API_KEY_HEADER;

const PATH_AUTH_MESSAGE: &str = "/auth/message";
const PATH_AUTH: &str = "/auth";
const PATH_ORDERS: &str = "/orders";
const PATH_ORDERS_REMOVE: &str = "/orders/remove";

/// Documented JWT lifetime when the token carries no readable `exp` claim.
const JWT_FALLBACK_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);
/// Refresh this long before expiry (token swap on the WS wallet topic needs
/// headroom — a fresh subscribe + reconcile must fit inside the margin).
const JWT_REFRESH_MARGIN: Duration = Duration::from_secs(10 * 60);
/// `POST /orders/remove` accepts at most 100 ids per call.
pub const REMOVE_ORDERS_MAX: usize = 100;

/// Wallet message signer (auth `POST /auth` EIP-191 today; the Predict
/// smart-account variant is a future implementation of this trait).
pub trait PredictMessageSigner: Send + Sync {
    /// The address sent as `signer` (checksummed wallet address).
    fn address(&self) -> Address;

    /// Signs the dynamic auth message (EIP-191 personal message for EOAs).
    ///
    /// # Errors
    /// Implementation-defined signing failure (never a placeholder value).
    fn sign_message(&self, message: &str) -> Result<String, PredictHttpError>;
}

/// Plain EOA signer (secp256k1 private key, EIP-191 personal signatures).
pub struct EoaPredictSigner {
    signer: PrivateKeySigner,
}

impl EoaPredictSigner {
    /// Creates the signer from a hex private key (with or without `0x`).
    ///
    /// # Errors
    /// Returns a transport-class error when the key is not a valid secp256k1
    /// private key — never falls back to an empty signer.
    pub fn from_key_hex(key_hex: &SecretString) -> Result<Self, PredictHttpError> {
        let key = key_hex.expose_secret();
        let signer = <PrivateKeySigner as std::str::FromStr>::from_str(key)
            .map_err(|e| PredictHttpError::Transport(format!("invalid wallet private key: {e}")))?;
        Ok(Self { signer })
    }
}

impl PredictMessageSigner for EoaPredictSigner {
    fn address(&self) -> Address {
        self.signer.address()
    }

    fn sign_message(&self, message: &str) -> Result<String, PredictHttpError> {
        use alloy::signers::SignerSync;
        let signature = self
            .signer
            .sign_message_sync(message.as_bytes())
            .map_err(|e| PredictHttpError::Transport(format!("EIP-191 signing failed: {e}")))?;
        Ok(format!("0x{}", alloy_primitives::hex::encode(signature.as_bytes())))
    }
}

/// A cached wallet JWT with its refresh deadline.
struct WalletJwt {
    token: SecretString,
    refresh_at: Instant,
}

impl WalletJwt {
    fn new(token: SecretString) -> Self {
        let lifetime = jwt_exp_lifetime(token.expose_secret()).unwrap_or(JWT_FALLBACK_LIFETIME);
        let refresh_at = Instant::now()
            + lifetime
                .saturating_sub(JWT_REFRESH_MARGIN)
                .max(Duration::from_secs(1));
        Self { token, refresh_at }
    }
}

/// Decodes a compact JWS's `exp` claim (base64url payload, no padding).
/// Garbage or a missing claim yields `None` — the caller then uses the
/// documented fixed lifetime; this is a freshness hint, never an auth
/// decision.
fn jwt_exp_lifetime(token: &str) -> Option<Duration> {
    use base64::Engine as _;
    let payload = token.split('.').nth(1)?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    let exp = claims.get("exp")?.as_u64()?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    exp.checked_sub(now).map(Duration::from_secs)
}

// ── Wire DTOs (documented field names, camelCase; no defaults) ───────────────

/// Order strategy (`strategy` wire enum).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PredictOrderStrategy {
    #[serde(rename = "LIMIT")]
    Limit,
    #[serde(rename = "MARKET")]
    Market,
}

/// Order record status (`status` wire enum — order-lifecycle page).
/// Lexicon is closed: an unknown status is a loud decode error, never a
/// guess.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PredictOrderStatus {
    #[serde(rename = "OPEN")]
    Open,
    #[serde(rename = "FILLED")]
    Filled,
    #[serde(rename = "CANCELLED")]
    Cancelled,
    #[serde(rename = "EXPIRED")]
    Expired,
    #[serde(rename = "INVALIDATED")]
    Invalidated,
}

/// The signed EIP-712 order exactly as placed (wei string amounts, numeric
/// enums per the documented order model). Sent back verbatim from the order
/// query — this type is both request payload and response payload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedPredictOrderWire {
    pub hash: String,
    pub salt: String,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    #[serde(rename = "makerAmount")]
    pub maker_amount: String,
    #[serde(rename = "takerAmount")]
    pub taker_amount: String,
    pub expiration: u64,
    pub nonce: String,
    #[serde(rename = "feeRateBps")]
    pub fee_rate_bps: String,
    pub side: u8,
    #[serde(rename = "signatureType")]
    pub signature_type: u8,
    pub signature: String,
}

/// Placement settings beside the order (place-orders page). Optional fields
/// are strategy-conditional venue inputs — absent means absent, no default.
#[derive(Clone, Debug, Serialize)]
pub struct PlaceOrderSettings {
    /// Wei string, required by the venue for every placement.
    #[serde(rename = "pricePerShare")]
    pub price_per_share: String,
    pub strategy: PredictOrderStrategy,
    /// LIMIT only.
    #[serde(rename = "isPostOnly", skip_serializing_if = "Option::is_none")]
    pub is_post_only: Option<bool>,
    /// MARKET only.
    #[serde(rename = "isFillOrKill", skip_serializing_if = "Option::is_none")]
    pub is_fill_or_kill: Option<bool>,
    /// MARKET only ("0"–"10000" string).
    #[serde(rename = "slippageBps", skip_serializing_if = "Option::is_none")]
    pub slippage_bps: Option<String>,
    /// MARKET only.
    #[serde(rename = "isMinAmountOut", skip_serializing_if = "Option::is_none")]
    pub is_min_amount_out: Option<bool>,
    #[serde(
        rename = "reservedBalancePolicy",
        skip_serializing_if = "Option::is_none"
    )]
    pub reserved_balance_policy: Option<String>,
    #[serde(
        rename = "selfTradePrevention",
        skip_serializing_if = "Option::is_none"
    )]
    pub self_trade_prevention: Option<String>,
}

#[derive(Serialize)]
struct PlaceOrderData<'a> {
    order: &'a SignedPredictOrderWire,
    #[serde(flatten)]
    settings: &'a PlaceOrderSettings,
}

#[derive(Serialize)]
struct PlaceOrderBody<'a> {
    data: PlaceOrderData<'a>,
}

/// `201` acceptance (`data: {code, orderId, orderHash, removalLockedUntil}`).
#[derive(Clone, Debug, Deserialize)]
pub struct PlacedOrder {
    pub code: Option<String>,
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "orderHash")]
    pub order_hash: String,
    #[serde(rename = "removalLockedUntil")]
    pub removal_locked_until: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct RemoveOrdersData<'a> {
    ids: &'a [String],
}

#[derive(Serialize)]
struct RemoveOrdersBody<'a> {
    data: RemoveOrdersData<'a>,
}

/// Removal outcome: `noop` = already filled/removed/cancelled,
/// `rejected` = still inside the removal-lock window (retryable later).
#[derive(Clone, Debug, Deserialize)]
pub struct RemoveOrdersOutcome {
    pub success: bool,
    pub removed: Vec<String>,
    pub noop: Vec<String>,
    pub rejected: Vec<String>,
}

/// One own-order record (`GET /orders/{hash}` → `data`).
#[derive(Clone, Debug, Deserialize)]
pub struct PredictOrderRecord {
    pub id: String,
    pub order: SignedPredictOrderWire,
    #[serde(rename = "marketId")]
    pub market_id: i64,
    pub currency: String,
    /// Wei string.
    pub amount: String,
    /// Wei string — the settled portion; the reconciliation truth.
    #[serde(rename = "amountFilled")]
    pub amount_filled: String,
    #[serde(rename = "isNegRisk")]
    pub is_neg_risk: bool,
    #[serde(rename = "isYieldBearing")]
    pub is_yield_bearing: bool,
    pub strategy: PredictOrderStrategy,
    pub status: PredictOrderStatus,
    #[serde(rename = "rewardEarningRate")]
    pub reward_earning_rate: serde_json::Value,
}

#[derive(Deserialize)]
struct AuthMessageBody {
    data: AuthMessageData,
}

#[derive(Deserialize)]
struct AuthMessageData {
    message: String,
}

#[derive(Deserialize)]
struct AuthTokenBody {
    data: AuthTokenData,
}

#[derive(Deserialize)]
struct AuthTokenData {
    token: SecretString,
}

#[derive(Deserialize)]
struct PlacedOrderBody {
    data: PlacedOrder,
}

#[derive(Deserialize)]
struct OrderRecordBody {
    data: PredictOrderRecord,
}

#[derive(Deserialize)]
struct RemoveOutcomeBody {
    data: RemoveOrdersOutcome,
}

/// Typed trading REST client: JWT-gated order actions over the same
/// transport conventions as the data client (standard headers, retry with
/// `RateLimit-Reset` awareness, structured errors).
pub struct PredictTradingClient {
    client: HttpClient,
    base_url: String,
    retry_manager: Arc<RetryManager<PredictHttpError>>,
    signer: Arc<dyn PredictMessageSigner>,
    /// Cached JWT + in-flight refresh single-flight (mutex held across the
    /// whole fetch-sign-post flow).
    jwt: tokio::sync::Mutex<Option<WalletJwt>>,
}

impl std::fmt::Debug for PredictTradingClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never formats the signer or cached token (secret material).
        f.debug_struct("PredictTradingClient")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl PredictTradingClient {
    /// Creates the trading client for the explicit base URL (already
    /// including `/v1`) and credentials. Credentials come only from this
    /// explicit configuration — no environment fallback exists here.
    ///
    /// # Errors
    /// Returns an error if the underlying transport cannot be created.
    pub fn new(
        base_url: &str,
        api_key: &SecretString,
        signer: Arc<dyn PredictMessageSigner>,
        timeout_secs: u64,
        proxy_url: Option<&nautilus_network::websocket::proxy::ProxyUrl>,
    ) -> Result<Self, HttpClientError> {
        let mut headers: HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert(
            API_KEY_HEADER.to_string(),
            api_key.expose_secret().to_string(),
        );
        Ok(Self {
            client: HttpClient::builder()
                .headers(headers)
                .header_keys(vec![HEADER_RATE_LIMIT_RESET.to_string()])
                .timeout_secs(timeout_secs)
                .maybe_proxy_url(proxy_url.map(|url| url.expose().to_string()))
                .build()?,
            base_url: base_url.trim_end_matches('/').to_string(),
            retry_manager: Arc::new(RetryManager::new(RetryConfig {
                max_retries: 2,
                initial_delay_ms: 250,
                max_delay_ms: 1_000,
                max_elapsed_ms: Some(timeout_secs.saturating_mul(3).saturating_add(2_000)),
                ..RetryConfig::default()
            })),
            signer,
            jwt: tokio::sync::Mutex::new(None),
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    async fn send<T, F, Fut>(
        &self,
        endpoint: &'static str,
        mut request: F,
    ) -> Result<T, PredictHttpError>
    where
        T: DeserializeOwned,
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<HttpResponse, HttpClientError>>,
    {
        let retry_after: std::sync::Mutex<Option<Duration>> = std::sync::Mutex::new(None);
        Box::pin(
            self.retry_manager
                .invocation(
                    endpoint,
                    || {
                        let fut = request();
                        async {
                            *retry_after.lock().expect("retry_after poisoned") = None;
                            let response: HttpResponse =
                                fut.await.map_err(PredictHttpError::from_http_client)?;
                            *retry_after.lock().expect("retry_after poisoned") = response
                                .headers
                                .get(HEADER_RATE_LIMIT_RESET)
                                .and_then(|value| {
                                    PredictHttpError::parse_rate_limit_reset(value)
                                });
                            match decode_response::<T>(endpoint, &response) {
                                Err(error @ PredictHttpError::RateLimit { .. }) => Err(
                                    error.with_retry_after(
                                        *retry_after.lock().expect("retry_after poisoned"),
                                    ),
                                ),
                                other => other,
                            }
                        }
                    },
                    PredictHttpError::is_retryable,
                    |e| match e {
                        RetryError::OperationTimeout { .. } => PredictHttpError::Timeout,
                        RetryError::InvalidConfiguration { message } => {
                            PredictHttpError::Transport(message)
                        }
                        e => PredictHttpError::Transport(e.to_string()),
                    },
                )
                .retry_delay(&|_| *retry_after.lock().expect("retry_after poisoned"))
                .execute(),
        )
        .await
    }

    /// 单次发送（submit-once 纪律）：无自动重试——order placement 专用。
    /// 超时/传输错误的结局是 unknown（对账域），重发 POST 是一次**新**
    /// 提交、其首次结果可能已丢失——这正是文件头自禁的形态。
    async fn send_once<T: DeserializeOwned>(
        &self,
        endpoint: &'static str,
        request: impl std::future::Future<
            Output = Result<HttpResponse, nautilus_network::http::HttpClientError>,
        >,
    ) -> Result<T, PredictHttpError> {
        let response: HttpResponse =
            request.await.map_err(PredictHttpError::from_http_client)?;
        decode_response::<T>(endpoint, &response)
    }

    fn bearer_headers(token: &WalletJwt) -> HashMap<String, String> {
        HashMap::from([(
            "Authorization".to_string(),
            format!("Bearer {}", token.token.expose_secret()),
        )])
    }

    // ── Auth ──────────────────────────────────────────────────────────────

    /// Fetches the dynamic message to sign (`GET /auth/message`).
    async fn auth_message(&self) -> Result<String, PredictHttpError> {
        let body: AuthMessageBody = self
            .send("auth/message", || async {
                    self.client
                        .request_with_params::<serde_json::Value>(
                            Method::GET,
                            self.url(PATH_AUTH_MESSAGE),
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                        .await
                },
            )
            .await?;
        Ok(body.data.message)
    }

    /// Runs the full three-step auth flow and returns the cached JWT. The
    /// mutex is held across the whole flow — concurrent callers coalesce
    /// into one refresh (§6.2 并发刷新合并).
    async fn authenticate(&self) -> Result<WalletJwt, PredictHttpError> {
        let mut guard = self.jwt.lock().await;
        if let Some(jwt) = guard.as_ref() {
            if Instant::now() < jwt.refresh_at {
                return Ok(WalletJwt {
                    token: SecretString::from(jwt.token.expose_secret()),
                    refresh_at: jwt.refresh_at,
                });
            }
        }
        let message = self.auth_message().await?;
        let signature = self.signer.sign_message(&message)?;
        let signer = self.signer.address().to_checksum(None);
        let body = serde_json::to_vec(&serde_json::json!({
            "signer": signer,
            "signature": signature,
            "message": message,
        }))
        .map_err(PredictHttpError::Serde)?;
        let payload = body;
        let token_body: AuthTokenBody = self
            .send("auth", || {
                let payload = payload.clone();
                async {
                    self.client
                        .request(
                            Method::POST,
                            self.url(PATH_AUTH),
                            None,
                            None,
                            Some(payload),
                            None,
                            None,
                        )
                        .await
                }
            })
            .await?;
        let jwt = WalletJwt::new(token_body.data.token);
        let refresh_at = jwt.refresh_at;
        *guard = Some(WalletJwt {
            token: SecretString::from(jwt.token.expose_secret()),
            refresh_at,
        });
        Ok(jwt)
    }

    /// A usable JWT (cached until its refresh deadline). A `401` from a
    /// wallet endpoint drops the cache and re-authenticates once.
    /// 钱包流订阅用的 JWT（缓存未到期即复用——每次（重）连换新 token 的
    /// 语义由调用方在代边界驱动；到期前同 token 订阅合法）。
    pub async fn wallet_jwt(&self) -> Result<SecretString, PredictHttpError> {
        Ok(SecretString::from(
            self.jwt(false).await?.token.expose_secret(),
        ))
    }

    async fn jwt(&self, force_refresh: bool) -> Result<WalletJwt, PredictHttpError> {
        if force_refresh {
            self.jwt.lock().await.take();
        }
        self.authenticate().await
    }

    // ── Order actions ─────────────────────────────────────────────────────

    /// Places a signed order (`POST /orders`). Submit-once: transport
    /// failures surface as errors — the caller reconciles by `orderHash`
    /// (the venue rejects exact duplicates), this method never re-sends.
    ///
    /// # Errors
    /// Transport/venue errors, including auth failures after one
    /// re-authentication attempt.
    pub async fn place_order(
        &self,
        order: &SignedPredictOrderWire,
        settings: &PlaceOrderSettings,
    ) -> Result<PlacedOrder, PredictHttpError> {
        let body = PlaceOrderBody {
            data: PlaceOrderData {
                order,
                settings,
            },
        };
        let payload =
            serde_json::to_vec(&body).map_err(PredictHttpError::Serde)?;
        for force_refresh in [false, true] {
            let jwt = self.jwt(force_refresh).await?;
            let headers = Self::bearer_headers(&jwt);
            // submit-once：不可重试通路（自动重发 = 新提交且首次结果
            // 可能已丢失——超时结局是 unknown，归对账域）。
            let result: Result<PlacedOrderBody, PredictHttpError> = self
                .send_once("orders/create", {
                    let headers = headers.clone();
                    let payload = payload.clone();
                    async {
                        self.client
                            .request(
                                Method::POST,
                                self.url(PATH_ORDERS),
                                None,
                                Some(headers),
                                Some(payload),
                                None,
                                None,
                            )
                            .await
                    }
                })
                .await;
            match result {
                Ok(body) => return Ok(body.data),
                Err(PredictHttpError::Http { status: 401, .. }) => continue,
                Err(other) => return Err(other),
            }
        }
        Err(PredictHttpError::Http {
            status: 401,
            code: "invalid_credentials".to_string(),
            message: "orders/create: 401 after re-authentication".to_string(),
        })
    }

    /// Removes up to [`REMOVE_ORDERS_MAX`] resting orders by id
    /// (`POST /orders/remove`). Removal-lock hits come back in `rejected`
    /// and stay retryable — they are not an error.
    ///
    /// # Errors
    /// Transport/venue errors; more than 100 ids is a caller bug and fails
    /// loudly client-side.
    pub async fn remove_orders(
        &self,
        ids: &[String],
    ) -> Result<RemoveOrdersOutcome, PredictHttpError> {
        if ids.len() > REMOVE_ORDERS_MAX {
            return Err(PredictHttpError::Pagination(format!(
                "orders/remove: {} ids exceed the documented batch maximum {REMOVE_ORDERS_MAX}",
                ids.len()
            )));
        }
        let payload = serde_json::to_vec(&RemoveOrdersBody {
            data: RemoveOrdersData { ids },
        })
        .map_err(PredictHttpError::Serde)?;
        for force_refresh in [false, true] {
            let jwt = self.jwt(force_refresh).await?;
            let headers = Self::bearer_headers(&jwt);
            let result: Result<RemoveOutcomeBody, PredictHttpError> = self
                .send("orders/remove", || {
                        let headers = headers.clone();
                        let payload = payload.clone();
                        async {
                            self.client
                                .request(
                                    Method::POST,
                                    self.url(PATH_ORDERS_REMOVE),
                                    None,
                                    Some(headers),
                                    Some(payload),
                                    None,
                                    None,
                                )
                                .await
                        }
                    },
                )
                .await;
            match result {
                Ok(body) => return Ok(body.data),
                Err(PredictHttpError::Http { status: 401, .. }) => continue,
                Err(other) => return Err(other),
            }
        }
        Err(PredictHttpError::Http {
            status: 401,
            code: "invalid_credentials".to_string(),
            message: "orders/remove: 401 after re-authentication".to_string(),
        })
    }

    /// Fetches one own-order record by hash (`GET /orders/{hash}` —
    /// reconciliation source of truth: `status` + `amountFilled`).
    ///
    /// # Errors
    /// Transport/venue errors, including one re-authentication attempt on
    /// 401.
    pub async fn get_order(&self, order_hash: &str) -> Result<PredictOrderRecord, PredictHttpError> {
        let path = format!("{PATH_ORDERS}/{order_hash}");
        for force_refresh in [false, true] {
            let jwt = self.jwt(force_refresh).await?;
            let headers = Self::bearer_headers(&jwt);
            let result: Result<OrderRecordBody, PredictHttpError> = self
                .send("orders/get", || {
                        let headers = headers.clone();
                        async {
                            self.client
                                .request(
                                    Method::GET,
                                    self.url(&path),
                                    None,
                                    Some(headers),
                                    None,
                                    None,
                                    None,
                                )
                                .await
                        }
                    },
                )
                .await;
            match result {
                Ok(body) => return Ok(body.data),
                Err(PredictHttpError::Http { status: 401, .. }) => continue,
                Err(other) => return Err(other),
            }
        }
        Err(PredictHttpError::Http {
            status: 401,
            code: "invalid_credentials".to_string(),
            message: "orders/get: 401 after re-authentication".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Documented `201` acceptance shape (place-orders page).
    #[test]
    fn placed_order_decodes_documented_shape() {
        let body = br#"{"success":true,"data":{"code":"ORDER_ACCEPTED","orderId":"ord-1","orderHash":"0xabc","removalLockedUntil":null}}"#;
        let placed: PlacedOrderBody = serde_json::from_slice(body).unwrap();
        assert_eq!(placed.data.order_id, "ord-1");
        assert_eq!(placed.data.order_hash, "0xabc");
        assert_eq!(placed.data.code.as_deref(), Some("ORDER_ACCEPTED"));
    }

    /// Documented removal outcome (three arrays; `rejected` = retryable
    /// removal-lock hits).
    #[test]
    fn remove_outcome_decodes_three_arrays() {
        let body = br#"{"success":true,"data":{"success":true,"removed":["a"],"noop":["b"],"rejected":["c"]}}"#;
        let outcome: RemoveOutcomeBody = serde_json::from_slice(body).unwrap();
        assert_eq!(outcome.data.removed, vec!["a".to_string()]);
        assert_eq!(outcome.data.noop, vec!["b".to_string()]);
        assert_eq!(outcome.data.rejected, vec!["c".to_string()]);
    }

    /// Order-record decode: OPEN limit order with wei amounts; the full
    /// 13-field signed order returns verbatim.
    #[test]
    fn order_record_decodes_open_order() {
        let body = br#"{"success":true,"data":{
            "id":"ord-9","order":{
                "hash":"0xh","salt":"1","maker":"0xm","signer":"0xm","taker":"0x0000000000000000000000000000000000000000",
                "tokenId":"7","makerAmount":"4000000000000000000","takerAmount":"10000000000000000000",
                "expiration":0,"nonce":"0","feeRateBps":"0","side":0,"signatureType":0,"signature":"0xs"
            },
            "marketId":42,"currency":"USDT","amount":"10000000000000000000","amountFilled":"0",
            "isNegRisk":false,"isYieldBearing":false,"strategy":"LIMIT","status":"OPEN","rewardEarningRate":0
        }}"#;
        let record: OrderRecordBody = serde_json::from_slice(body).unwrap();
        assert_eq!(record.data.status, PredictOrderStatus::Open);
        assert_eq!(record.data.strategy, PredictOrderStrategy::Limit);
        assert_eq!(record.data.amount_filled, "0");
        assert_eq!(record.data.order.maker_amount, "4000000000000000000");
    }

    /// Status lexicon is closed: an unknown status is a loud decode error,
    /// never a defaulted guess.
    #[test]
    fn unknown_status_is_loud() {
        let body = br#"{"success":true,"data":{
            "id":"x","order":{"hash":"0xh","salt":"1","maker":"0xm","signer":"0xm","taker":"0x0","tokenId":"7",
            "makerAmount":"1","takerAmount":"1","expiration":0,"nonce":"0","feeRateBps":"0","side":0,
            "signatureType":0,"signature":"0xs"},
            "marketId":1,"currency":"USDT","amount":"1","amountFilled":"0","isNegRisk":false,
            "isYieldBearing":false,"strategy":"MARKET","status":"PARTIALLY_FILLED","rewardEarningRate":0
        }}"#;
        assert!(serde_json::from_slice::<OrderRecordBody>(body).is_err());
    }

    /// JWT `exp` decode: real compact-JWS shape with a future exp decodes;
    /// garbage and exp-less tokens fall back to `None`.
    #[test]
    fn jwt_exp_decodes_or_falls_back() {
        use base64::Engine as _;
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3_600;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(format!(r#"{{"exp":{exp}}}"#));
        let token = format!("header.{payload}.sig");
        assert_eq!(
            jwt_exp_lifetime(&token).map(|d| d.as_secs() > 3_000),
            Some(true)
        );
        assert_eq!(jwt_exp_lifetime("not-a-jwt"), None);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"sub":"wallet"}"#);
        assert_eq!(jwt_exp_lifetime(&format!("h.{payload}.s")), None);
    }

    /// EOA EIP-191 golden vector: fixed public test key + fixed message →
    /// deterministic RFC-6979 signature (offline vector only).
    #[test]
    fn eoa_signer_golden_eip191_vector() {
        // Canonical anvil key #0 (published deterministic dev key pair).
        let key = SecretString::from(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        );
        let signer = EoaPredictSigner::from_key_hex(&key).unwrap();
        assert_eq!(
            signer.address().to_checksum(None),
            "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
        );
        let sig = signer.sign_message("predict-fixture-message").unwrap();
        assert!(sig.starts_with("0x") && sig.len() == 132, "65-byte hex: {sig}");
        // Deterministic (RFC-6979): same inputs → same bytes.
        assert_eq!(sig, signer.sign_message("predict-fixture-message").unwrap());
    }

    /// Invalid private keys fail loudly — never an empty signer.
    #[test]
    fn eoa_signer_rejects_bad_key() {
        assert!(EoaPredictSigner::from_key_hex(&SecretString::from("not-a-key")).is_err());
    }
}
