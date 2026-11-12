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

//! Polygon reads for Deposit Wallet identity and V2 position operations.

use std::{collections::HashMap, fmt::Debug};

use alloy::{sol, sol_types::SolCall};
use alloy_primitives::{Address, Bytes, U256};
use nautilus_core::string::secret::REDACTED;
use nautilus_network::{
    http::{HttpClient, HttpRedirectPolicy, Method, create_standard_nautilus_headers},
    websocket::proxy::ProxyUrl,
};
use serde::Deserialize;
use serde_json::json;

use crate::{
    http::error::{Error, Result},
    signing::eip712::{
        DEPOSIT_WALLET_FACTORY, POLYMARKET_COLLATERAL_TOKEN, POSITION_MANAGER, ROUTER,
    },
};

const DEFAULT_RPC_URL: &str = "https://polygon.drpc.org";

sol! {
    function id() external view returns (bytes32);
    function owner() external view returns (address);
    function nonce() external view returns (uint256);
    function predictWalletAddress(bytes32 id) external view returns (address);
    function predictLegacyWalletAddress(bytes32 id) external view returns (address);
    function balanceOf(address account) external view returns (uint256);
    function allowance(address owner, address spender) external view returns (uint256);
    function isApprovedForAll(address owner, address operator) external view returns (bool);
    interface PositionLedger {
        function balanceOf(address owner, uint256 id) external view returns (uint256);
    }
    function getPayout(uint256 positionId, uint256 amount) external view returns (uint256);
}

pub(super) struct WalletVerifier {
    client: HttpClient,
    rpc_url: String,
}

impl WalletVerifier {
    pub(super) fn new(timeout_secs: u64, proxy_url: Option<ProxyUrl>) -> Result<Self> {
        let mut headers: HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        headers.insert("Content-Type".into(), "application/json".into());

        let client = HttpClient::builder()
            .headers(headers)
            .redirect_policy(HttpRedirectPolicy::Reject)
            .redact_url(true)
            .timeout_secs(timeout_secs)
            .maybe_proxy_url(proxy_url.map(|url| url.expose().to_string()))
            .build()
            .map_err(Error::from_http_client)?;
        Ok(Self {
            client,
            rpc_url: DEFAULT_RPC_URL.into(),
        })
    }

    pub(super) fn set_rpc_url(&mut self, rpc_url: String) {
        self.rpc_url = rpc_url;
    }

    pub(super) async fn verify(&self, signer: Address, wallet: Address) -> Result<U256> {
        let code = self
            .rpc("eth_getCode", json!([format!("{wallet:#x}"), "latest"]))
            .await?;

        if code.is_empty() {
            return Err(Error::bad_request("Deposit Wallet is not deployed"));
        }

        let identity = self
            .call(wallet, idCall {}.abi_encode())
            .await
            .map_err(|e| match e {
                Error::Decode(message) => Error::decode(format!(
                    "Deposit Wallet identity call failed; check wallet type and RPC: {message}"
                )),
                error => error,
            })?;

        let id = idCall::abi_decode_returns_validate(&identity).map_err(|_| {
            Error::bad_request(
                "Funder lacks Deposit Wallet identity; Safe and Proxy wallets are unsupported",
            )
        })?;

        let predicted = self
            .predict(predictWalletAddressCall { id }.abi_encode())
            .await?;

        if predicted != wallet {
            let legacy = self
                .predict(predictLegacyWalletAddressCall { id }.abi_encode())
                .await?;

            if legacy != wallet {
                return Err(Error::bad_request(
                    "Funder is not a canonical Deposit Wallet for this signer; Safe, Proxy, and EOA wallets are unsupported",
                ));
            }
        }

        let ownership = self.call(wallet, ownerCall {}.abi_encode()).await?;
        let owner = ownerCall::abi_decode_returns_validate(&ownership)
            .map_err(|e| Error::decode(format!("Invalid Deposit Wallet owner: {e}")))?;
        if owner != signer {
            return Err(Error::bad_request("Deposit Wallet signer is not its owner"));
        }

        let nonce = self.call(wallet, nonceCall {}.abi_encode()).await?;
        nonceCall::abi_decode_returns_validate(&nonce)
            .map_err(|e| Error::decode(format!("Invalid Deposit Wallet nonce: {e}")))
    }

    /// Fetches the raw transaction receipt for `tx_hash` (read-only; §8.4
    /// real-asset-change evidence). `Ok(None)` when the transaction is not
    /// yet mined.
    pub(super) async fn transaction_receipt(
        &self,
        tx_hash: &str,
    ) -> Result<Option<Vec<RawReceiptLog>>> {
        let receipt = self
            .rpc_json("eth_getTransactionReceipt", json!([tx_hash]))
            .await?;
        if receipt.is_null() {
            return Ok(None);
        }
        let logs = receipt
            .get("logs")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| Error::decode("receipt omitted its logs"))?;
        let mut out = Vec::with_capacity(logs.len());
        for log in logs {
            let address = log
                .get("address")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| value.parse::<Address>().ok())
                .ok_or_else(|| Error::decode("receipt log omitted a valid address"))?;
            let topics = log
                .get("topics")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| Error::decode("receipt log omitted topics"))?
                .iter()
                .map(|topic| {
                    topic
                        .as_str()
                        .and_then(|value| {
                            alloy_primitives::hex::decode(
                                value.strip_prefix("0x").unwrap_or(value),
                            )
                            .ok()
                        })
                        .filter(|bytes| bytes.len() == 32)
                        .map(|bytes| {
                            let mut topic = [0u8; 32];
                            topic.copy_from_slice(&bytes);
                            topic
                        })
                        .ok_or_else(|| Error::decode("receipt log topic malformed"))
                })
                .collect::<Result<Vec<_>>>()?;
            let data = log
                .get("data")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| {
                    alloy_primitives::hex::decode(value.strip_prefix("0x").unwrap_or(value)).ok()
                })
                .ok_or_else(|| Error::decode("receipt log data malformed"))?;
            out.push(RawReceiptLog {
                address,
                topics,
                data,
            });
        }
        Ok(Some(out))
    }

    /// Reads the Deposit Wallet's current on-chain batch nonce (read-only —
    /// recovery evidence for reconciling prepared submissions; §8.4).
    pub(super) async fn nonce_of(&self, wallet: Address) -> Result<U256> {
        let nonce = self.call(wallet, nonceCall {}.abi_encode()).await?;
        nonceCall::abi_decode_returns_validate(&nonce)
            .map_err(|e| Error::decode(format!("Invalid Deposit Wallet nonce: {e}")))
    }

    pub(super) async fn verify_v2_split(&self, owner: Address, amount: U256) -> Result<()> {
        let balance = self
            .read(
                POLYMARKET_COLLATERAL_TOKEN,
                balanceOfCall { account: owner },
            )
            .await?;

        if balance < amount {
            return Err(Error::bad_request(
                "Deposit Wallet has insufficient pUSD for split",
            ));
        }

        let allowance = self
            .read(
                POLYMARKET_COLLATERAL_TOKEN,
                allowanceCall {
                    owner,
                    spender: ROUTER,
                },
            )
            .await?;

        if allowance < amount {
            return Err(Error::bad_request(
                "Deposit Wallet pUSD allowance for Router is insufficient",
            ));
        }

        Ok(())
    }

    pub(super) async fn verify_v2_positions(
        &self,
        owner: Address,
        position_ids: &[U256],
        amount: U256,
    ) -> Result<()> {
        let approved = self
            .read(
                POSITION_MANAGER,
                isApprovedForAllCall {
                    owner,
                    operator: ROUTER,
                },
            )
            .await?;

        if !approved {
            return Err(Error::bad_request(
                "Deposit Wallet must approve Router as a PositionManager operator",
            ));
        }

        for &id in position_ids {
            let balance = self
                .read(
                    POSITION_MANAGER,
                    PositionLedger::balanceOfCall { owner, id },
                )
                .await?;

            if balance < amount {
                return Err(Error::bad_request(format!(
                    "Deposit Wallet has insufficient V2 position balance for {id}"
                )));
            }
        }

        Ok(())
    }

    pub(super) async fn payout(&self, position_id: U256, amount: U256) -> Result<U256> {
        self.read(
            POSITION_MANAGER,
            getPayoutCall {
                positionId: position_id,
                amount,
            },
        )
        .await
    }

    async fn read<C: SolCall>(&self, target: Address, call: C) -> Result<C::Return> {
        let response = self.call(target, call.abi_encode()).await?;
        C::abi_decode_returns_validate(&response)
            .map_err(|_| Error::decode(format!("Invalid Polygon RPC result for {}", C::SIGNATURE)))
    }

    async fn predict(&self, data: Vec<u8>) -> Result<Address> {
        let response = self.call(DEPOSIT_WALLET_FACTORY, data).await?;
        predictWalletAddressCall::abi_decode_returns_validate(&response)
            .map_err(|e| Error::decode(format!("Invalid Deposit Wallet prediction: {e}")))
    }

    async fn call(&self, target: Address, data: Vec<u8>) -> Result<Bytes> {
        self.rpc("eth_call", json!([
            {"to": format!("{target:#x}"), "data": format!("0x{}", alloy_primitives::hex::encode(data))},
            "latest"
        ])).await
    }

    /// Raw JSON-RPC returning a JSON value (receipt reads).
    async fn rpc_json(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let body = serde_json::to_vec(
            &json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}),
        )?;
        let response = self
            .client
            .request(
                Method::POST,
                self.rpc_url.clone(),
                None,
                None,
                Some(body),
                None,
                None,
            )
            .await
            .map_err(Error::from_http_client)?;
        if !response.status.is_success() {
            return Err(Error::from_status_code(
                response.status.as_u16(),
                b"Polygon RPC request failed",
            ));
        }
        let response: RpcJsonResponse = serde_json::from_slice(&response.body)
            .map_err(|_| Error::decode("Polygon RPC returned an invalid response"))?;
        if let Some(error) = response.error {
            let message = match error.get("code").and_then(serde_json::Value::as_i64) {
                Some(code) => format!("Polygon RPC {method} failed (code {code})"),
                None => format!("Polygon RPC {method} failed"),
            };
            return Err(Error::decode(message));
        }
        Ok(response.result.unwrap_or(serde_json::Value::Null))
    }

    async fn rpc(&self, method: &str, params: serde_json::Value) -> Result<Bytes> {
        let body = serde_json::to_vec(
            &json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}),
        )?;
        let response = self
            .client
            .request(
                Method::POST,
                self.rpc_url.clone(),
                None,
                None,
                Some(body),
                None,
                None,
            )
            .await
            .map_err(Error::from_http_client)?;
        if !response.status.is_success() {
            return Err(Error::from_status_code(
                response.status.as_u16(),
                b"Polygon RPC request failed",
            ));
        }
        let response: RpcResponse = serde_json::from_slice(&response.body)
            .map_err(|_| Error::decode("Polygon RPC returned an invalid response"))?;
        if response.jsonrpc != "2.0" || response.id != 1 {
            return Err(Error::decode(
                "Polygon RPC did not return a successful matching response",
            ));
        }

        if let Some(error) = response.error {
            let message = match error.get("code").and_then(serde_json::Value::as_i64) {
                Some(code) => format!("Polygon RPC {method} failed (code {code})"),
                None => format!("Polygon RPC {method} failed"),
            };

            return Err(Error::decode(message));
        }

        response
            .result
            .ok_or_else(|| Error::decode("Polygon RPC omitted its result"))
    }
}

impl Debug for WalletVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(WalletVerifier))
            .field("rpc_url", &REDACTED)
            .finish_non_exhaustive()
    }
}

/// A decoded receipt log (owned form for transport across awaits).
pub(super) struct RawReceiptLog {
    pub address: Address,
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
}

#[derive(Deserialize)]
struct RpcJsonResponse {
    result: Option<serde_json::Value>,
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct RpcResponse {
    jsonrpc: String,
    id: u64,
    result: Option<Bytes>,
    error: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{
        common::credential::{EvmPrivateKey, RelayerApiKey},
        http::relayer::PolymarketRelayerHttpClient,
        signing::eip712::OrderSigner,
    };

    #[rstest]
    fn test_rpc_url_is_redacted_from_debug() {
        let mut verifier = WalletVerifier::new(2, None).unwrap();
        verifier.set_rpc_url("https://rpc.example/dummy-path-secret?key=dummy-query-secret".into());
        assert_eq!(
            format!("{verifier:?}"),
            "WalletVerifier { rpc_url: \"<redacted>\", .. }"
        );
    }

    #[tokio::test]
    #[ignore = "requires Polymarket credentials and network access to Polygon and the Relayer"]
    async fn test_live_deposit_wallet_security_reads() {
        let private_key = EvmPrivateKey::new(&std::env::var("POLYMARKET_PK").unwrap()).unwrap();
        let signer = OrderSigner::new(&private_key).unwrap();
        let wallet = std::env::var("POLYMARKET_FUNDER").unwrap().parse().unwrap();
        WalletVerifier::new(20, None)
            .unwrap()
            .verify(signer.address(), wallet)
            .await
            .unwrap();
        PolymarketRelayerHttpClient::new(RelayerApiKey::from_env().unwrap(), None, 20)
            .unwrap()
            .get_wallet_nonce(signer.address())
            .await
            .unwrap();
    }
}
