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
    struct RecoveryCall { address target; uint256 value; bytes data; }
    struct RecoveryBatch { address wallet; uint256 nonce; uint256 deadline; RecoveryCall[] calls; }
    function proxy(RecoveryBatch[] batches, bytes[] signatures);
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

fn verifies_prepared_batch(input: &[u8], operation: &super::PreparedWalletOperation) -> bool {
    use crate::signing::eip712::{DepositWalletCall, deposit_wallet_batch_hash};
    let Ok(decoded) = proxyCall::abi_decode_validate(input) else {
        return false;
    };
    decoded
        .batches
        .iter()
        .zip(&decoded.signatures)
        .any(|(batch, signature)| {
            if batch.wallet != operation.deposit_wallet
                || batch.nonce != operation.nonce
                || batch.deadline != operation.deadline
                || batch.calls.len() != operation.calls.len()
            {
                return false;
            }
            if !batch
                .calls
                .iter()
                .zip(&operation.calls)
                .all(|(observed, saved)| {
                    observed.target == saved.target
                        && observed.value == saved.value
                        && alloy_primitives::hex::decode(saved.data.trim_start_matches("0x"))
                            .is_ok_and(|data| data.as_slice() == observed.data.as_ref())
                })
            {
                return false;
            }
            let calls: Vec<_> = batch
                .calls
                .iter()
                .map(|call| DepositWalletCall {
                    target: call.target,
                    value: call.value,
                    data: call.data.clone(),
                })
                .collect();
            let digest =
                deposit_wallet_batch_hash(batch.wallet, batch.nonce, batch.deadline, &calls);
            alloy_primitives::Signature::try_from(signature.as_ref())
                .and_then(|signature| signature.recover_address_from_prehash(&digest))
                .is_ok_and(|signer| signer == operation.signer)
        })
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

    pub(super) async fn wallet_receipt_value(&self, hash: &str) -> Result<serde_json::Value> {
        self.rpc_json("eth_getTransactionReceipt", json!([hash]))
            .await
    }

    pub(super) async fn call_at(
        &self,
        token: Address,
        data: Vec<u8>,
        block: &str,
    ) -> Result<Vec<u8>> {
        Ok(self.rpc("eth_call", json!([
            {"to": format!("{token:#x}"), "data": format!("0x{}", alloy_primitives::hex::encode(data))},
            block,
        ])).await?.to_vec())
    }

    pub(super) async fn position_balance(
        &self,
        token: Address,
        owner: Address,
        id: U256,
    ) -> Result<U256> {
        self.read(token, PositionLedger::balanceOfCall { owner, id })
            .await
    }

    /// Reads the Deposit Wallet's current on-chain batch nonce (read-only —
    /// recovery evidence for reconciling prepared submissions; §8.4).
    pub(super) async fn nonce_of(&self, wallet: Address) -> Result<U256> {
        let nonce = self.call(wallet, nonceCall {}.abi_encode()).await?;
        nonceCall::abi_decode_returns_validate(&nonce)
            .map_err(|e| Error::decode(format!("Invalid Deposit Wallet nonce: {e}")))
    }

    /// Finds the one finalized block where this wallet consumed the nonce.
    /// RPCs without historical state cannot prove recovery.
    async fn nonce_at_block(&self, wallet: Address, block: u64) -> Result<U256> {
        let result = self.rpc("eth_call", json!([
            {"to": format!("{wallet:#x}"), "data": format!("0x{}", alloy_primitives::hex::encode(nonceCall {}.abi_encode()))},
            format!("{block:#x}"),
        ])).await?;
        if result.is_empty() {
            return Ok(U256::ZERO);
        }
        nonceCall::abi_decode_returns_validate(&result)
            .map_err(|error| Error::decode(format!("historical wallet nonce: {error}")))
    }

    pub(super) async fn recover_batch_hash(
        &self,
        operation: &super::PreparedWalletOperation,
    ) -> Result<Option<String>> {
        let head = self
            .rpc_json("eth_getBlockByNumber", json!(["finalized", false]))
            .await?;
        let mut upper = head["number"]
            .as_str()
            .and_then(|value| u64::from_str_radix(value.trim_start_matches("0x"), 16).ok())
            .ok_or_else(|| Error::decode("no finalized Polygon block available"))?;
        let wallet = operation.deposit_wallet;
        if self.nonce_at_block(wallet, upper).await? <= operation.nonce {
            return Ok(None);
        }
        let mut width = 128;
        let mut lower = upper.saturating_sub(width);
        while lower != 0 && self.nonce_at_block(wallet, lower).await? > operation.nonce {
            upper = lower;
            width = width.saturating_mul(2);
            lower = upper.saturating_sub(width);
        }
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.nonce_at_block(wallet, middle).await? > operation.nonce {
                upper = middle;
            } else {
                lower = middle + 1;
            }
        }
        let topic = alloy_primitives::keccak256("BatchExecuted(uint256)");
        let nonce_topic = alloy_primitives::B256::from(operation.nonce.to_be_bytes::<32>());
        let block = format!("{upper:#x}");
        let logs = self
            .rpc_json(
                "eth_getLogs",
                json!([{
                    "address": format!("{wallet:#x}"),
                    "fromBlock": block,
                    "toBlock": block,
                    "topics": [topic, nonce_topic],
                }]),
            )
            .await?;
        let Some(logs) = logs.as_array() else {
            return Ok(None);
        };
        for log in logs {
            if log["removed"].as_bool() == Some(true)
                || log["address"].as_str().and_then(|value| value.parse().ok()) != Some(wallet)
                || log["blockNumber"].as_str() != Some(block.as_str())
                || log["topics"][0]
                    .as_str()
                    .and_then(|value| value.parse().ok())
                    != Some(topic)
                || log["topics"][1]
                    .as_str()
                    .and_then(|value| value.parse().ok())
                    != Some(nonce_topic)
            {
                continue;
            }
            let Some(hash) = log["transactionHash"]
                .as_str()
                .filter(|value| value.parse::<alloy_primitives::B256>().is_ok())
            else {
                continue;
            };
            let transaction = self
                .rpc_json("eth_getTransactionByHash", json!([hash]))
                .await?;
            if transaction["blockNumber"].as_str() != Some(block.as_str()) {
                continue;
            }
            if transaction["to"]
                .as_str()
                .and_then(|value| value.parse::<Address>().ok())
                != Some(DEPOSIT_WALLET_FACTORY)
            {
                continue;
            }
            let Some(input) = transaction["input"].as_str().and_then(|value| {
                alloy_primitives::hex::decode(value.trim_start_matches("0x")).ok()
            }) else {
                continue;
            };
            if verifies_prepared_batch(&input, operation) {
                return Ok(Some(hash.to_owned()));
            }
        }
        Ok(None)
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

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::{
        common::credential::EvmPrivateKey,
        positions::{PreparedCall, PreparedWalletOperation},
        signing::eip712::{DepositWalletCall, OrderSigner},
    };

    #[test]
    fn missing_relayer_id_requires_same_signed_wallet_batch() {
        let fixture_key = EvmPrivateKey::new(&format!("{:064x}", 1)).unwrap();
        let signer = OrderSigner::new(&fixture_key).unwrap();
        let wallet = Address::repeat_byte(0x01);
        let call = DepositWalletCall {
            target: Address::repeat_byte(0x02),
            value: U256::ZERO,
            data: Bytes::from(vec![0xa0, 0xb1]),
        };
        let nonce = U256::from(42);
        let deadline = U256::from(12345);
        let signature = signer
            .sign_deposit_wallet_batch(wallet, nonce, deadline, &[call.clone()])
            .unwrap();
        let input = proxyCall {
            batches: vec![RecoveryBatch {
                wallet,
                nonce,
                deadline,
                calls: vec![RecoveryCall {
                    target: call.target,
                    value: call.value,
                    data: call.data.clone(),
                }],
            }],
            signatures: vec![
                alloy_primitives::hex::decode(signature.trim_start_matches("0x"))
                    .unwrap()
                    .into(),
            ],
        }
        .abi_encode();
        let mut operation = PreparedWalletOperation {
            deposit_wallet: wallet,
            signer: signer.address(),
            nonce,
            deadline,
            metadata: "fixture".into(),
            calls: vec![PreparedCall {
                target: call.target,
                value: call.value,
                data: "0xa0b1".into(),
            }],
        };
        assert!(verifies_prepared_batch(&input, &operation));
        operation.signer = Address::repeat_byte(0x03);
        assert!(!verifies_prepared_batch(&input, &operation));
        operation.signer = signer.address();
        operation.calls[0].data = "0xa0b2".into();
        assert!(!verifies_prepared_batch(&input, &operation));
    }
}
