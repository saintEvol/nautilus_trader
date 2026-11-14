//! BNB Mainnet on-chain reads for the Predict execution client (§8.2
//! account-state face): ERC-20 `balanceOf` / `allowance` via raw
//! `eth_call` JSON-RPC over `nautilus_network::HttpClient`（Polymarket
//! wallet verifier 同款形态——不引入 provider 全家桶）。
//!
//! 铸造/赎回等写操作**不在本面**（fork position client 是独立增量）；
//! 本面只读——账户状态的余额真源。

use alloy_primitives::{Address, U256};
use alloy::{sol, sol_types::SolCall};
use nautilus_network::http::{HttpClient, HttpClientError, Method};

use crate::common::consts::USDT_BNB_ADDRESS;

sol! {
    #[derive(Debug)]
    function balanceOf(address account) external view returns (uint256);
    #[derive(Debug)]
    function allowance(address owner, address spender) external view returns (uint256);
}

/// 只读链上账户面（USDT 余额 + 对各 exchange 合约的授权额度）。
pub struct ChainAccountReader {
    client: HttpClient,
    rpc_url: String,
    usdt: Address,
}

/// 一次账户读取的结果（wei 原值——精度换算归调用方）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainBalances {
    pub usdt_balance: U256,
    /// 对neg-risk exchange 的 USDT 授权（0 = 未授权——下单会被拒）。
    pub usdt_allowance_neg_risk: U256,
    /// 对标准 exchange 的 USDT 授权。
    pub usdt_allowance_standard: U256,
}

impl ChainAccountReader {
    /// # Errors
    /// HTTP client 构造失败。
    pub fn new(rpc_url: &str, timeout_secs: u64, proxy_url: Option<String>) -> Result<Self, HttpClientError> {
        Ok(Self {
            client: HttpClient::builder()
                .timeout_secs(timeout_secs)
                .maybe_proxy_url(proxy_url)
                .build()?,
            rpc_url: rpc_url.trim_end_matches('/').to_owned(),
            usdt: USDT_BNB_ADDRESS
                .parse()
                .expect("pinned USDT contract address"),
        })
    }

    async fn eth_call(&self, target: Address, data: Vec<u8>) -> Result<Vec<u8>, String> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [
                {"to": format!("{target:#x}"), "data": format!("0x{}", alloy_primitives::hex::encode(&data))},
                "latest"
            ]
        });
        let response = self
            .client
            .request(
                Method::POST,
                self.rpc_url.clone(),
                None,
                None,
                Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                None,
                None,
            )
            .await
            .map_err(|e: HttpClientError| e.to_string())?;
        if !response.status.is_success() {
            return Err(format!(
                "BNB RPC eth_call failed: HTTP {}",
                response.status.as_u16()
            ));
        }
        let value: serde_json::Value =
            serde_json::from_slice(&response.body).map_err(|e| e.to_string())?;
        value
            .get("result")
            .and_then(|v| v.as_str())
            .and_then(|hex| alloy_primitives::hex::decode(hex.trim_start_matches("0x")).ok())
            .ok_or_else(|| format!("BNB RPC eth_call malformed response: {value}"))
    }

    async fn read<C: SolCall>(&self, target: Address, call: C) -> Result<C::Return, String> {
        let response = self.eth_call(target, call.abi_encode()).await?;
        C::abi_decode_returns_validate(&response)
            .map_err(|e| format!("invalid BNB RPC result for {}: {e}", C::SIGNATURE))
    }

    /// 读取账户的 USDT 余额与对两类 exchange 合约的授权。
    ///
    /// # Errors
    /// 任一 RPC 读失败（不部分披露——半截余额是假账户状态）。
    pub async fn read_balances(
        &self,
        owner: Address,
        neg_risk_exchange: Address,
        standard_exchange: Address,
    ) -> Result<ChainBalances, String> {
        let (usdt_balance, usdt_allowance_neg_risk, usdt_allowance_standard) = tokio::join!(
            self.read(self.usdt, balanceOfCall { account: owner }),
            self.read(
                self.usdt,
                allowanceCall {
                    owner,
                    spender: neg_risk_exchange,
                }
            ),
            self.read(
                self.usdt,
                allowanceCall {
                    owner,
                    spender: standard_exchange,
                }
            ),
        );
        // 单返回值 call 的 Return 类型即裸 U256（alloy sol! 无命名字段
        // 包装——Polymarket wallet.rs 同形态）。
        Ok(ChainBalances {
            usdt_balance: usdt_balance?,
            usdt_allowance_neg_risk: usdt_allowance_neg_risk?,
            usdt_allowance_standard: usdt_allowance_standard?,
        })
    }
}
