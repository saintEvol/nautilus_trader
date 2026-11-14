//! Predict EOA 直发交易面（position ops 发送层——Manage positions 合同：
//! 无 relayer，钱包直发 + 自付 BNB gas）。
//!
//! 职责：calldata（position_ops 编码产物）→ EIP-155 签名交易 →
//! `eth_sendRawTransaction` → tx hash。nonce/gas 由 RPC
//! （`eth_getTransactionCount`/`eth_estimateGas`）逐次取——不缓存
//! （低频操作，正确性优先）。
//!
//! prepare/submit/reconcile 分离归 octopus 门户（position_operations
//! 表）；本层只是**发送原语**：同 description 多次调用发多笔交易——
//! 幂等控制不在本层。

use alloy::signers::local::PrivateKeySigner;
use alloy_primitives::{Address, B256, U256, hex};
use nautilus_network::http::{HttpClient, HttpClientError, Method};

/// 直发交易签名器 + RPC 提交器。
pub struct EoaTxSender {
    signer: PrivateKeySigner,
    client: HttpClient,
    rpc_url: String,
    chain_id: u64,
}

/// 一笔已签名的待发交易（raw bytes）。
pub struct SignedTx {
    pub raw: Vec<u8>,
    pub tx_hash: B256,
}

#[derive(Clone, Debug)]
struct EthTxEnvelope {
    nonce: U256,
    gas_price: U256,
    gas_limit: U256,
    to: Address,
    value: U256,
    data: Vec<u8>,
    chain_id: u64,
}

impl EoaTxSender {
    /// # Errors
    /// 私钥非法 / HTTP client 构造失败。
    pub fn new(
        key_hex: &str,
        rpc_url: &str,
    chain_id: u64,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, String> {
        let signer = <PrivateKeySigner as std::str::FromStr>::from_str(key_hex)
            .map_err(|e| format!("invalid wallet key: {e}"))?;
        Ok(Self {
            signer,
            client: HttpClient::builder()
                .timeout_secs(timeout_secs)
                .maybe_proxy_url(proxy_url)
                .build()
                .map_err(|e| format!("rpc client: {e}"))?,
            rpc_url: rpc_url.trim_end_matches('/').to_owned(),
            chain_id,
        })
    }

    #[must_use]
    pub fn address(&self) -> Address {
        self.signer.address()
    }

    async fn rpc(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value, String> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params
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
            return Err(format!("RPC {method}: HTTP {}", response.status.as_u16()));
        }
        let value: serde_json::Value =
            serde_json::from_slice(&response.body).map_err(|e| e.to_string())?;
        if let Some(err) = value.get("error") {
            return Err(format!("RPC {method}: {err}"));
        }
        Ok(value)
    }

    async fn next_nonce(&self) -> Result<U256, String> {
        let v = self
            .rpc(
                "eth_getTransactionCount",
                serde_json::json!([format!("{:#x}", self.signer.address()), "pending"]),
            )
            .await?;
        v.get("result")
            .and_then(|r| r.as_str())
            .and_then(|s| U256::from_str_radix(s.trim_start_matches("0x"), 16).ok())
            .ok_or_else(|| format!("eth_getTransactionCount malformed: {v}"))
    }

    async fn estimate_gas(&self, to: Address, data: &[u8]) -> Result<U256, String> {
        let v = self
            .rpc(
                "eth_estimateGas",
                serde_json::json!([{
                    "from": format!("{:#x}", self.signer.address()),
                    "to": format!("{to:#x}"),
                    "data": format!("0x{}", hex::encode(data)),
                }]),
            )
            .await?;
        v.get("result")
            .and_then(|r| r.as_str())
            .and_then(|s| U256::from_str_radix(s.trim_start_matches("0x"), 16).ok())
            .ok_or_else(|| format!("eth_estimateGas malformed: {v}"))
    }

    async fn gas_price(&self) -> Result<U256, String> {
        let v = self.rpc("eth_gasPrice", serde_json::json!([])).await?;
        v.get("result")
            .and_then(|r| r.as_str())
            .and_then(|s| U256::from_str_radix(s.trim_start_matches("0x"), 16).ok())
            .ok_or_else(|| format!("eth_gasPrice malformed: {v}"))
    }

    /// 构造 + 签名一笔 legacy EIP-155 交易（calldata 直发——position
    /// ops 的 ERC-1155 调用不预付 value）。
    ///
    /// # Errors
    /// RPC 读失败（nonce/gas）或签名失败。
    pub async fn sign(
        &self,
        to: Address,
        data: Vec<u8>,
    ) -> Result<SignedTx, String> {
        let nonce = self.next_nonce().await?;
        let gas_price = self.gas_price().await?;
        let estimated = self.estimate_gas(to, &data).await?;
        // 估计值 + 30% 余量（1155 合约的 partition 数组路径有分支开销）。
        let gas_limit = estimated.saturating_mul(U256::from(13)) / U256::from(10);
        let tx = EthTxEnvelope {
            nonce,
            gas_price,
            gas_limit,
            to,
            value: U256::ZERO,
            data,
            chain_id: self.chain_id,
        };
        let (raw, tx_hash) = sign_legacy(&self.signer, &tx)?;
        Ok(SignedTx { raw, tx_hash })
    }

    /// 提交已签交易（`eth_sendRawTransaction`）——返回 tx hash（与
    /// SignedTx.tx_hash 一致；提交失败时交易**可能已在网络中**——
    /// 调用方按 hash 对账，不得盲重发）。
    ///
    /// # Errors
    /// RPC 提交失败（mempool 拒绝等——结局未知归对账域）。
    pub async fn send_raw(&self, raw: &[u8]) -> Result<B256, String> {
        let v = self
            .rpc(
                "eth_sendRawTransaction",
                serde_json::json!([format!("0x{}", hex::encode(raw))]),
            )
            .await?;
        v.get("result")
            .and_then(|r| r.as_str())
            .and_then(|s| alloy_primitives::hex::decode(s.trim_start_matches("0x")).ok())
            .and_then(|bytes| B256::try_from(bytes.as_slice()).ok())
            .ok_or_else(|| format!("eth_sendRawTransaction malformed: {v}"))
    }

    /// 回执查询（reconcile 面：None = 未上链/未确认）。
    ///
    /// # Errors
    /// RPC 失败。
    pub async fn receipt(&self, tx_hash: B256) -> Result<Option<serde_json::Value>, String> {
        let v = self
            .rpc("eth_getTransactionReceipt", serde_json::json!([format!("{tx_hash:#x}")]))
            .await?;
        Ok(v.get("result").cloned().filter(|r| !r.is_null()))
    }
}

/// legacy (type-0) EIP-155 签名：RLP(nonce, gasPrice, gasLimit, to,
/// value, data, chainId, 0, 0) → keccak → secp256k1 → raw + hash。
/// 手写 RLP（依赖面最小——alloy 的 builder 面面向 provider 全家桶）。
fn sign_legacy(
    signer: &PrivateKeySigner,
    tx: &EthTxEnvelope,
) -> Result<(Vec<u8>, B256), String> {
    use alloy::signers::SignerSync;

    fn rlp_uint(out: &mut Vec<u8>, v: U256) {
        let bytes = v.to_be_bytes::<32>();
        let first_nonzero = bytes.iter().position(|b| *b != 0).unwrap_or(31);
        let trimmed = &bytes[first_nonzero..];
        if trimmed.is_empty() || trimmed[0] == 0 {
            out.push(0x80);
        } else if trimmed.len() == 1 && trimmed[0] < 0x80 {
            out.push(trimmed[0]);
        } else {
            out.push(0x80 + trimmed.len() as u8);
            out.extend_from_slice(trimmed);
        }
    }

    fn rlp_bytes(out: &mut Vec<u8>, data: &[u8]) {
        if data.is_empty() {
            out.push(0x80);
        } else if data.len() == 1 && data[0] < 0x80 {
            out.push(data[0]);
        } else {
            out.push(0x80 + data.len() as u8);
            out.extend_from_slice(data);
        }
    }

    fn rlp_list(out: &mut Vec<u8>, payload: &[u8]) {
        if payload.len() <= 55 {
            out.push(0xc0 + payload.len() as u8);
        } else {
            let len_bytes = payload.len().to_be_bytes();
            let first = len_bytes.iter().position(|b| *b != 0).unwrap_or(7);
            let trimmed = &len_bytes[first..];
            out.push(0xf7 + trimmed.len() as u8);
            out.extend_from_slice(trimmed);
        }
        out.extend_from_slice(payload);
    }

    // EIP-155 结构：签名交易 = [nonce, gasPrice, gasLimit, to, value, data,
    // v, r, s]；未签名摘要 payload = 同前六字段 + [chainId, 0, 0]（占位 r/s）。
    // 签名后 (v, r, s) **替换**占位三词——不是追加（golden 对照锚死）。
    let mut base = Vec::with_capacity(128 + tx.data.len());
    rlp_uint(&mut base, tx.nonce);
    rlp_uint(&mut base, tx.gas_price);
    rlp_uint(&mut base, tx.gas_limit);
    if tx.to == Address::ZERO {
        base.push(0x80);
    } else {
        base.push(0x80 + 20);
        base.extend_from_slice(tx.to.as_slice());
    }
    rlp_uint(&mut base, tx.value);
    rlp_bytes(&mut base, &tx.data);

    let mut unsigned_payload = base.clone();
    rlp_uint(&mut unsigned_payload, U256::from(tx.chain_id));
    rlp_uint(&mut unsigned_payload, U256::ZERO);
    rlp_uint(&mut unsigned_payload, U256::ZERO);
    let mut unsigned = Vec::with_capacity(unsigned_payload.len() + 4);
    rlp_list(&mut unsigned, &unsigned_payload);
    let digest = alloy_primitives::keccak256(&unsigned);
    let signature = signer
        .sign_hash_sync(&digest)
        .map_err(|e| format!("tx signing: {e}"))?;
    let sig_bytes = signature.as_bytes();
    // EIP-155 v = chainId*2 + 35 + parity（u64 全程，不做 u8 收窄——chainId
    // ≥111 时 147+ 会被截断产错签）。经 rlp_uint 编码：v ≥ 0x80 时 RLP 要求
    // 0x81 前缀两字节形态（BSC chain 56 → v=147/148 恒在此区间）。
    let v = tx.chain_id * 2 + 35 + u64::from(signature.v());

    // signed payload: 前六字段 + v(chainId*2+35+parity) + r + s（替换占位）。
    let mut signed_payload = base;
    rlp_uint(&mut signed_payload, U256::from(v));
    let r_bytes: [u8; 32] = sig_bytes[..32].try_into().expect("r");
    let s_bytes: [u8; 32] = sig_bytes[32..64].try_into().expect("s");
    let r = U256::from_be_bytes(r_bytes);
    let s = U256::from_be_bytes(s_bytes);
    rlp_uint(&mut signed_payload, r);
    rlp_uint(&mut signed_payload, s);
    let mut raw = Vec::with_capacity(signed_payload.len() + 4);
    rlp_list(&mut raw, &signed_payload);
    let tx_hash = alloy_primitives::keccak256(&raw);
    Ok((raw, tx_hash))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_signer() -> PrivateKeySigner {
        <PrivateKeySigner as std::str::FromStr>::from_str(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        .unwrap()
    }

    /// EIP-155 golden：手写 RLP 与 alloy_consensus 官方 TxLegacy 签名编码
    /// **逐字节对照**（同一签名者 + RFC-6979 确定性签名 → 两编码器产物必须
    /// 全等）。v ≥ 0x80 的两字节 RLP 形态（BSC chain 56 → v=147/148）在此锚
    /// 死——单字节 push(v) 的错编在此红。
    #[test]
    fn legacy_signing_matches_alloy_consensus_encoding() {
        use alloy::signers::SignerSync;

        let signer = test_signer();
        let tx = EthTxEnvelope {
            nonce: U256::from(7),
            gas_price: U256::from(5_000_000_000u64),
            gas_limit: U256::from(200_000),
            to: "0x22DA1810B194ca018378464a58f6Ac2B10C9d244"
                .parse()
                .unwrap(),
            value: U256::ZERO,
            data: vec![0xde, 0xad, 0xbe, 0xef],
            chain_id: 56,
        };
        let (raw, hash) = sign_legacy(&signer, &tx).unwrap();

        let legacy = alloy::consensus::TxLegacy {
            chain_id: Some(56),
            nonce: 7,
            gas_price: 5_000_000_000,
            gas_limit: 200_000,
            to: alloy_primitives::TxKind::Call(
                "0x22DA1810B194ca018378464a58f6Ac2B10C9d244"
                    .parse()
                    .unwrap(),
            ),
            value: U256::ZERO,
            input: vec![0xde, 0xad, 0xbe, 0xef].into(),
        };
        let digest = {
            use alloy::consensus::SignableTransaction as _;
            legacy.signature_hash()
        };
        let sig = signer.sign_hash_sync(&digest).unwrap();
        let signed = {
            use alloy::consensus::SignableTransaction as _;
            legacy.into_signed(sig)
        };
        // wire 形态是 envelope（Signed 本身不实现 Encodable）。
        let envelope = alloy::consensus::TxEnvelope::Legacy(signed);
        let mut official = Vec::new();
        alloy::rlp::Encodable::encode(&envelope, &mut official);

        assert_eq!(raw, official, "raw 与 alloy_consensus 编码逐字节一致");
        assert_eq!(hash.as_slice(), &alloy_primitives::keccak256(&official)[..]);
        // 确定性（RFC-6979）：同输入同输出。
        let (raw2, hash2) = sign_legacy(&signer, &tx).unwrap();
        assert_eq!(raw, raw2);
        assert_eq!(hash, hash2);
    }

    /// 大 chain_id 不截断：v = chainId*2+35+parity 以 U256 编码——chain
    /// 31337（anvil）v=67710/67711 ≥ 0x80 两字节形态；极端 chain_id（如
    /// 133713371337）也不溢出（B2 锚）。
    #[test]
    fn large_chain_id_v_is_not_truncated() {
        use alloy::signers::SignerSync;

        let signer = test_signer();
        for chain_id in [31_337u64, 1_337_133_713_337] {
            let tx = EthTxEnvelope {
                nonce: U256::ZERO,
                gas_price: U256::from(1_000_000_000u64),
                gas_limit: U256::from(21_000),
                to: "0x22DA1810B194ca018378464a58f6Ac2B10C9d244"
                    .parse()
                    .unwrap(),
                value: U256::ZERO,
                data: Vec::new(),
                chain_id,
            };
            let (raw, _hash) = sign_legacy(&signer, &tx).unwrap();
            let legacy = alloy::consensus::TxLegacy {
                chain_id: Some(chain_id),
                nonce: 0,
                gas_price: 1_000_000_000,
                gas_limit: 21_000,
                to: alloy_primitives::TxKind::Call(
                    "0x22DA1810B194ca018378464a58f6Ac2B10C9d244"
                        .parse()
                        .unwrap(),
                ),
                value: U256::ZERO,
                input: Vec::<u8>::new().into(),
            };
            let sig = {
                use alloy::consensus::SignableTransaction as _;
                signer.sign_hash_sync(&legacy.signature_hash()).unwrap()
            };
            let mut official = Vec::new();
            let envelope = {
                use alloy::consensus::SignableTransaction as _;
                alloy::consensus::TxEnvelope::Legacy(legacy.into_signed(sig))
            };
            alloy::rlp::Encodable::encode(&envelope, &mut official);
            assert_eq!(raw, official, "chain {chain_id} 编码与官方一致（v 不截断）");
        }
    }
}
