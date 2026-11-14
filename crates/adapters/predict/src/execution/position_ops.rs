//! Predict on-chain position operations (§6.2 positions face)：
//! split/merge/redeem/convert 的 ABI calldata 编码 + 路由（四合约矩阵）。
//!
//! 合同（dev.predict.fun/concepts/positions + resources/deployed-contracts，
//! 2026-11-14 逐字核对）：
//! - 二元市场：直呼 ConditionalTokens——
//!   `splitPosition(collateralToken, parentCollectionId, conditionId,
//!    partition, amount)` / `mergePositions(同参)` /
//!   `redeemPositions(collateralToken, parentCollectionId, conditionId,
//!    indexSets)`；collateral=USDT、parentCollectionId=0x0、partition=
//!   [1,2]。
//! - neg-risk 市场：NegRiskAdapter——`splitPosition(conditionId, amount)` /
//!   `mergePositions(conditionId, amount)` /
//!   `redeemPositions(conditionId, [yesAmount, noAmount])` /
//!   `convertPositions(negRiskOnChainId, indexSet, amount)`（仅 neg-risk）。
//! - 四合约矩阵（isNegRisk × isYieldBearing）路由主网地址已钉。
//!
//! 本模块只编码与路由——**发送面**（交易构造/签名/提交/回执对账）复用
//! Polymarket prepare/submit/reconcile 同款边界（另一增量；编码正确性
//! 在此离线锚定）。

use alloy::{primitives::Bytes, sol_types::SolCall};
use alloy_primitives::{Address, U256};
use anyhow::{Result, bail};
use rust_decimal::Decimal;

use crate::execution::amounts::wei_per_unit;

/// 主网合约矩阵（deployed-contracts 逐字钉）。neg-risk 操作**恒走 adapter**
/// （isYieldBearing 选择走哪个 adapter——官方 SDK OrderBuilder 路由）；
/// ConditionalTokens 直呼面只服务二元（非 neg-risk）市场。
pub mod bnb_mainnet {
    use alloy_primitives::{Address, address};

    /// 非生息 ConditionalTokens（二元市场直呼）。
    pub const CONDITIONAL_TOKENS: Address = address!("0x22DA1810B194ca018378464a58f6Ac2B10C9d244");
    /// 生息 ConditionalTokens。
    pub const YIELD_BEARING_CONDITIONAL_TOKENS: Address =
        address!("0x9400F8Ad57e9e0F352345935d6D3175975eb1d9F");
    /// NegRiskAdapter（neg-risk 非生息）。
    pub const NEG_RISK_ADAPTER: Address = address!("0xc3Cf7c252f65E0d8D88537dF96569AE94a7F1A6E");
    /// 生息 YieldBearingNegRiskAdapter（neg-risk 生息）。
    pub const YIELD_BEARING_NEG_RISK_ADAPTER: Address =
        address!("0x41dCe1A4B8FB5e6327701750aF6231B7CD0B2A40");
    /// USDT 抵押。
    pub const USDT: Address = address!("0x55d398326f99059fF775485246999027B3197955");
}

/// ConditionalTokens（二元市场直呼面——四参/五参形态）。
mod ctf {
    alloy::sol! {
        function splitPosition(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] partition,
            uint256 amount
        );
        function mergePositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] partition,
            uint256 amount
        );
        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] indexSets
        );
    }
}

/// NegRiskAdapter 面（短参形态 + convert）。全部 **bytes32**（官方 SDK
/// NegRiskAdapter ABI 逐字：splitPosition(bytes32,uint256) /
/// mergePositions(bytes32,uint256) / redeemPositions(bytes32,uint256[]) /
/// convertPositions(bytes32,uint256,uint256)）。
mod neg_risk {
    alloy::sol! {
        function splitPosition(bytes32 conditionId, uint256 amount);
        function mergePositions(bytes32 conditionId, uint256 amount);
        function redeemPositions(bytes32 conditionId, uint256[] amounts);
        function convertPositions(bytes32 negRiskOnChainId, uint256 indexSet, uint256 amount);
    }
}

/// 市场变体（合约矩阵路由键）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PositionMarketVariant {
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
}

/// 一条待发链上操作（target + calldata——发送面消费）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedPositionOp {
    pub target: Address,
    pub data: Bytes,
}

/// 目标合约路由（四矩阵）：二元市场直呼 ConditionalTokens；neg-risk 市场
/// 恒走 adapter（生息与否选择哪个 adapter）。词表外组合不存在（枚举闭集）。
#[must_use]
pub fn route_target(variant: PositionMarketVariant) -> Address {
    match (variant.is_neg_risk, variant.is_yield_bearing) {
        (false, false) => bnb_mainnet::CONDITIONAL_TOKENS,
        (false, true) => bnb_mainnet::YIELD_BEARING_CONDITIONAL_TOKENS,
        (true, false) => bnb_mainnet::NEG_RISK_ADAPTER,
        (true, true) => bnb_mainnet::YIELD_BEARING_NEG_RISK_ADAPTER,
    }
}

fn condition_bytes32(condition_id: &str) -> Result<[u8; 32]> {
    let stripped = condition_id.strip_prefix("0x").unwrap_or(condition_id);
    let bytes = alloy_primitives::hex::decode(stripped)
        .map_err(|e| anyhow::anyhow!("conditionId {condition_id:?}: {e}"))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("conditionId {condition_id:?} 不是 32 字节"))?;
    Ok(arr)
}

fn units_to_wei(amount: Decimal) -> Result<U256> {
    let wei = amount * wei_per_unit();
    if wei.fract() != Decimal::ZERO {
        bail!("amount {amount} has sub-wei remainder");
    }
    U256::from_str_radix(&wei.trunc().to_string(), 10)
        .map_err(|e| anyhow::anyhow!("wei overflow: {e}"))
}

/// split：USDT → Yes+No 完整套。amount 单位 = USDT（18dp）。
///
/// # Errors
/// conditionId 形状/sub-wei 余量。
pub fn encode_split(
    condition_id: &str,
    amount: Decimal,
    variant: PositionMarketVariant,
) -> Result<EncodedPositionOp> {
    let target = route_target(variant);
    let data = match variant.is_neg_risk {
        false => ctf::splitPositionCall {
            collateralToken: bnb_mainnet::USDT,
            parentCollectionId: [0u8; 32].into(),
            conditionId: condition_bytes32(condition_id)?.into(),
            partition: vec![U256::from(1), U256::from(2)],
            amount: units_to_wei(amount)?,
        }
        .abi_encode(),
        true => neg_risk::splitPositionCall {
            conditionId: condition_bytes32(condition_id)?.into(),
            amount: units_to_wei(amount)?,
        }
        .abi_encode(),
    };
    Ok(EncodedPositionOp { target, data: Bytes::from(data) })
}

/// merge：Yes+No 完整套 → USDT。
///
/// # Errors
/// 同 split。
pub fn encode_merge(
    condition_id: &str,
    amount: Decimal,
    variant: PositionMarketVariant,
) -> Result<EncodedPositionOp> {
    let target = route_target(variant);
    let data = match variant.is_neg_risk {
        false => ctf::mergePositionsCall {
            collateralToken: bnb_mainnet::USDT,
            parentCollectionId: [0u8; 32].into(),
            conditionId: condition_bytes32(condition_id)?.into(),
            partition: vec![U256::from(1), U256::from(2)],
            amount: units_to_wei(amount)?,
        }
        .abi_encode(),
        true => neg_risk::mergePositionsCall {
            conditionId: condition_bytes32(condition_id)?.into(),
            amount: units_to_wei(amount)?,
        }
        .abi_encode(),
    };
    Ok(EncodedPositionOp { target, data: Bytes::from(data) })
}

/// redeem：已裁决市场的获胜份额 → USDT。二元面 indexSets=[1,2]（合约
/// 按裁决结果只付获胜方）；neg-risk 面 amounts=[yes, no] 逐腿数量。
///
/// # Errors
/// 同 split。
pub fn encode_redeem(
    condition_id: &str,
    variant: PositionMarketVariant,
    neg_risk_amounts: Option<(Decimal, Decimal)>,
) -> Result<EncodedPositionOp> {
    let target = route_target(variant);
    let data = match variant.is_neg_risk {
        false => ctf::redeemPositionsCall {
            collateralToken: bnb_mainnet::USDT,
            parentCollectionId: [0u8; 32].into(),
            conditionId: condition_bytes32(condition_id)?.into(),
            indexSets: vec![U256::from(1), U256::from(2)],
        }
        .abi_encode(),
        true => {
            let (yes, no) = neg_risk_amounts
                .ok_or_else(|| anyhow::anyhow!("neg-risk redeem requires (yes, no) amounts"))?;
            neg_risk::redeemPositionsCall {
                conditionId: condition_bytes32(condition_id)?.into(),
                amounts: vec![units_to_wei(yes)?, units_to_wei(no)?],
            }
            .abi_encode()
        }
    };
    Ok(EncodedPositionOp { target, data: Bytes::from(data) })
}

/// convert（仅 neg-risk）：多结果类别内 `negRiskOnChainId`（类别事实，
/// 非市场 conditionId）+ indexSet → 份额互换。
///
/// # Errors
/// 非 neg-risk 拒；形状错误拒。
pub fn encode_convert(
    neg_risk_on_chain_id: &str,
    index_set: u64,
    amount: Decimal,
    variant: PositionMarketVariant,
) -> Result<EncodedPositionOp> {
    anyhow::ensure!(
        variant.is_neg_risk,
        "convertPositions 仅 neg-risk 市场（合同面）"
    );
    let target = route_target(variant);
    let call = neg_risk::convertPositionsCall {
        negRiskOnChainId: condition_bytes32(neg_risk_on_chain_id)?.into(),
        indexSet: U256::from(index_set),
        amount: units_to_wei(amount)?,
    };
    Ok(EncodedPositionOp {
        target,
        data: Bytes::from(call.abi_encode()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr as _;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    const CONDITION: &str = "0x5688f2f1cff01a54d1c1697d5211b175fcb26c1ec9f8dd7b0f23d6bce4d8d203";

    /// 路由矩阵：二元两形态直呼 CTF；neg-risk 恒走 adapter（生息选择哪个
    /// adapter——官方 SDK OrderBuilder 路由）。
    #[test]
    fn route_matrix_four_variants() {
        assert_eq!(
            route_target(PositionMarketVariant { is_neg_risk: false, is_yield_bearing: false }),
            bnb_mainnet::CONDITIONAL_TOKENS
        );
        assert_eq!(
            route_target(PositionMarketVariant { is_neg_risk: false, is_yield_bearing: true }),
            bnb_mainnet::YIELD_BEARING_CONDITIONAL_TOKENS
        );
        assert_eq!(
            route_target(PositionMarketVariant { is_neg_risk: true, is_yield_bearing: false }),
            bnb_mainnet::NEG_RISK_ADAPTER
        );
        assert_eq!(
            route_target(PositionMarketVariant { is_neg_risk: true, is_yield_bearing: true }),
            bnb_mainnet::YIELD_BEARING_NEG_RISK_ADAPTER
        );
    }

    /// selector golden（官方 ABI 签名 keccak 前 4 字节——锚死签名形态，
    /// bytes31 之类的形态捏造在此红）：calldata 前 4 字节必须等于
    /// keccak(签名串)[..4]。
    #[test]
    fn selectors_match_official_abi_signatures() {
        let cases: &[(&str, fn() -> Result<EncodedPositionOp, anyhow::Error>)] = &[
            (
                "splitPosition(address,bytes32,bytes32,uint256[],uint256)",
                || {
                    encode_split(CONDITION, d("1"), PositionMarketVariant {
                        is_neg_risk: false,
                        is_yield_bearing: false,
                    })
                },
            ),
            (
                "mergePositions(address,bytes32,bytes32,uint256[],uint256)",
                || {
                    encode_merge(CONDITION, d("1"), PositionMarketVariant {
                        is_neg_risk: false,
                        is_yield_bearing: false,
                    })
                },
            ),
            (
                "redeemPositions(address,bytes32,bytes32,uint256[])",
                || {
                    encode_redeem(CONDITION, PositionMarketVariant {
                        is_neg_risk: false,
                        is_yield_bearing: false,
                    }, None)
                },
            ),
            (
                "splitPosition(bytes32,uint256)",
                || {
                    encode_split(CONDITION, d("1"), PositionMarketVariant {
                        is_neg_risk: true,
                        is_yield_bearing: false,
                    })
                },
            ),
            (
                "mergePositions(bytes32,uint256)",
                || {
                    encode_merge(CONDITION, d("1"), PositionMarketVariant {
                        is_neg_risk: true,
                        is_yield_bearing: false,
                    })
                },
            ),
            (
                "redeemPositions(bytes32,uint256[])",
                || {
                    encode_redeem(CONDITION, PositionMarketVariant {
                        is_neg_risk: true,
                        is_yield_bearing: false,
                    }, Some((d("3"), d("2"))))
                },
            ),
            (
                "convertPositions(bytes32,uint256,uint256)",
                || {
                    encode_convert(CONDITION, 1, d("1"), PositionMarketVariant {
                        is_neg_risk: true,
                        is_yield_bearing: false,
                    })
                },
            ),
        ];
        for (signature, build) in cases {
            let op = build().expect("编码应成功");
            let digest = alloy_primitives::keccak256(signature.as_bytes());
            assert_eq!(
                &op.data[..4],
                &digest[..4],
                "selector 与官方签名 {signature} 不符"
            );
        }
    }

    /// 二元 split calldata 形状：五参 ABI 完整 calldata + 末位 wei amount
    /// 十进制读回（1 unit = 1e18）。
    #[test]
    fn binary_split_calldata_shape() {
        let op = encode_split(CONDITION, d("1"), PositionMarketVariant {
            is_neg_risk: false,
            is_yield_bearing: false,
        })
        .unwrap();
        assert_eq!(op.target, bnb_mainnet::CONDITIONAL_TOKENS);
        assert!(op.data.len() > 4 + 32 * 5, "五参 + 数组头的完整 calldata");
        let amount_word = &op.data[4 + 32 * 4..4 + 32 * 5];
        let mut raw = [0u8; 32];
        raw.copy_from_slice(amount_word);
        let value = alloy_primitives::U256::from_be_bytes(raw);
        assert_eq!(value, alloy_primitives::U256::from(1_000_000_000_000_000_000_u64));
    }

    /// neg-risk 面 selector 与二元不同（短参形态 vs 五参形态）。
    #[test]
    fn neg_risk_selector_differs_from_binary() {
        let binary = encode_merge(CONDITION, d("1"), PositionMarketVariant {
            is_neg_risk: false,
            is_yield_bearing: false,
        })
        .unwrap();
        let neg = encode_merge(CONDITION, d("1"), PositionMarketVariant {
            is_neg_risk: true,
            is_yield_bearing: false,
        })
        .unwrap();
        assert_ne!(binary.target, neg.target, "不同合约");
        assert_ne!(&binary.data[..4], &neg.data[..4], "不同 selector");
    }

    /// convert 仅 neg-risk；二元拒。
    #[test]
    fn convert_rejects_non_neg_risk() {
        assert!(encode_convert(CONDITION, 1, d("1"), PositionMarketVariant {
            is_neg_risk: false,
            is_yield_bearing: false,
        })
        .is_err());
        assert!(encode_convert(CONDITION, 1, d("1"), PositionMarketVariant {
            is_neg_risk: true,
            is_yield_bearing: false,
        })
        .is_ok());
    }

    /// sub-wei 余量响亮拒（金额合同与 amounts.rs 同语义）。
    #[test]
    fn sub_wei_amount_is_loud() {
        assert!(encode_split(CONDITION, d("0.0000000000000000001"), PositionMarketVariant {
            is_neg_risk: false,
            is_yield_bearing: false,
        })
        .is_err());
    }

    /// neg-risk redeem 需逐腿数量；缺失拒。
    #[test]
    fn neg_risk_redeem_requires_leg_amounts() {
        assert!(encode_redeem(
            CONDITION,
            PositionMarketVariant { is_neg_risk: true, is_yield_bearing: false },
            None
        )
        .is_err());
        assert!(encode_redeem(
            CONDITION,
            PositionMarketVariant { is_neg_risk: true, is_yield_bearing: false },
            Some((d("3"), d("2")))
        )
        .is_ok());
    }
}
