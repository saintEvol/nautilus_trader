//! Predict order EIP-712 signing contracts (POLYMARKET_PREDICT §6.2 signing,
//! P0 golden-vector pinning).
//!
//! Source of truth: the official Python SDK (`PredictDotFun/sdk-python`,
//! MIT), read directly — domain constants, the 12-field `Order` structure,
//! and the four exchange contracts selected by `isNegRisk × isYieldBearing`.
//! The SDK's own test suite carries a hash vector cross-verified against the
//! official TS SDK; that vector is pinned verbatim below (fixed input →
//! expected digest). Full request-signing wiring (EOA vs Predict-account
//! Kernel flow, JWT, cancellation) is P3 work — this module fixes the typed
//! data + digest contract only.

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolValue, sol};

sol! {
    #[derive(Debug)]
    struct PredictOrder {
        uint256 salt;
        address maker;
        address signer;
        address taker;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint256 expiration;
        uint256 nonce;
        uint256 feeRateBps;
        uint8 side;
        uint8 signatureType;
    }
}

/// Protocol identity (SDK constants: `predict.fun CTF Exchange`, version 1).
pub const PROTOCOL_NAME: &str = "predict.fun CTF Exchange";
pub const PROTOCOL_VERSION: &str = "1";

/// The four signing contexts — `isNegRisk × isYieldBearing` selects the
/// verifying exchange contract (SDK `_get_exchange_identifier`). Addresses
/// pinned from the SDK constants; `isYieldBearing` changes the CONTRACT, not
/// the type structure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PredictSigningContext {
    pub chain_id: u64,
    pub exchange: Address,
}

/// BNB Mainnet exchange contracts (SDK `ADDRESSES_BY_CHAIN_ID[56]`).
pub mod bnb_mainnet {
    use alloy_primitives::address;

    pub const CTF_EXCHANGE: alloy_primitives::Address =
        address!("8BC070BEdAB741406F4B1Eb65A72bee27894B689");
    pub const NEG_RISK_CTF_EXCHANGE: alloy_primitives::Address =
        address!("365fb81bd4A24D6303cd2F19c349dE6894D8d58A");
    pub const YIELD_BEARING_CTF_EXCHANGE: alloy_primitives::Address =
        address!("6bEb5a40C032AFc305961162d8204CDA16DECFa5");
    pub const YIELD_BEARING_NEG_RISK_CTF_EXCHANGE: alloy_primitives::Address =
        address!("8A289d458f5a134bA40015085A8F50Ffb681B41d");
}

/// Resolves the verifying-contract context for the four market variants.
#[must_use]
pub fn signing_context(
    chain: PredictChain,
    is_neg_risk: bool,
    is_yield_bearing: bool,
) -> Option<PredictSigningContext> {
    let exchange = match (chain, is_neg_risk, is_yield_bearing) {
        (PredictChain::BnbMainnet, false, false) => bnb_mainnet::CTF_EXCHANGE,
        (PredictChain::BnbMainnet, true, false) => bnb_mainnet::NEG_RISK_CTF_EXCHANGE,
        (PredictChain::BnbMainnet, false, true) => bnb_mainnet::YIELD_BEARING_CTF_EXCHANGE,
        (PredictChain::BnbMainnet, true, true) => {
            bnb_mainnet::YIELD_BEARING_NEG_RISK_CTF_EXCHANGE
        }
        (PredictChain::BnbTestnet, _, _) => {
            // Testnet addresses exist in the SDK; pinned when P3 wiring needs
            // them (no golden vector depends on them yet).
            return None;
        }
    };
    Some(PredictSigningContext {
        chain_id: chain.id(),
        exchange,
    })
}

/// The EIP-712 domain separator for the given signing context.
#[must_use]
pub fn domain_separator(context: PredictSigningContext) -> B256 {
    // encodeData of EIP712Domain(string name, string version, uint256
    // chainId, address verifyingContract)
    let type_hash = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let name_hash = keccak256(PROTOCOL_NAME.as_bytes());
    let version_hash = keccak256(PROTOCOL_VERSION.as_bytes());
    let mut encoded = Vec::with_capacity(160);
    encoded.extend_from_slice(type_hash.as_slice());
    encoded.extend_from_slice(name_hash.as_slice());
    encoded.extend_from_slice(version_hash.as_slice());
    encoded.extend_from_slice(&U256::from(context.chain_id).abi_encode());
    encoded.extend_from_slice(&context.exchange.abi_encode());
    keccak256(&encoded)
}

/// The `Order` EIP-712 type hash: keccak256 of the canonical type string.
#[must_use]
pub fn order_type_hash() -> B256 {
    keccak256(
        concat!(
            "Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,",
            "uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,",
            "uint256 feeRateBps,uint8 side,uint8 signatureType)",
        )
        .as_bytes(),
    )
}

/// The `Order` struct hash (EIP-712 hashStruct: typeHash ‖ encoded fields —
/// all twelve fields are value types, so the encoding is the concatenation
/// of their static encodings, NOT the bare tuple encoding).
#[must_use]
pub fn order_struct_hash(order: &PredictOrder) -> B256 {
    let mut encoded = Vec::with_capacity(32 * 12 + 32);
    encoded.extend_from_slice(order_type_hash().as_slice());
    for value in [
        order.salt.abi_encode(),
        order.maker.abi_encode(),
        order.signer.abi_encode(),
        order.taker.abi_encode(),
        order.tokenId.abi_encode(),
        order.makerAmount.abi_encode(),
        order.takerAmount.abi_encode(),
        order.expiration.abi_encode(),
        order.nonce.abi_encode(),
        order.feeRateBps.abi_encode(),
        U256::from(u64::from(order.side)).abi_encode(),
        U256::from(u64::from(order.signatureType)).abi_encode(),
    ] {
        encoded.extend_from_slice(&value);
    }
    keccak256(&encoded)
}

/// The final EIP-712 signing digest: `keccak256(0x1901 ‖ domainSeparator ‖
/// hashStruct(message))`.
#[must_use]
pub fn order_typed_data_hash(context: PredictSigningContext, order: &PredictOrder) -> B256 {
    let separator = domain_separator(context);
    let struct_hash = order_struct_hash(order);
    let mut bytes = Vec::with_capacity(66);
    bytes.extend_from_slice(&[0x19, 0x01]);
    bytes.extend_from_slice(separator.as_slice());
    bytes.extend_from_slice(struct_hash.as_slice());
    keccak256(&bytes)
}

/// Chains the SDK ships addresses for (SDK `ChainId` enum: BNB 56/97).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredictChain {
    BnbMainnet,
    BnbTestnet,
}

impl PredictChain {
    #[must_use]
    pub const fn id(self) -> u64 {
        match self {
            Self::BnbMainnet => 56,
            Self::BnbTestnet => 97,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, hex};

    /// SDK golden vector (test_order_builder.py::test_build_typed_data_hash_
    /// matches_ts_sdk — cross-verified against the official TS SDK). The fixed
    /// input produces this digest; any divergence in domain/type/encoding
    /// breaks real signatures.
    #[test]
    fn typed_data_hash_matches_official_sdk_golden_vector() {
        let order = PredictOrder {
            salt: U256::from(123_456_789u64),
            maker: address!("1234567890123456789012345678901234567890"),
            signer: address!("1234567890123456789012345678901234567890"),
            taker: Address::ZERO,
            tokenId: U256::from(12_345u64),
            makerAmount: U256::from(1_000_000_000_000_000_000u64),
            takerAmount: U256::from(2_000_000_000_000_000_000u64),
            expiration: U256::from(4_102_444_800u64),
            nonce: U256::ZERO,
            feeRateBps: U256::from(100u64),
            side: 0,
            signatureType: 0,
        };
        let context = signing_context(PredictChain::BnbMainnet, false, false)
            .expect("standard context");

        let digest = order_typed_data_hash(context, &order);
        assert_eq!(
            hex::encode_prefixed(digest),
            "0x814000c89efa61ae42a2bcc4c98e06e90c11480b95a12edea00e3411ec76821d",
            "digest must match the official SDK/TS golden vector"
        );
    }

    /// The four market variants resolve to four DISTINCT verifying contracts
    /// (isNegRisk × isYieldBearing both matter — the plan's §6.2 point).
    #[test]
    fn four_market_variants_have_distinct_contracts() {
        let std = signing_context(PredictChain::BnbMainnet, false, false).unwrap();
        let neg = signing_context(PredictChain::BnbMainnet, true, false).unwrap();
        let yb = signing_context(PredictChain::BnbMainnet, false, true).unwrap();
        let neg_yb = signing_context(PredictChain::BnbMainnet, true, true).unwrap();
        let all = [std.exchange, neg.exchange, yb.exchange, neg_yb.exchange];
        for i in 0..all.len() {
            for j in i + 1..all.len() {
                assert_ne!(all[i], all[j], "variant contracts must be distinct");
            }
        }
        // Same order + same domain except contract → different digest (the
        // isYieldBearing contract switch is load-bearing for signature
        // validity).
        let order = PredictOrder {
            salt: U256::from(1u64),
            maker: Address::ZERO,
            signer: Address::ZERO,
            taker: Address::ZERO,
            tokenId: U256::from(7u64),
            makerAmount: U256::from(1u64),
            takerAmount: U256::from(2u64),
            expiration: U256::ZERO,
            nonce: U256::ZERO,
            feeRateBps: U256::ZERO,
            side: 0,
            signatureType: 0,
        };
        assert_ne!(
            order_typed_data_hash(std, &order),
            order_typed_data_hash(yb, &order),
            "yield-bearing switch must change the digest"
        );
    }
}
