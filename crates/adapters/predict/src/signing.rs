//! Predict order EIP-712 signing contracts (POLYMARKET_PREDICT §6.2 signing,
//! golden-vector pinning).
//!
//! Source of truth: the official Python SDK (`PredictDotFun/sdk-python`,
//! MIT), read directly — domain constants, the 12-field `Order` structure,
//! and the four exchange contracts selected by `isNegRisk × isYieldBearing`.
//! The SDK's own test suite carries a hash vector cross-verified against the
//! official TS SDK; that vector is pinned verbatim below (fixed input →
//! expected digest), alongside the order-signing doc vector. The Predict
//! account (Kernel smart account v0.3.1) wrapping below follows the official
//! contract exactly: `signatureType` stays `0` for a regular wallet and for
//! a Predict account alike; the account flow differs by `maker`/`signer`
//! carrying the ACCOUNT address and by the 86-byte `0x01 ‖ validator ‖ sig`
//! signature format (ERC-1271 via the ECDSA validator).

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

// ---------------------------------------------------------------------------------
// Predict account (Kernel smart account) signing — official contract:
// dev.predict.fun/resources/order-signing + SDK `sign_predict_account_message`.
//
// For a hash H (an order's EIP-712 hash, or the EIP-191 hash of an auth
// message):
//   1. wrapped = keccak256(abi.encode(keccak256("Kernel(bytes32 hash)"), H))
//   2. digest  = keccak256(0x1901 ‖ kernelDomainSeparator ‖ wrapped), the
//      domain being name "Kernel", version "0.3.1", chainId, and the ACCOUNT
//      address as verifyingContract
//   3. the owner (Privy wallet) signs `digest` as a personal message (EIP-191
//      over its 32 bytes)
//   4. signature = 0x01 ‖ ECDSA validator address ‖ 65-byte signature (86
//      bytes total)
// ---------------------------------------------------------------------------------

/// Kernel smart-account domain identity (SDK `KERNEL_DOMAIN_BY_CHAIN_ID`).
pub const KERNEL_DOMAIN_NAME: &str = "Kernel";
pub const KERNEL_DOMAIN_VERSION: &str = "0.3.1";

/// The ECDSA validator every Predict account signature prefixes (same
/// address on BNB Mainnet and BNB Testnet — SDK `Addresses`).
pub const ECDSA_VALIDATOR: Address = alloy_primitives::address!("845ADb2C711129d4f3966735eD98a9F09fC4cE57");

/// `keccak256("Kernel(bytes32 hash)")` — the Kernel wrapper type hash.
#[must_use]
pub fn kernel_type_hash() -> B256 {
    keccak256(b"Kernel(bytes32 hash)")
}

/// The EIP-712 domain separator of the account's Kernel domain.
#[must_use]
pub fn kernel_domain_separator(chain_id: u64, account: Address) -> B256 {
    let type_hash = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let name_hash = keccak256(KERNEL_DOMAIN_NAME.as_bytes());
    let version_hash = keccak256(KERNEL_DOMAIN_VERSION.as_bytes());
    let mut encoded = Vec::with_capacity(160);
    encoded.extend_from_slice(type_hash.as_slice());
    encoded.extend_from_slice(name_hash.as_slice());
    encoded.extend_from_slice(version_hash.as_slice());
    encoded.extend_from_slice(&U256::from(chain_id).abi_encode());
    encoded.extend_from_slice(&account.abi_encode());
    keccak256(&encoded)
}

/// Step 1: wraps a payload hash into the Kernel struct
/// `Kernel(bytes32 hash)`.
#[must_use]
pub fn kernel_wrapped_hash(payload: B256) -> B256 {
    let mut encoded = Vec::with_capacity(64);
    encoded.extend_from_slice(kernel_type_hash().as_slice());
    encoded.extend_from_slice(payload.as_slice());
    keccak256(&encoded)
}

/// Steps 1–2: the digest the account's owner key signs for payload hash `h`.
#[must_use]
pub fn predict_account_digest(payload: B256, chain_id: u64, account: Address) -> B256 {
    let mut bytes = Vec::with_capacity(66);
    bytes.extend_from_slice(&[0x19, 0x01]);
    bytes.extend_from_slice(kernel_domain_separator(chain_id, account).as_slice());
    bytes.extend_from_slice(kernel_wrapped_hash(payload).as_slice());
    keccak256(&bytes)
}

/// The EIP-191 personal-message hash of a 32-byte payload — step 3 prehash
/// (`keccak256("\x19Ethereum Signed Message:\n32" ‖ payload)`).
#[must_use]
pub fn eip191_personal_hash32(payload: B256) -> B256 {
    let mut bytes = Vec::with_capacity(28 + 32);
    bytes.extend_from_slice(b"\x19Ethereum Signed Message:\n32");
    bytes.extend_from_slice(payload.as_slice());
    keccak256(&bytes)
}

/// Step 4: assembles the 86-byte account signature
/// `0x01 ‖ validator ‖ owner-signature`.
#[must_use]
pub fn predict_account_signature_hex(owner_signature: &[u8; 65]) -> String {
    let mut signature = Vec::with_capacity(1 + 20 + 65);
    signature.push(0x01);
    signature.extend_from_slice(ECDSA_VALIDATOR.as_slice());
    signature.extend_from_slice(owner_signature);
    format!("0x{}", alloy_primitives::hex::encode(&signature))
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

    /// Order-signing doc vector (dev.predict.fun/resources/order-signing):
    /// fixed order against the BNB Mainnet standard exchange — type hash,
    /// domain separator and order hash all pinned. Independent of the SDK
    /// test vector above, so any encoding drift trips at least one anchor.
    #[test]
    fn typed_data_hash_matches_dev_docs_golden_vector() {
        let order = PredictOrder {
            salt: U256::from(123_456_789u64),
            maker: address!("1111111111111111111111111111111111111111"),
            signer: address!("1111111111111111111111111111111111111111"),
            taker: Address::ZERO,
            tokenId: U256::from(1_000_000_000_000_000_000u64),
            makerAmount: U256::from(4_000_000_000_000_000_000u64),
            takerAmount: U256::from(10_000_000_000_000_000_000u64),
            expiration: U256::from(4_102_444_800u64),
            nonce: U256::ZERO,
            feeRateBps: U256::from(200u64),
            side: 0,
            signatureType: 0,
        };
        let context = signing_context(PredictChain::BnbMainnet, false, false)
            .expect("standard context");

        assert_eq!(
            hex::encode(order_type_hash()),
            "a852566c4e14d00869b6db0220888a9090a13eccdaea03713ff0a3d27bf9767c",
            "type hash must match the dev-docs vector"
        );
        assert_eq!(
            hex::encode(domain_separator(context)),
            "61e5a410200861954c73b7ec87a9054507b7b09d95035434808a719bf33bac8b",
            "domain separator must match the dev-docs vector"
        );
        assert_eq!(
            hex::encode_prefixed(order_typed_data_hash(context, &order)),
            "0xe6a0edf257e9904e5fd4a1b8fdc3dd092e8789c4fa596ff6275097515ee4fd34",
            "order hash must match the dev-docs vector"
        );
    }

    /// Kernel wrap determinism and shape: the account digest differs from
    /// the raw payload, from the EOA digest, and per account/chain.
    #[test]
    fn kernel_account_digest_is_deterministic_and_domain_bound() {
        let payload = B256::from([7u8; 32]);
        let account = address!("2222222222222222222222222222222222222222");
        let other_account = address!("3333333333333333333333333333333333333333");

        let digest = predict_account_digest(payload, 56, account);
        assert_eq!(digest, predict_account_digest(payload, 56, account));
        assert_ne!(digest, payload, "the kernel wrap must change the digest");
        assert_ne!(
            digest,
            predict_account_digest(payload, 56, other_account),
            "the verifying account is part of the kernel domain"
        );
        assert_ne!(
            digest,
            predict_account_digest(payload, 97, account),
            "the chain id is part of the kernel domain"
        );

        // eip191_personal_hash32 must equal eth_account's encode_defunct hash
        // of a 32-byte primitive: keccak("\x19Ethereum Signed Message:\n32" ‖ x).
        assert_eq!(
            eip191_personal_hash32(B256::ZERO),
            keccak256(
                b"\x19Ethereum Signed Message:\n32\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
            )
        );
    }

    /// The 86-byte account signature format: `0x01 ‖ validator ‖ sig65`.
    #[test]
    fn account_signature_hex_prefixes_validator() {
        let signature = predict_account_signature_hex(&[0xab; 65]);
        assert_eq!(signature.len(), 2 + 2 * 86);
        assert!(signature.starts_with("0x01845adb2c711129d4f3966735ed98a9f09fc4ce57"));
        assert!(signature.ends_with(&"ab".repeat(65)));
    }

    /// End-to-end golden vector for the account-signing chain — kernel wrap →
    /// Kernel EIP-712 digest → EIP-191 personal sign → 86-byte assembly —
    /// byte-identical to the official Python SDK's `sign_predict_account_message`.
    ///
    /// Vector source (no vector shipped in the SDK's tests; generated by
    /// executing the SDK once): github.com/PredictDotFun/sdk-python @
    /// 55381bfed02323c5edbc6eed0f5c603ae84172ef (2026-11-16), offline via the
    /// `OrderBuilder` constructor (`OrderBuilder.make` runs a live RPC
    /// account-ownership check that offline vectors must bypass):
    ///
    /// ```text
    /// KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
    /// ACCOUNT=0x2222222222222222222222222222222222222222
    /// PAYLOAD=0x814000c89efa61ae42a2bcc4c98e06e90c11480b95a12edea00e3411ec76821d
    /// # (payload = the SDK/TS golden order hash, see the test above)
    /// python - <<'EOF'
    /// from eth_account import Account
    /// from predict_sdk.constants import ADDRESSES_BY_CHAIN_ID, ChainId
    /// from predict_sdk._internal.utils import generate_order_salt
    /// from predict_sdk.logger import Logger
    /// from predict_sdk.order_builder import OrderBuilder
    /// builder = OrderBuilder(ChainId.BNB_MAINNET, 18,
    ///     ADDRESSES_BY_CHAIN_ID[ChainId.BNB_MAINNET], generate_order_salt,
    ///     Logger("INFO"),
    ///     signer=Account.from_key(KEY), predict_account=ACCOUNT)
    /// print(builder.sign_predict_account_message({"raw": PAYLOAD}))
    /// EOF
    /// # → 0x01845adb…61b  (86 bytes, lowercased below)
    /// ```
    ///
    /// The owner key is hardhat test account #0 (address
    /// `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266`); ECDSA nonces are
    /// RFC 6979 deterministic in both implementations, so the signature is
    /// reproducible byte-for-byte.
    #[test]
    fn account_signature_chain_matches_official_sdk_golden_vector() {
        let payload = alloy_primitives::b256!(
            "814000c89efa61ae42a2bcc4c98e06e90c11480b95a12edea00e3411ec76821d"
        );
        let account = address!("2222222222222222222222222222222222222222");

        // Step 1–2: the Kernel-wrapped digest must match the SDK's
        // `eip712_wrap_hash` output for the same payload/account/chain.
        let digest = predict_account_digest(payload, 56, account);
        assert_eq!(
            hex::encode_prefixed(digest),
            "0x6e3172bd824cfabf0e5b7054bd663c9449437135adb438e0dff433a47e2d7f0a",
            "kernel-wrapped digest must match the official SDK vector"
        );

        // Steps 3–4: the owner key personal-signs the digest and the 65-byte
        // signature assembles into the exact SDK 86-byte output (validator
        // prefix + RFC 6979 deterministic r‖s‖v, v ∈ {27, 28}).
        let key = <alloy::signers::local::PrivateKeySigner as std::str::FromStr>::from_str(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        .expect("hardhat #0 key");
        use alloy::signers::SignerSync as _;
        let signature = key
            .sign_hash_sync(&eip191_personal_hash32(digest))
            .expect("owner signature");

        assert_eq!(
            predict_account_signature_hex(&signature.as_bytes()),
            "0x01845adb2c711129d4f3966735ed98a9f09fc4ce5763e8895bdabeea6948af30fee36bf8fefb5d1210bf15d737f959ada8d741dd247818634036faed693708bb041e2f8bcad53e1ec1162dc609c87926fdd38c63f61b",
            "the full account-signing chain must reproduce the official SDK signature byte-for-byte"
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
