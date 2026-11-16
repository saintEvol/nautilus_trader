//! Signed Predict order request assembly (§6.2/§8.2).
//!
//! Combines the pure layers into the exact `POST /orders` payload:
//! sized amounts (`amounts`) + EIP-712 typed data (`signing`) + the wallet
//! key → [`SignedPredictOrderWire`] + placement settings. Both signer
//! variants ride the same typed-data path: an EOA signs the order digest
//! directly, a Predict account (Kernel smart account) has its owner key sign
//! the kernel-wrapped digest and emits the 86-byte
//! `0x01 ‖ validator ‖ signature` format with `maker == signer ==` the
//! ACCOUNT address (`signatureType` stays 0 for both — official
//! order-signing contract). Deterministic offline anchors pin: digest
//! signing (same key+digest → same RFC-6979 signature), maker == signer
//! (venue rule), and the GTD/GTC expiration window (≥ 2 min out, < 2100).

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use alloy_primitives::{Address, U256};
use nautilus_core::string::secret::SecretString;

use crate::execution::amounts::{PredictOrderSide, SizedOrder};
use crate::http::trading::{PlaceOrderSettings, SignedPredictOrderWire};
use crate::signing::{
    PredictChain, PredictOrder, PredictSigningContext, eip191_personal_hash32,
    order_typed_data_hash, predict_account_digest, predict_account_signature_hex, signing_context,
};

/// A market variant for signing-context selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketVariant {
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
}

/// Errors as plain strings (module-internal boundary; the caller wraps).
type Result<T> = std::result::Result<T, String>;

/// Verifying-contract context for the market variant.
pub fn context_for(chain: PredictChain, variant: MarketVariant) -> Result<PredictSigningContext> {
    signing_context(chain, variant.is_neg_risk, variant.is_yield_bearing)
        .ok_or_else(|| "no pinned exchange contract for this chain variant".to_owned())
}

/// How orders are signed: a plain EOA wallet, or a Predict account (Kernel
/// smart account) whose owner key signs through the kernel wrap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignerIdentity {
    /// The wallet key's own address signs (`maker == signer == key address`).
    Eoa,
    /// The Predict account's address rides `maker`/`signer`; the wallet key
    /// (the account owner) signs the kernel-wrapped digest.
    PredictAccount { account: Address },
}

impl SignerIdentity {
    /// The address carried by `maker` and `signer`.
    #[must_use]
    pub fn address(self, owner: Address) -> Address {
        match self {
            Self::Eoa => owner,
            Self::PredictAccount { account } => account,
        }
    }
}

/// Builds the EIP-712 order struct from sized amounts + identity.
///
/// `maker == signer` is the venue's rule for both flows (the account's own
/// address for a Predict account). `signatureType` stays 0 for a regular
/// wallet and for a Predict account alike — the account flow is
/// distinguished by its 86-byte signature format, not by this field
/// (official order-signing contract).
#[must_use]
pub fn build_order_struct(
    signer: Address,
    token_id: U256,
    side: PredictOrderSide,
    sized: &SizedOrder,
    salt: U256,
    fee_rate_bps: u64,
) -> PredictOrder {
    PredictOrder {
        salt,
        maker: signer,
        signer,
        taker: Address::ZERO,
        tokenId: token_id,
        makerAmount: U256::try_from(sized.maker_amount_wei).unwrap_or(U256::MAX),
        takerAmount: U256::try_from(sized.taker_amount_wei).unwrap_or(U256::MAX),
        expiration: U256::ZERO, // filled by the caller-facing builder below
        nonce: U256::ZERO,
        feeRateBps: U256::from(fee_rate_bps),
        side: side.eip712_code(),
        signatureType: 0, // EOA and Predict account alike (official contract)
    }
}

/// Expiration policy (place-orders page): GTC has no expiry (0); GTD must
/// be ≥ 2 minutes out and before 2100-01-01.
#[derive(Clone, Copy, Debug)]
pub enum Expiration {
    Gtc,
    Gtd { unix_seconds: u64 },
}

impl Expiration {
    /// Validates the GTD window against `now_unix`.
    ///
    /// # Errors
    /// GTD closer than 2 minutes or at/after 2100-01-01.
    pub fn validate(self, now_unix: u64) -> Result<u64> {
        const MIN_OFFSET: u64 = 120;
        const MAX: u64 = 4_102_444_800; // 2100-01-01T00:00:00Z
        match self {
            Self::Gtc => Ok(0),
            Self::Gtd { unix_seconds } => {
                if unix_seconds < now_unix.saturating_add(MIN_OFFSET) {
                    return Err(format!(
                        "GTD expiration {unix_seconds} is closer than 2 minutes (now {now_unix})"
                    ));
                }
                if unix_seconds >= MAX {
                    return Err(format!("GTD expiration {unix_seconds} is at/after 2100-01-01"));
                }
                Ok(unix_seconds)
            }
        }
    }
}

/// A wallet that signs typed-data digests (RFC-6979 deterministic). For a
/// Predict account the wallet key is the account's OWNER and the signature
/// carries the kernel wrap + validator prefix.
pub struct PredictRequestSigner {
    signer: PrivateKeySigner,
    identity: SignerIdentity,
    chain: PredictChain,
}

impl PredictRequestSigner {
    /// Plain EOA signer.
    ///
    /// # Errors
    /// Invalid private key text.
    pub fn from_key_hex(key: &SecretString) -> Result<Self> {
        Self::from_key_hex_with_identity(key, SignerIdentity::Eoa, PredictChain::BnbMainnet)
    }

    /// Signer under an explicit identity — the wallet key is the owner
    /// signer in both variants; a Predict account wraps the digest through
    /// its Kernel domain before the owner signs.
    ///
    /// # Errors
    /// Invalid private key text.
    pub fn from_key_hex_with_identity(
        key: &SecretString,
        identity: SignerIdentity,
        chain: PredictChain,
    ) -> Result<Self> {
        let signer = <PrivateKeySigner as std::str::FromStr>::from_str(key.expose_secret())
            .map_err(|e| format!("invalid wallet private key: {e}"))?;
        Ok(Self {
            signer,
            identity,
            chain,
        })
    }

    /// The address carried by `maker`/`signer` (key address for an EOA, the
    /// account address for a Predict account).
    #[must_use]
    pub fn address(&self) -> Address {
        self.identity.address(self.signer.address())
    }

    #[must_use]
    pub fn identity(&self) -> SignerIdentity {
        self.identity
    }

    /// The deployment chain this signer domains its signatures for (single
    /// source of truth for both the order EIP-712 domain and the account
    /// Kernel wrap).
    #[must_use]
    pub const fn chain(&self) -> PredictChain {
        self.chain
    }

    /// Signs an order digest (65-byte hex for an EOA; the 86-byte
    /// `0x01 ‖ validator ‖ signature` format for a Predict account —
    /// deterministic for key+digest in both).
    ///
    /// # Errors
    /// Signing failure.
    pub fn sign_digest(&self, digest: [u8; 32]) -> Result<String> {
        let digest = alloy_primitives::B256::from(digest);
        let signature = match self.identity {
            SignerIdentity::Eoa => self
                .signer
                .sign_hash_sync(&digest)
                .map_err(|e| format!("digest signing failed: {e}"))?,
            SignerIdentity::PredictAccount { account } => {
                // Official account contract: kernel-wrap the order hash, sign
                // the wrapped digest as a personal message with the owner key.
                let wrapped = predict_account_digest(digest, self.chain.id(), account);
                let personal = eip191_personal_hash32(wrapped);
                let signature = self
                    .signer
                    .sign_hash_sync(&personal)
                    .map_err(|e| format!("account digest signing failed: {e}"))?;
                return Ok(predict_account_signature_hex(&signature.as_bytes()));
            }
        };
        Ok(format!(
            "0x{}",
            alloy_primitives::hex::encode(signature.as_bytes())
        ))
    }
}

/// Full request assembly: sized order → typed data → digest → signature →
/// wire order + settings. `price_per_share_wei` is the venue-required
/// wei-string price the amounts were sized at. The signing domain's chain is
/// the signer's own (config-driven) chain — never a second caller-supplied
/// value that could diverge from the account Kernel wrap.
///
/// # Errors
/// Context/signing failures or an invalid GTD window.
pub fn build_signed_request(
    signer: &PredictRequestSigner,
    variant: MarketVariant,
    token_id: U256,
    side: PredictOrderSide,
    sized: &SizedOrder,
    price_per_share_wei: i128,
    salt: U256,
    expiration: Expiration,
    now_unix: u64,
    fee_rate_bps: u64,
    settings_extra: PlacementToggles,
) -> Result<(SignedPredictOrderWire, PlaceOrderSettings)> {
    let context = context_for(signer.chain(), variant)?;
    let expiration_value = expiration.validate(now_unix)?;
    // 负金额是上游算错——签零额/回绕单是静默损坏，响亮拒。
    if sized.maker_amount_wei <= 0 || sized.taker_amount_wei <= 0 {
        return Err(format!(
            "sized order carries non-positive amounts (maker {}, taker {})",
            sized.maker_amount_wei, sized.taker_amount_wei
        ));
    }
    let mut order =
        build_order_struct(signer.address(), token_id, side, sized, salt, fee_rate_bps);
    order.expiration = U256::from(expiration_value);
    let digest = order_typed_data_hash(context, &order);
    let signature = signer.sign_digest(digest.0)?;
    let wire = SignedPredictOrderWire {
        hash: format!("0x{}", alloy_primitives::hex::encode(digest.0)),
        salt: order.salt.to_string(),
        maker: order.maker.to_checksum(None),
        signer: order.signer.to_checksum(None),
        taker: order.taker.to_checksum(None),
        token_id: order.tokenId.to_string(),
        maker_amount: order.makerAmount.to_string(),
        taker_amount: order.takerAmount.to_string(),
        expiration: expiration_value,
        nonce: order.nonce.to_string(),
        fee_rate_bps: order.feeRateBps.to_string(),
        side: order.side,
        signature_type: order.signatureType,
        signature,
    };
    let settings = PlaceOrderSettings {
        price_per_share: price_per_share_wei.to_string(),
        strategy: settings_extra.strategy,
        is_post_only: settings_extra.is_post_only,
        is_fill_or_kill: settings_extra.is_fill_or_kill,
        slippage_bps: settings_extra.slippage_bps,
        is_min_amount_out: settings_extra.is_min_amount_out,
        reserved_balance_policy: None,
        self_trade_prevention: None,
    };
    Ok((wire, settings))
}

/// Caller-facing strategy toggles (LIMIT post-only / MARKET FOK+slippage)。
/// `Default` = LIMIT（word 面 GTC 缺省语义）。
#[derive(Clone, Debug)]
pub struct PlacementToggles {
    pub strategy: crate::http::trading::PredictOrderStrategy,
    pub is_post_only: Option<bool>,
    pub is_fill_or_kill: Option<bool>,
    pub slippage_bps: Option<String>,
    pub is_min_amount_out: Option<bool>,
}


impl Default for PlacementToggles {
    fn default() -> Self {
        Self {
            strategy: crate::http::trading::PredictOrderStrategy::Limit,
            is_post_only: None,
            is_fill_or_kill: None,
            slippage_bps: None,
            is_min_amount_out: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::amounts::limit_amounts;
    use rust_decimal::Decimal;
    use std::str::FromStr as _;

    fn sized() -> SizedOrder {
        limit_amounts(
            PredictOrderSide::Buy,
            Decimal::from_str("1").unwrap(),
            Decimal::from_str("0.4").unwrap(),
        )
        .unwrap()
    }

    fn signer() -> PredictRequestSigner {
        PredictRequestSigner::from_key_hex(&SecretString::from(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        ))
        .unwrap()
    }

    /// 确定性锚：同 key+digest → 同签名（RFC-6979）；请求 hash = typed
    /// data digest 的 hex。
    #[test]
    fn request_signing_is_deterministic_and_hash_is_digest() {
        let signer = signer();
        let sized = sized();
        let mut order = build_order_struct(
            signer.address(),
            U256::from(7u64),
            PredictOrderSide::Buy,
            &sized,
            U256::from(42u64),
            100,
        );
        order.expiration = U256::ZERO;
        let context = context_for(
            PredictChain::BnbMainnet,
            MarketVariant {
                is_neg_risk: false,
                is_yield_bearing: false,
            },
        )
        .unwrap();
        let digest = order_typed_data_hash(context, &order);
        let a = signer.sign_digest(digest.0).unwrap();
        let b = signer.sign_digest(digest.0).unwrap();
        assert_eq!(a, b);
        assert!(a.starts_with("0x") && a.len() == 132);
    }

    /// 组装链路锚：wire 每字段 = typed data 原值（maker==signer、taker
    /// 零、wei 串、strategy 字段携带）。
    #[test]
    fn full_request_assembles_wire_fields() {
        let signer = signer();
        let sized = sized();
        let (wire, settings) = build_signed_request(
            &signer,
            MarketVariant {
                is_neg_risk: false,
                is_yield_bearing: true,
            },
            U256::from(7u64),
            PredictOrderSide::Buy,
            &sized,
            400_000_000_000_000_000,
            U256::from(42u64),
            Expiration::Gtc,
            1_700_000_000,
            100,
            PlacementToggles {
                strategy: crate::http::trading::PredictOrderStrategy::Limit,
                is_post_only: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(wire.maker, wire.signer);
        assert_eq!(wire.taker.to_lowercase(), "0x0000000000000000000000000000000000000000");
        assert_eq!(wire.maker_amount, "400000000000000000");
        assert_eq!(wire.taker_amount, "1000000000000000000");
        assert_eq!(wire.expiration, 0);
        assert_eq!(settings.price_per_share, "400000000000000000");
        assert_eq!(settings.is_post_only, Some(true));
    }

    /// GTD 窗口：< 2 分钟拒；≥ 2 分钟且 < 2100 过；≥ 2100 拒。
    #[test]
    fn gtd_window_is_enforced() {
        let now = 1_700_000_000u64;
        assert_eq!(Expiration::Gtc.validate(now).unwrap(), 0);
        assert!(Expiration::Gtd { unix_seconds: now + 119 }.validate(now).is_err());
        assert_eq!(Expiration::Gtd { unix_seconds: now + 120 }.validate(now).unwrap(), now + 120);
        assert!(Expiration::Gtd { unix_seconds: 4_102_444_800 }.validate(now).is_err());
    }

    fn account_signer() -> PredictRequestSigner {
        PredictRequestSigner::from_key_hex_with_identity(
            &SecretString::from("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"),
            SignerIdentity::PredictAccount {
                account: alloy_primitives::address!("1111111111111111111111111111111111111111"),
            },
            PredictChain::BnbMainnet,
        )
        .unwrap()
    }

    /// Predict account（Kernel 智能账户）签名锚：同 key+digest → 同 86 字节
    /// `0x01 ‖ validator ‖ sig`（RFC-6979 确定性）；与 EOA 签名不同；
    /// maker == signer == 账户地址；signatureType 恒 0（官方合同）。
    #[test]
    fn account_signing_is_deterministic_86_bytes_and_validator_prefixed() {
        let owner = signer();
        let account = account_signer();
        assert_ne!(owner.address(), account.address());
        assert_eq!(
            account.address(),
            alloy_primitives::address!("1111111111111111111111111111111111111111")
        );

        let digest = [9u8; 32];
        let a = account.sign_digest(digest).unwrap();
        let b = account.sign_digest(digest).unwrap();
        assert_eq!(a, b, "RFC-6979: same key+digest → same signature");
        assert_eq!(a.len(), 2 + 86 * 2, "0x01 ‖ validator(20) ‖ signature(65)");
        assert!(
            a.starts_with("0x01845adb2c711129d4f3966735ed98a9f09fc4ce57"),
            "the ECDSA validator prefix is mandatory"
        );
        assert_ne!(a, owner.sign_digest(digest).unwrap());
    }

    /// 组装链路锚（账户流）：wire 的 maker/signer = 账户地址、
    /// signatureType = 0、签名 86 字节格式。
    #[test]
    fn full_request_assembles_for_predict_account() {
        let signer = account_signer();
        let sized = sized();
        let (wire, _settings) = build_signed_request(
            &signer,
            MarketVariant {
                is_neg_risk: false,
                is_yield_bearing: false,
            },
            U256::from(7u64),
            PredictOrderSide::Buy,
            &sized,
            400_000_000_000_000_000,
            U256::from(42u64),
            Expiration::Gtc,
            1_700_000_000,
            100,
            PlacementToggles::default(),
        )
        .unwrap();

        assert_eq!(
            wire.maker.to_lowercase(),
            "0x1111111111111111111111111111111111111111"
        );
        assert_eq!(wire.maker, wire.signer);
        assert_eq!(wire.signature_type, 0);
        assert_eq!(wire.signature.len(), 2 + 86 * 2);
        assert!(wire.signature.starts_with("0x01845a"));
    }
}
