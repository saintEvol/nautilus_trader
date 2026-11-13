//! Signed Predict order request assembly (§6.2/§8.2).
//!
//! Combines the pure layers into the exact `POST /orders` payload:
//! sized amounts (`amounts`) + EIP-712 typed data (`signing`) + the wallet
//! key → [`SignedPredictOrderWire`] + placement settings. Deterministic
//! offline anchors pin: digest signing (same key+digest → same RFC-6979
//! signature), maker == signer (venue rule), and the GTD/GTC expiration
//! window (≥ 2 min out, < 2100).

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use alloy_primitives::{Address, U256};
use nautilus_core::string::secret::SecretString;

use crate::execution::amounts::{PredictOrderSide, SizedOrder};
use crate::http::trading::{PlaceOrderSettings, PredictOrderStrategy, SignedPredictOrderWire};
use crate::signing::{
    PredictChain, PredictOrder, PredictSigningContext, order_typed_data_hash, signing_context,
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

/// Builds the EIP-712 order struct from sized amounts + identity.
///
/// `maker == signer` is the venue's rule for the EOA flow (the smart
/// account flow is not implemented — the portal rejects it loudly).
#[must_use]
pub fn build_order_struct(
    signer: Address,
    token_id: U256,
    side: PredictOrderSide,
    sized: &SizedOrder,
    salt: U256,
    expiration_unix: U64ish,
    fee_rate_bps: u64,
) -> PredictOrder {
    let _ = expiration_unix;
    PredictOrder {
        salt,
        maker: signer,
        signer,
        taker: Address::ZERO,
        tokenId: token_id,
        makerAmount: U256::try_from(sized.maker_amount_wei.max(0)).unwrap_or(U256::ZERO),
        takerAmount: U256::try_from(sized.taker_amount_wei.max(0)).unwrap_or(U256::ZERO),
        expiration: U256::ZERO, // filled by the caller-facing builder below
        nonce: U256::ZERO,
        feeRateBps: U256::from(fee_rate_bps),
        side: side.eip712_code(),
        signatureType: 0, // EOA
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

/// A wallet that signs typed-data digests (EOA; RFC-6979 deterministic).
pub struct PredictRequestSigner {
    signer: PrivateKeySigner,
}

impl PredictRequestSigner {
    /// # Errors
    /// Invalid private key text.
    pub fn from_key_hex(key: &SecretString) -> Result<Self> {
        let signer = <PrivateKeySigner as std::str::FromStr>::from_str(key.expose_secret())
            .map_err(|e| format!("invalid wallet private key: {e}"))?;
        Ok(Self { signer })
    }

    #[must_use]
    pub fn address(&self) -> Address {
        self.signer.address()
    }

    /// Signs an order digest (65-byte hex, deterministic for key+digest).
    ///
    /// # Errors
    /// Signing failure.
    pub fn sign_digest(&self, digest: [u8; 32]) -> Result<String> {
        let signature = self
            .signer
            .sign_hash_sync(&alloy_primitives::B256::from(digest))
            .map_err(|e| format!("digest signing failed: {e}"))?;
        Ok(format!(
            "0x{}",
            alloy_primitives::hex::encode(signature.as_bytes())
        ))
    }
}

/// Convenience alias used by [`build_order_struct`] callers before the
/// expiration is resolved.
pub type U64ish = u64;

/// Full request assembly: sized order → typed data → digest → signature →
/// wire order + settings. `price_per_share_wei` is the venue-required
/// wei-string price the amounts were sized at.
///
/// # Errors
/// Context/signing failures or an invalid GTD window.
pub fn build_signed_request(
    signer: &PredictRequestSigner,
    chain: PredictChain,
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
    let context = context_for(chain, variant)?;
    let expiration_value = expiration.validate(now_unix)?;
    let mut order = build_order_struct(
        signer.address(),
        token_id,
        side,
        sized,
        salt,
        expiration_value,
        fee_rate_bps,
    );
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
    pub strategy: PredictOrderStrategyWire,
    pub is_post_only: Option<bool>,
    pub is_fill_or_kill: Option<bool>,
    pub slippage_bps: Option<String>,
    pub is_min_amount_out: Option<bool>,
}

/// Re-export of the strategy enum (avoid a second name for the same wire
/// word).
pub use crate::http::trading::PredictOrderStrategy as PredictOrderStrategyWire;

impl Default for PlacementToggles {
    fn default() -> Self {
        Self {
            strategy: PredictOrderStrategyWire::Limit,
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
            0,
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
            PredictChain::BnbMainnet,
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
                strategy: PredictOrderStrategy::Limit,
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
}
