//! Fixed pre-submit descriptions and recovery reconciliation for Deposit
//! Wallet position operations (POLYMARKET_PREDICT §8.4 prepare/submit/
//! reconcile).
//!
//! The prepare product is the durable identity of an operation: everything
//! the Relayer will receive except the signature. Persisting it before
//! submission lets the platform survive lost submit responses — reconcile
//! re-derives the outcome from the described batch, never by re-minting a
//! fresh nonce or payload. Secrets never appear here (no keys, no signed
//! material).

use alloy_primitives::{Address, U256, keccak256};

use crate::positions::{PositionCall, PolymarketPositionClient, PolymarketPositionTransaction};

/// Deterministic digest of a prepared Deposit Wallet batch — the request
/// identity the platform persists and later reconciles against.
///
/// keccak256 over the canonical concatenation of every submit-relevant field
/// (wallet, signer, nonce, deadline, each call's target/value/data). Stable
/// across processes; equal digests prove equal batches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PreparedOperationDigest(pub [u8; 32]);

/// A single call within a prepared batch (non-secret projection of
/// [`crate::signing::eip712::DepositWalletCall`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedCall {
    pub target: Address,
    pub value: U256,
    /// Raw calldata hex (non-secret — contract calls carry no credentials).
    pub data: String,
}

impl PreparedCall {
    /// Digest inputs; invalid hex data is impossible via
    /// [`prepared_calls_from`] (it hex-encodes real bytes) — a hand-built
    /// call with undecodable data digests its raw STRING bytes instead, so
    /// two different malformed batches never collide on "empty data".
    fn digest_fields(&self) -> (Address, U256, Vec<u8>) {
        let stripped = self.data.strip_prefix("0x").unwrap_or(&self.data);
        let data = match alloy_primitives::hex::decode(stripped) {
            Ok(bytes) => bytes,
            Err(_) => stripped.as_bytes().to_vec(),
        };
        (self.target, self.value, data)
    }
}

/// The fixed pre-submit description of a Deposit Wallet operation (§8.4
/// prepare). Produced by read-only checks; consumed verbatim by
/// [`PolymarketPositionClient::submit_prepared`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedWalletOperation {
    /// Deposit Wallet that will execute the batch.
    pub deposit_wallet: Address,
    /// Signer (owner) authorizing the batch.
    pub signer: Address,
    /// Wallet nonce captured at prepare time — submit does not re-fetch it.
    pub nonce: U256,
    /// Absolute batch deadline (Unix seconds, genuinely in the future at
    /// prepare time).
    pub deadline: U256,
    /// Human operation label (for logs/receipts; not identity).
    pub metadata: String,
    /// Ordered batch calls.
    pub calls: Vec<PreparedCall>,
}

impl PreparedWalletOperation {
    /// Deterministic digest — persisted as the operation's request identity.
    #[must_use]
    pub fn digest(&self) -> PreparedOperationDigest {
        PreparedOperationDigest::of(self)
    }

    /// Whether the deadline has passed relative to `now_unix_secs`.
    ///
    /// An expired deadline alone never proves failure (§8.4: only an
    /// on-chain non-execution proof can) — callers combine it with
    /// reconciliation evidence.
    #[must_use]
    pub fn deadline_passed(&self, now_unix_secs: u64) -> bool {
        self.deadline.to::<u64>() < now_unix_secs
    }
}

impl PreparedOperationDigest {
    fn of(operation: &PreparedWalletOperation) -> Self {
        let mut encoded = Vec::with_capacity(160 + operation.calls.len() * 128);
        encoded.extend_from_slice(operation.deposit_wallet.as_slice());
        encoded.extend_from_slice(operation.signer.as_slice());
        encoded.extend_from_slice(&operation.nonce.to_be_bytes::<32>());
        encoded.extend_from_slice(&operation.deadline.to_be_bytes::<32>());
        for call in &operation.calls {
            let (target, value, data) = call.digest_fields();
            encoded.extend_from_slice(target.as_slice());
            encoded.extend_from_slice(&value.to_be_bytes::<32>());
            encoded.extend_from_slice(&(data.len() as u64).to_be_bytes());
            encoded.extend_from_slice(&data);
        }
        Self(keccak256(&encoded).0)
    }
}

/// Outcome of reconciling a prepared operation (§8.4 reconcile).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalletReconciliation {
    /// The Relayer confirmed execution (with the on-chain hash when known).
    Confirmed {
        transaction_id: String,
        transaction_hash: Option<String>,
    },
    /// The Relayer reported failure.
    Failed {
        transaction_id: String,
        transaction_hash: Option<String>,
        error_msg: Option<String>,
    },
    /// The Relayer reported the submission invalid.
    Invalid {
        transaction_id: String,
        error_msg: Option<String>,
    },
    /// The outcome cannot be uniquely proven: no terminal Relayer state and
    /// the on-chain wallet nonce has not advanced past the described nonce —
    /// the batch may still execute or may never have reached the chain.
    /// Recovery keeps the operation open; never re-sign a new batch for it.
    Unknown {
        /// On-chain wallet nonce at reconciliation time.
        onchain_nonce: U256,
    },
}

/// A prepared operation plus its Relayer acceptance (returned by
/// [`PolymarketPositionClient::submit_prepared`]).
#[derive(Debug)]
pub struct SubmittedWalletOperation {
    pub prepared: PreparedWalletOperation,
    /// Relayer transaction handle for terminal-state polling.
    pub transaction: PolymarketPositionTransaction,
}

impl SubmittedWalletOperation {
    /// Relayer transaction id assigned at submit. The submit path errors when
    /// the response omits the id (that outcome is unknown — reconcile the
    /// prepared operation instead of resubmitting), so a constructed
    /// [`SubmittedWalletOperation`] always carries one.
    #[must_use]
    pub fn transaction_id(&self) -> &str {
        self.transaction.transaction_id()
    }
}

impl PolymarketPositionClient {
    /// Submits a previously prepared operation verbatim (§8.4 submit): signs
    /// exactly the described batch — no nonce re-fetch, no parameter or
    /// deadline changes. Callers persist the prepared description (and its
    /// digest) before invoking this; a lost response is recovered via
    /// [`Self::reconcile_prepared`].
    ///
    /// # Errors
    ///
    /// Returns an error if the Relayer rejects the submission or its response
    /// is ambiguous (a lost-response error still means the batch may have
    /// been sent — reconcile, never blind-resign).
    pub async fn submit_prepared(
        &self,
        prepared: &PreparedWalletOperation,
    ) -> Result<SubmittedWalletOperation, crate::http::error::Error> {
        use crate::http::relayer::RelayerWalletSubmit;
        use crate::signing::eip712::DepositWalletCall;

        let wallet_calls: Vec<DepositWalletCall> = prepared
            .calls
            .iter()
            .map(|call| {
                let data = alloy_primitives::hex::decode(
                    call.data
                        .strip_prefix("0x")
                        .unwrap_or(&call.data),
                )
                .map_err(|e| {
                    crate::http::error::Error::bad_request(format!(
                        "prepared call data is not hex: {e}"
                    ))
                })?;
                Ok(DepositWalletCall {
                    target: call.target,
                    value: call.value,
                    data: alloy_primitives::Bytes::from(data),
                })
            })
            .collect::<Result<Vec<_>, crate::http::error::Error>>()?;

        let signature = self.signer_ref().sign_deposit_wallet_batch(
            prepared.deposit_wallet,
            prepared.nonce,
            prepared.deadline,
            &wallet_calls,
        )?;

        for wallet_call in &wallet_calls {
            log::info!(
                "Deposit Wallet submission: wallet={:#x}, nonce={}, deadline={}, operation={}, target={:#x}, value={}, data={}",
                prepared.deposit_wallet,
                prepared.nonce,
                prepared.deadline,
                prepared.metadata,
                wallet_call.target,
                wallet_call.value,
                wallet_call.data,
            );
        }

        let submitted = self
            .relayer_ref()
            .submit_wallet_batch(RelayerWalletSubmit {
                signer: prepared.signer,
                deposit_wallet: prepared.deposit_wallet,
                nonce: prepared.nonce,
                deadline: prepared.deadline,
                signature: &signature,
                metadata: &prepared.metadata,
                calls: &wallet_calls,
            })
            .await?;

        let Some(transaction_id) = submitted.transaction_id else {
            return Err(crate::http::error::Error::decode(
                "Relayer submit response omitted transaction_id; batch outcome is unknown — reconcile the prepared operation",
            ));
        };

        Ok(SubmittedWalletOperation {
            prepared: prepared.clone(),
            transaction: PolymarketPositionTransaction::new(
                self.relayer_ref().clone(),
                transaction_id,
                self.wait_timeout_ref().clone(),
                self.poll_interval_ref().clone(),
            ),
        })
    }
}

/// Converts encoded [`PositionCall`]s into the durable non-secret projection.
pub(crate) fn prepared_calls_from(calls: &[PositionCall]) -> Vec<PreparedCall> {
    calls
        .iter()
        .map(|call| PreparedCall {
            target: call.target,
            value: U256::ZERO,
            data: format!("0x{}", alloy_primitives::hex::encode(call.data.as_ref())),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, hex};

    fn prepared(nonce: u64, deadline: u64, data_hex: &str) -> PreparedWalletOperation {
        PreparedWalletOperation {
            deposit_wallet: address!("1111111111111111111111111111111111111111"),
            signer: address!("2222222222222222222222222222222222222222"),
            nonce: U256::from(nonce),
            deadline: U256::from(deadline),
            metadata: "Split position".into(),
            calls: vec![PreparedCall {
                target: address!("3333333333333333333333333333333333333333"),
                value: U256::ZERO,
                data: data_hex.into(),
            }],
        }
    }

    /// Digest determinism: identical batches digest equal; any submit-relevant
    /// field change breaks the digest (the persisted identity really covers
    /// the whole batch).
    #[test]
    fn digest_is_deterministic_and_covers_every_field() {
        let a = prepared(7, 1_800, "0xdeadbeef");
        let b = prepared(7, 1_800, "0xdeadbeef");
        assert_eq!(a.digest(), b.digest());

        assert_ne!(a.digest(), prepared(8, 1_800, "0xdeadbeef").digest());
        assert_ne!(a.digest(), prepared(7, 1_801, "0xdeadbeef").digest());
        assert_ne!(a.digest(), prepared(7, 1_800, "0xdeadbee0").digest());

        // data length is bound in the encoding — prefix collisions impossible
        let short = prepared(7, 1_800, "0xdeadbeef");
        let long_suffix = format!("0x{}", hex::encode([0xde, 0xad, 0xbe, 0xef, 0x00]));
        assert_ne!(short.digest(), prepared(7, 1_800, &long_suffix).digest());
    }

    #[test]
    fn deadline_passing_is_a_plain_comparison() {
        let op = prepared(1, 100, "0x01");
        assert!(op.deadline_passed(101));
        assert!(!op.deadline_passed(100));
        assert!(!op.deadline_passed(99));
    }
}
