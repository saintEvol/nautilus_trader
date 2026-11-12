//! On-chain receipt decoding for Deposit Wallet operations: the real asset
//! changes of a confirmed submission (POLYMARKET_PREDICT §8.4 — never the
//! request's expected values masquerading as outcomes).
//!
//! Decodes the three event shapes a position batch can emit for the wallet:
//! ERC-20 `Transfer` (pUSD collateral movements), ERC-1155 `TransferSingle`
//! and `TransferBatch` (outcome position mint/burn). Raw logs stay the source
//! of truth — unparseable or foreign events are skipped, never guessed.

use alloy_primitives::{Address, U256, keccak256};

/// A decoded wallet-relevant asset movement from a transaction receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AssetChange {
    /// ERC-20 `Transfer` — collateral in/out of the wallet.
    Erc20 {
        token: Address,
        from: Address,
        to: Address,
        value: U256,
    },
    /// ERC-1155 `TransferSingle` — one outcome position id.
    Erc1155Single {
        operator: Address,
        from: Address,
        to: Address,
        id: U256,
        value: U256,
    },
    /// ERC-1155 `TransferBatch` — a merged set of outcome movements.
    Erc1155Batch {
        operator: Address,
        from: Address,
        to: Address,
        ids: Vec<U256>,
        values: Vec<U256>,
    },
}

impl AssetChange {
    /// Whether this movement touches `wallet` on either side.
    #[must_use]
    pub fn involves(&self, wallet: Address) -> bool {
        match self {
            Self::Erc20 { from, to, .. } => *from == wallet || *to == wallet,
            Self::Erc1155Single { from, to, .. } => *from == wallet || *to == wallet,
            Self::Erc1155Batch { from, to, .. } => *from == wallet || *to == wallet,
        }
    }
}

/// ERC-20 `Transfer(address,address,uint256)` topic — keccak256 of the
/// canonical signature (standard constant; asserted against `keccak256` in
/// tests so the literal can never silently drift).
pub const TRANSFER_ERC20_SELECTOR: [u8; 32] = [
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d,
    0xaa, 0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23,
    0xb3, 0xef,
];
/// ERC-1155 `TransferSingle(address,address,address,uint256,uint256)` topic.
pub const TRANSFER_SINGLE_SELECTOR: [u8; 32] = [
    0xc3, 0xd5, 0x81, 0x68, 0xc5, 0xae, 0x73, 0x97, 0x73, 0x1d, 0x06, 0x3d, 0x5b, 0xbf, 0x3d,
    0x65, 0x78, 0x54, 0x42, 0x73, 0x43, 0xf4, 0xc0, 0x83, 0x24, 0x0f, 0x7a, 0xac, 0xaa, 0x2d,
    0x0f, 0x62,
];
/// ERC-1155 `TransferBatch(address,address,address,uint256[],uint256[])` topic.
pub const TRANSFER_BATCH_SELECTOR: [u8; 32] = [
    0x4a, 0x39, 0xdc, 0x06, 0xd4, 0xc0, 0xdb, 0xc6, 0x4b, 0x70, 0xaf, 0x90, 0xfd, 0x69, 0x8a,
    0x23, 0x3a, 0x51, 0x8a, 0xa5, 0xd0, 0x7e, 0x59, 0x5d, 0x98, 0x3b, 0x8c, 0x05, 0x26, 0xc8,
    0xf7, 0xfb,
];

/// One raw receipt log: address + topics + data, as an RPC returns them.
#[derive(Clone, Debug)]
pub struct RawLog<'a> {
    pub address: Address,
    pub topics: &'a [[u8; 32]],
    pub data: &'a [u8],
}

/// Decodes wallet-relevant asset changes from receipt logs. Foreign or
/// malformed events are skipped (real effects are never invented).
#[must_use]
pub fn decode_asset_changes<'a>(logs: impl IntoIterator<Item = RawLog<'a>>) -> Vec<AssetChange> {
    let mut out = Vec::new();
    for log in logs {
        let Some(selector) = log.topics.first().copied() else {
            continue;
        };
        if selector == TRANSFER_ERC20_SELECTOR && log.topics.len() == 3 {
            if let (Some(from), Some(to)) = (
                topic_as_address(log.topics[1]),
                topic_as_address(log.topics[2]),
            ) && let Some(value) = word_at(log.data, 0)
            {
                out.push(AssetChange::Erc20 {
                    token: log.address,
                    from,
                    to,
                    value,
                });
            }
        } else if selector == TRANSFER_SINGLE_SELECTOR && log.topics.len() == 4 {
            if let (Some(operator), Some(from), Some(to)) = (
                topic_as_address(log.topics[1]),
                topic_as_address(log.topics[2]),
                topic_as_address(log.topics[3]),
            ) && let (Some(id), Some(value)) = (word_at(log.data, 0), word_at(log.data, 1))
            {
                out.push(AssetChange::Erc1155Single {
                    operator,
                    from,
                    to,
                    id,
                    value,
                });
            }
        } else if selector == TRANSFER_BATCH_SELECTOR && log.topics.len() == 4 {
            if let (Some(operator), Some(from), Some(to)) = (
                topic_as_address(log.topics[1]),
                topic_as_address(log.topics[2]),
                topic_as_address(log.topics[3]),
            ) && let Some((ids, values)) = decode_id_value_arrays(log.data)
            {
                out.push(AssetChange::Erc1155Batch {
                    operator,
                    from,
                    to,
                    ids,
                    values,
                });
            }
        }
    }
    out
}

/// A 32-byte topic reinterpreted as an address (lower 20 bytes).
fn topic_as_address(topic: [u8; 32]) -> Option<Address> {
    Some(Address::from_slice(&topic[12..]))
}

/// Reads the `index`-th 32-byte word of ABI-encoded calldata.
fn word_at(data: &[u8], index: usize) -> Option<U256> {
    let start = index.checked_mul(32)?;
    let end = start.checked_add(32)?;
    let word = data.get(start..end)?;
    Some(U256::from_be_slice(word))
}

/// Decodes the trailing `uint256[] ids, uint256[] values` dynamic pair of
/// `TransferBatch`.
fn decode_id_value_arrays(data: &[u8]) -> Option<(Vec<U256>, Vec<U256>)> {
    let ids_offset = word_at(data, 0)?.to::<usize>();
    let ids = decode_u256_array(data, ids_offset)?;
    let values_offset = word_at(data, 1)?.to::<usize>();
    let values = decode_u256_array(data, values_offset)?;
    (ids.len() == values.len()).then_some((ids, values))
}

/// Decodes one ABI dynamic `uint256[]` at a **byte** offset from the start of
/// `data` (the heads' offsets are byte offsets, not word indices).
fn decode_u256_array(data: &[u8], byte_offset: usize) -> Option<Vec<U256>> {
    let len = read_word(data, byte_offset)?.to::<usize>();
    let base = byte_offset.checked_add(32)?;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        out.push(read_word(data, base.checked_add(i.checked_mul(32)?)?)?);
    }
    Some(out)
}

/// Reads the 32-byte word at a **byte** offset.
fn read_word(data: &[u8], byte_offset: usize) -> Option<U256> {
    let end = byte_offset.checked_add(32)?;
    Some(U256::from_be_slice(data.get(byte_offset..end)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, hex};

    fn topic_from(addr: Address) -> [u8; 32] {
        let mut topic = [0u8; 32];
        topic[12..].copy_from_slice(addr.as_slice());
        topic
    }

    /// ERC-20 transfer decode: topics (selector, from, to) + one value word.
    #[test]
    fn decodes_erc20_transfer() {
        let wallet = address!("1111111111111111111111111111111111111111");
        let token = address!("2791bca1f2de4661ed88a30c99a7a9449aa84174");
        let source = address!("2222222222222222222222222222222222222222");
        let data = hex::decode(concat!(
            "0000000000000000000000000000000000000000000000000000000005f5e100" // 100e6
        ))
        .unwrap();
        let logs = [RawLog {
            address: token,
            topics: &[TRANSFER_ERC20_SELECTOR, topic_from(source), topic_from(wallet)],
            data: &data,
        }];
        let changes = decode_asset_changes(logs);
        assert_eq!(
            changes,
            vec![AssetChange::Erc20 {
                token,
                from: source,
                to: wallet,
                value: U256::from(100_000_000u64),
            }],
        );
        assert!(changes[0].involves(wallet));
        assert!(changes[0].involves(source), "either side counts");
        assert!(!changes[0].involves(Address::ZERO));
    }

    /// ERC-1155 TransferSingle: 4 topics + (id, value).
    #[test]
    fn decodes_erc1155_single() {
        let operator = address!("3333333333333333333333333333333333333333");
        let from = Address::ZERO;
        let wallet = address!("1111111111111111111111111111111111111111");
        let mut data = Vec::new();
        data.extend_from_slice(&[0u8; 24]);
        data.extend_from_slice(&7u64.to_be_bytes()); // id = 7
        data.extend_from_slice(&[0u8; 24]);
        data.extend_from_slice(&500u64.to_be_bytes()); // value = 500
        let logs = [RawLog {
            address: address!("4444444444444444444444444444444444444444"),
            topics: &[
                TRANSFER_SINGLE_SELECTOR,
                topic_from(operator),
                topic_from(from),
                topic_from(wallet),
            ],
            data: &data,
        }];
        let changes = decode_asset_changes(logs);
        assert_eq!(
            changes,
            vec![AssetChange::Erc1155Single {
                operator,
                from,
                to: wallet,
                id: U256::from(7u64),
                value: U256::from(500u64),
            }],
        );
    }

    /// TransferBatch dynamic-array decode + malformed skip.
    #[test]
    fn decodes_erc1155_batch_and_skips_malformed() {
        let operator = address!("3333333333333333333333333333333333333333");
        let wallet = address!("1111111111111111111111111111111111111111");
        // ids at offset 2*32, values at offset 2*32+32+2*32 (len 2)
        let mut data = Vec::new();
        data.extend_from_slice(&U256::from(64usize).to_be_bytes::<32>()); // ids offset
        data.extend_from_slice(&U256::from(160usize).to_be_bytes::<32>()); // values offset
        data.extend_from_slice(&U256::from(2usize).to_be_bytes::<32>()); // ids len
        data.extend_from_slice(&U256::from(11u64).to_be_bytes::<32>());
        data.extend_from_slice(&U256::from(12u64).to_be_bytes::<32>());
        data.extend_from_slice(&U256::from(2usize).to_be_bytes::<32>()); // values len
        data.extend_from_slice(&U256::from(100u64).to_be_bytes::<32>());
        data.extend_from_slice(&U256::from(200u64).to_be_bytes::<32>());
        let good = RawLog {
            address: address!("4444444444444444444444444444444444444444"),
            topics: &[
                TRANSFER_BATCH_SELECTOR,
                topic_from(operator),
                topic_from(Address::ZERO),
                topic_from(wallet),
            ],
            data: &data,
        };
        let truncated = RawLog {
            address: good.address,
            topics: good.topics,
            data: &data[..data.len() - 4], // malformed tail
        };
        let changes = decode_asset_changes([good, truncated]);
        assert_eq!(changes.len(), 1, "malformed batch skipped, never guessed");
        assert!(matches!(&changes[0], AssetChange::Erc1155Batch { ids, values, .. }
            if ids == &vec![U256::from(11u64), U256::from(12u64)]
                && values == &vec![U256::from(100u64), U256::from(200u64)]));
    }

    /// Selector literals must equal keccak256 of the canonical signatures —
    /// the constants can never silently drift.
    #[test]
    fn selector_literals_match_keccak() {
        assert_eq!(
            TRANSFER_ERC20_SELECTOR,
            keccak256("Transfer(address,address,uint256)".as_bytes()).0
        );
        assert_eq!(
            TRANSFER_SINGLE_SELECTOR,
            keccak256("TransferSingle(address,address,address,uint256,uint256)".as_bytes()).0
        );
        assert_eq!(
            TRANSFER_BATCH_SELECTOR,
            keccak256("TransferBatch(address,address,address,uint256[],uint256[])".as_bytes()).0
        );
    }

    /// Foreign events (unknown selectors) never produce changes.
    #[test]
    fn foreign_events_are_ignored() {
        let logs = [RawLog {
            address: address!("5555555555555555555555555555555555555555"),
            topics: &[[0xaa; 32]],
            data: &[],
        }];
        assert!(decode_asset_changes(logs).is_empty());
    }
}
