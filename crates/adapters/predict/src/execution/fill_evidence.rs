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

//! Confirmed matching and receipt evidence for one Predict order.

use std::collections::HashSet;

use alloy::sol;
use alloy::sol_types::SolEvent;
use alloy_primitives::{Address, B256, U256, hex};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::LiquiditySide,
    identifiers::TradeId,
    types::{Money, Price, Quantity, fixed::FIXED_PRECISION},
};
use rust_decimal::Decimal;

use super::{amounts::wei_per_unit, chain_account::ChainAccountReader};
use crate::{
    common::{consts::collateral_currency, enums::PredictFeeType},
    http::{
        client::{PredictHttpClient, WalkEnd},
        models::PredictMatchesQuery,
        trading::PredictOrderRecord,
    },
    signing::{PredictChain, signing_context},
};

sol! {
    event OrderFilled(
        bytes32 indexed orderHash,
        address indexed maker,
        address indexed taker,
        uint256 makerAssetId,
        uint256 takerAssetId,
        uint256 makerAmountFilled,
        uint256 takerAmountFilled,
        uint256 fee
    );
}

pub(super) struct PredictFillEvidence {
    pub trade_id: TradeId,
    pub shares_wei: i128,
    pub quantity: Quantity,
    pub price: Price,
    pub commission: Option<Money>,
    pub commission_shares: Option<Quantity>,
    pub liquidity: LiquiditySide,
    pub ts_event: UnixNanos,
}

impl PredictFillEvidence {
    /// Loads complete settled observations for one order. Public matches
    /// carry the realized fee, while exchange logs identify the quantity
    /// and consideration of this particular order within each transaction.
    pub(super) async fn load(
        http: &PredictHttpClient,
        chain: &ChainAccountReader,
        network: PredictChain,
        order: &PredictOrderRecord,
    ) -> anyhow::Result<Vec<Self>> {
        let hash: B256 = order.order.hash.parse()?;
        let owner: Address = order.order.maker.parse()?;
        let token: U256 = order.order.token_id.parse()?;
        let exchange = signing_context(network, order.is_neg_risk, order.is_yield_bearing)
            .ok_or_else(|| anyhow::anyhow!("Predict settlement exchange unavailable for chain"))?
            .exchange;
        let history = http
            .walk_matches(
                &PredictMatchesQuery {
                    first: Some(150),
                    order_hashes: Some(order.order.hash.clone()),
                    ..Default::default()
                },
                None,
                100,
            )
            .await?;
        anyhow::ensure!(
            history.end == WalkEnd::Exhausted,
            "Predict match history truncated for {hash}"
        );
        let mut observations = Vec::new();
        let mut seen = HashSet::new();
        for row in history.rows {
            let (matched, liquidity) = if row.taker.hash.eq_ignore_ascii_case(&order.order.hash) {
                (&row.taker, LiquiditySide::Taker)
            } else if let Some(maker) = row
                .makers
                .iter()
                .find(|maker| maker.hash.eq_ignore_ascii_case(&order.order.hash))
            {
                (maker, LiquiditySide::Maker)
            } else {
                continue;
            };
            let tx_hash: B256 = row.transaction_hash.parse()?;
            anyhow::ensure!(
                seen.insert(tx_hash),
                "ambiguous duplicate Predict match for {hash} in {tx_hash}"
            );
            let receipt = chain.confirmed_receipt(&row.transaction_hash).await?;
            let (shares, collateral) = order_amounts(&receipt, exchange, hash, owner, token)?;
            let price = collateral
                .checked_div(shares)
                .ok_or_else(|| anyhow::anyhow!("Predict fill price cannot be represented"))?;
            anyhow::ensure!(
                price > Decimal::ZERO && price <= Decimal::ONE,
                "Predict settled fill price outside its domain"
            );
            let quantity = Quantity::from_decimal(shares / wei_per_unit())?;
            let shares_wei = shares.try_into()?;
            let fee = matched.fee.amount / wei_per_unit();
            anyhow::ensure!(fee >= Decimal::ZERO, "negative Predict exchange fee");
            let (commission, commission_shares) = match matched.fee.fee_type {
                PredictFeeType::Collateral => (
                    Some(Money::from_decimal_exact(fee, collateral_currency())?),
                    None,
                ),
                PredictFeeType::Shares => (None, Some(Quantity::from_decimal(fee)?)),
            };
            let ts_event = UnixNanos::from(u64::try_from(
                row.executed_at.parse::<jiff::Timestamp>()?.as_nanosecond(),
            )?);
            let mut identity = [0; 64];
            identity[..32].copy_from_slice(hash.as_slice());
            identity[32..].copy_from_slice(tx_hash.as_slice());
            let digest = alloy_primitives::keccak256(identity);
            observations.push(Self {
                trade_id: TradeId::new(hex::encode(&digest.as_slice()[..16])),
                shares_wei,
                quantity,
                price: Price::from_decimal_dp(price, FIXED_PRECISION)?,
                commission,
                commission_shares,
                liquidity,
                ts_event,
            });
        }
        observations.sort_by(|left, right| {
            left.ts_event
                .cmp(&right.ts_event)
                .then_with(|| left.trade_id.as_str().cmp(right.trade_id.as_str()))
        });
        Ok(observations)
    }
}

fn order_amounts(
    receipt: &serde_json::Value,
    exchange: Address,
    order: B256,
    wallet: Address,
    token: U256,
) -> anyhow::Result<(Decimal, Decimal)> {
    let logs = receipt["logs"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Predict settlement receipt has no logs"))?;
    let mut shares = Decimal::ZERO;
    let mut collateral = Decimal::ZERO;
    for log in logs {
        let address = log["address"]
            .as_str()
            .and_then(|address| address.parse::<Address>().ok());
        if address != Some(exchange) {
            continue;
        }
        let topics = log["topics"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("receipt log has no topics"))?;
        let topic = |index: usize| {
            topics
                .get(index)
                .and_then(|value| value.as_str())
                .and_then(|value| value.parse::<B256>().ok())
        };
        if topic(0) != Some(OrderFilled::SIGNATURE_HASH) || topic(1) != Some(order) {
            continue;
        }
        let indexed = (0..4)
            .map(|i| topic(i).ok_or_else(|| anyhow::anyhow!("invalid Predict OrderFilled topics")))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let bytes = hex::decode(
            log["data"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing OrderFilled data"))?
                .trim_start_matches("0x"),
        )?;
        let event = OrderFilled::decode_raw_log_validate(indexed, &bytes)?;
        anyhow::ensure!(
            event.maker == wallet,
            "Predict fill belongs to another wallet"
        );
        let (share_amount, collateral_amount) = match (event.makerAssetId, event.takerAssetId) {
            (maker, taker) if maker.is_zero() && taker == token => {
                (event.takerAmountFilled, event.makerAmountFilled)
            }
            (maker, taker) if maker == token && taker.is_zero() => {
                (event.makerAmountFilled, event.takerAmountFilled)
            }
            _ => anyhow::bail!("Predict order fill asset IDs contradict the order token"),
        };
        let decimal = |value: U256| value.to_string().parse::<Decimal>();
        shares = shares
            .checked_add(decimal(share_amount)?)
            .ok_or_else(|| anyhow::anyhow!("Predict shares exceed the supported range"))?;
        collateral = collateral
            .checked_add(decimal(collateral_amount)?)
            .ok_or_else(|| anyhow::anyhow!("Predict collateral exceeds the supported range"))?;
    }
    anyhow::ensure!(
        shares > Decimal::ZERO,
        "Predict receipt has no fill for {order}"
    );
    Ok((shares, collateral))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn receipt_is_scoped_to_exchange_wallet_order_and_outcome() {
        let exchange = Address::repeat_byte(0x11);
        let wallet = Address::repeat_byte(0x22);
        let order = B256::repeat_byte(0x33);
        let token = U256::from(42);
        let fill = OrderFilled {
            orderHash: order,
            maker: wallet,
            taker: Address::ZERO,
            makerAssetId: U256::ZERO,
            takerAssetId: token,
            makerAmountFilled: U256::from(100),
            takerAmountFilled: U256::from(200),
            fee: U256::ZERO,
        };
        let log = |address: Address, fill: &OrderFilled| {
            let topics: Vec<_> = fill
                .encode_topics()
                .iter()
                .map(|topic| format!("{:#x}", topic.0))
                .collect();
            serde_json::json!({
                "address": format!("{address:#x}"),
                "topics": topics,
                "data": format!("0x{}", hex::encode(fill.encode_data()))
            })
        };
        let receipt = serde_json::json!({"logs": [
            log(Address::repeat_byte(0x44), &fill), log(exchange, &fill)
        ]});
        assert_eq!(
            order_amounts(&receipt, exchange, order, wallet, token).unwrap(),
            (Decimal::from(200), Decimal::from(100))
        );
        assert!(order_amounts(&receipt, exchange, order, wallet, U256::from(43)).is_err());
        assert!(order_amounts(&receipt, exchange, B256::ZERO, wallet, token).is_err());
    }
}
