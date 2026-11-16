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

//! Binary-option settlement from authoritative contract-expiration closes.
//!
//! The [`ExecutionEngine`](super::ExecutionEngine) applies these rules to closes it receives, and
//! event-store cache replay applies the same rules to captured closes.

use ahash::AHashMap;
use nautilus_common::cache::Cache;
use nautilus_core::UUID4;
use nautilus_model::{
    data::InstrumentClose,
    enums::InstrumentCloseType,
    events::PositionClosed,
    identifiers::{AccountId, InstrumentId, PositionId},
    instruments::{Instrument, InstrumentAny},
    position::Position,
    types::{Currency, Money, Quantity},
};
use rust_decimal::Decimal;

/// Settles every open position in the binary option named by a contract-expiration `close`.
///
/// The first contract-expiration close cached for the instrument is authoritative. A later close
/// settles any position still open on the cached terms, and a conflicting price is logged. Returns
/// a transient copy of each settled position, without stored history, with the quantity the
/// settlement closed. The list is empty when `close` is not a contract expiration for a cached
/// binary option. A position that cannot be settled is logged and skipped.
///
/// # Errors
///
/// Returns an error if caching the close fails.
pub fn settle_instrument_close(
    cache: &mut Cache,
    close: InstrumentClose,
) -> anyhow::Result<Vec<(Position, Quantity)>> {
    let instrument_id = close.instrument_id;

    if close.close_type != InstrumentCloseType::ContractExpired
        || !matches!(
            cache.instrument(&instrument_id),
            Some(InstrumentAny::BinaryOption(_))
        )
    {
        return Ok(Vec::new());
    }

    let close = if let Some(settled) = settlement_close(cache, &instrument_id) {
        if settled.close_price != close.close_price {
            log::error!(
                "Ignoring conflicting instrument close for {instrument_id}: settled at {}, received {}",
                settled.close_price,
                close.close_price,
            );
        }

        settled
    } else {
        cache.add_instrument_close(close)?;
        close
    };

    let positions: Vec<(PositionId, Quantity)> = cache
        .positions_open(None, Some(&instrument_id), None, None, None)
        .iter()
        .map(|position| (position.id, position.quantity))
        .collect();
    let mut settled = Vec::with_capacity(positions.len());

    for (position_id, last_qty) in positions {
        match cache.update_position_from_instrument_close(position_id, close) {
            Ok(position) => settled.push((position, last_qty)),
            Err(e) => log::error!("Cannot settle position {position_id}: {e}"),
        }
    }

    Ok(settled)
}

/// Returns the contract-expiration close that settled binary option `instrument_id`, if any.
#[must_use]
pub fn settlement_close(cache: &Cache, instrument_id: &InstrumentId) -> Option<InstrumentClose> {
    if !matches!(
        cache.instrument(instrument_id),
        Some(InstrumentAny::BinaryOption(_))
    ) {
        return None;
    }

    cache
        .instrument_close(instrument_id)
        .copied()
        .filter(|close| close.close_type == InstrumentCloseType::ContractExpired)
}

/// The fill-free settlement outcome of a binary-option contract expiration
/// (`POLYMARKET_PREDICT` §6.3 A): per-account cash proceeds and per-position
/// `PositionClosed` events, ready for the consumer's own account-adjust and
/// publication steps.
#[derive(Debug, Default)]
pub struct BinaryCloseSettlement {
    /// `quantity × payout` aggregated per benefiting account.
    pub proceeds_by_account: AHashMap<AccountId, Money>,
    /// One event per settled position.
    pub position_events: Vec<PositionClosed>,
}

/// Aggregates `quantity × payout` per benefiting account across `positions`.
///
/// # Errors
///
/// Returns an error when a position's proceeds overflow the quote-currency
/// [`Money`] range or an account's aggregate overflows.
fn proceeds_by_account(
    positions: &[(PositionId, AccountId, Quantity)],
    payout: Decimal,
    quote: Currency,
) -> anyhow::Result<AHashMap<AccountId, Money>> {
    let mut proceeds_by_account: AHashMap<AccountId, Money> = AHashMap::new();

    for (position_id, account_id, last_qty) in positions {
        // Overflow fails the settlement loudly — money never vanishes.
        let amount = last_qty
            .as_decimal()
            .checked_mul(payout)
            .and_then(|value| Money::from_decimal(value, quote).ok())
            .ok_or_else(|| anyhow::anyhow!("settlement proceeds overflow for position {position_id}"))?;
        let entry = proceeds_by_account.entry(*account_id);
        match entry {
            std::collections::hash_map::Entry::Occupied(mut occupied) => {
                let sum = occupied.get().checked_add(amount).ok_or_else(|| {
                    anyhow::anyhow!("Settlement proceeds overflowed account {account_id}")
                })?;
                occupied.insert(sum);
            }
            std::collections::hash_map::Entry::Vacant(vacant) => {
                vacant.insert(amount);
            }
        }
    }

    Ok(proceeds_by_account)
}

/// Settles a certified binary-option close without trading artifacts and
/// returns what still must be booked by the consumer (`POLYMARKET_PREDICT`
/// §6.3 A — the shared path of the backtest `SimulatedExchange` and the
/// sandbox execution client).
///
/// Shares close their economic position via the position's own
/// instrument-close application (realized PnL from the open basis and the
/// certified payout — no fills, no fee model), cash proceeds are computed as
/// `quantity × payout` in the instrument's quote currency, and a
/// `PositionClosed` event is created per position. The proceeds arithmetic is
/// pre-flighted on the open positions BEFORE any cache side effect — an
/// overflow bails with every position still open, so the caller's retry keeps
/// its settlement material. A repeated identical close settles nothing further
/// (idempotence via the cached close and the absence of open positions); a
/// conflicting close for an already-settled instrument fails loudly.
///
/// # Errors
///
/// Returns an error when the close conflicts with an already-settled
/// instrument, settling the positions fails, or proceeds arithmetic overflows.
pub fn settle_binary_close(
    cache: &mut Cache,
    close: InstrumentClose,
    ts_init: nautilus_core::UnixNanos,
) -> anyhow::Result<BinaryCloseSettlement> {
    let instrument_id = close.instrument_id;

    // Idempotence / conflict gate on the cached close.
    if let Some(settled) = settlement_close(cache, &instrument_id)
        && settled.close_price != close.close_price
    {
        anyhow::bail!(
            "Conflicting instrument close for {instrument_id}: settled at {}, received {}",
            settled.close_price,
            close.close_price
        );
    }

    let open: Vec<(PositionId, AccountId, Quantity)> = cache
        .positions_open(None, Some(&instrument_id), None, None, None)
        .iter()
        .map(|position| (position.id, position.account_id, position.quantity))
        .collect();

    if open.is_empty() {
        // Nothing to settle: cache the close (idempotence material) only.
        settle_instrument_close(cache, close)?;
        return Ok(BinaryCloseSettlement::default());
    }

    let quote = cache
        .instrument(&instrument_id)
        .map(Instrument::quote_currency)
        .ok_or_else(|| anyhow::anyhow!("no instrument for settled close {instrument_id}"))?;
    let payout = close.close_price.as_decimal();

    // Pre-flight: every position's `quantity × payout` (and each account's
    // aggregate) must fit the quote currency before any position closes.
    proceeds_by_account(&open, payout, quote)?;

    // Commit: close the open positions and derive the settlement from what
    // actually closed. The arithmetic is already validated (and the settled
    // set is a subset of the validated open set), so this cannot fail between
    // side effects.
    let settled = settle_instrument_close(cache, close)?;
    let settled_positions: Vec<(PositionId, AccountId, Quantity)> = settled
        .iter()
        .map(|(position, last_qty)| (position.id, position.account_id, *last_qty))
        .collect();
    let proceeds = proceeds_by_account(&settled_positions, payout, quote)?;

    let position_events = settled
        .iter()
        .map(|(position, last_qty)| {
            PositionClosed::create_from_instrument_close(
                position,
                &close,
                *last_qty,
                UUID4::new(),
                ts_init,
            )
        })
        .collect();

    Ok(BinaryCloseSettlement {
        proceeds_by_account: proceeds,
        position_events,
    })
}
