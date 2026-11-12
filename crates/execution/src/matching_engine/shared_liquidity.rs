//! Per-market shared liquidity budget (`POLYMARKET_PREDICT` §6.3 D).
//!
//! A Predict market streams ONE real book: the Yes/No instruments are two
//! projected views of it, so their simulated depth budgets must be one
//! budget, not one per leg. The budget identity is
//! `(simulation venue instance, market_id, native side, native price)` —
//! snapshotted frames and sequence numbers never enter it: unchanged
//! levels keep their consumed quantity, and a re-appearing level starts
//! fresh only because the snapshot confirmed its absence in between.
//!
//! The native axis is the Yes axis. A leg whose prices are complemented
//! (`No bid q ⇔ Yes ask 1−q`) maps every consumption onto the native axis
//! by flipping the side and complementing the price at the market
//! precision — both exact integer operations on raw prices.

use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use nautilus_model::enums::OrderSide;

use super::{PriceRaw, QuantityRaw};

/// The per-level budget record: `(original_size, consumed)`.
pub type LevelBudget = (QuantityRaw, QuantityRaw);

type NativeMaps = IndexMap<OrderSide, IndexMap<PriceRaw, LevelBudget>>;

/// The shared, thread-less budget store for one market. Cloned handles
/// share the same budget; only an exchange `reset` clears it wholesale.
///
/// Size verbatim requirement: the legs' books must be exact projections of
/// one frame with sizes carried verbatim (the `project_no_book` contract).
/// The original-size reconciliation sees each leg's level size in turn —
/// any non-verbatim projection alternates sizes and silently resets the
/// whole market budget.
#[derive(Clone, Debug)]
pub struct SharedMarketLiquidity {
    maps: Arc<Mutex<NativeMaps>>,
}

impl SharedMarketLiquidity {
    /// Creates an empty shared budget for one market.
    #[must_use]
    pub fn new() -> Self {
        Self {
            maps: Arc::new(Mutex::new(IndexMap::from([
                (OrderSide::Buy, IndexMap::new()),
                (OrderSide::Sell, IndexMap::new()),
            ]))),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, NativeMaps> {
        self.maps
            .lock()
            .expect("shared liquidity lock is not poisoned")
    }

    /// Records the journal prior for `(side, price)` without consuming.
    pub fn prior(&self, side: OrderSide, price: PriceRaw) -> Option<LevelBudget> {
        self.lock().get(&side)?.get(&price).copied()
    }

    /// Consumes from `(side, price)` with the original-size reconciliation
    /// semantics of the engine-private maps: a changed level size resets
    /// the budget (fresh data), an unchanged size keeps its consumed
    /// quantity.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn consume(
        &self,
        side: OrderSide,
        price: PriceRaw,
        level_size: QuantityRaw,
        qty: QuantityRaw,
        remaining: QuantityRaw,
    ) -> QuantityRaw {
        let mut maps = self.lock();
        let map = maps.get_mut(&side).expect("side map is always present");
        let (original_size, consumed) = map.entry(price).or_insert((level_size, 0));
        if *original_size != level_size {
            *original_size = level_size;
            *consumed = 0;
        }
        let available = original_size.saturating_sub(*consumed);
        let adjusted = qty.min(available).min(remaining);
        *consumed += adjusted;
        adjusted
    }

    /// Restores a journaled prior (`None` removes the level) — FOK
    /// failure rollback on the shared budget.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn restore(&self, side: OrderSide, price: PriceRaw, prior: Option<LevelBudget>) {
        let mut maps = self.lock();
        let map = maps.get_mut(&side).expect("side map is always present");
        match prior {
            Some(prior) => {
                map.insert(price, prior);
            }
            None => {
                map.shift_remove(&price);
            }
        }
    }

    /// Consumes an externally observed trade level (reconciling the level
    /// size first), mirroring `consume_trade_level`.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn consume_level(
        &self,
        side: OrderSide,
        price: PriceRaw,
        level_size: QuantityRaw,
        remaining: &mut QuantityRaw,
    ) {
        let mut maps = self.lock();
        let map = maps.get_mut(&side).expect("side map is always present");
        let entry = map.entry(price).or_insert((level_size, 0));
        if entry.0 != level_size {
            entry.0 = level_size;
            entry.1 = 0;
        }
        let available = level_size.saturating_sub(entry.1);
        let consume = (*remaining).min(available);
        entry.1 += consume;
        *remaining -= consume;
    }

    /// Prices whose budget record exists but whose level is confirmed
    /// absent by the probing closure (snapshot-confirmed deletion).
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn absent_prices(
        &self,
        side: OrderSide,
        present: &impl Fn(PriceRaw) -> bool,
    ) -> Vec<PriceRaw> {
        self.lock()
            .get(&side)
            .expect("side map is always present")
            .keys()
            .filter(|price| !present(**price))
            .copied()
            .collect()
    }

    /// Removes a budget record (a re-appearing level is a fresh budget).
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn remove(&self, side: OrderSide, price: PriceRaw) {
        self.lock()
            .get_mut(&side)
            .expect("side map is always present")
            .shift_remove(&price);
    }

    /// Clears the whole budget (exchange `reset` only — reconnects and
    /// local batch ids never reset depth).
    pub fn clear(&self) {
        let mut maps = self.lock();
        for map in maps.values_mut() {
            map.clear();
        }
    }

    /// The consumed quantity recorded at `(side, price)`, if any.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn consumed_at(&self, side: OrderSide, price: PriceRaw) -> Option<QuantityRaw> {
        self.lock()
            .get(&side)
            .expect("side map is always present")
            .get(&price)
            .map(|(_, consumed)| *consumed)
    }
}

impl Default for SharedMarketLiquidity {
    fn default() -> Self {
        Self::new()
    }
}

/// Maps an engine-local consumption site onto the native axis: the
/// identity for a native-axis (Yes) leg; side flip + price complement for
/// a complemented (No) leg. `parity` is `1.0` in raw price units at the
/// market precision.
#[derive(Clone, Copy, Debug)]
pub struct NativeAxis {
    parity: PriceRaw,
    inverted: bool,
}

impl NativeAxis {
    /// Identity mapping (native-axis leg).
    #[must_use]
    pub const fn identity() -> Self {
        Self {
            parity: 0,
            inverted: false,
        }
    }

    /// Complementing mapping at the market precision.
    #[must_use]
    pub const fn complementing(parity: PriceRaw) -> Self {
        Self {
            parity,
            inverted: true,
        }
    }

    /// Whether this mapping complements prices.
    #[must_use]
    pub const fn inverted(&self) -> bool {
        self.inverted
    }

    /// The raw `1.0` of the mapping (0 for the identity leg).
    #[must_use]
    pub const fn parity(&self) -> PriceRaw {
        self.parity
    }

    /// Maps `(engine side, engine price)` to `(native side, native price)`.
    /// Prices live strictly inside `(0, 1)`, so the complement stays exact.
    #[must_use]
    pub fn map(&self, side: OrderSide, price: PriceRaw) -> (OrderSide, PriceRaw) {
        if !self.inverted {
            return (side, price);
        }
        let native_side = match side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };
        (native_side, self.parity - price)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complement_mapping_is_exact_and_side_flipping() {
        let axis = NativeAxis::complementing(1_000);
        // No-leg ask consumption at 0.40 ⇔ native Yes bid at 0.60.
        assert_eq!(axis.map(OrderSide::Sell, 400), (OrderSide::Buy, 600));
        assert_eq!(axis.map(OrderSide::Buy, 250), (OrderSide::Sell, 750));
        let identity = NativeAxis::identity();
        assert_eq!(identity.map(OrderSide::Buy, 400), (OrderSide::Buy, 400));
    }

    #[test]
    fn shared_budget_spends_once_across_legs() {
        let shared = SharedMarketLiquidity::new();
        // Native Yes ask at 0.60 with size 100: the Yes leg buys 60 …
        assert_eq!(shared.consume(OrderSide::Buy, 600, 100, 60, 100), 60);
        // … and the No leg selling at 0.40 (same native level) sees 40 left.
        assert_eq!(shared.consume(OrderSide::Buy, 600, 100, 60, 100), 40);
        assert_eq!(shared.consumed_at(OrderSide::Buy, 600), Some(100));
        // A changed level size resets the budget (fresh data).
        assert_eq!(shared.consume(OrderSide::Buy, 600, 200, 50, 100), 50);
    }

    #[test]
    fn journal_restore_and_absence_prune_round_trip() {
        let shared = SharedMarketLiquidity::new();
        let prior = shared.prior(OrderSide::Sell, 300);
        assert_eq!(shared.consume(OrderSide::Sell, 300, 80, 30, 80), 30);
        assert_eq!(shared.consumed_at(OrderSide::Sell, 300), Some(30));
        // Snapshot confirms the level gone → record removed; a re-appearance
        // is fresh.
        let absent = shared.absent_prices(OrderSide::Sell, &|price| price != 300);
        assert_eq!(absent, vec![300]);
        shared.remove(OrderSide::Sell, 300);
        assert_eq!(shared.consumed_at(OrderSide::Sell, 300), None);
        // Restore semantics (FOK rollback).
        shared.restore(OrderSide::Sell, 300, prior);
        assert_eq!(shared.consumed_at(OrderSide::Sell, 300), None);
        shared.consume(OrderSide::Sell, 300, 80, 10, 80);
        let prior = shared.prior(OrderSide::Sell, 300);
        shared.consume(OrderSide::Sell, 300, 80, 70, 80);
        shared.restore(OrderSide::Sell, 300, prior);
        assert_eq!(shared.consumed_at(OrderSide::Sell, 300), Some(10));
    }
}

/// The engine's depth-consumption budget: engine-private maps (default,
/// every instrument that owns its book) or one shared per-market budget
/// with native-axis mapping (projected legs of a common real book).
///
/// Side convention throughout: the INCOMING order side keys the map —
/// `OrderSide::Buy` consumes asks, `OrderSide::Sell` consumes bids.
#[derive(Debug)]
pub enum ConsumptionStore {
    /// Engine-private budgets (this instrument owns its book).
    Local {
        bids: IndexMap<PriceRaw, LevelBudget>,
        asks: IndexMap<PriceRaw, LevelBudget>,
    },
    /// One shared per-market budget; this leg maps onto the native axis.
    Shared {
        store: SharedMarketLiquidity,
        axis: NativeAxis,
    },
}

impl ConsumptionStore {
    /// A private, empty budget.
    #[must_use]
    pub fn local() -> Self {
        Self::Local {
            bids: IndexMap::new(),
            asks: IndexMap::new(),
        }
    }

    /// A shared budget with the given native-axis mapping.
    #[must_use]
    pub fn shared(store: SharedMarketLiquidity, axis: NativeAxis) -> Self {
        Self::Shared { store, axis }
    }

    fn local_map(
        &mut self,
        side: OrderSide,
    ) -> &mut IndexMap<PriceRaw, LevelBudget> {
        match (self, side) {
            (Self::Local { asks, .. }, OrderSide::Buy) => asks,
            (Self::Local { bids, .. }, OrderSide::Sell) => bids,
            (Self::Shared { .. }, _) => unreachable!("local_map on shared store"),
        }
    }

    fn local_map_ref(&self, side: OrderSide) -> &IndexMap<PriceRaw, LevelBudget> {
        match (self, side) {
            (Self::Local { asks, .. }, OrderSide::Buy) => asks,
            (Self::Local { bids, .. }, OrderSide::Sell) => bids,
            (Self::Shared { .. }, _) => unreachable!("local_map_ref on shared store"),
        }
    }

    /// The recorded budget prior at `(side, price)`, if any.
    pub fn prior(&self, side: OrderSide, price: PriceRaw) -> Option<LevelBudget> {
        match self {
            Self::Local { .. } => self.local_map_ref(side).get(&price).copied(),
            Self::Shared { store, axis } => {
                let (side, price) = axis.map(side, price);
                store.prior(side, price)
            }
        }
    }

    /// Consumes with the original-size reconciliation: a changed level
    /// size resets the budget (fresh data); an unchanged size keeps its
    /// consumed quantity.
    pub fn consume(
        &mut self,
        side: OrderSide,
        price: PriceRaw,
        level_size: QuantityRaw,
        qty: QuantityRaw,
        remaining: QuantityRaw,
    ) -> QuantityRaw {
        match self {
            Self::Local { .. } => {
                let (original_size, consumed) =
                    self.local_map(side).entry(price).or_insert((level_size, 0));
                if *original_size != level_size {
                    *original_size = level_size;
                    *consumed = 0;
                }
                let available = original_size.saturating_sub(*consumed);
                let adjusted = qty.min(available).min(remaining);
                *consumed += adjusted;
                adjusted
            }
            Self::Shared { store, axis } => {
                let (side, price) = axis.map(side, price);
                store.consume(side, price, level_size, qty, remaining)
            }
        }
    }

    /// Restores a journaled prior (`None` removes the record).
    pub fn restore(&mut self, side: OrderSide, price: PriceRaw, prior: Option<LevelBudget>) {
        match self {
            Self::Local { .. } => match prior {
                Some(prior) => {
                    self.local_map(side).insert(price, prior);
                }
                None => {
                    self.local_map(side).shift_remove(&price);
                }
            },
            Self::Shared { store, axis } => {
                let (side, price) = axis.map(side, price);
                store.restore(side, price, prior);
            }
        }
    }

    /// Consumes an externally observed trade level.
    pub fn consume_level(
        &mut self,
        side: OrderSide,
        price: PriceRaw,
        level_size: QuantityRaw,
        remaining: &mut QuantityRaw,
    ) {
        match self {
            Self::Local { .. } => {
                let entry = self.local_map(side).entry(price).or_insert((level_size, 0));
                if entry.0 != level_size {
                    entry.0 = level_size;
                    entry.1 = 0;
                }
                let available = level_size.saturating_sub(entry.1);
                let consume = (*remaining).min(available);
                entry.1 += consume;
                *remaining -= consume;
            }
            Self::Shared { store, axis } => {
                let (side, price) = axis.map(side, price);
                store.consume_level(side, price, level_size, remaining);
            }
        }
    }

    /// Prices whose budget record exists but whose level is confirmed
    /// absent (snapshot-confirmed deletion; re-appearance is fresh).
    pub fn absent_prices(
        &self,
        side: OrderSide,
        present: &impl Fn(PriceRaw) -> bool,
    ) -> Vec<PriceRaw> {
        // The probe closure speaks ENGINE-local prices; a shared store must
        // translate the absence verdict back onto the engine-local axis.
        match self {
            Self::Local { .. } => self
                .local_map_ref(side)
                .keys()
                .filter(|price| !present(**price))
                .copied()
                .collect(),
            Self::Shared { store, axis } => {
                let (native_side, _) = axis.map(side, 0);
                store
                    .absent_prices(native_side, &|native_price| {
                        // Map the native price back onto this engine's axis.
                        let engine_price = if axis.inverted() {
                            axis.parity() - native_price
                        } else {
                            native_price
                        };
                        present(engine_price)
                    })
                    .into_iter()
                    .map(|native_price| {
                        if axis.inverted() {
                            axis.parity() - native_price
                        } else {
                            native_price
                        }
                    })
                    .collect()
            }
        }
    }

    /// Removes a budget record.
    pub fn remove(&mut self, side: OrderSide, price: PriceRaw) {
        match self {
            Self::Local { .. } => {
                self.local_map(side).shift_remove(&price);
            }
            Self::Shared { store, axis } => {
                let (side, price) = axis.map(side, price);
                store.remove(side, price);
            }
        }
    }

    /// The engine-private bid map (engine tests and introspection; a
    /// shared budget is not addressable as one leg's local map).
    ///
    /// # Panics
    ///
    /// Panics on a shared store — inspect it via the native-axis API.
    pub fn bids(&self) -> &IndexMap<PriceRaw, LevelBudget> {
        match self {
            Self::Local { bids, .. } => bids,
            Self::Shared { .. } => panic!("shared liquidity has no local bid map"),
        }
    }

    /// The engine-private ask map, mutable (tests seeding budgets).
    ///
    /// # Panics
    ///
    /// Panics on a shared store.
    pub fn asks_mut(&mut self) -> &mut IndexMap<PriceRaw, LevelBudget> {
        match self {
            Self::Local { asks, .. } => asks,
            Self::Shared { .. } => panic!("shared liquidity has no local ask map"),
        }
    }

    /// The engine-private bid map, mutable (tests seeding budgets).
    ///
    /// # Panics
    ///
    /// Panics on a shared store.
    pub fn bids_mut(&mut self) -> &mut IndexMap<PriceRaw, LevelBudget> {
        match self {
            Self::Local { bids, .. } => bids,
            Self::Shared { .. } => panic!("shared liquidity has no local bid map"),
        }
    }

    /// The engine-private ask map (engine tests and introspection).
    ///
    /// # Panics
    ///
    /// Panics on a shared store.
    pub fn asks(&self) -> &IndexMap<PriceRaw, LevelBudget> {
        match self {
            Self::Local { asks, .. } => asks,
            Self::Shared { .. } => panic!("shared liquidity has no local ask map"),
        }
    }

    /// Clears the whole budget (engine/exchange `reset` only).
    pub fn clear(&mut self) {
        match self {
            Self::Local { bids, asks } => {
                bids.clear();
                asks.clear();
            }
            Self::Shared { store, .. } => store.clear(),
        }
    }
}
