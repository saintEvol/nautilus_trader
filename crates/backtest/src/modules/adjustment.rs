//! Joint cash/inventory adjustment batches (`POLYMARKET_PREDICT` §6.3 C).
//!
//! The types, conservation constructors and structural validation moved to
//! [`nautilus_common::adjustment`] so the sandbox execution client can run
//! the identical semantics on live-paper state (§8.5(e)); this path is the
//! stable backtest API re-export (zero break for existing importers of
//! `nautilus_backtest::modules::adjustment::*` / `modules::SimulationAdjustmentBatch`).

pub use nautilus_common::adjustment::{
    AdjustmentAction, AdjustmentSource, OutcomeAdjustment, SimulationAdjustmentBatch,
};
