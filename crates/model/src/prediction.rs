//! Prediction-market accounting primitives (`POLYMARKET_PREDICT` §6.3/§8.3 P0).
//!
//! Pure, offline-verifiable contracts shared by backtest settlement,
//! simulated split/merge/convert batches and live request checks. Live
//! execution always prefers the venue's reported fees and receipts; these
//! functions fix the deterministic simulated track and the validation rules.

mod fees;
mod market_book;
mod payout;
mod settlement;
mod split_merge;

pub use fees::{predict_taker_fee, predict_taker_fee_buy};
pub use market_book::{BookLevel, FrameStatus, PredictionFrameStaging, project_no_book};
pub use payout::{Payout, PAYOUT_LOSE, PAYOUT_HALF, PAYOUT_WIN};
pub use settlement::settle_outcome_position;
pub use split_merge::{convert_tokens, merge_leg_accounting, split_leg_accounting};
