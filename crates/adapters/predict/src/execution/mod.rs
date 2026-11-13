//! Predict order execution face (POLYMARKET_PREDICT §8.2).
//!
//! Layered: pure amount mathematics (`amounts`) is venue-contract-pinned
//! and offline-tested first; the EIP-712 request signing, wallet-events
//! stream and the nautilus `ExecutionClient` shell land on top in
//! subsequent commits.

pub mod amounts;
pub mod client;
pub mod lifecycle;
pub mod request;
