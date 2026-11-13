//! WebSocket client module for the Predict adapter.

pub mod client;
pub mod messages;
pub mod user;

pub use client::{PredictWsEvent, PredictWebSocketClient};
pub use messages::{
    PredictHeartbeatReply, PredictWsDecodeError, PredictWsMarketChanged, PredictWsMarketStatus,
    PredictWsMessage, PredictWsOrderbook, PredictWsRequest, PredictWsResolvedOutcome,
    PredictWsSettledOrder, PredictWsTopOfBook, PredictWsTradingStatus, decode_frame, market_topic,
    split_topic,
};
