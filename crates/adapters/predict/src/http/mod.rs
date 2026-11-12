//! HTTP client module for the Predict adapter.

pub mod client;
pub mod error;
pub mod models;
mod pagination;

pub use client::{ChanceWalk, MatchesWalk, PredictHttpClient, WalkEnd};
pub use error::{PredictHttpError, decode_response};
pub use models::{
    PredictBookLevelDto, PredictCategoryDto, PredictErrorDto, PredictMarketDto,
    PredictMarketQuoteDto, PredictMarketsQuery, PredictMatchesQuery, PredictOrderbookDto,
    PredictOutcomeDto, PredictPage, PredictQuoteLevelDto, PredictSeriesDto,
    PredictSettledOrderDto,
};
