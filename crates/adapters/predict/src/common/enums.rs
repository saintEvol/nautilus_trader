//! Wire enums for the Predict adapter (values pinned from the official
//! OpenAPI/AsyncAPI documents, 2026-11-12).

use serde::{Deserialize, Serialize};

/// Deployment environment (selects base URLs and chain).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PredictEnvironment {
    /// BNB Mainnet (chain id 56).
    #[default]
    Mainnet,
    /// BNB Testnet (chain id 97).
    Testnet,
}

/// Market trading status derived from visibility and deletion state
/// (wire: `OPEN | MATCHING_NOT_ENABLED | CANCEL_ONLY | CLOSED`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PredictTradingStatus {
    #[serde(rename = "OPEN")]
    Open,
    #[serde(rename = "MATCHING_NOT_ENABLED")]
    MatchingNotEnabled,
    #[serde(rename = "CANCEL_ONLY")]
    CancelOnly,
    #[serde(rename = "CLOSED")]
    Closed,
}

/// Market lifecycle state from registration through resolution
/// (wire: `REGISTERED | PRICE_PROPOSED | PRICE_DISPUTED | PAUSED | UNPAUSED |
/// RESOLVED | REMOVED`; `REMOVED` is synthetic for withdrawn markets).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PredictMarketStatus {
    #[serde(rename = "REGISTERED")]
    Registered,
    #[serde(rename = "PRICE_PROPOSED")]
    PriceProposed,
    #[serde(rename = "PRICE_DISPUTED")]
    PriceDisputed,
    #[serde(rename = "PAUSED")]
    Paused,
    #[serde(rename = "UNPAUSED")]
    Unpaused,
    #[serde(rename = "RESOLVED")]
    Resolved,
    #[serde(rename = "REMOVED")]
    Removed,
}

/// `GET /markets` status filter (paused markets only appear unfiltered).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PredictMarketStatusFilter {
    #[serde(rename = "OPEN")]
    Open,
    #[serde(rename = "RESOLVED")]
    Resolved,
}

/// Resolution status of a market outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PredictOutcomeStatus {
    #[serde(rename = "WON")]
    Won,
    #[serde(rename = "LOST")]
    Lost,
}

/// Order quote side (wire: `Bid | Ask`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PredictQuoteType {
    #[serde(rename = "Bid")]
    Bid,
    #[serde(rename = "Ask")]
    Ask,
}

/// Binary market outcome side (wire: `Yes | No`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PredictOutcomeSide {
    #[serde(rename = "Yes")]
    Yes,
    #[serde(rename = "No")]
    No,
}

/// Which asset a fee was charged in (wire: `COLLATERAL | SHARES`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PredictFeeType {
    #[serde(rename = "COLLATERAL")]
    Collateral,
    #[serde(rename = "SHARES")]
    Shares,
}

/// Order strategy (wire: `LIMIT | MARKET`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PredictOrderKind {
    #[serde(rename = "LIMIT")]
    Limit,
    #[serde(rename = "MARKET")]
    Market,
}

/// Timeseries metric (the only documented value today).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PredictMetric {
    #[serde(rename = "chance")]
    Chance,
}

impl PredictMetric {
    /// Returns the wire representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Chance => "chance",
        }
    }
}

/// Timeseries sampling bucket (wire: `1m | 5m | 1h | 1d | 1w | 1M`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PredictTimeseriesResolution {
    #[serde(rename = "1m")]
    M1,
    #[serde(rename = "5m")]
    M5,
    #[serde(rename = "1h")]
    H1,
    #[serde(rename = "1d")]
    D1,
    #[serde(rename = "1w")]
    W1,
    #[serde(rename = "1M")]
    Mo1,
}

impl PredictTimeseriesResolution {
    /// Bucket length in seconds (calendar months are approximated as 30
    /// days for window arithmetic — the server keeps authority over actual
    /// down-sampling and returns the real resolution in every response).
    #[must_use]
    pub const fn seconds(self) -> i64 {
        match self {
            Self::M1 => 60,
            Self::M5 => 300,
            Self::H1 => 3_600,
            Self::D1 => 86_400,
            Self::W1 => 604_800,
            Self::Mo1 => 2_592_000,
        }
    }

    /// Parses the resolution string the server reported (`resolution`
    /// fields carry these verbatim).
    ///
    /// # Errors
    ///
    /// Returns an error naming the unknown value.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "1m" => Ok(Self::M1),
            "5m" => Ok(Self::M5),
            "1h" => Ok(Self::H1),
            "1d" => Ok(Self::D1),
            "1w" => Ok(Self::W1),
            "1M" => Ok(Self::Mo1),
            other => Err(format!("unknown timeseries resolution {other:?}")),
        }
    }

    /// Returns the wire representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::H1 => "1h",
            Self::D1 => "1d",
            Self::W1 => "1w",
            Self::Mo1 => "1M",
        }
    }
}

/// `GET /series/{slug}/rounds` filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PredictRoundStatusFilter {
    #[serde(rename = "ACTIVE")]
    Active,
    #[serde(rename = "ENDED")]
    Ended,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_values_pin_the_official_enums() {
        assert_eq!(
            serde_json::to_string(&PredictTradingStatus::MatchingNotEnabled).unwrap(),
            "\"MATCHING_NOT_ENABLED\""
        );
        assert_eq!(
            serde_json::to_string(&PredictMarketStatus::PriceDisputed).unwrap(),
            "\"PRICE_DISPUTED\""
        );
        assert_eq!(
            serde_json::to_string(&PredictQuoteType::Ask).unwrap(),
            "\"Ask\""
        );
        assert_eq!(
            serde_json::to_string(&PredictFeeType::Shares).unwrap(),
            "\"SHARES\""
        );
        assert_eq!(
            serde_json::to_string(&PredictTimeseriesResolution::Mo1).unwrap(),
            "\"1M\""
        );
        // Case-sensitive buckets: "1m" (minute) and "1M" (month) differ.
        assert_eq!(
            PredictTimeseriesResolution::parse("1m").unwrap(),
            PredictTimeseriesResolution::M1
        );
        assert_eq!(
            PredictTimeseriesResolution::parse("1M").unwrap(),
            PredictTimeseriesResolution::Mo1
        );
        assert!(PredictTimeseriesResolution::parse("2h").is_err());
    }
}
