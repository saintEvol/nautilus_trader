//! Configuration for the Predict data client.
//!
//! The data API key is only ever consumed from this explicit configuration
//! — no environment-variable fallback exists in the adapter (the platform
//! assembly passes resolved key material explicitly; CLI from-env helpers,
//! if ever needed, belong to the application entry point).

use std::collections::HashMap;

use bon::Builder;
use nautilus_common::config::{ConfigError, ConfigResult};
use nautilus_core::string::secret::SecretString;
use nautilus_model::identifiers::InstrumentId;
use serde::{Deserialize, Serialize};

use crate::common::enums::PredictEnvironment;

/// Default interval between public-trades polls.
pub const DEFAULT_TRADES_POLL_INTERVAL_SECS: u64 = 5;
/// Default overlap window re-scanned each poll to absorb late-arriving
/// rows; dedup keeps every economic trade single.
pub const DEFAULT_TRADES_POLL_OVERLAP_MS: i64 = 30_000;
/// Default page budget per poll tick (bounded incremental walk).
pub const DEFAULT_TRADES_POLL_MAX_PAGES: u32 = 4;
/// Default interval between full market-catalog refreshes.
pub const DEFAULT_UPDATE_INSTRUMENTS_INTERVAL_MINS: u64 = 60;

/// Configuration for [`crate::data::PredictDataClient`].
#[derive(Clone, Debug, Builder, Deserialize, Serialize)]
#[builder(on(String, into))]
#[serde(default, deny_unknown_fields)]
pub struct PredictDataClientConfig {
    /// Data API key (`x-api-key`). Required: the public data endpoints and
    /// the WS handshake reject requests without it.
    pub api_key: Option<SecretString>,
    /// Deployment environment (selects default base URLs).
    #[builder(default)]
    #[serde(default)]
    pub environment: PredictEnvironment,
    /// Overrides the HTTP base URL (must already include `/v1`).
    pub base_url_http: Option<String>,
    /// Overrides the WebSocket URL.
    pub base_url_ws: Option<String>,
    /// Optional HTTP/WS proxy URL.
    pub proxy_url: Option<SecretString>,
    /// HTTP request timeout per attempt.
    #[builder(default = 60)]
    #[serde(default = "default_http_timeout")]
    pub http_timeout_secs: u64,
    /// WS command acknowledgment / ready timeout.
    #[builder(default = 30)]
    #[serde(default = "default_ws_timeout")]
    pub ws_timeout_secs: u64,
    /// WS connect timeout.
    #[builder(default = 10)]
    #[serde(default = "default_ws_connect_timeout")]
    pub ws_connect_timeout_secs: u64,
    /// The instruments this client is wired for; both legs of a market are
    /// resolved through the shared market context.
    #[builder(default)]
    #[serde(default)]
    pub instrument_ids: Vec<InstrumentId>,
    /// Interval between public-trades polls per market.
    #[builder(default = DEFAULT_TRADES_POLL_INTERVAL_SECS)]
    #[serde(default = "default_trades_poll_interval")]
    pub trades_poll_interval_secs: u64,
    /// Overlap window re-scanned per trades poll (milliseconds).
    #[builder(default = DEFAULT_TRADES_POLL_OVERLAP_MS)]
    #[serde(default = "default_trades_poll_overlap")]
    pub trades_poll_overlap_ms: i64,
    /// Page budget per trades poll.
    #[builder(default = DEFAULT_TRADES_POLL_MAX_PAGES)]
    #[serde(default = "default_trades_poll_max_pages")]
    pub trades_poll_max_pages: u32,
    /// Interval between market-catalog refreshes (minutes).
    #[builder(default = DEFAULT_UPDATE_INSTRUMENTS_INTERVAL_MINS)]
    #[serde(default = "default_update_instruments_interval")]
    pub update_instruments_interval_mins: u64,
    /// Page budget for instrument discovery walks.
    #[builder(default = 100)]
    #[serde(default = "default_discovery_max_pages")]
    pub discovery_max_pages: u32,
}

fn default_http_timeout() -> u64 {
    60
}

fn default_ws_timeout() -> u64 {
    30
}

fn default_ws_connect_timeout() -> u64 {
    10
}

fn default_trades_poll_interval() -> u64 {
    DEFAULT_TRADES_POLL_INTERVAL_SECS
}

fn default_trades_poll_overlap() -> i64 {
    DEFAULT_TRADES_POLL_OVERLAP_MS
}

fn default_trades_poll_max_pages() -> u32 {
    DEFAULT_TRADES_POLL_MAX_PAGES
}

fn default_update_instruments_interval() -> u64 {
    DEFAULT_UPDATE_INSTRUMENTS_INTERVAL_MINS
}

fn default_discovery_max_pages() -> u32 {
    100
}

impl Default for PredictDataClientConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            environment: PredictEnvironment::Mainnet,
            base_url_http: None,
            base_url_ws: None,
            proxy_url: None,
            http_timeout_secs: 60,
            ws_timeout_secs: 30,
            ws_connect_timeout_secs: 10,
            instrument_ids: Vec::new(),
            trades_poll_interval_secs: DEFAULT_TRADES_POLL_INTERVAL_SECS,
            trades_poll_overlap_ms: DEFAULT_TRADES_POLL_OVERLAP_MS,
            trades_poll_max_pages: DEFAULT_TRADES_POLL_MAX_PAGES,
            update_instruments_interval_mins: DEFAULT_UPDATE_INSTRUMENTS_INTERVAL_MINS,
            discovery_max_pages: 100,
        }
    }
}

impl PredictDataClientConfig {
    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the API key is missing, URLs are invalid, or
    /// a numeric field is out of range.
    pub fn validate(&self) -> ConfigResult<()> {
        if self.api_key.is_none() {
            return Err(ConfigError::invalid_value(
                "api_key",
                "a predict.fun data API key is required (public-data key; no trading authority)",
            ));
        }
        // Production endpoints are TLS-only; plain http/ws is allowed
        // exclusively for loopback overrides (offline test peers) — the
        // host is parsed and compared exactly, so prefix look-alikes
        // (127.0.0.1.evil.com) cannot capture the API key over plaintext.
        for (field, url, scheme) in [
            ("base_url_http", &self.base_url_http, "https://"),
            ("base_url_ws", &self.base_url_ws, "wss://"),
        ] {
            let Some(url) = url else { continue };
            if url.starts_with(scheme) {
                continue;
            }
            let parsed = url::Url::parse(url)
                .map_err(|_| ConfigError::invalid_value(field, "must be a valid URL"))?;
            let loopback = matches!(
                parsed.host_str(),
                Some("127.0.0.1") | Some("localhost") | Some("::1")
            );
            if !loopback {
                return Err(ConfigError::invalid_value(
                    field,
                    "must be an https://wss:// URL (plain schemes only for loopback)",
                ));
            }
        }
        if self.http_timeout_secs == 0 || self.ws_timeout_secs == 0 || self.ws_connect_timeout_secs == 0
        {
            return Err(ConfigError::invalid_value(
                "timeouts",
                "must be nonzero",
            ));
        }
        if self.trades_poll_interval_secs == 0 {
            return Err(ConfigError::invalid_value(
                "trades_poll_interval_secs",
                "must be nonzero",
            ));
        }
        if self.trades_poll_overlap_ms < 0 {
            return Err(ConfigError::invalid_value(
                "trades_poll_overlap_ms",
                "must not be negative",
            ));
        }
        if self.trades_poll_max_pages == 0 {
            return Err(ConfigError::invalid_value(
                "trades_poll_max_pages",
                "must be nonzero",
            ));
        }
        if self.discovery_max_pages == 0 {
            return Err(ConfigError::invalid_value(
                "discovery_max_pages",
                "must be nonzero",
            ));
        }
        Ok(())
    }

    /// The effective HTTP base URL.
    #[must_use]
    pub fn http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| crate::common::consts::http_base_url(self.environment).to_string())
    }

    /// The effective WebSocket URL.
    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| crate::common::consts::websocket_url(self.environment).to_string())
    }

    /// The resolved instrument ids with duplicates removed.
    #[must_use]
    pub fn unique_instrument_ids(&self) -> Vec<InstrumentId> {
        let mut seen = HashMap::new();
        self.instrument_ids
            .iter()
            .filter(|id| seen.insert(**id, ()).is_none())
            .copied()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> PredictDataClientConfig {
        PredictDataClientConfig::builder()
            .api_key(SecretString::from("predict-data-key"))
            .build()
    }

    #[test]
    fn missing_api_key_is_rejected() {
        let error = PredictDataClientConfig::default()
            .validate()
            .expect_err("api key is required");
        assert!(error.to_string().contains("api_key"), "{error}");
    }

    #[test]
    fn defaults_validate_and_resolve_mainnet_urls() {
        let config = config();
        config.validate().unwrap();
        assert_eq!(config.http_url(), "https://api.predict.fun/v1");
        assert_eq!(config.ws_url(), "wss://ws.predict.fun/ws");
    }

    #[test]
    fn testnet_urls_resolve() {
        let config = PredictDataClientConfig::builder()
            .api_key(SecretString::from("k"))
            .environment(PredictEnvironment::Testnet)
            .build();
        assert_eq!(config.http_url(), "https://api-testnet.predict.fun/v1");
        assert_eq!(config.ws_url(), "wss://ws-testnet.predict.fun/ws");
    }

    #[test]
    fn non_https_override_rejected() {
        let config = PredictDataClientConfig::builder()
            .api_key(SecretString::from("k"))
            .base_url_http("http://insecure".to_string())
            .build();
        assert!(config.validate().is_err());
    }

    #[test]
    fn duplicate_instrument_ids_collapse() {
        let id = InstrumentId::from("0xcond-1.PREDICT");
        let config = PredictDataClientConfig::builder()
            .api_key(SecretString::from("k"))
            .instrument_ids(vec![id, id])
            .build();
        assert_eq!(config.unique_instrument_ids().len(), 1);
    }
}
