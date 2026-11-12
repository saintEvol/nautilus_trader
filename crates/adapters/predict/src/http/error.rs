//! HTTP error type for the Predict adapter.
//!
//! The venue answers every error path with the structured `ErrorResponse`
//! body (`{success: false, code, error, message, timestamp}`), which is kept
//! alongside the HTTP status.

use std::time::Duration;

use nautilus_network::http::{HttpClientError, HttpResponse};
use serde::de::DeserializeOwned;
use thiserror::Error;

use super::models::PredictErrorDto;

/// Error type for Predict HTTP operations.
#[derive(Debug, Error)]
pub enum PredictHttpError {
    #[error("transport error: {0}")]
    Transport(String),

    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("timeout")]
    Timeout,

    #[error(
        "HTTP 429 rate limit on {endpoint}: retry_after={retry_after:?}: {message}"
    )]
    RateLimit {
        endpoint: &'static str,
        /// Delay derived from the real `RateLimit-Reset` response header when
        /// present (the venue does not promise `Retry-After`).
        retry_after: Option<Duration>,
        message: String,
    },

    #[error("HTTP {status} {code}: {message}")]
    Http {
        status: u16,
        /// Stable machine-readable venue error code (e.g. `unauthorized`,
        /// `invalid_cursor`).
        code: String,
        message: String,
    },

    #[error("pagination error: {0}")]
    Pagination(String),
}

impl PredictHttpError {
    /// Classifies a raw HTTP status + body into the error taxonomy.
    #[must_use]
    pub fn from_status(endpoint: &'static str, status: u16, body: &[u8]) -> Self {
        let venue = serde_json::from_slice::<PredictErrorDto>(body).ok();
        let message = venue
            .as_ref()
            .map_or_else(|| String::from_utf8_lossy(body).into_owned(), |e| e.message.clone());
        let code = venue.as_ref().map_or_else(String::new, |e| e.error.clone());
        match status {
            429 => Self::RateLimit {
                endpoint,
                retry_after: None,
                message,
            },
            _ => Self::Http {
                status,
                code,
                message,
            },
        }
    }

    /// Attaches a delay parsed from the `RateLimit-Reset` response header.
    #[must_use]
    pub fn with_retry_after(self, retry_after: Option<Duration>) -> Self {
        match (self, retry_after) {
            (
                Self::RateLimit {
                    endpoint,
                    message,
                    ..
                },
                retry_after,
            ) => Self::RateLimit {
                endpoint,
                retry_after,
                message,
            },
            (this, _) => this,
        }
    }

    /// Maps a transport-layer client error.
    #[must_use]
    pub fn from_http_client(error: HttpClientError) -> Self {
        match error {
            HttpClientError::TimeoutError(_) => Self::Timeout,
            error => Self::Transport(format!("HTTP client error: {error}")),
        }
    }

    /// Whether a retry attempt is worthwhile for this error.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) | Self::Timeout => true,
            Self::RateLimit { .. } => true,
            Self::Http { status, .. } => *status >= 500,
            Self::Serde(_) | Self::Pagination(_) => false,
        }
    }

    /// Parses the `RateLimit-Reset` header value into a delay.
    ///
    /// The venue documents `RateLimit-Reset` but not its unit; a value that
    /// looks like a unix epoch (seconds) is converted to a remaining delay,
    /// anything smaller is treated as a delta in seconds. The heuristic is
    /// registered as a live-sample follow-through.
    const EPOCH_FLOOR: i64 = 1_000_000_000;

    #[must_use]
    pub fn parse_rate_limit_reset(value: &str) -> Option<Duration> {
        let trimmed = value.trim();
        let seconds: i64 = trimmed.parse().ok()?;
        let delta_secs = if seconds > Self::EPOCH_FLOOR {
            seconds - Self::epoch_seconds()
        } else if seconds >= 0 {
            seconds
        } else {
            return None;
        };
        u64::try_from(delta_secs).ok().map(Duration::from_secs)
    }
}

impl PredictHttpError {
    fn epoch_seconds() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or_default()
    }
}

/// Decodes a response body: success statuses parse as `T`, error statuses
/// are classified into the taxonomy (venue `ErrorResponse` body preferred).
///
/// # Errors
///
/// Returns a [`PredictHttpError`] on non-success status or a body that does
/// not parse as `T`.
pub fn decode_response<T: DeserializeOwned>(
    endpoint: &'static str,
    response: &HttpResponse,
) -> Result<T, PredictHttpError> {
    if response.status.is_success() {
        serde_json::from_slice(&response.body).map_err(PredictHttpError::Serde)
    } else {
        Err(PredictHttpError::from_status(
            endpoint,
            response.status.as_u16(),
            &response.body,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_structured_venue_error() {
        let body = br#"{"success":false,"code":401,"error":"unauthorized","message":"Missing API key","timestamp":"2026-11-12T00:00:00Z"}"#;
        let error = PredictHttpError::from_status("/markets", 401, body);
        match &error {
            PredictHttpError::Http { status, code, .. } => {
                assert_eq!(*status, 401);
                assert_eq!(code, "unauthorized");
            }
            other => panic!("expected Http, got {other:?}"),
        }
        assert!(!error.is_retryable());
    }

    #[test]
    fn rate_limit_carries_reset_header_delay() {
        let error = PredictHttpError::from_status(
            "/markets",
            429,
            br#"{"success":false,"code":429,"error":"too_many_requests","message":"Slow down","timestamp":"2026-11-12T00:00:00Z"}"#,
        );
        let error = error.with_retry_after(Some(Duration::from_secs(3)));
        match error {
            PredictHttpError::RateLimit { retry_after, .. } => {
                assert_eq!(retry_after, Some(Duration::from_secs(3)));
            }
            other => panic!("expected RateLimit, got {other:?}"),
        }
    }

    #[test]
    fn server_errors_are_retryable() {
        let error = PredictHttpError::from_status("/markets", 503, b"unavailable");
        assert!(error.is_retryable());
    }

    #[test]
    fn reset_header_epoch_and_delta_both_parse() {
        let delta = PredictHttpError::parse_rate_limit_reset("7");
        assert_eq!(delta, Some(Duration::from_secs(7)));
        // Far-future unix epoch (seconds) resolves to a bounded delay.
        let now = PredictHttpError::epoch_seconds();
        let epoch = PredictHttpError::parse_rate_limit_reset(&(now + 60).to_string());
        assert!(epoch.is_some());
        assert!(epoch.unwrap().as_secs() <= 61);
        assert!(PredictHttpError::parse_rate_limit_reset("nope").is_none());
    }
}
