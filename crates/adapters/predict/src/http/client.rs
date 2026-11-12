//! `PredictHttpClient` — typed REST transport over `nautilus_network`'s
//! [`HttpClient`] for the public data API (`https://api.predict.fun/v1`).
//!
//! Every request carries the `x-api-key` header (a data API key contains no
//! trading authority). Transient failures retry with bounded backoff; a 429
//! waits on the real `RateLimit-Reset` response header when present (the
//! venue does not promise `Retry-After`). List endpoints walk the cursor
//! protocol with structured completion — a page budget never masquerades as
//! exhaustion.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use nautilus_core::string::secret::SecretString;
use nautilus_network::http::{
    HttpClient, HttpClientError, HttpResponse, Method, create_standard_nautilus_headers,
};
use nautilus_network::retry::{RetryConfig, RetryError, RetryManager};
use nautilus_network::websocket::proxy::ProxyUrl;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::error::{PredictHttpError, decode_response};
use super::models::{
    PredictCategoryDto, PredictMarketDto, PredictMarketQuoteDto, PredictMarketsQuery,
    PredictMatchesQuery, PredictOrderbookDto, PredictPage, PredictSeriesDto,
};
use super::pagination::{Pages, advance};
use crate::common::consts::{
    API_KEY_HEADER, CATEGORIES_FIRST_MAX, MATCHES_FIRST_MAX, ORDERBOOKS_BATCH_MAX,
    QUOTES_BATCH_MAX, STATUS_LOGS_BATCH_MAX,
};
use crate::common::enums::{
    PredictMarketStatusFilter, PredictMetric, PredictRoundStatusFilter, PredictTimeseriesResolution,
};
use crate::dtos::{
    PredictChancePoint, PredictChanceSeriesEnvelope, PredictMatchData, PredictMatchesEnvelope,
    PredictStatusLogsEnvelope,
};

/// Response header whitelisted for the 429 wait (unit undocumented; parsed
/// as unix-epoch seconds or a delta — live-sample follow-through).
const HEADER_RATE_LIMIT_RESET: &str = "RateLimit-Reset";

const PATH_MARKETS: &str = "/markets";
const PATH_MARKETS_ORDERBOOKS: &str = "/markets/orderbooks";
const PATH_MARKETS_QUOTES: &str = "/markets/quotes";
const PATH_MARKETS_STATUS_LOGS: &str = "/markets/status-logs";
const PATH_CATEGORIES: &str = "/categories";
const PATH_SERIES: &str = "/series";
const PATH_MATCHES: &str = "/orders/matches";

/// How a bounded walk ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalkEnd {
    /// The source returned no further cursor.
    Exhausted,
    /// The caller's page budget ran out with more pages owed.
    PageBudget,
}

/// Structured outcome of a matches walk (rows + how it ended + resume).
#[derive(Clone, Debug)]
pub struct MatchesWalk {
    pub rows: Vec<PredictMatchData>,
    pub end: WalkEnd,
    pub next_cursor: Option<String>,
}

impl MatchesWalk {
    /// The stable identity of one match row: the off-chain settlement plus
    /// its transaction. Sub-fill granularity (order hashes) is preserved in
    /// the row itself for consumers that need it.
    #[must_use]
    pub fn row_identity(row: &PredictMatchData) -> (String, String) {
        (row.settlement_id.clone(), row.transaction_hash.clone())
    }
}

/// Structured outcome of a chance-timeseries walk. The server-reported
/// resolution is carried verbatim — down-sampling is the server's answer,
/// never the request.
#[derive(Clone, Debug)]
pub struct ChanceWalk {
    pub points: Vec<PredictChancePoint>,
    /// Actual sampling bucket the server used for this window.
    pub resolution: String,
    pub end: WalkEnd,
    pub next_cursor: Option<String>,
}

/// Typed REST client for the Predict public data API.
#[derive(Debug)]
pub struct PredictHttpClient {
    client: HttpClient,
    base_url: String,
    retry_manager: Arc<RetryManager<PredictHttpError>>,
}

impl PredictHttpClient {
    /// Creates a client for the explicit base URL (already including `/v1`)
    /// and data API key. The key is only ever consumed from this explicit
    /// configuration — no environment-variable fallback exists in the
    /// adapter.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying transport cannot be created.
    pub fn new(
        base_url: &str,
        api_key: &SecretString,
        timeout_secs: u64,
        proxy_url: Option<ProxyUrl>,
    ) -> Result<Self, HttpClientError> {
        let mut headers: HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert(
            API_KEY_HEADER.to_string(),
            api_key.expose_secret().to_string(),
        );
        Ok(Self {
            client: HttpClient::builder()
                .headers(headers)
                .header_keys(vec![HEADER_RATE_LIMIT_RESET.to_string()])
                .timeout_secs(timeout_secs)
                .maybe_proxy_url(proxy_url.map(|url| url.expose().to_string()))
                .build()?,
            base_url: base_url.trim_end_matches('/').to_string(),
            retry_manager: Arc::new(RetryManager::new(RetryConfig {
                max_retries: 2,
                initial_delay_ms: 250,
                max_delay_ms: 1_000,
                max_elapsed_ms: Some(timeout_secs.saturating_mul(3).saturating_add(2_000)),
                ..RetryConfig::default()
            })),
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    async fn send_get<T: DeserializeOwned, P: Serialize>(
        &self,
        endpoint: &'static str,
        path: &str,
        params: Option<&P>,
    ) -> Result<T, PredictHttpError> {
        let url = self.url(path);
        let retry_after: std::sync::Mutex<Option<std::time::Duration>> =
            std::sync::Mutex::new(None);
        Box::pin(
            self.retry_manager
                .invocation(
                    endpoint,
                    || {
                        let request = self.client.request_with_params(
                            Method::GET,
                            url.clone(),
                            params,
                            None,
                            None,
                            None,
                            None,
                        );
                        async {
                            *retry_after.lock().expect("retry_after poisoned") = None;
                            let response: HttpResponse = request
                                .await
                                .map_err(PredictHttpError::from_http_client)?;
                            *retry_after.lock().expect("retry_after poisoned") = response
                                .headers
                                .get(HEADER_RATE_LIMIT_RESET)
                                .and_then(|value| {
                                    PredictHttpError::parse_rate_limit_reset(value)
                                });
                            match decode_response::<T>(endpoint, &response) {
                                Err(error @ PredictHttpError::RateLimit { .. }) => Err(
                                    error.with_retry_after(
                                        *retry_after.lock().expect("retry_after poisoned"),
                                    ),
                                ),
                                other => other,
                            }
                        }
                    },
                    PredictHttpError::is_retryable,
                    |e| match e {
                        RetryError::OperationTimeout { .. } => PredictHttpError::Timeout,
                        RetryError::InvalidConfiguration { message } => {
                            PredictHttpError::Transport(message)
                        }
                        e => PredictHttpError::Transport(e.to_string()),
                    },
                )
                .retry_delay(&|_| {
                    *retry_after.lock().expect("retry_after poisoned")
                })
                .execute(),
        )
        .await
    }

    /// Generic cursor walk: fetch pages until the cursor runs out or the
    /// page budget does. Duplicate identities within a walk are dropped; a
    /// page of exclusively duplicates that still carries a cursor is a loud
    /// pagination error.
    async fn walk_pages<T, K, F, Fut>(
        &self,
        endpoint: &'static str,
        resume_from: Option<String>,
        max_pages: u32,
        mut fetch: F,
        identity: impl Fn(&T) -> K,
    ) -> Result<Pages<T>, PredictHttpError>
    where
        F: FnMut(Option<String>) -> Fut,
        Fut: Future<Output = Result<(Vec<T>, Option<String>), PredictHttpError>>,
        K: Hash + Eq + Clone,
    {
        let mut cursor = resume_from;
        let mut previous = None;
        let mut seen: HashSet<K> = HashSet::new();
        let mut records = Vec::new();
        let mut pages = 0u32;
        let complete;
        loop {
            if pages >= max_pages {
                complete = false;
                break;
            }
            let (rows, next) = fetch(cursor.take()).await?;
            pages += 1;
            let before = records.len();
            records.extend(rows.into_iter().filter(|row| seen.insert(identity(row))));
            let fresh = records.len() - before;
            if fresh == 0 && next.is_some() {
                return Err(PredictHttpError::Pagination(format!(
                    "{endpoint}: page contains no new identities"
                )));
            }
            match advance(endpoint, &mut previous, fresh, next)? {
                Some(next) => cursor = Some(next),
                None => {
                    complete = true;
                    break;
                }
            }
        }
        Ok(Pages {
            records,
            complete,
            next_cursor: cursor,
        })
    }

    // ------------------------------------------------------------------
    // Catalog
    // ------------------------------------------------------------------

    /// Fetches one page of markets (`GET /markets`).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_markets_page(
        &self,
        query: &PredictMarketsQuery,
    ) -> Result<PredictPage<Vec<PredictMarketDto>>, PredictHttpError> {
        let params = pairs(query)?;
        self.send_get("/markets", PATH_MARKETS, Some(&params)).await
    }

    /// Walks every market page (dedup by market id) up to `max_pages`.
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_markets(
        &self,
        status: Option<PredictMarketStatusFilter>,
        max_pages: u32,
    ) -> Result<Pages<PredictMarketDto>, PredictHttpError> {
        self.walk_pages(
            "/markets",
            None,
            max_pages,
            |cursor| async {
                let query = PredictMarketsQuery {
                    first: None,
                    after: cursor,
                    status,
                };
                let page = self.get_markets_page(&query).await?;
                Ok((page.data, page.cursor))
            },
            |market: &PredictMarketDto| market.id,
        )
        .await
    }

    /// Fetches one market by id (`GET /markets/{id}`).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_market(&self, market_id: i64) -> Result<PredictMarketDto, PredictHttpError> {
        let path = format!("{PATH_MARKETS}/{market_id}");
        let envelope: PredictPage<PredictMarketDto> =
            self.send_get("/markets/:id", &path, Option::<&String>::None)
                .await?;
        Ok(envelope.data)
    }

    /// Fetches one page of a series' rounds (`GET /series/{slug}/rounds` —
    /// rounds are categories).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_series_rounds_page(
        &self,
        slug: &str,
        status: Option<PredictRoundStatusFilter>,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<PredictPage<Vec<PredictCategoryDto>>, PredictHttpError> {
        #[derive(Serialize)]
        struct Query {
            #[serde(skip_serializing_if = "Option::is_none")]
            status: Option<PredictRoundStatusFilter>,
            #[serde(skip_serializing_if = "Option::is_none")]
            first: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            after: Option<String>,
        }
        let params = pairs(&Query {
            status,
            first,
            after,
        })?;
        let path = format!("{PATH_SERIES}/{slug}/rounds");
        self.send_get("/series/:slug/rounds", &path, Some(&params))
            .await
    }

    /// Walks every round of a series (dedup by category id).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_series_rounds(
        &self,
        slug: &str,
        status: Option<PredictRoundStatusFilter>,
        max_pages: u32,
    ) -> Result<Pages<PredictCategoryDto>, PredictHttpError> {
        self.walk_pages(
            "/series/:slug/rounds",
            None,
            max_pages,
            |cursor| async {
                let page = self
                    .get_series_rounds_page(slug, status, None, cursor)
                    .await?;
                Ok((page.data, page.cursor))
            },
            |category: &PredictCategoryDto| category.id,
        )
        .await
    }

    /// Fetches one page of categories (`GET /categories`).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_categories_page(
        &self,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<PredictPage<Vec<PredictCategoryDto>>, PredictHttpError> {
        #[derive(Serialize)]
        struct Query {
            #[serde(skip_serializing_if = "Option::is_none")]
            first: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            after: Option<String>,
        }
        let first = first.map(|first| first.min(CATEGORIES_FIRST_MAX));
        let params = pairs(&Query { first, after })?;
        self.send_get("/categories", PATH_CATEGORIES, Some(&params))
            .await
    }

    /// Walks every category page (dedup by category id).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_categories(&self, max_pages: u32) -> Result<Pages<PredictCategoryDto>, PredictHttpError> {
        self.walk_pages(
            "/categories",
            None,
            max_pages,
            |cursor| async {
                let page = self.get_categories_page(None, cursor).await?;
                Ok((page.data, page.cursor))
            },
            |category: &PredictCategoryDto| category.id,
        )
        .await
    }

    /// Fetches one series definition (`GET /series/{slug}`).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_series(&self, slug: &str) -> Result<PredictSeriesDto, PredictHttpError> {
        let path = format!("{PATH_SERIES}/{slug}");
        let envelope: PredictPage<PredictSeriesDto> =
            self.send_get("/series/:slug", &path, Option::<&String>::None)
                .await?;
        Ok(envelope.data)
    }

    // ------------------------------------------------------------------
    // Books / quotes
    // ------------------------------------------------------------------

    /// Fetches the current full book of one market
    /// (`GET /markets/{id}/orderbook`).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_orderbook(
        &self,
        market_id: i64,
    ) -> Result<PredictOrderbookDto, PredictHttpError> {
        let path = format!("{PATH_MARKETS}/{market_id}/orderbook");
        let envelope: PredictPage<PredictOrderbookDto> = self
            .send_get("/markets/:id/orderbook", &path, Option::<&String>::None)
            .await?;
        Ok(envelope.data)
    }

    /// Fetches the books of up to [`ORDERBOOKS_BATCH_MAX`] markets in one
    /// request (`GET /markets/orderbooks?ids=...`). A longer list is
    /// rejected before it is sent.
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_orderbooks(
        &self,
        market_ids: &[i64],
    ) -> Result<Vec<PredictOrderbookDto>, PredictHttpError> {
        check_batch("orderbooks", market_ids.len(), ORDERBOOKS_BATCH_MAX)?;
        let params = vec![("ids".to_string(), join_ids(market_ids))];
        let envelope: PredictPage<Vec<PredictOrderbookDto>> = self
            .send_get("/markets/orderbooks", PATH_MARKETS_ORDERBOOKS, Some(&params))
            .await?;
        Ok(envelope.data)
    }

    /// Fetches the top of one market's book (`GET /markets/{id}/quote`).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_quote(
        &self,
        market_id: i64,
    ) -> Result<PredictMarketQuoteDto, PredictHttpError> {
        let path = format!("{PATH_MARKETS}/{market_id}/quote");
        let envelope: PredictPage<PredictMarketQuoteDto> = self
            .send_get("/markets/:id/quote", &path, Option::<&String>::None)
            .await?;
        Ok(envelope.data)
    }

    /// Fetches quotes for up to [`QUOTES_BATCH_MAX`] markets in one request
    /// (`GET /markets/quotes?ids=...`).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_quotes(
        &self,
        market_ids: &[i64],
    ) -> Result<Vec<PredictMarketQuoteDto>, PredictHttpError> {
        check_batch("quotes", market_ids.len(), QUOTES_BATCH_MAX)?;
        let params = vec![("ids".to_string(), join_ids(market_ids))];
        let envelope: PredictPage<Vec<PredictMarketQuoteDto>> = self
            .send_get("/markets/quotes", PATH_MARKETS_QUOTES, Some(&params))
            .await?;
        Ok(envelope.data)
    }

    // ------------------------------------------------------------------
    // Matches (public trades)
    // ------------------------------------------------------------------

    /// Fetches one matches page (`GET /orders/matches`). Rows sort by
    /// `executedAt` descending; `first` is clamped to [`MATCHES_FIRST_MAX`]
    /// before sending.
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_matches_page(
        &self,
        query: &PredictMatchesQuery,
    ) -> Result<PredictMatchesEnvelope, PredictHttpError> {
        let mut query = query.clone();
        if let Some(first) = query.first {
            query.first = Some(first.min(MATCHES_FIRST_MAX));
        }
        let params = pairs(&query)?;
        self.send_get("/orders/matches", PATH_MATCHES, Some(&params))
            .await
    }

    /// Walks matches pages (dedup by the stable row identity
    /// `(settlementId, transactionHash)`), resumable from a previous walk's
    /// `next_cursor`.
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn walk_matches(
        &self,
        query: &PredictMatchesQuery,
        resume_from: Option<&str>,
        max_pages: u32,
    ) -> Result<MatchesWalk, PredictHttpError> {
        let pages = self
            .walk_pages(
                "/orders/matches",
                resume_from.map(str::to_string),
                max_pages,
                |cursor| async {
                    let mut query = query.clone();
                    query.after = cursor;
                    let page = self.get_matches_page(&query).await?;
                    Ok((page.data, page.cursor))
                },
                MatchesWalk::row_identity,
            )
            .await?;
        Ok(MatchesWalk {
            end: if pages.complete {
                WalkEnd::Exhausted
            } else {
                WalkEnd::PageBudget
            },
            rows: pages.records,
            next_cursor: pages.next_cursor,
        })
    }

    // ------------------------------------------------------------------
    // Chance timeseries / status logs
    // ------------------------------------------------------------------

    /// Fetches one chance-timeseries page
    /// (`GET /markets/{id}/timeseries?metric=chance`). `from`/`to` are unix
    /// seconds, both inclusive, at most 365 days apart.
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_chance_page(
        &self,
        market_id: i64,
        from_secs: i64,
        to_secs: Option<i64>,
        resolution: Option<PredictTimeseriesResolution>,
        limit: Option<u32>,
        after: Option<String>,
    ) -> Result<PredictChanceSeriesEnvelope, PredictHttpError> {
        #[derive(Serialize)]
        struct Query {
            metric: &'static str,
            #[serde(skip_serializing_if = "Option::is_none")]
            resolution: Option<&'static str>,
            from: i64,
            #[serde(skip_serializing_if = "Option::is_none")]
            to: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            limit: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            after: Option<String>,
        }
        let params = pairs(&Query {
            metric: PredictMetric::Chance.as_str(),
            resolution: resolution.map(|r| r.as_str()),
            from: from_secs,
            to: to_secs,
            limit,
            after,
        })?;
        let path = format!("{PATH_MARKETS}/{market_id}/timeseries");
        self.send_get("/markets/:id/timeseries", &path, Some(&params))
            .await
    }

    /// Walks every chance point in a window (dedup by the `x` timestamp),
    /// bounded by `max_pages`. The returned `resolution` is what the server
    /// actually sampled for this window — a coarser answer than requested
    /// is the server's word, reported to the caller, never silently
    /// accepted as the requested one.
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn walk_chance_series(
        &self,
        market_id: i64,
        from_secs: i64,
        to_secs: Option<i64>,
        resolution: Option<PredictTimeseriesResolution>,
        max_pages: u32,
    ) -> Result<ChanceWalk, PredictHttpError> {
        let reported: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
        let pages = self
            .walk_pages(
                "/markets/:id/timeseries",
                None,
                max_pages,
                |cursor| async {
                    let page = self
                        .get_chance_page(market_id, from_secs, to_secs, resolution, None, cursor)
                        .await?;
                    let mut reported = reported.lock().expect("resolution lock poisoned");
                    if reported.is_none() {
                        *reported = Some(page.data.resolution);
                    }
                    Ok((page.data.series, page.cursor))
                },
                |point: &PredictChancePoint| point.x,
            )
            .await?;
        let resolution = reported
            .into_inner()
            .expect("resolution lock poisoned")
            .unwrap_or_default();
        Ok(ChanceWalk {
            points: pages.records,
            resolution,
            end: if pages.complete {
                WalkEnd::Exhausted
            } else {
                WalkEnd::PageBudget
            },
            next_cursor: pages.next_cursor,
        })
    }

    /// Fetches the latest chance value
    /// (`GET /markets/{id}/timeseries/latest`).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_chance_latest(
        &self,
        market_id: i64,
    ) -> Result<PredictChancePoint, PredictHttpError> {
        #[derive(serde::Deserialize)]
        struct Envelope {
            #[allow(dead_code)]
            success: bool,
            data: PredictChancePoint,
        }
        let path = format!("{PATH_MARKETS}/{market_id}/timeseries/latest");
        let envelope: Envelope = self
            .send_get(
                "/markets/:id/timeseries/latest",
                &path,
                Option::<&String>::None,
            )
            .await?;
        Ok(envelope.data)
    }

    /// Fetches status logs for 1..=[`STATUS_LOGS_BATCH_MAX`] markets in one
    /// request (`GET /markets/status-logs?ids=...`).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_status_logs(
        &self,
        market_ids: &[i64],
    ) -> Result<PredictStatusLogsEnvelope, PredictHttpError> {
        if market_ids.is_empty() || market_ids.len() > STATUS_LOGS_BATCH_MAX {
            return Err(PredictHttpError::Pagination(format!(
                "status-logs batch of {} outside the documented range 1..={STATUS_LOGS_BATCH_MAX}",
                market_ids.len()
            )));
        }
        let params = vec![("ids".to_string(), join_ids(market_ids))];
        self.send_get("/markets/status-logs", PATH_MARKETS_STATUS_LOGS, Some(&params))
            .await
    }
}

/// Serializes a query struct into the raw `(name, value)` pairs the
/// network client expects — the transport applies percent-encoding itself,
/// so values are decoded back here to avoid double encoding.
fn pairs(value: &impl Serialize) -> Result<Vec<(String, String)>, PredictHttpError> {
    let encoded = serde_urlencoded::to_string(value)
        .map_err(|e| PredictHttpError::Transport(e.to_string()))?;
    Ok(encoded
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (name, value) = part.split_once('=').unwrap_or((part, ""));
            (
                percent_decode(name),
                percent_decode(value),
            )
        })
        .collect())
}

/// Reverses percent-encoding applied by `serde_urlencoded`.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("");
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                out.push(byte);
                index += 3;
                continue;
            }
        }
        if bytes[index] == b'+' {
            out.push(b' ');
        } else {
            out.push(bytes[index]);
        }
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn check_batch(name: &str, len: usize, max: usize) -> Result<(), PredictHttpError> {
    if len == 0 || len > max {
        return Err(PredictHttpError::Pagination(format!(
            "{name} batch of {len} outside the documented range 1..={max}"
        )));
    }
    Ok(())
}

fn join_ids(market_ids: &[i64]) -> String {
    market_ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}
