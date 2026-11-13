//! `PredictDataClient` — the `DataClient` implementation wiring HTTP, WS,
//! the instrument provider and the dual-projection book path together.
//!
//! Subscription model (§5.2): the wire subscription is per market id.
//! Yes/No legs and quote/book/status/close consumers name the same topics
//! and share one reference count — the final owner releases it. Public
//! trades have no WS topic (the venue offers none): they come from a
//! bounded incremental REST poll per market with an overlap window,
//! stable-identity dedup and a high-water mark. Pages arrive newest-first,
//! so even a budget-exhausted walk has scanned the newest rows and the
//! watermark still advances (older rows are backfill territory).

mod parse;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use nautilus_common::clients::{DataClient, execute_instrument_subscriptions};
use nautilus_common::messages::{DataEvent, data::*};
use nautilus_core::time::{AtomicTime, get_atomic_clock_realtime};
use nautilus_core::UnixNanos;
use nautilus_model::data::Data;
use nautilus_model::enums::BookType;
use nautilus_model::identifiers::{ClientId, InstrumentId};
use nautilus_model::instruments::{BinaryOption, InstrumentAny};
use nautilus_model::orderbook::OrderBook;
use nautilus_live::task::TaskGroup;
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

use crate::book::{project_book_frame, ProjectedBook};
use crate::common::consts::{
    PREDICT_CLIENT_ID, PREDICT_VENUE, TOPIC_MARKET_CHANGED, TOPIC_MARKET_STATUS, TOPIC_ORDERBOOK,
    TOPIC_TRADING_STATUS,
};
use crate::common::enums::PredictMarketStatus;
use crate::common::parse::parse_instrument_id;
use crate::config::PredictDataClientConfig;
use crate::data::parse::{
    market_status_event, match_to_trade_tick, resolved_outcome_close, trading_status_event,
};
use crate::http::client::{PredictHttpClient, WalkEnd};
use crate::http::error::PredictHttpError;
use crate::http::models::PredictMatchesQuery;
use crate::providers::{PredictInstrumentProvider, request_bounds};
use crate::http::models::PredictMarketDto;
use crate::websocket::client::{PredictWsEvent, PredictWebSocketClient};
use crate::websocket::messages::{PredictWsMessage, market_topic};

/// Page budget for one-shot trades requests (caller-driven; far above
/// the poller's incremental budget).
const REQUEST_TRADES_MAX_PAGES: u32 = 100;
const TASK_GRACE: Duration = Duration::from_secs(2);
const TASK_FORCE: Duration = Duration::from_secs(2);

/// Both legs of one market plus its identity facts.
#[derive(Clone, Debug)]
pub struct MarketContext {
    pub market_id: i64,
    pub condition_id: String,
    /// The pricing-axis instrument (`indexSet == 1`) — the book's Yes leg.
    pub yes: BinaryOption,
    /// The complement leg (binary markets carry exactly one).
    pub no: Option<BinaryOption>,
}

impl MarketContext {
    /// Every instrument of the market.
    pub fn instruments(&self) -> impl Iterator<Item = &BinaryOption> {
        std::iter::once(&self.yes).chain(self.no.iter())
    }

    /// The instrument matching `instrument_id`, if any.
    #[must_use]
    pub fn lookup(&self, instrument_id: &InstrumentId) -> Option<&BinaryOption> {
        self.instruments().find(|i| i.id == *instrument_id)
    }

    /// The outcome's on-chain id an instrument encodes in its symbol.
    #[must_use]
    pub fn on_chain_id_of(&self, instrument_id: &InstrumentId) -> Option<&str> {
        self.lookup(instrument_id).map(|i| {
            i.raw_symbol
                .as_str()
                .rsplit('-')
                .next()
                .unwrap_or_default()
        })
    }
}

/// The subscription channels with their wire-topic mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Channel {
    /// L2 book deltas AND quotes: both derive from the same full order
    /// book frames, so they share one `predictOrderbook` topic per market.
    Book,
    Quotes,
    /// Trading + market lifecycle status and closes share the two status
    /// topics per market.
    Status,
    Close,
    /// Public trades: REST-polled, no WS topic.
    Trades,
}

/// Per-instrument subscription demand (each count is one consumer).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct InstrumentDemand {
    book: u32,
    quotes: u32,
    status: u32,
    close: u32,
    trades: u32,
}

impl InstrumentDemand {
    fn count(&self, channel: Channel) -> u32 {
        match channel {
            Channel::Book => self.book,
            Channel::Quotes => self.quotes,
            Channel::Status => self.status,
            Channel::Close => self.close,
            Channel::Trades => self.trades,
        }
    }

    fn slot(&mut self, channel: Channel) -> &mut u32 {
        match channel {
            Channel::Book => &mut self.book,
            Channel::Quotes => &mut self.quotes,
            Channel::Status => &mut self.status,
            Channel::Close => &mut self.close,
            Channel::Trades => &mut self.trades,
        }
    }

    fn is_empty(&self) -> bool {
        [self.book, self.quotes, self.status, self.close, self.trades]
            .into_iter()
            .all(|count| count == 0)
    }
}

/// Wire topics ONE consumer of a channel needs (per-market, shared across
/// legs). Every channel maps the full topic set its consumers rely on —
/// book and quotes both ride `predictOrderbook` frames; status and close
/// both need the lifecycle pair; definition changes matter to all of them.
/// Each consumer adds/releases one reference per topic, so two channels on
/// the same instrument correctly hold two references.
const fn channel_topics(channel: Channel) -> &'static [&'static str] {
    match channel {
        Channel::Book | Channel::Quotes => {
            &[TOPIC_ORDERBOOK, TOPIC_MARKET_CHANGED]
        }
        Channel::Status | Channel::Close => {
            &[TOPIC_TRADING_STATUS, TOPIC_MARKET_STATUS, TOPIC_MARKET_CHANGED]
        }
        Channel::Trades => &[],
    }
}

/// Incremental trades state per market: high-water mark + recently seen row
/// identities with their timestamps (pruned past twice the overlap window).
#[derive(Debug, Default)]
struct TradesState {
    watermark_ms: Option<i64>,
    seen: HashMap<(String, String), i64>,
}

type Shared<T> = Arc<Mutex<T>>;
type DemandMap = HashMap<InstrumentId, InstrumentDemand>;
type MarketMap = HashMap<i64, MarketContext>;
type InstrumentIndex = HashMap<InstrumentId, i64>;

/// The Predict data client.
pub struct PredictDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: PredictDataClientConfig,
    http: Arc<PredictHttpClient>,
    provider: Arc<PredictInstrumentProvider>,
    ws_client: Option<PredictWebSocketClient>,
    connected: Arc<AtomicBool>,
    disposed: bool,
    sender: nautilus_common::live::sender::EventSender<DataEvent>,
    markets: Shared<MarketMap>,
    by_instrument: Shared<InstrumentIndex>,
    demand: Shared<DemandMap>,
    trades: Shared<HashMap<i64, TradesState>>,
    tasks: TaskGroup,
}

impl std::fmt::Debug for PredictDataClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(PredictDataClient))
            .field("client_id", &self.client_id)
            .field("connected", &self.is_connected())
            .finish_non_exhaustive()
    }
}

impl PredictDataClient {
    /// Creates the client under the venue's default client id.
    ///
    /// # Errors
    ///
    /// Returns an error when the configuration is invalid or transport
    /// setup fails.
    pub fn new(
        config: PredictDataClientConfig,
        sender: nautilus_common::live::sender::EventSender<DataEvent>,
    ) -> anyhow::Result<Self> {
        Self::new_with_client_id(*PREDICT_CLIENT_ID, config, sender, None)
    }

    /// Creates the client under a caller-supplied client id (factories
    /// derive it from the node-assigned name so two predict clients in one
    /// node do not collide).
    ///
    /// # Errors
    ///
    /// Returns an error when the configuration is invalid or transport
    /// setup fails.
    pub fn new_with_client_id(
        client_id: ClientId,
        config: PredictDataClientConfig,
        sender: nautilus_common::live::sender::EventSender<DataEvent>,
        http: Option<Arc<PredictHttpClient>>,
    ) -> anyhow::Result<Self> {
        Self::assemble(client_id, config, sender, http)
    }

    /// Shared assembly for every constructor entry point.
    fn assemble(
        client_id: ClientId,
        config: PredictDataClientConfig,
        sender: nautilus_common::live::sender::EventSender<DataEvent>,
        http: Option<Arc<PredictHttpClient>>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let api_key = config
            .api_key
            .clone()
            .expect("validate() enforces an api key");
        let http = match http {
            Some(http) => http,
            None => Arc::new(PredictHttpClient::new(
                &config.http_url(),
                &api_key,
                config.http_timeout_secs,
                config
                    .proxy_url
                    .as_ref()
                    .and_then(|url| nautilus_network::websocket::proxy::ProxyUrl::parse(url.expose_secret()).ok()),
            )?),
        };
        Ok(Self {
            clock: get_atomic_clock_realtime(),
            client_id,
            provider: Arc::new(PredictInstrumentProvider::new(Arc::clone(&http))),
            config,
            http,
            ws_client: None,
            connected: Arc::new(AtomicBool::new(false)),
            disposed: false,
            sender,
            markets: Arc::default(),
            by_instrument: Arc::default(),
            demand: Arc::default(),
            trades: Arc::default(),
            tasks: TaskGroup::new(),
        })
    }

    /// The market context of an instrument (None when unknown).
    #[must_use]
    pub fn market_of(&self, instrument_id: &InstrumentId) -> Option<MarketContext> {
        self.context_maps().and_then(|(markets, index)| {
            index
                .get(instrument_id)
                .and_then(|market_id| markets.get(market_id).cloned())
        })
    }

    fn context_maps(&self) -> Option<(MarketMap, InstrumentIndex)> {
        // One scope, declared order (demand → by_instrument → markets):
        // two separate scopes can snapshot across a reindex.
        let (markets, index) = {
            let index = self.by_instrument.lock().ok()?;
            let markets = self.markets.lock().ok()?;
            (markets.clone(), index.clone())
        };
        Some((markets, index))
    }

    /// Resolves every configured instrument through one catalog walk and
    /// (re)builds the market context.
    ///
    /// # Errors
    ///
    /// Returns an error when the catalog walk fails or its page budget
    /// runs out (truncated discovery must never look complete).
    pub async fn refresh_context(&self) -> anyhow::Result<()> {
        let ids = self.config.unique_instrument_ids();
        let pages = self
            .http
            .get_markets(None, self.config.discovery_max_pages)
            .await
            .map_err(|e: PredictHttpError| anyhow::anyhow!("catalog refresh: {e}"))?;
        if !pages.complete {
            anyhow::bail!(
                "catalog refresh page budget exhausted after {} pages",
                self.config.discovery_max_pages
            );
        }
        let mut missing = Vec::new();
        {
            // Lock order everywhere: demand → by_instrument → markets.
            let mut index = self.lock_by_instrument()?;
            let mut markets = self.lock_markets()?;
            for instrument_id in ids {
                let Ok((condition_id, on_chain_id)) = parse_instrument_id(&instrument_id) else {
                    missing.push(format!("{instrument_id}: not a PREDICT symbol"));
                    continue;
                };
                let Some(market) = pages
                    .records
                    .iter()
                    .find(|market| market.condition_id == condition_id)
                else {
                    missing.push(format!("{instrument_id}: market {condition_id} not found"));
                    continue;
                };
                let context = build_context(market)?;
                if context.lookup(&instrument_id).is_some() {
                    for instrument in context.instruments() {
                        index.insert(instrument.id, context.market_id);
                    }
                    markets.insert(context.market_id, context);
                } else {
                    missing.push(format!(
                        "{instrument_id}: market has no outcome {on_chain_id}"
                    ));
                }
            }
        }
        if !missing.is_empty() {
            log::warn!("unresolved instruments: {}", missing.join("; "));
        }
        Ok(())
    }

    fn lock_markets(&self) -> anyhow::Result<std::sync::MutexGuard<'_, MarketMap>> {
        self.markets
            .lock()
            .map_err(|_| anyhow::anyhow!("markets lock poisoned"))
    }

    fn lock_by_instrument(&self) -> anyhow::Result<std::sync::MutexGuard<'_, InstrumentIndex>> {
        self.by_instrument
            .lock()
            .map_err(|_| anyhow::anyhow!("by_instrument lock poisoned"))
    }

    /// Adjusts one consumer's demand on one channel and drives the wire
    /// topics: subscriptions are shared per market across both legs and
    /// every channel; the last owner releases the topic.
    fn adjust_demand(
        &self,
        instrument_id: &InstrumentId,
        channel: Channel,
        delta: i32,
    ) -> anyhow::Result<()> {
        let Some(market_id) = self.market_of(instrument_id).map(|m| m.market_id) else {
            // Pre-connect demand for a configured instrument is recorded
            // and replayed once connect() resolves the market context;
            // instruments outside the configured bootstrap set are a loud
            // rejection (mirrors the kalshi client).
            anyhow::ensure!(
                self.config.instrument_ids.contains(instrument_id),
                "instrument {instrument_id} is not a configured PREDICT instrument"
            );
            let mut demand = self.lock_demand()?;
            let entry = demand.entry(*instrument_id).or_default();
            let current = entry.count(channel);
            if delta < 0 && current == 0 {
                anyhow::bail!(
                    "unsubscribed {instrument_id} channel {channel:?} with no live demand"
                );
            }
            *entry.slot(channel) = current.saturating_add_signed(delta);
            demand.retain(|_, entry| !entry.is_empty());
            return Ok(());
        };
        // Topics are read BEFORE the delta on release: the set the channel
        // belonged to is what this consumer releases; computing it after
        // would skip the last owner's unsubscribe entirely. Subscribe reads
        // them after, when the new channel joins an existing mapping.
        let topics = channel_topics(channel);
        let mut release_topics: Vec<&'static str> = Vec::new();
        let acquire_topics: Vec<&'static str>;
        {
            let mut demand = self.lock_demand()?;
            let entry = demand.entry(*instrument_id).or_default();
            let current = entry.count(channel);
            if delta < 0 && current == 0 {
                // Over-release: this consumer never held the channel.
                anyhow::bail!(
                    "unsubscribed {instrument_id} channel {channel:?} with no live demand"
                );
            }
            if delta < 0 {
                release_topics = topics.to_vec();
            }
            *entry.slot(channel) = current.saturating_add_signed(delta);
            acquire_topics = topics.to_vec();
        }
        if delta < 0 {
            let mut demand = self.lock_demand()?;
            demand.retain(|_, entry| !entry.is_empty());
        }
        let Some(ws) = &self.ws_client else {
            // Demand recorded pre-connect: intent is replayed on connect
            // via refresh + resubscribe.
            if delta > 0 {
                log::debug!(
                    "demand for {instrument_id} recorded before connect; intent replays on connect"
                );
            }
            return Ok(());
        };
        for prefix in release_topics {
            // Release exactly this consumer's reference per topic the
            // channel belonged to (other legs/channels may still hold the
            // same wire topic; the WS layer drops it with its last owner).
            ws.unsubscribe(&market_topic(prefix, market_id))?;
        }
        if delta > 0 {
            // Acquire only on NEW demand: on a release the acquire set is
            // a subset of the release set (sibling channels on the same
            // topic), and re-subscribing would strand the shared
            // reference count at one — the topic would never release.
            for prefix in acquire_topics {
                ws.subscribe(&market_topic(prefix, market_id))?;
            }
        }
        Ok(())
    }

    fn lock_demand(&self) -> anyhow::Result<std::sync::MutexGuard<'_, DemandMap>> {
        self.demand
            .lock()
            .map_err(|_| anyhow::anyhow!("demand lock poisoned"))
    }
}

/// Builds the market context: the `indexSet == 1` outcome is the pricing
/// axis, every other outcome is a complement leg (binary markets only).
fn build_context(market: &PredictMarketDto) -> anyhow::Result<MarketContext> {
    let ts_init = UnixNanos::default();
    let mut yes: Option<BinaryOption> = None;
    let mut no: Option<BinaryOption> = None;
    for outcome in &market.outcomes {
        let instrument = crate::providers::parse_instrument(market, outcome, ts_init)?;
        if outcome.index_set == 1 {
            anyhow::ensure!(
                yes.is_none(),
                "market {} has two pricing outcomes",
                market.id
            );
            yes = Some(instrument);
        } else {
            anyhow::ensure!(
                no.is_none(),
                "market {} has multiple complement outcomes (only binary supported)",
                market.id
            );
            no = Some(instrument);
        }
    }
    let yes =
        yes.ok_or_else(|| anyhow::anyhow!("market {} has no indexSet-1 outcome", market.id))?;
    Ok(MarketContext {
        market_id: market.id,
        condition_id: market.condition_id.clone(),
        yes,
        no,
    })
}

macro_rules! unsupported {
    ($($name:ident($command:ty)),* $(,)?) => { $(
        fn $name(&mut self, _command: $command) -> anyhow::Result<()> {
            anyhow::bail!(concat!("Predict does not support ", stringify!($name))
            )
        }
    )* };
}

#[async_trait(?Send)]
impl DataClient for PredictDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<nautilus_model::identifiers::Venue> {
        Some(*PREDICT_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.disposed, "Predict data client is disposed");
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.disposed = true;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
            && self.ws_client.as_ref().is_some_and(|ws| ws.is_active())
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.disposed, "Predict data client is disposed");
        if self.is_connected() {
            return Ok(());
        }
        // TaskGroup generations start Open: drain before starting a new
        // generation, or start_generation refuses with NOT_DRAINED (the
        // kalshi client performs the same dance).
        if self.tasks.is_open() {
            self.tasks.begin_shutdown();
            self.tasks.finish_shutdown(TASK_GRACE, TASK_FORCE).await?;
        }
        self.tasks.start_generation()?;
        // The previous shell dies BEFORE its replacement exists: a leftover
        // would keep reconnecting while `connected` stays true — a silent
        // data blackhole that looks healthy.
        if let Some(old) = self.ws_client.take() {
            old.stop().await;
        }
        self.connected.store(false, Ordering::Release);
        let (ws_client, receiver) = PredictWebSocketClient::new(
            self.config.ws_url(),
            self.config
                .api_key
                .as_ref()
                .expect("validate() enforces an api key"),
            self.config.ws_connect_timeout_secs,
            self.config.ws_timeout_secs,
            self.config
                .proxy_url
                .as_ref()
                .map(|url| url.expose_secret().to_string()),
        );
        // Any failure below must not leak the just-created WS handler
        // (its task keeps reconnecting forever with its receiver dropped).
        if let Err(e) = ws_client.connect().await {
            ws_client.stop().await;
            return Err(e);
        }
        if let Err(e) = self.refresh_context().await {
            ws_client.stop().await;
            return Err(e);
        }

        let session = Session {
            clock: self.clock,
            http: Arc::clone(&self.http),
            receiver,
            sender: self.sender.clone(),
            markets: Arc::clone(&self.markets),
            by_instrument: Arc::clone(&self.by_instrument),
            demand: Arc::clone(&self.demand),
            trades: Arc::clone(&self.trades),
            book_instant: Arc::default(),
            closed_markets: Arc::default(),
            refresh_interval_mins: self.config.update_instruments_interval_mins,
            trades_poll_interval_secs: self.config.trades_poll_interval_secs,
            trades_poll_overlap_ms: self.config.trades_poll_overlap_ms,
            trades_poll_max_pages: self.config.trades_poll_max_pages,
            cancellation: self.tasks.cancellation_token(),
            generation: 0,
        };
        // Replay demand recorded pre-connect onto the live connection.
        {
            let demand = self.lock_demand()?;
            let index = self
                .by_instrument
                .lock()
                .map_err(|_| anyhow::anyhow!("by_instrument lock poisoned"))?;
            let markets = self
                .markets
                .lock()
                .map_err(|_| anyhow::anyhow!("markets lock poisoned"))?;
            for (instrument_id, entry) in demand.iter() {
                let Some(market_id) = index.get(instrument_id) else {
                    continue;
                };
                let Some(market) = markets.get(market_id) else {
                    continue;
                };
                // Each recorded consumer adds its own reference — per
                // channel, per consumer count.
                for channel in [
                    Channel::Book,
                    Channel::Quotes,
                    Channel::Status,
                    Channel::Close,
                    Channel::Trades,
                ] {
                    for _ in 0..entry.count(channel) {
                        for prefix in channel_topics(channel) {
                            ws_client.subscribe(&market_topic(prefix, market.market_id))?;
                        }
                    }
                }
            }
        }
        self.ws_client = Some(ws_client);
        self.tasks.spawn(session.run())?;
        self.connected.store(true, Ordering::Release);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected.store(false, Ordering::Release);
        self.tasks.begin_shutdown();
        if let Some(ws) = self.ws_client.take() {
            ws.stop().await;
        }
        self.tasks.finish_shutdown(TASK_GRACE, TASK_FORCE).await?;
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, command: SubscribeBookDeltas) -> anyhow::Result<()> {
        anyhow::ensure!(
            command.book_type == BookType::L2_MBP && command.depth.is_none(),
            "Predict streams full L2_MBP books only"
        );
        execute_instrument_subscriptions(&command.instrument_ids, |id| {
            self.adjust_demand(&id, Channel::Book, 1)
        })
    }

    fn unsubscribe_book_deltas(&mut self, command: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&command.instrument_ids, |id| {
            self.adjust_demand(&id, Channel::Book, -1)
        })
    }

    fn subscribe_quotes(&mut self, command: SubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&command.instrument_ids, |id| {
            self.adjust_demand(&id, Channel::Quotes, 1)
        })
    }

    fn unsubscribe_quotes(&mut self, command: &UnsubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&command.instrument_ids, |id| {
            self.adjust_demand(&id, Channel::Quotes, -1)
        })
    }

    fn subscribe_trades(&mut self, command: SubscribeTrades) -> anyhow::Result<()> {
        // Public trades are REST-polled; no WS topic exists.
        execute_instrument_subscriptions(&command.instrument_ids, |id| {
            self.adjust_demand(&id, Channel::Trades, 1)
        })
    }

    fn unsubscribe_trades(&mut self, command: &UnsubscribeTrades) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&command.instrument_ids, |id| {
            self.adjust_demand(&id, Channel::Trades, -1)
        })
    }

    fn subscribe_instrument_status(&mut self, command: SubscribeInstrumentStatus) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&command.instrument_ids, |id| {
            self.adjust_demand(&id, Channel::Status, 1)
        })
    }

    fn unsubscribe_instrument_status(
        &mut self,
        command: &UnsubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&command.instrument_ids, |id| {
            self.adjust_demand(&id, Channel::Status, -1)
        })
    }

    fn subscribe_instrument_close(&mut self, command: SubscribeInstrumentClose) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&command.instrument_ids, |id| {
            self.adjust_demand(&id, Channel::Close, 1)
        })
    }

    fn unsubscribe_instrument_close(
        &mut self,
        command: &UnsubscribeInstrumentClose,
    ) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&command.instrument_ids, |id| {
            self.adjust_demand(&id, Channel::Close, -1)
        })
    }

    fn request_instrument(&self, mut request: RequestInstrument) -> anyhow::Result<()> {
        let instrument_id = request.single_instrument_id()?;
        parse_instrument_id(&instrument_id).map_err(|e| anyhow::anyhow!(e))?;
        request.client_id.get_or_insert(self.client_id);
        let provider = Arc::clone(&self.provider);
        let sender = self.sender.clone();
        let client_id = self.client_id;
        let now = self.clock.get_time_ns();
        self.tasks.spawn(async move {
            match provider
                .load_instrument(&instrument_id, UnixNanos::default())
                .await
            {
                Ok(instrument) => {
                    let response = InstrumentResponse::new(
                        request.request_id,
                        client_id,
                        instrument_id,
                        InstrumentAny::BinaryOption(instrument),
                        None,
                        None,
                        now,
                        request.params,
                    );
                    let _ = sender.send(DataEvent::Response(DataResponse::Instrument(Box::new(
                        response,
                    ))));
                }
                Err(e) => {
                    log::error!("Predict instrument request for {instrument_id} failed: {e}");
                }
            }
        })?;
        Ok(())
    }

    fn request_instruments(&self, mut request: RequestInstruments) -> anyhow::Result<()> {
        anyhow::ensure!(
            request.venue.is_none_or(|v| v == *PREDICT_VENUE),
            "instrument request venue must be PREDICT"
        );
        request.client_id.get_or_insert(self.client_id);
        let provider = Arc::clone(&self.provider);
        let sender = self.sender.clone();
        let client_id = self.client_id;
        let max_pages = self.config.discovery_max_pages;
        let now = self.clock.get_time_ns();
        self.tasks.spawn(async move {
            match provider.load_instruments(max_pages).await {
                Ok(loaded) => {
                    let instruments: Vec<InstrumentAny> = loaded
                        .into_iter()
                        .map(InstrumentAny::BinaryOption)
                        .collect();
                    let response = InstrumentsResponse::new(
                        request.request_id,
                        client_id,
                        *PREDICT_VENUE,
                        instruments,
                        None,
                        None,
                        now,
                        request.params,
                    );
                    let _ = sender.send(DataEvent::Response(DataResponse::Instruments(response)));
                }
                Err(e) => log::error!("Predict instruments request failed: {e}"),
            }
        })?;
        Ok(())
    }

    fn request_book_snapshot(&self, request: RequestBookSnapshot) -> anyhow::Result<()> {
        let instrument_id = request.single_instrument_id()?;
        let Some(market) = self.market_of(&instrument_id) else {
            anyhow::bail!("instrument {instrument_id} has no market context");
        };
        let http = Arc::clone(&self.http);
        let sender = self.sender.clone();
        let client_id = self.client_id;
        let now = self.clock.get_time_ns();
        self.tasks.spawn(async move {
            match http.get_orderbook(market.market_id).await {
                Ok(book) => {
                    let bids: Vec<(Decimal, Decimal)> =
                        book.bids.iter().map(|l| (l.price(), l.size())).collect();
                    let asks: Vec<(Decimal, Decimal)> =
                        book.asks.iter().map(|l| (l.price(), l.size())).collect();
                    let Some(no) = market.no.as_ref() else {
                        log::error!("market {} lost its complement leg", market.market_id);
                        return;
                    };
                    let projected: ProjectedBook =
                        match project_book_frame(&market.yes, no, &bids, &asks, book.update_timestamp_ms, now)
                        {
                            Ok(projected) => projected,
                            Err(e) => {
                                log::error!("Predict book snapshot projection failed: {e}");
                                return;
                            }
                        };
                    let batch = if instrument_id == projected.yes.instrument_id {
                        projected.yes
                    } else {
                        projected.no
                    };
                    let mut order_book = OrderBook::new(instrument_id, BookType::L2_MBP);
                    if let Err(e) = order_book.apply_deltas(&batch) {
                        log::error!("book snapshot apply failed for {instrument_id}: {e}");
                        return;
                    }
                    let response = BookResponse::new(
                        request.request_id,
                        client_id,
                        instrument_id,
                        order_book,
                        None,
                        None,
                        now,
                        request.params,
                    );
                    let _ = sender.send(DataEvent::Response(DataResponse::Book(response)));
                }
                Err(e) => log::error!("Predict book snapshot for {instrument_id} failed: {e}"),
            }
        })?;
        Ok(())
    }

    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let instrument_id = request.single_instrument_id()?;
        let Some(market) = self.market_of(&instrument_id) else {
            anyhow::bail!("instrument {instrument_id} has no market context");
        };
        let start_ms = request.start.map(|t| t.as_millisecond());
        let end_ms = request.end.map(|t| t.as_millisecond());
        let http = Arc::clone(&self.http);
        let sender = self.sender.clone();
        let client_id = self.client_id;
        let now = self.clock.get_time_ns();
        let (start, end) = request_bounds(request.start, request.end)?;
        self.tasks.spawn(async move {
            let query = PredictMatchesQuery {
                first: None,
                after: None,
                market_id: Some(market.market_id),
                executed_after_ms: start_ms,
                executed_before_ms: end_ms,
            };
            let mut walk = match http.walk_matches(&query, None, REQUEST_TRADES_MAX_PAGES).await {
                Ok(walk) => walk,
                Err(e) => {
                    log::error!("Predict trades request for {instrument_id} failed: {e}");
                    return;
                }
            };
            if walk.end == WalkEnd::PageBudget {
                // A truncated result must never look complete: no response
                // is sent and the caller's request fails loudly upstream.
                log::error!(
                    "Predict trades request for {instrument_id} exhausted its page budget;                      no response sent (retry with a narrower window)"
                );
                return;
            }
            let ts_init = now;
            let mut ticks = Vec::new();
            for row in &walk.rows {
                let Some(instrument) = market.lookup(&instrument_id) else {
                    continue;
                };
                if row.taker.outcome.on_chain_id
                    != market.on_chain_id_of(&instrument_id).unwrap_or_default()
                {
                    continue;
                }
                match match_to_trade_tick(row, instrument, ts_init) {
                    Ok(Some(tick)) => ticks.push(tick),
                    Ok(None) => {}
                    Err(e) => log::warn!("dropping unconvertible match row: {e}"),
                }
            }
            walk.rows.clear();
            ticks.sort_by_key(|tick| tick.ts_event);
            let response = TradesResponse::new(
                request.request_id,
                client_id,
                instrument_id,
                ticks,
                start,
                end,
                now,
                request.params,
            );
            let _ = sender.send(DataEvent::Response(DataResponse::Trades(response)));
        })?;
        Ok(())
    }

    fn request_quotes(&self, _request: RequestQuotes) -> anyhow::Result<()> {
        anyhow::bail!("Predict provides no historical quotes endpoint")
    }

    unsupported! {
        subscribe(SubscribeCustomData), subscribe_instruments(SubscribeInstruments),
        subscribe_instrument(SubscribeInstrument), subscribe_book_depth(SubscribeBookDepth),
        subscribe_mark_prices(SubscribeMarkPrices), subscribe_index_prices(SubscribeIndexPrices),
        subscribe_funding_rates(SubscribeFundingRates), subscribe_bars(SubscribeBars),
        subscribe_option_greeks(SubscribeOptionGreeks),
        unsubscribe(&UnsubscribeCustomData), unsubscribe_instruments(&UnsubscribeInstruments),
        unsubscribe_instrument(&UnsubscribeInstrument), unsubscribe_book_depth(&UnsubscribeBookDepth),
        unsubscribe_mark_prices(&UnsubscribeMarkPrices), unsubscribe_index_prices(&UnsubscribeIndexPrices),
        unsubscribe_funding_rates(&UnsubscribeFundingRates), unsubscribe_bars(&UnsubscribeBars),
        unsubscribe_option_greeks(&UnsubscribeOptionGreeks),
    }
}

/// The background session: WS events → published data, trades polling,
/// REST refetch after reconnects.
struct Session {
    clock: &'static AtomicTime,
    http: Arc<PredictHttpClient>,
    receiver: tokio::sync::mpsc::UnboundedReceiver<PredictWsEvent>,
    sender: nautilus_common::live::sender::EventSender<DataEvent>,
    markets: Shared<MarketMap>,
    by_instrument: Shared<InstrumentIndex>,
    demand: Shared<DemandMap>,
    trades: Shared<HashMap<i64, TradesState>>,
    /// Last published book instant per market — late stale frames are
    /// dropped (the venue docs mandate updateTimestampMs for this).
    book_instant: Shared<HashMap<i64, i64>>,
    /// Markets whose RESOLVED close has already been published (REST
    /// refetches after reconnect must not duplicate it).
    closed_markets: Shared<HashMap<i64, ()>>,
    refresh_interval_mins: u64,
    trades_poll_interval_secs: u64,
    trades_poll_overlap_ms: i64,
    trades_poll_max_pages: u32,
    cancellation: CancellationToken,
    generation: u64,
}

impl Session {
    async fn run(mut self) {
        let mut trades_tick = tokio::time::interval(Duration::from_secs(
            self.trades_poll_interval_secs.max(1),
        ));
        trades_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        trades_tick.tick().await; // First tick completes immediately.
        let mut refresh_tick = tokio::time::interval(Duration::from_secs(
            self.refresh_interval_mins.max(1).saturating_mul(60),
        ));
        refresh_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        refresh_tick.tick().await; // First tick completes immediately.

        loop {
            tokio::select! {
                biased;
                () = self.cancellation.cancelled() => break,
                _ = refresh_tick.tick() => {
                    self.refresh_definitions().await;
                }
                _ = trades_tick.tick() => {
                    if let Err(e) = self.poll_trades().await {
                        log::warn!("Predict trades poll: {e}");
                    }
                }
                event = self.receiver.recv() => match event {
                    None => break,
                    Some(event) => self.handle_event(event).await,
                },
            }
        }
    }

    async fn handle_event(&mut self, event: PredictWsEvent) {
        match event {
            PredictWsEvent::Ready { generation } => {
                // The venue pushes each status topic's latest value right
                // after its subscribe ack, so the FIRST ready needs no REST
                // refetch — only reconnect gaps do (nothing is replayed).
                if self.generation != 0 && generation != self.generation {
                    // Reconnect: messages were never replayed — reset the
                    // frame-freshness guard and refetch the REST state of
                    // every market with book demand.
                    log::info!("Predict WS generation {generation}: refetching REST state");
                    if let Ok(mut instants) = self.book_instant.lock() {
                        instants.clear();
                    }
                    self.refetch_books().await;
                    // Status/close consumers equally lost any lifecycle
                    // transitions during the gap: definitions re-answer
                    // trading status and winner-only resolutions.
                    self.refetch_lifecycle().await;
                }
                self.generation = generation;
            }
            PredictWsEvent::Disconnected { generation } => {
                log::warn!("Predict WS generation {generation} disconnected; reconnecting");
            }
            PredictWsEvent::Error { message, .. } => {
                log::warn!("Predict WS: {message}");
            }
            PredictWsEvent::Message { message, .. } => self.handle_message(message).await,
        }
    }

    async fn handle_message(&self, message: PredictWsMessage) {
        match message {
            // 钱包事件属执行面（ExecutionClient 消费）——数据 client 收到
            // 即表面事实记录（不消费不吞）。
            PredictWsMessage::WalletEvent(event) => {
                log::debug!("Predict WS wallet event surfaced (data client does not consume): {event:?}");
            }
            PredictWsMessage::Orderbook(frame) => {
                let Some(market) = self.lookup_market(frame.market_id) else {
                    return;
                };
                let Some(no) = market.no.as_ref() else {
                    log::warn!(
                        "market {} has no complement leg; book frames cannot be dual-projected",
                        frame.market_id
                    );
                    return;
                };
                // Drop frames that move the book backwards.
                {
                    let Ok(mut instants) = self.book_instant.lock() else { return };
                    let last = instants.entry(frame.market_id).or_default();
                    if frame.update_timestamp_ms <= *last {
                        log::debug!(
                            "dropping stale book frame for market {} ({} <= {})",
                            frame.market_id,
                            frame.update_timestamp_ms,
                            last
                        );
                        return;
                    }
                    *last = frame.update_timestamp_ms;
                }
                let ts_init = self.now();
                let bids: Vec<(Decimal, Decimal)> =
                    frame.bids.iter().map(|l| (l.0, l.1)).collect();
                let asks: Vec<(Decimal, Decimal)> =
                    frame.asks.iter().map(|l| (l.0, l.1)).collect();
                let projected = match project_book_frame(
                    &market.yes,
                    no,
                    &bids,
                    &asks,
                    frame.update_timestamp_ms,
                    ts_init,
                ) {
                    Ok(projected) => projected,
                    Err(e) => {
                        log::warn!("dropping unprojectable book frame: {e}");
                        return;
                    }
                };
                self.publish_projection(&projected);
            }
            PredictWsMessage::TradingStatus(status) => {
                let Some(market) = self.lookup_market(status.market_id) else {
                    return;
                };
                let ts_init = self.now();
                for instrument in market.instruments() {
                    if self.has_demand(&instrument.id, |d| d.status > 0) {
                        let event = trading_status_event(
                            instrument.id,
                            status.trading_status,
                            status.ts_ms,
                            ts_init,
                        );
                        let _ = self
                            .sender
                            .send(DataEvent::Data(Data::InstrumentStatus(event)));
                    }
                }
            }
            PredictWsMessage::MarketStatus(status) => {
                let Some(market) = self.lookup_market(status.market_id) else {
                    return;
                };
                let ts_init = self.now();
                for instrument in market.instruments() {
                    let wants_status = self.has_demand(&instrument.id, |d| d.status > 0);
                    let wants_close = self.has_demand(&instrument.id, |d| d.close > 0);
                    if wants_status
                        && let Some(event) =
                            market_status_event(instrument.id, status.status, status.ts_ms, ts_init)
                    {
                        let _ = self
                            .sender
                            .send(DataEvent::Data(Data::InstrumentStatus(event)));
                    }
                    if wants_close && status.status == PredictMarketStatus::Resolved {
                        let on_chain_id = market.on_chain_id_of(&instrument.id);
                        let outcome = status
                            .market_outcomes
                            .iter()
                            .find(|outcome| Some(outcome.on_chain_id.as_str()) == on_chain_id);
                        match outcome {
                            Some(outcome) => {
                                match resolved_outcome_close(
                                    instrument.id,
                                    outcome,
                                    instrument.price_precision,
                                    status.ts_ms,
                                    ts_init,
                                ) {
                                    Ok(close) => {
                                        let _ = self
                                            .sender
                                            .send(DataEvent::Data(Data::InstrumentClose(close)));
                                        // Record the close so the reconnect
                                        // refetch never duplicates it.
                                        if let Ok(mut closed) = self.closed_markets.lock() {
                                            closed.insert(status.market_id, ());
                                        }
                                    }
                                    Err(e) => {
                                        log::warn!("resolved outcome close rejected: {e}");
                                    }
                                }
                            }
                            None => log::warn!(
                                "market {} resolved without an outcome for {}",
                                status.market_id,
                                instrument.id
                            ),
                        }
                    }
                }
            }
            PredictWsMessage::MarketChanged(changed) => {
                // Refetch the market definition; republish both legs and
                // reindex identity (outcome ids can change with the
                // definition).
                match self.http.get_market(changed.market_id).await {
                    Ok(market) => {
                        if let Ok(context) = build_context(&market) {
                            if let Ok(mut index) = self.by_instrument.lock() {
                                index.retain(|_, market_id| *market_id != changed.market_id);
                                for instrument in context.instruments() {
                                    index.insert(instrument.id, changed.market_id);
                                }
                            }
                            if let Ok(mut markets) = self.markets.lock() {
                                markets.insert(changed.market_id, context.clone());
                            }
                            for instrument in context.instruments() {
                                let _ = self.sender.send(DataEvent::Data(Data::Instrument(
                                    Box::new(InstrumentAny::BinaryOption(instrument.clone())),
                                )));
                            }
                        }
                    }
                    Err(e) => log::warn!("market {} refetch failed: {e}", changed.market_id),
                }
            }
            // Top-of-book is not subscribed (quotes derive from full
            // frames); wallet events are P5 execution work; heartbeats and
            // responses are handled inside the WS client.
            PredictWsMessage::TopOfBook(_)
            | PredictWsMessage::Other { .. }
            | PredictWsMessage::Heartbeat { .. }
            | PredictWsMessage::ReconnectNotice(_)
            | PredictWsMessage::Response { .. } => {}
        }
    }

    /// Publishes a projection per instrument demand: each leg only where a
    /// book consumer asked, quotes only where a quote consumer asked.
    fn publish_projection(&self, projected: &ProjectedBook) {
        if self.has_demand(&projected.yes.instrument_id, |d| d.book > 0) {
            let _ = self
                .sender
                .send(DataEvent::Data(Data::BookDeltas(Box::new(
                    projected.yes.clone(),
                ))));
        }
        if self.has_demand(&projected.no.instrument_id, |d| d.book > 0) {
            let _ = self
                .sender
                .send(DataEvent::Data(Data::BookDeltas(Box::new(
                    projected.no.clone(),
                ))));
        }
        for quote in [&projected.yes_quote, &projected.no_quote].into_iter().flatten() {
            if self.has_demand(&quote.instrument_id, |d| d.quotes > 0) {
                let _ = self.sender.send(DataEvent::Data(Data::Quote(*quote)));
            }
        }
    }

    async fn poll_trades(&self) -> anyhow::Result<()> {
        let markets_with_trades: Vec<MarketContext> = {
            let (Some(demand), Some(index), Some(markets)) = (
                self.lock_demand(),
                self.lock_by_instrument(),
                self.lock_markets(),
            ) else {
                return Ok(());
            };
            demand
                .iter()
                .filter(|(_, d)| d.trades > 0)
                .filter_map(|(instrument_id, _)| {
                    index
                        .get(instrument_id)
                        .and_then(|market_id| markets.get(market_id).cloned())
                })
                .collect()
        };
        let mut seen_markets = std::collections::HashSet::new();
        for market in markets_with_trades {
            // Both legs of one market poll once.
            if !seen_markets.insert(market.market_id) {
                continue;
            }
            self.poll_market_trades(&market).await?;
        }
        Ok(())
    }

    async fn poll_market_trades(&self, market: &MarketContext) -> anyhow::Result<()> {
        let from_ms = {
            let Some(trades) = self.lock_trades() else { return Ok(()) };
            trades
                .get(&market.market_id)
                .and_then(|state| state.watermark_ms)
                .map_or(0, |watermark| watermark - self.trades_poll_overlap_ms)
        };
        let query = PredictMatchesQuery {
            first: None,
            after: None,
            market_id: Some(market.market_id),
            executed_after_ms: Some(from_ms),
            executed_before_ms: None,
        };
        let walk = self
            .http
            .walk_matches(&query, None, self.trades_poll_max_pages)
            .await
            .map_err(|e: PredictHttpError| anyhow::anyhow!("matches walk: {e}"))?;
        if walk.end == WalkEnd::PageBudget {
            // Pages sort newest-first, so even an exhausted walk has seen
            // the newest rows: the watermark still advances (a static
            // watermark here would wedge the poller forever on any market
            // with more history than one budget). Rows older than the
            // budget are backfill territory (request_trades), not live
            // poll scope.
            log::debug!(
                "matches walk for market {} hit the page budget; older rows are backfill territory",
                market.market_id
            );
        }

        // Filter to fresh identities under the lock, publish outside it.
        let ts_init = self.now();
        let fresh_rows: Vec<(i64, crate::dtos::PredictMatchData)> = {
            let Some(mut trades) = self.lock_trades() else {
                return Ok(());
            };
            let state = trades.entry(market.market_id).or_default();
            walk.rows
                .iter()
                .filter_map(|row| {
                    let identity = (row.settlement_id.clone(), row.transaction_hash.clone());
                    if state.seen.contains_key(&identity) {
                        return None;
                    }
                    let executed_ms = match crate::data::parse::executed_at_ms(&row.executed_at) {
                        Ok(executed_ms) => executed_ms,
                        Err(e) => {
                            // Unparseable timestamp: skip loudly and
                            // consistently — never abort the batch. The
                            // identity is recorded at the current watermark
                            // so the row is not re-fetched and re-warned
                            // on every future poll.
                            log::warn!("skipping match with unparseable executedAt: {e}");
                            state.seen.insert(
                                identity,
                                state.watermark_ms.unwrap_or_default(),
                            );
                            return None;
                        }
                    };
                    state.seen.insert(identity, executed_ms);
                    Some((executed_ms, row.clone()))
                })
                .collect()
        };
        // Publish oldest-first (venue pages arrive newest-first).
        let mut fresh_rows = fresh_rows;
        fresh_rows.sort_by_key(|(executed_ms, _)| *executed_ms);
        let mut max_executed_ms: Option<i64> = None;
        for (executed_ms, row) in &fresh_rows {
            max_executed_ms = Some(max_executed_ms.map_or(*executed_ms, |m: i64| m.max(*executed_ms)));
            for instrument in market.instruments() {
                if !self.has_demand(&instrument.id, |d| d.trades > 0) {
                    continue;
                }
                if row.taker.outcome.on_chain_id
                    != market.on_chain_id_of(&instrument.id).unwrap_or_default()
                {
                    continue;
                }
                match match_to_trade_tick(row, instrument, ts_init) {
                    Ok(Some(tick)) => {
                        let _ = self.sender.send(DataEvent::Data(Data::Trade(tick)));
                    }
                    Ok(None) => {}
                    Err(e) => log::warn!("dropping unconvertible match row: {e}"),
                }
            }
        }
        if let Some(max_ms) = max_executed_ms {
            let Some(mut trades) = self.lock_trades() else { return Ok(()) };
            let state = trades.entry(market.market_id).or_default();
            state.watermark_ms = Some(state.watermark_ms.map_or(max_ms, |m| m.max(max_ms)));
            // Prune identities older than twice the overlap window — they
            // can no longer reappear in the scanned range.
            if let Some(watermark) = state.watermark_ms {
                let keep_from = watermark - 2 * self.trades_poll_overlap_ms;
                state.seen.retain(|_, seen_ms| *seen_ms >= keep_from);
            }
        }
        Ok(())
    }

    async fn refetch_books(&self) {
        let markets: Vec<MarketContext> = {
            let (Some(demand), Some(index), Some(markets)) = (
                self.lock_demand(),
                self.lock_by_instrument(),
                self.lock_markets(),
            ) else {
                return;
            };
            demand
                .iter()
                .filter(|(_, d)| d.book > 0 || d.quotes > 0)
                .filter_map(|(instrument_id, _)| {
                    index
                        .get(instrument_id)
                        .and_then(|market_id| markets.get(market_id).cloned())
                })
                .collect()
        };
        let mut seen_markets = std::collections::HashSet::new();
        let markets: Vec<MarketContext> = markets
            .into_iter()
            .filter(|market| seen_markets.insert(market.market_id))
            .collect();
        for market in markets {
            match self.http.get_orderbook(market.market_id).await {
                Ok(book) => {
                    let Some(no) = market.no.as_ref() else {
                        continue;
                    };
                    let ts_init = self.now();
                    let bids: Vec<(Decimal, Decimal)> =
                        book.bids.iter().map(|l| (l.price(), l.size())).collect();
                    let asks: Vec<(Decimal, Decimal)> =
                        book.asks.iter().map(|l| (l.price(), l.size())).collect();
                    match project_book_frame(
                        &market.yes,
                        no,
                        &bids,
                        &asks,
                        book.update_timestamp_ms,
                        ts_init,
                    ) {
                        Ok(projected) => self.publish_projection(&projected),
                        Err(e) => {
                            log::warn!("post-reconnect projection of market {} failed: {e}", market.market_id);
                        }
                    }
                }
                Err(e) => log::warn!(
                    "REST refetch of market {} after reconnect failed: {e}",
                    market.market_id
                ),
            }
        }
    }

    fn now(&self) -> UnixNanos {
        self.clock.get_time_ns()
    }

    /// Refreshes every held market definition and republishes both legs.
    async fn refresh_definitions(&self) {
        let ids: Vec<i64> = self
            .markets
            .lock()
            .map(|markets| markets.keys().copied().collect())
            .unwrap_or_default();
        for market_id in ids {
            match self.http.get_market(market_id).await {
                Ok(market) => {
                    if let Ok(context) = build_context(&market) {
                        if let Ok(mut index) = self.by_instrument.lock() {
                            index.retain(|_, id| *id != market_id);
                            for instrument in context.instruments() {
                                index.insert(instrument.id, market_id);
                            }
                        }
                        if let Ok(mut markets) = self.markets.lock() {
                            markets.insert(market_id, context.clone());
                        }
                        for instrument in context.instruments() {
                            let _ = self.sender.send(DataEvent::Data(Data::Instrument(
                                Box::new(InstrumentAny::BinaryOption(instrument.clone())),
                            )));
                        }
                    }
                }
                Err(e) => {
                    log::warn!("periodic refresh of market {market_id} failed: {e}");
                }
            }
        }
    }

    /// Refetches definitions for markets with status/close demand after a
    /// reconnect: trading status re-derives from `tradingStatus`, and a
    /// winner-only RESOLVED market re-publishes its close (fractional 0.5
    /// resolutions ride the WS event only — the REST market object cannot
    /// express them).
    async fn refetch_lifecycle(&self) {
        let markets: Vec<MarketContext> = {
            let (Some(demand), Some(index), Some(markets)) = (
                self.lock_demand(),
                self.lock_by_instrument(),
                self.lock_markets(),
            ) else {
                return;
            };
            demand
                .iter()
                .filter(|(_, d)| d.status > 0 || d.close > 0)
                .filter_map(|(instrument_id, _)| {
                    index
                        .get(instrument_id)
                        .and_then(|market_id| markets.get(market_id).cloned())
                })
                .collect()
        };
        let mut seen_markets = std::collections::HashSet::new();
        for market in markets {
            if !seen_markets.insert(market.market_id) {
                continue;
            }
            let already_closed = self
                .closed_markets
                .lock()
                .is_ok_and(|closed| closed.contains_key(&market.market_id));
            match self.http.get_market(market.market_id).await {
                Ok(dto) => {
                    let ts_init = self.now();
                    for instrument in market.instruments() {
                        if self.has_demand(&instrument.id, |d| d.status > 0) {
                            // REST facts carry no event instant: ts_event
                            // rides the observation time.
                            let event = trading_status_event(
                                instrument.id,
                                dto.trading_status,
                                ts_init.as_u64() as i64 / 1_000_000,
                                ts_init,
                            );
                            let _ = self
                                .sender
                                .send(DataEvent::Data(Data::InstrumentStatus(event)));
                        }
                    }
                    if already_closed {
                        continue;
                    }
                    // Winner-only close derivation from the resolved outcome.
                    if dto.status == PredictMarketStatus::Resolved
                        && let Some(winner) = &dto.resolution
                        && let Some(outcome_status) = winner.status
                    {
                        let payout = match outcome_status {
                            crate::common::enums::PredictOutcomeStatus::Won => {
                                rust_decimal::Decimal::ONE
                            }
                            crate::common::enums::PredictOutcomeStatus::Lost => {
                                rust_decimal::Decimal::ZERO
                            }
                        };
                        for instrument in market.instruments() {
                            if !self.has_demand(&instrument.id, |d| d.close > 0) {
                                continue;
                            }
                            let instrument_won = market
                                .on_chain_id_of(&instrument.id)
                                .is_some_and(|id| id == winner.on_chain_id);
                            let price = if instrument_won {
                                payout
                            } else {
                                rust_decimal::Decimal::ONE - payout
                            };
                            match crate::data::parse::resolved_outcome_close(
                                instrument.id,
                                &crate::websocket::messages::PredictWsResolvedOutcome {
                                    label: winner.name.clone(),
                                    index_set: i64::from(winner.index_set),
                                    price: price
                                        * rust_decimal::Decimal::new(1_000_000_000_000_000_000, 0),
                                    on_chain_id: winner.on_chain_id.clone(),
                                },
                                instrument.price_precision,
                                ts_init.as_u64() as i64 / 1_000_000,
                                ts_init,
                            ) {
                                Ok(close) => {
                                    let _ = self
                                        .sender
                                        .send(DataEvent::Data(Data::InstrumentClose(close)));
                                }
                                Err(e) => {
                                    log::warn!("reconnect close derivation rejected: {e}");
                                }
                            }
                        }
                        if let Ok(mut closed) = self.closed_markets.lock() {
                            closed.insert(market.market_id, ());
                        }
                    }
                }
                Err(e) => log::warn!(
                    "lifecycle refetch of market {} after reconnect failed: {e}",
                    market.market_id
                ),
            }
        }
    }

    fn lookup_market(&self, market_id: i64) -> Option<MarketContext> {
        self.markets.lock().ok()?.get(&market_id).cloned()
    }

    fn has_demand(
        &self,
        instrument_id: &InstrumentId,
        predicate: impl Fn(&InstrumentDemand) -> bool,
    ) -> bool {
        self.demand
            .lock()
            .ok()
            .and_then(|demand| demand.get(instrument_id).map(&predicate))
            .unwrap_or(false)
    }

    fn lock_markets(&self) -> Option<std::sync::MutexGuard<'_, MarketMap>> {
        self.markets.lock().ok()
    }

    fn lock_by_instrument(&self) -> Option<std::sync::MutexGuard<'_, InstrumentIndex>> {
        self.by_instrument.lock().ok()
    }

    fn lock_demand(&self) -> Option<std::sync::MutexGuard<'_, DemandMap>> {
        self.demand.lock().ok()
    }

    fn lock_trades(&self) -> Option<std::sync::MutexGuard<'_, HashMap<i64, TradesState>>> {
        self.trades.lock().ok()
    }
}
