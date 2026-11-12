//! `PredictWebSocketClient` — lifecycle and persistent subscription intent
//! for the Predict WS API (`wss://ws.predict.fun/ws`).
//!
//! Contract (AsyncAPI): subscribe/unsubscribe requests are answered by
//! responses carrying the same `requestId`; every 15 s the server sends a
//! `heartbeat` topic message whose `data` must be echoed back verbatim;
//! messages are never replayed and subscriptions never carry over a
//! reconnect. This wrapper therefore:
//! - reference-counts topics (Yes/No legs and quote/book/status consumers
//!   of one market name the same topic string and share the wire
//!   subscription; the final owner releases it) via `nautilus_network`'s
//!   [`SubscriptionState`];
//! - owns reconnection (transport auto-reconnect is disabled): after a
//!   drop it reconnects with backoff, resubscribes every retained topic,
//!   and bumps a generation — downstream consumers refetch REST state on
//!   generation change instead of trusting replayed events;
//! - echoes heartbeats immediately, epoch-bound so a stale echo is never
//!   replayed onto a fresh connection.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nautilus_core::string::secret::SecretString;
use nautilus_network::transport::Message;
use nautilus_network::websocket::{
    EpochMessageHandler, SubscriptionState, WebSocketClient, WebSocketConfig,
};
use tokio_util::sync::CancellationToken;

use super::messages::{PredictHeartbeatReply, PredictWsErrorBody, PredictWsMessage, PredictWsRequest, decode_frame};
use crate::common::consts::API_KEY_HEADER;

const RECONNECT_INITIAL_MS: u64 = 1_000;
const RECONNECT_MAX_MS: u64 = 30_000;
/// Health/pending-expiry tick. Also the granularity at which a silently
/// dead transport (no close frame) is detected via `is_active()`.
const HEALTH_TICK_MS: u64 = 250;

/// Events from one connection generation.
#[derive(Debug, Clone)]
pub enum PredictWsEvent {
    /// Every retained topic is confirmed on this connection (also emitted
    /// after each reconnect — consumers refetch REST state then).
    Ready { generation: u64 },
    /// A decoded frame for a topic subscribed in this generation.
    Message {
        generation: u64,
        message: PredictWsMessage,
    },
    /// A sanitized protocol, subscription, or transport failure.
    Error { generation: u64, message: String },
    /// The transport dropped; this client reconnects by itself and a new
    /// generation follows. Nothing is replayed across the gap.
    Disconnected { generation: u64 },
}

/// Commands into the connection handler task.
enum Command {
    Frame {
        /// The handler generation the connection belonged to.
        generation: u64,
        epoch: u64,
        message: Message,
    },
    Reconcile,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingAction {
    Subscribe,
    Unsubscribe,
}

struct Pending {
    topic: String,
    action: PendingAction,
    sent_at: std::time::Instant,
}

#[derive(Default)]
struct ClientState {
    sender: Option<tokio::sync::mpsc::UnboundedSender<Command>>,
    disposed: bool,
}

struct Shared {
    url: String,
    proxy_url: Option<String>,
    headers: Vec<(String, String)>,
    connect_timeout_ms: u64,
    timeout_secs: u64,
    state: Mutex<ClientState>,
    generation: AtomicU64,
    ready: AtomicBool,
    ready_notify: tokio::sync::Notify,
    subscriptions: SubscriptionState,
    events: tokio::sync::mpsc::UnboundedSender<PredictWsEvent>,
    cancellation: CancellationToken,
}

/// A self-reconnecting Predict WebSocket client with reference-counted
/// topic subscriptions.
///
/// Clones share lifecycle, subscription intent, and the handler task.
#[derive(Clone)]
pub struct PredictWebSocketClient {
    inner: Arc<Shared>,
}

impl std::fmt::Debug for PredictWebSocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(PredictWebSocketClient))
            .field("active", &self.is_active())
            .finish_non_exhaustive()
    }
}

impl PredictWebSocketClient {
    /// Creates a disconnected client and its unbounded event receiver.
    #[must_use]
    pub fn new(
        url: String,
        api_key: &SecretString,
        connect_timeout_secs: u64,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> (Self, tokio::sync::mpsc::UnboundedReceiver<PredictWsEvent>) {
        let (events, receiver) = tokio::sync::mpsc::unbounded_channel();
        let headers = vec![(
            API_KEY_HEADER.to_string(),
            api_key.expose_secret().to_string(),
        )];
        let inner = Arc::new(Shared {
            url,
            proxy_url,
            headers,
            connect_timeout_ms: connect_timeout_secs.saturating_mul(1_000),
            timeout_secs,
            state: Mutex::new(ClientState::default()),
            generation: AtomicU64::new(0),
            ready: AtomicBool::new(false),
            ready_notify: tokio::sync::Notify::new(),
            subscriptions: SubscriptionState::new('/'),
            events,
            cancellation: CancellationToken::new(),
        });
        (Self { inner }, receiver)
    }

    /// Connects and waits until every retained topic is confirmed (or the
    /// timeout elapses). The handler task keeps the connection alive from
    /// here on, including reconnects.
    ///
    /// # Errors
    ///
    /// Returns an error when disposed or the first ready signal times out.
    pub async fn connect(&self) -> anyhow::Result<()> {
        let sender = {
            let mut state = self.lock_state();
            anyhow::ensure!(!state.disposed, "Predict WebSocket client is disposed");
            if state.sender.is_some() {
                return Ok(()); // Already connected (or connecting).
            }
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            // The handler observes the sender during its first establish():
            // install it BEFORE spawning, or the connect races a spurious
            // "handler is stopped" failure.
            state.sender = Some(sender.clone());
            let handler = Handler {
                shared: Arc::clone(&self.inner),
                ws_client: None,
                receiver,
                epoch: 0,
                request_seq: 1,
                pending: HashMap::new(),
            };
            tokio::spawn(handler.run());
            sender
        };
        // Kick reconciliation so intent recorded before connect is issued
        // once the first connection is up.
        let _ = sender.send(Command::Reconcile);
        let timeout = Duration::from_secs(self.inner.timeout_secs);
        tokio::time::timeout(timeout, self.wait_ready())
            .await
            .map_err(|_| anyhow::anyhow!("Predict WebSocket ready signal timed out"))?;
        Ok(())
    }

    async fn wait_ready(&self) {
        loop {
            let notified = self.inner.ready_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.is_active() {
                return;
            }

            notified.await;
        }
    }

    /// Whether a live connection has all retained topics confirmed.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.inner.ready.load(Ordering::Acquire)
    }

    /// The current connection generation (bumped on every reconnect).
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.inner.generation.load(Ordering::Acquire)
    }

    /// Adds one reference to a topic. Yes/No legs and quote/book/status
    /// consumers of one market name the same topic string and share the
    /// wire subscription; the first reference triggers the subscribe.
    ///
    /// # Errors
    ///
    /// Returns an error when the client is disposed or the handler stopped.
    pub fn subscribe(&self, topic: &str) -> anyhow::Result<()> {
        let state = self.lock_state();
        anyhow::ensure!(!state.disposed, "Predict WebSocket client is disposed");
        if self.inner.subscriptions.add_reference(topic) {
            self.inner.subscriptions.mark_subscribe(topic);
            notify_handler(&state)?;
        }
        Ok(())
    }

    /// Removes one reference; the final removal releases the wire
    /// subscription.
    ///
    /// # Errors
    ///
    /// Returns an error when the handler stopped.
    pub fn unsubscribe(&self, topic: &str) -> anyhow::Result<()> {
        let state = self.lock_state();
        if self.inner.subscriptions.remove_reference(topic) {
            self.inner.subscriptions.mark_unsubscribe(topic);
            notify_handler(&state)?;
        }
        Ok(())
    }

    /// Stops the connection and clears subscription intent.
    pub async fn stop(&self) {
        self.inner.cancellation.cancel();
        if let Ok(mut state) = self.inner.state.lock() {
            state.sender = None;
        }
        self.inner.ready.store(false, Ordering::Release);
        self.inner.subscriptions.clear();
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ClientState> {
        self.inner
            .state
            .lock()
            .expect("client state lock is not poisoned")
    }
}

fn notify_handler(state: &ClientState) -> anyhow::Result<()> {
    if let Some(sender) = &state.sender {
        sender
            .send(Command::Reconcile)
            .map_err(|_| anyhow::anyhow!("Predict WebSocket handler stopped"))?;
    }
    Ok(())
}

/// The connection handler task: owns the transport, request/response
/// correlation, heartbeat echo, and reconnection.
struct Handler {
    shared: Arc<Shared>,
    ws_client: Option<Arc<WebSocketClient>>,
    receiver: tokio::sync::mpsc::UnboundedReceiver<Command>,
    epoch: u64,
    request_seq: i64,
    pending: HashMap<i64, Pending>,
}

impl Handler {
    async fn run(mut self) {
        let mut backoff_ms = RECONNECT_INITIAL_MS;
        let mut tick = tokio::time::interval(Duration::from_millis(HEALTH_TICK_MS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            if self.shared.cancellation.is_cancelled() {
                break;
            }
            // The generation is bumped BEFORE the connection exists so
            // every frame can carry it: a fresh transport is created per
            // reconnect and its connection epoch does not distinguish
            // generations, so generation tagging (not the transport epoch)
            // is the stale-frame gate.
            let generation = self.shared.generation.fetch_add(1, Ordering::AcqRel) + 1;
            match self.establish(generation).await {
                Ok(()) => {
                    backoff_ms = RECONNECT_INITIAL_MS;
                }
                Err(message) => {
                    self.emit_error(format!("connection failed: {message}"));
                    tokio::select! {
                        () = self.shared.cancellation.cancelled() => break,
                        () = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                    }
                    backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
                    continue;
                }
            }

            self.pending.clear();
            self.shared.ready.store(false, Ordering::Release);
            // Reconnects carry nothing over: retained intent is re-issued
            // on the fresh connection (reference counts survive).
            self.shared.subscriptions.reset_after_reconnect();
            if let Err(message) = self.reconcile().await {
                self.emit_error(message);
            }
            self.refresh_ready();

            let transport_gone = loop {
                tokio::select! {
                    biased;
                    () = self.shared.cancellation.cancelled() => break false,
                    _ = tick.tick() => {
                        if self.ws_client.as_ref().is_none_or(|c| !c.is_active()) {
                            break true; // Silent death: no close frame ever arrived.
                        }
                        self.expire_stale_pending();
                        if let Err(message) = self.reconcile().await {
                            self.emit_error(message);
                        }
                        self.refresh_ready();
                    }
                    command = self.receiver.recv() => match command {
                        Some(Command::Frame { generation: frame_generation, epoch, message }) => {
                            if frame_generation != self.generation() {
                                continue; // Queued frame from a dead connection.
                            }
                            // Remember the live connection's epoch so
                            // out-of-frame sends (subscribe/unsubscribe)
                            // target it.
                            self.epoch = epoch;
                            if let Err(message) = self.handle_frame(epoch, message).await {
                                self.emit_error(message);
                            }
                            self.refresh_ready();
                        }
                        Some(Command::Reconcile) => {
                            if let Err(message) = self.reconcile().await {
                                self.emit_error(message);
                            }
                            self.refresh_ready();
                        }
                        // The shell dropped its sender: shutdown.
                        None => break false,
                    },
                }
            };

            if transport_gone {
                let generation = self.shared.generation.load(Ordering::Acquire);
                let _ = self
                    .shared
                    .events
                    .send(PredictWsEvent::Disconnected { generation });
                self.shared.ready.store(false, Ordering::Release);
                if let Some(client) = self.ws_client.take() {
                    client.disconnect().await;
                }
                tokio::select! {
                    () = self.shared.cancellation.cancelled() => break,
                    () = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                }
                backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
            }
        }
        self.shared.ready.store(false, Ordering::Release);
        if let Ok(mut state) = self.shared.state.lock() {
            state.sender = None;
        }
    }

    /// Opens a fresh connection. The shell's command channel stays the
    /// same across reconnects; only the transport is replaced.
    fn generation(&self) -> u64 {
        self.shared.generation.load(Ordering::Acquire)
    }

    async fn establish(&mut self, generation: u64) -> Result<(), String> {
        let config = WebSocketConfig::builder()
            .url(self.shared.url.clone())
            .headers(self.shared.headers.clone())
            .connect_timeout_ms(self.shared.connect_timeout_ms)
            // Reconnection is owned here, not by the transport: every new
            // connection must re-issue subscriptions and bump the
            // generation before any frame is trusted.
            .reconnect_max_attempts(0)
            .maybe_proxy_url(self.shared.proxy_url.clone())
            .build()
            .map_err(|e| e.to_string())?;

        let input = {
            let state = self
                .shared
                .state
                .lock()
                .map_err(|_| "client state lock poisoned".to_string())?;
            state
                .sender
                .clone()
                .ok_or_else(|| "handler is stopped".to_string())?
        };
        let handler: EpochMessageHandler = Arc::new(move |epoch, message| {
            let _ = input.send(Command::Frame {
                generation,
                epoch,
                message,
            });
        });

        let client = WebSocketClient::epoch_builder()
            .config(config)
            .epoch_handler(handler)
            .connect()
            .await
            .map_err(|e| e.redacted_message())?;
        self.ws_client = Some(Arc::new(client));
        Ok(())
    }

    async fn handle_frame(&mut self, epoch: u64, message: Message) -> Result<(), String> {
        let bytes: &[u8] = match &message {
            Message::Text(text) => text.as_ref(),
            Message::Binary(bytes) => bytes.as_ref(),
            _ => return Ok(()),
        };
        let decoded = decode_frame(bytes).map_err(|e| format!("malformed frame: {e}"))?;
        match decoded {
            PredictWsMessage::Heartbeat { server_time_ms } => {
                let echo = PredictHeartbeatReply::new(server_time_ms);
                self.send_text(serde_json::to_string(&echo).expect("static shape"), epoch)
                    .await
            }
            PredictWsMessage::Response {
                request_id,
                success,
                error,
            } => {
                // Acks for expired-and-requeued requests arrive late
                // routinely — noise, not protocol damage.
                if self
                    .acknowledge(request_id, success, error)
                    .is_none()
                {
                    log::debug!("response for unknown (expired) requestId {request_id}");
                }
                Ok(())
            }
            PredictWsMessage::ReconnectNotice(notice) => {
                // Grace period keeps delivering; drop the connection early
                // to start the resubscription before the server does.
                log::info!(
                    "Predict server draining ({} grace {}ms); reconnecting",
                    notice.reason,
                    notice.grace_ms
                );
                if let Some(client) = self.ws_client.take() {
                    client.disconnect().await;
                }
                Ok(())
            }
            other => {
                if let Some(topic) = topic_of(&other)
                    && !self.is_subscribed_topic(&topic)
                {
                    log::debug!("dropping frame for unsubscribed topic {topic}");
                    return Ok(());
                }
                let generation = self.shared.generation.load(Ordering::Acquire);
                let _ = self.shared.events.send(PredictWsEvent::Message {
                    generation,
                    message: other,
                });
                Ok(())
            }
        }
    }

    async fn send_text(&self, text: String, epoch: u64) -> Result<(), String> {
        let Some(client) = &self.ws_client else {
            return Err("send without a live connection".to_string());
        };
        // Epoch-bound: never replayed onto a later connection by the
        // transport's writer buffer.
        client
            .send_text_on_connection(text, None, epoch)
            .await
            .map_err(|e| format!("send failed: {e}"))
    }

    fn acknowledge(
        &mut self,
        request_id: i64,
        success: bool,
        error: Option<PredictWsErrorBody>,
    ) -> Option<()> {
        let pending = self.pending.remove(&request_id)?;
        match (pending.action, success) {
            (PendingAction::Subscribe, true) => {
                self.shared.subscriptions.confirm_subscribe(&pending.topic);
            }
            (PendingAction::Unsubscribe, true) => {
                self.shared.subscriptions.confirm_unsubscribe(&pending.topic);
            }
            (_, false) => {
                // Failed requests return their topic to the pending set so
                // the next reconciliation retries.
                self.shared.subscriptions.mark_failure(&pending.topic);
                let message = error.map_or_else(
                    || format!("request for {} failed without an error body", pending.topic),
                    |e| {
                        format!(
                            "request for {} failed: {}: {}",
                            pending.topic,
                            e.code,
                            e.message.unwrap_or_default()
                        )
                    },
                );
                self.emit_error(message);
            }
        }
        Some(())
    }

    async fn reconcile(&mut self) -> Result<(), String> {
        if self.ws_client.is_none() {
            return Ok(()); // Nothing live yet; retained intent waits.
        }
        let subscribes = self.shared.subscriptions.pending_subscribe_topics();
        for topic in subscribes {
            self.dispatch_request(&topic, PendingAction::Subscribe).await?;
        }
        let unsubscribes = self.shared.subscriptions.pending_unsubscribe_topics();
        for topic in unsubscribes {
            self.dispatch_request(&topic, PendingAction::Unsubscribe).await?;
        }
        Ok(())
    }

    async fn dispatch_request(&mut self, topic: &str, action: PendingAction) -> Result<(), String> {
        // One in-flight request per (topic, action): a reconcile tick
        // during an unacked request must not double-send.
        if self
            .pending
            .values()
            .any(|pending| pending.topic == topic && pending.action == action)
        {
            return Ok(());
        }
        let epoch = self.epoch;
        let request_id = self.request_seq;
        self.request_seq += 1;
        let request = match action {
            PendingAction::Subscribe => PredictWsRequest::subscribe(request_id, topic),
            PendingAction::Unsubscribe => PredictWsRequest::unsubscribe(request_id, topic),
        };
        self.send_text(serde_json::to_string(&request).expect("static shape"), epoch)
            .await?;
        self.pending.insert(
            request_id,
            Pending {
                topic: topic.to_string(),
                action,
                sent_at: std::time::Instant::now(),
            },
        );
        Ok(())
    }

    fn expire_stale_pending(&mut self) {
        let timeout = Duration::from_secs(self.shared.timeout_secs);
        let stale: Vec<i64> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.sent_at.elapsed() > timeout)
            .map(|(id, _)| *id)
            .collect();
        for id in stale {
            if let Some(pending) = self.pending.remove(&id) {
                self.shared.subscriptions.mark_failure(&pending.topic);
                self.emit_error(format!(
                    "request for {} timed out without a response",
                    pending.topic
                ));
            }
        }
    }

    fn refresh_ready(&self) {
        let settled = self
            .shared
            .subscriptions
            .pending_subscribe_topics()
            .is_empty()
            && self
                .shared
                .subscriptions
                .pending_unsubscribe_topics()
                .is_empty();
        if settled && self.ws_client.as_ref().is_some_and(|c| c.is_active()) {
            if !self.shared.ready.swap(true, Ordering::AcqRel) {
                let generation = self.shared.generation.load(Ordering::Acquire);
                let _ = self
                    .shared
                    .events
                    .send(PredictWsEvent::Ready { generation });
                self.shared.ready_notify.notify_waiters();
            }
        } else {
            self.shared.ready.store(false, Ordering::Release);
        }
    }

    fn is_subscribed_topic(&self, topic: &str) -> bool {
        let Some((channel, symbol)) = topic.split_once('/') else {
            return false;
        };
        let channel = ustr::Ustr::from(channel);
        let symbol = ustr::Ustr::from(symbol);
        self.shared.subscriptions.is_subscribed(&channel, &symbol)
    }

    fn emit_error(&self, message: String) {
        let generation = self.shared.generation.load(Ordering::Acquire);
        log::warn!("Predict WebSocket: {message}");
        let _ = self.shared.events.send(PredictWsEvent::Error {
            generation,
            message,
        });
    }
}

fn topic_of(message: &PredictWsMessage) -> Option<String> {
    use super::messages::market_topic;
    match message {
        PredictWsMessage::Orderbook(data) => {
            Some(market_topic(crate::common::consts::TOPIC_ORDERBOOK, data.market_id))
        }
        PredictWsMessage::TopOfBook(data) => Some(market_topic(
            crate::common::consts::TOPIC_TOP_OF_BOOK,
            data.market_id,
        )),
        PredictWsMessage::TradingStatus(data) => Some(market_topic(
            crate::common::consts::TOPIC_TRADING_STATUS,
            data.market_id,
        )),
        PredictWsMessage::MarketStatus(data) => Some(market_topic(
            crate::common::consts::TOPIC_MARKET_STATUS,
            data.market_id,
        )),
        PredictWsMessage::MarketChanged(data) => Some(market_topic(
            crate::common::consts::TOPIC_MARKET_CHANGED,
            data.market_id,
        )),
        PredictWsMessage::Other { topic, .. } => Some(topic.clone()),
        PredictWsMessage::Response { .. }
        | PredictWsMessage::Heartbeat { .. }
        | PredictWsMessage::ReconnectNotice(_) => None,
    }
}
