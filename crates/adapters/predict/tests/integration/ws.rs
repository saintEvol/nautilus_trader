//! Local-peer protocol tests for the Predict WebSocket client (P3
//! completion evidence — mock source, fully offline):
//! - handshake presents the `x-api-key` header;
//! - reference-counted shared subscriptions: N owners → ONE wire
//!   subscribe, released only by the last owner (§5.2);
//! - server heartbeats are echoed verbatim before the next one;
//! - a dropped connection is reconnected and every retained topic is
//!   re-subscribed on the new generation (nothing is replayed).

// Tests assert through panics while keeping Result signatures for `?`.
#![allow(clippy::panic_in_result_fn)]
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use nautilus_core::string::secret::SecretString;
use nautilus_predict::websocket::PredictWebSocketClient;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;

type Frames = Arc<Mutex<Vec<(String, String)>>>; // (method, topic-or-value)

/// A frame-counting Predict WS peer. Serves any number of sequential
/// connections; every incoming frame is recorded, every request is acked,
/// and scripted pushes (heartbeats, close drops) fire per connection.
struct CountingPeer {
    listener: TcpListener,
    frames: Frames,
    push: Option<String>,
    drop_after: Option<Duration>,
}

impl CountingPeer {
    async fn bind() -> Self {
        Self::bind_with(None, None).await
    }

    async fn bind_with(push: Option<String>, drop_after: Option<Duration>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        Self {
            listener,
            frames: Arc::new(Mutex::new(Vec::new())),
            push,
            drop_after,
        }
    }

    fn url(&self) -> String {
        format!("ws://{}/ws", self.listener.local_addr().unwrap())
    }

    /// Serves `connections` sequential connections, then stops.
    #[allow(clippy::result_large_err)] // tungstenite's callback error type is fixed
    async fn serve(self, connections: usize, key: &'static str) -> Frames {
        let frames = Arc::clone(&self.frames);
        let recorder = Arc::clone(&frames);
        let listener = self.listener;
        let push = self.push.clone();
        let drop_after = self.drop_after;
        tokio::spawn(async move {
            for _ in 0..connections {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let seen = Arc::new(Mutex::new(None));
                let seen_key = Arc::clone(&seen);
                let ws = match tokio_tungstenite::accept_hdr_async(
                    stream,
                    move |req: &Request, resp: Response| {
                        *seen_key.lock().unwrap() = req
                            .headers()
                            .get("x-api-key")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        Ok(resp)
                    },
                )
                .await
                {
                    Ok(ws) => ws,
                    Err(_) => continue,
                };
                assert_eq!(seen.lock().unwrap().as_deref(), Some(key));
                if let Err(e) = serve_connection(ws, &recorder, &push, drop_after).await {
                    eprintln!("peer connection ended: {e}");
                }
            }
        });
        frames
    }
}

async fn serve_connection(
    mut ws: tokio_tungstenite::WebSocketStream<TcpStream>,
    frames: &Frames,
    push: &Option<String>,
    drop_after: Option<Duration>,
) -> anyhow::Result<()> {
    if let Some(push) = push {
        ws.send(Message::Text(push.clone().into())).await?;
    }
    let deadline = drop_after.map(|d| tokio::time::Instant::now() + d);
    loop {
        let next = futures_util::future::OptionFuture::from(
            deadline.map(|d| tokio::time::sleep_until(d)),
        );
        tokio::select! {
            _ = next, if deadline.is_some() => {
                // Abrupt drop: no close frame (silent death — the client
                // must detect it via its liveness check).
                return Ok(());
            }
            message = ws.next() => {
                let Some(message) = message else { return Ok(()) };
                let Message::Text(text) = message? else { continue };
                let value: serde_json::Value = serde_json::from_str(&text)?;
                let method = value["method"].as_str().unwrap_or_default().to_string();
                let topic = value["params"][0]
                    .as_str()
                    .or(value["data"].as_i64().map(|d| d.to_string()).as_deref())
                    .unwrap_or_default()
                    .to_string();
                frames.lock().unwrap().push((method.clone(), topic));
                match method.as_str() {
                    "subscribe" | "unsubscribe" => {
                        let response = serde_json::json!({
                            "type": "R",
                            "requestId": value["requestId"],
                            "success": true,
                        });
                        ws.send(Message::Text(response.to_string().into())).await?;
                    }
                    _ => {}
                }
            }
        }
    }
}

fn count(frames: &Frames, method: &str, topic: &str) -> usize {
    frames
        .lock()
        .unwrap()
        .iter()
        .filter(|(m, t)| m == method && t == topic)
        .count()
}

fn client(url: &str) -> (PredictWebSocketClient, tokio::sync::mpsc::UnboundedReceiver<nautilus_predict::websocket::PredictWsEvent>) {
    PredictWebSocketClient::new(
        url.to_string(),
        &SecretString::from("predict-data-key"),
        5,
        5,
        None,
    )
}

#[tokio::test]
async fn shared_subscription_release_happens_with_last_owner() -> anyhow::Result<()> {
    let peer = CountingPeer::bind().await;
    let url = peer.url();
    let frames = peer.serve(1, "predict-data-key").await;
    let (ws, mut events) = client(&url);
    ws.connect().await?;

    // Three owners share one topic: both legs' book consumers plus a
    // quote consumer of the same market (§5.2 shared reference count).
    ws.subscribe("predictOrderbook/123")?;
    ws.subscribe("predictOrderbook/123")?;
    ws.subscribe("predictOrderbook/123")?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        count(&frames, "subscribe", "predictOrderbook/123"),
        1,
        "three owners must produce exactly one wire subscribe"
    );
    // A Ready event settled after the ack.
    let mut saw_ready = false;
    while let Ok(event) = events.try_recv() {
        if matches!(event, nautilus_predict::websocket::PredictWsEvent::Ready { .. }) {
            saw_ready = true;
        }
    }
    assert!(saw_ready, "ready event must follow the ack");

    // Two owners release: the wire subscription must survive.
    ws.unsubscribe("predictOrderbook/123")?;
    ws.unsubscribe("predictOrderbook/123")?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(count(&frames, "unsubscribe", "predictOrderbook/123"), 0);

    // Last owner releases: exactly one wire unsubscribe.
    ws.unsubscribe("predictOrderbook/123")?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(count(&frames, "unsubscribe", "predictOrderbook/123"), 1);
    assert_eq!(
        count(&frames, "subscribe", "predictOrderbook/123"),
        1,
        "no re-subscribe may occur from a release"
    );

    ws.stop().await;
    Ok(())
}

#[tokio::test]
async fn heartbeat_is_echoed_verbatim() -> anyhow::Result<()> {
    let heartbeat = r#"{"type":"M","topic":"heartbeat","data":1738000000123}"#.to_string();
    let peer = CountingPeer::bind_with(Some(heartbeat), None).await;
    let url = peer.url();
    let frames = peer.serve(1, "predict-data-key").await;
    let (ws, _events) = client(&url);
    ws.connect().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(count(&frames, "heartbeat", "1738000000123"), 1);
    ws.stop().await;
    Ok(())
}

#[tokio::test]
async fn reconnect_resubscribes_retained_topics() -> anyhow::Result<()> {
    let peer = CountingPeer::bind_with(None, Some(Duration::from_millis(400))).await;
    let url = peer.url();
    let frames = peer.serve(2, "predict-data-key").await;
    let (ws, mut events) = client(&url);
    ws.connect().await?;
    ws.subscribe("predictOrderbook/77")?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(count(&frames, "subscribe", "predictOrderbook/77"), 1);

    // The peer drops the connection silently; the client must detect it,
    // reconnect and re-subscribe the retained topic on the new generation.
    let mut saw_disconnect = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        match events.try_recv() {
            Ok(nautilus_predict::websocket::PredictWsEvent::Disconnected { .. }) => {
                saw_disconnect = true;
            }
            Ok(nautilus_predict::websocket::PredictWsEvent::Ready { .. }) if saw_disconnect => {
                break;
            }
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(saw_disconnect, "silent death must be detected");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        count(&frames, "subscribe", "predictOrderbook/77"),
        2,
        "the retained topic must be re-subscribed once per connection"
    );
    ws.stop().await;
    Ok(())
}
