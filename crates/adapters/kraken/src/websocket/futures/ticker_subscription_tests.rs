// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use nautilus_model::identifiers::InstrumentId;
use nautilus_network::websocket::SubscriptionState;
use rstest::rstest;
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    time::{Instant, timeout},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use ustr::Ustr;

use super::{KrakenFuturesWebSocketClient, KrakenFuturesWsMessage};
use crate::websocket::futures::{
    messages::{KrakenFuturesEvent, KrakenFuturesFeed, KrakenFuturesSubscriptionResponse},
    ticker_subscriptions::TickerSubscriptions,
};

const INVALID_PRODUCT_ALERT: &str = "Couldn't subscribe to invalid product `PF_ANTHROPICXUSD`";

async fn request(socket: &mut WebSocketStream<TcpStream>) -> Value {
    timeout(Duration::from_secs(8), async {
        while let Some(message) = socket.next().await {
            if let Message::Text(text) = message.unwrap() {
                return serde_json::from_str(&text).unwrap();
            }
        }
        panic!("socket closed without ticker request");
    })
    .await
    .expect("ticker request timed out")
}

async fn respond(socket: &mut WebSocketStream<TcpStream>, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

#[tokio::test]
async fn ticker_subscriptions_confirm_only_acked_products_and_retry_with_backoff() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = KrakenFuturesWebSocketClient::new(format!("ws://{addr}"), 30, None);
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(socket).await.unwrap();

        assert_eq!(
            request(&mut socket).await,
            json!({
                "event":"subscribe","feed":"ticker","product_ids":["PF_ETHUSD","PF_XBTUSD"]
            })
        );
        socket
    });
    client.connect().await.unwrap();
    let eth: InstrumentId = "PF_ETHUSD.KRAKEN".parse().unwrap();
    let xbt: InstrumentId = "PF_XBTUSD.KRAKEN".parse().unwrap();
    client.subscribe_funding_rate(xbt).await.unwrap();
    client.subscribe_funding_rate(eth).await.unwrap();
    client.subscribe_mark_price(xbt).await.unwrap();
    let ticker = Ustr::from("ticker");
    let mut socket = server.await.unwrap();
    assert!(client.subscriptions.confirmed().get(&ticker).is_none());

    // Partial acknowledgment followed by an alert with no product identity
    respond(
        &mut socket,
        json!({
            "event":"subscribed","feed":"ticker","product_ids":["PF_XBTUSD","PF_NOT_REQUESTED"]
        }),
    )
    .await;
    let retry_started = Instant::now();
    respond(
        &mut socket,
        json!({"event":"alert","message":"rate_limited"}),
    )
    .await;
    assert_eq!(
        request(&mut socket).await,
        json!({
            "event":"subscribe","feed":"ticker","product_ids":["PF_ETHUSD"]
        })
    );
    assert!(retry_started.elapsed() >= Duration::from_millis(1_200));
    let confirmed = client.subscriptions.confirmed();
    assert_eq!(confirmed[&ticker].len(), 1);
    assert!(confirmed[&ticker].contains(&Ustr::from("PF_XBTUSD")));

    respond(
        &mut socket,
        json!({
            "event":"subscribed","feed":"ticker","product_ids":["PF_ETHUSD"]
        }),
    )
    .await;
    // The handler forwards valid ticker messages after preceding control frames
    respond(
        &mut socket,
        json!({"feed":"ticker","product_id":"PF_ETHUSD"}),
    )
    .await;
    let mut output = client.take_output_rx().unwrap();
    timeout(Duration::from_secs(2), output.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(client.subscriptions.confirmed()[&ticker].len(), 2);

    client.unsubscribe_funding_rate(xbt).await.unwrap();
    client.unsubscribe_mark_price(xbt).await.unwrap();
    assert_eq!(
        request(&mut socket).await,
        json!({
            "event":"unsubscribe","feed":"ticker","product_ids":["PF_XBTUSD"]
        })
    );
    let retry_started = Instant::now();
    respond(
        &mut socket,
        json!({"event":"unsubscribed_failed","feed":"ticker","product_ids":["PF_XBTUSD"]}),
    )
    .await;
    assert_eq!(
        request(&mut socket).await,
        json!({"event":"unsubscribe","feed":"ticker","product_ids":["PF_XBTUSD"]}),
    );
    assert!(retry_started.elapsed() >= Duration::from_millis(1_200));
    assert!(client.subscriptions.pending_unsubscribe()[&ticker].contains(&Ustr::from("PF_XBTUSD")));
    respond(
        &mut socket,
        json!({
            "event":"unsubscribed","feed":"ticker","product_ids":["PF_XBTUSD"]
        }),
    )
    .await;
    respond(
        &mut socket,
        json!({"feed":"ticker","product_id":"PF_ETHUSD"}),
    )
    .await;
    timeout(Duration::from_secs(2), output.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        client
            .subscriptions
            .pending_unsubscribe()
            .get(&ticker)
            .is_none()
    );
    client.close().await.unwrap();
}

#[tokio::test]
async fn ticker_subscriptions_reconnect_in_batches_and_require_fresh_acknowledgments() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(socket).await.unwrap();
        assert_eq!(
            request(&mut socket).await,
            json!({"event":"subscribe","feed":"ticker","product_ids":["PF_ETHUSD","PF_XBTUSD"]}),
        );
        respond(
            &mut socket,
            json!({"event":"subscribed","feed":"ticker","product_ids":["PF_ETHUSD","PF_XBTUSD"]}),
        )
        .await;
        respond(
            &mut socket,
            json!({"feed":"ticker","product_id":"PF_ETHUSD"}),
        )
        .await;
        ready_tx.send(()).unwrap();
        resume_rx.await.unwrap();
        drop(socket);

        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(socket).await.unwrap();
        assert_eq!(
            request(&mut socket).await,
            json!({"event":"subscribe","feed":"ticker","product_ids":["PF_ETHUSD","PF_XBTUSD"]}),
        );
        socket
    });

    let mut client = KrakenFuturesWebSocketClient::new(format!("ws://{addr}"), 30, None);
    client.connect().await.unwrap();
    let mut output = client.take_output_rx().unwrap();
    client
        .subscribe_funding_rate(InstrumentId::from("PF_ETHUSD.KRAKEN"))
        .await
        .unwrap();
    client
        .subscribe_funding_rate(InstrumentId::from("PF_XBTUSD.KRAKEN"))
        .await
        .unwrap();

    ready_rx.await.unwrap();
    timeout(Duration::from_secs(2), output.recv())
        .await
        .unwrap()
        .unwrap();
    let ticker = Ustr::from("ticker");
    assert_eq!(client.subscriptions.confirmed()[&ticker].len(), 2);
    resume_tx.send(()).unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(8), output.recv())
            .await
            .unwrap(),
        Some(KrakenFuturesWsMessage::Reconnected),
    ));
    let mut socket = server.await.unwrap();
    assert!(client.subscriptions.confirmed().get(&ticker).is_none());
    respond(
        &mut socket,
        json!({"event":"subscribed","feed":"ticker","product_ids":["PF_ETHUSD","PF_XBTUSD"]}),
    )
    .await;
    respond(
        &mut socket,
        json!({"feed":"ticker","product_id":"PF_ETHUSD"}),
    )
    .await;
    timeout(Duration::from_secs(2), output.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(client.subscriptions.confirmed()[&ticker].len(), 2);
    client.close().await.unwrap();
}

#[tokio::test]
async fn ticker_subscriptions_complete_invalid_product_unsubscribe_and_continue() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        accept_async(socket).await.unwrap()
    });
    let mut client = KrakenFuturesWebSocketClient::new(format!("ws://{addr}"), 30, None);
    client.connect().await.unwrap();
    let mut output = client.take_output_rx().unwrap();
    let mut socket = server.await.unwrap();
    let removed = InstrumentId::from("PF_ANTHROPICXUSD.KRAKEN");
    let xbt = InstrumentId::from("PF_XBTUSD.KRAKEN");
    let eth = InstrumentId::from("PF_ETHUSD.KRAKEN");

    client.subscribe_funding_rate(removed).await.unwrap();
    client.subscribe_funding_rate(xbt).await.unwrap();
    assert_eq!(
        request(&mut socket).await,
        json!({"event":"subscribe","feed":"ticker","product_ids":["PF_ANTHROPICXUSD","PF_XBTUSD"]}),
    );
    respond(
        &mut socket,
        json!({"event":"subscribed","feed":"ticker","product_ids":["PF_ANTHROPICXUSD","PF_XBTUSD"]}),
    )
    .await;
    respond(
        &mut socket,
        json!({"feed":"ticker","product_id":"PF_XBTUSD"}),
    )
    .await;
    timeout(Duration::from_secs(2), output.recv())
        .await
        .expect("initial subscription marker")
        .unwrap();

    client.unsubscribe_funding_rate(removed).await.unwrap();
    client.unsubscribe_funding_rate(xbt).await.unwrap();
    assert_eq!(
        request(&mut socket).await,
        json!({"event":"unsubscribe","feed":"ticker","product_ids":["PF_ANTHROPICXUSD","PF_XBTUSD"]}),
    );
    client.subscribe_funding_rate(eth).await.unwrap();
    respond(
        &mut socket,
        json!({"event":"alert","message":INVALID_PRODUCT_ALERT}),
    )
    .await;
    respond(
        &mut socket,
        json!({"event":"unsubscribed","feed":"ticker","product_ids":["PF_XBTUSD"]}),
    )
    .await;
    assert_eq!(
        timeout(Duration::from_secs(2), request(&mut socket))
            .await
            .expect("next subscribe must be sent before the unsubscribe timeout"),
        json!({"event":"subscribe","feed":"ticker","product_ids":["PF_ETHUSD"]}),
    );
    assert!(
        client
            .subscriptions
            .pending_unsubscribe()
            .get(&Ustr::from("ticker"))
            .is_none()
    );
    client.close().await.unwrap();
}

#[rstest]
fn ticker_subscriptions_retry_invalid_subscribe_without_delaying_other_products() {
    let mut ticker = TickerSubscriptions::new();
    let subscriptions = SubscriptionState::new(':');
    let now = Instant::now();
    subscriptions.mark_subscribe("ticker:PF_ANTHROPICXUSD");
    let request = ticker.next_request(&subscriptions, now).unwrap();
    assert_eq!(request.product_ids, ["PF_ANTHROPICXUSD"]);

    ticker.handle_alert(
        "Couldn't subscribe to invalid product `PF_ETHUSD`",
        &subscriptions,
        now,
    );
    assert!(ticker.deadline().unwrap() > now + Duration::from_secs(2));

    ticker.handle_alert(INVALID_PRODUCT_ALERT, &subscriptions, now);
    assert!(subscriptions.confirmed().is_empty());
    let advance = ticker.deadline().unwrap();
    assert!(ticker.next_request(&subscriptions, advance).is_none());
    let retry_at = ticker.deadline().unwrap();
    assert!(retry_at > advance);

    let new_intent_at = advance + Duration::from_secs(1);
    subscriptions.mark_subscribe("ticker:PF_XBTUSD");
    ticker.sync(new_intent_at);
    let next = ticker.deadline().unwrap();
    assert!(next < retry_at);
    let request = ticker.next_request(&subscriptions, next).unwrap();
    assert_eq!(request.event, KrakenFuturesEvent::Subscribe);
    assert_eq!(request.product_ids, ["PF_XBTUSD"]);
    ticker.acknowledge(
        &KrakenFuturesSubscriptionResponse {
            event: KrakenFuturesEvent::Subscribed,
            feed: KrakenFuturesFeed::Ticker,
            product_ids: request.product_ids,
        },
        &subscriptions,
        next,
    );
    let retry = ticker.next_request(&subscriptions, retry_at).unwrap();
    assert_eq!(retry.product_ids, ["PF_ANTHROPICXUSD"]);
}
