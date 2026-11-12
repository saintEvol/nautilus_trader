//! Whole-client integration tests: `PredictDataClient` assembled against
//! local HTTP + WS peers (P3 assembly evidence — the layer the unit tests
//! do not reach): connect lifecycle (the TaskGroup generation gate),
//! demand-layer shared subscriptions across both legs, dual-projection
//! publication, and last-owner wire release.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use nautilus_common::clients::DataClient as _;
use nautilus_common::messages::{DataEvent, data::*};
use nautilus_core::UUID4;
use nautilus_core::string::secret::SecretString;
use nautilus_core::UnixNanos;
use nautilus_model::{data::Data, enums::BookType};
use tokio::io::AsyncReadExt as _;
use nautilus_predict::config::PredictDataClientConfig;
use nautilus_predict::data::PredictDataClient;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

const MARKET_JSON: &str = r#"{
    "id": 4242, "imageUrl": "i", "title": "T", "question": "Q?", "description": "d",
    "tradingStatus": "OPEN", "status": "REGISTERED", "isVisible": true,
    "isNegRisk": false, "isYieldBearing": false, "feeRateBps": 100, "makerFeeBps": 0,
    "oracleQuestionId": "o", "conditionId": "0xcond", "resolverAddress": "r",
    "outcomes": [
        {"name": "Yes", "indexSet": 1, "onChainId": "111"},
        {"name": "No", "indexSet": 2, "onChainId": "222"}
    ],
    "spreadThreshold": 0.01, "shareThreshold": 1, "isBoosted": false,
    "polymarketConditionIds": [], "categorySlug": "c",
    "createdAt": "2026-10-01T00:00:00Z", "decimalPrecision": 3, "marketVariant": "DEFAULT"
}"#;

const BOOK_FRAME: &str = r#"{"type":"M","topic":"predictOrderbook/4242","data":{
    "version":1,"marketId":4242,"updateTimestampMs":1727910141000,"orderCount":2,
    "asks":[[0.492,30192.26]],
    "bids":[[0.491,303518.1]],
    "lastOrderSettled":null,
    "settlementsPending":{"asks":[],"bids":[]}
}}"#;

/// One HTTP peer serving the catalog walk, plus one WS peer.
struct Peers {
    http_addr: std::net::SocketAddr,
    ws_addr: std::net::SocketAddr,
    ws_frames: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
}

impl Peers {
    async fn start() -> Self {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let ws_frames = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        // HTTP: serve the catalog page for every request.
        {
            tokio::spawn(async move {
                let body = format!("{{\"success\":true,\"data\":[{MARKET_JSON}]}}");
                loop {
                    let Ok((mut stream, _)) = http_listener.accept().await else {
                        break;
                    };
                    let mut buffer = [0u8; 8192];
                    let Ok(read) = stream.read(&mut buffer).await else { break };
                    let _ = read;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.try_write(response.as_bytes());
                }
            });
        }

        // WS: ack requests (record frames), push one book frame after the
        // first subscribe ack, keep serving.
        {
            let ws_frames = std::sync::Arc::clone(&ws_frames);
            let mut book_pushed = false;
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = ws_listener.accept().await else {
                        break;
                    };
                    let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                        continue;
                    };
                    while let Some(Ok(message)) = ws.next().await {
                        let Message::Text(text) = message else { continue };
                        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                        let method = value["method"].as_str().unwrap_or_default().to_string();
                        let topic = value["params"][0].as_str().unwrap_or_default().to_string();
                        ws_frames
                            .lock()
                            .unwrap()
                            .push((method.clone(), topic.clone()));
                        if method == "subscribe" || method == "unsubscribe" {
                            let response = serde_json::json!({
                                "type": "R",
                                "requestId": value["requestId"],
                                "success": true,
                            });
                            ws.send(Message::Text(response.to_string().into()))
                                .await
                                .unwrap();
                            if !book_pushed {
                                book_pushed = true;
                                ws.send(Message::Text(BOOK_FRAME.into())).await.unwrap();
                            }
                        }
                    }
                }
            });
        }

        Self {
            http_addr,
            ws_addr,
            ws_frames,
        }
    }

    fn config(&self) -> PredictDataClientConfig {
        PredictDataClientConfig::builder()
            .api_key(SecretString::from("predict-data-key"))
            .base_url_http(format!("http://{}", self.http_addr))
            .base_url_ws(format!("ws://{}", self.ws_addr))
            .trades_poll_interval_secs(3600) // keep the poller quiet
            .instrument_ids(vec![
                "0xcond-111.PREDICT".parse().unwrap(),
                "0xcond-222.PREDICT".parse().unwrap(),
            ])
            .build()
    }
}

fn count(frames: &std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>, method: &str, topic: &str) -> usize {
    frames
        .lock()
        .unwrap()
        .iter()
        .filter(|(m, t)| m == method && t == topic)
        .count()
}

#[tokio::test]
async fn client_connects_shares_subscriptions_across_legs_and_releases_with_last_owner() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let peers = Peers::start().await;
        let (sender, mut events) = tokio::sync::mpsc::unbounded_channel();
        let mut client = PredictDataClient::new(peers.config(), sender.into()).unwrap();

        // Demand BEFORE connect (the assembly layer must replay it).
        client
            .subscribe_book_deltas(SubscribeBookDeltas::new(
                vec!["0xcond-111.PREDICT".parse().unwrap()],
                BookType::L2_MBP,
                None,
                Some(nautilus_model::identifiers::Venue::from("PREDICT")),
                UUID4::new(),
                UnixNanos::default(),
                None,
                false,
                None,
                None,
            ))
            .unwrap();
        client
            .subscribe_quotes(SubscribeQuotes::new(
                vec!["0xcond-222.PREDICT".parse().unwrap()],
                None,
                Some(nautilus_model::identifiers::Venue::from("PREDICT")),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();
        // The No leg also wants the book: three consumers, still ONE wire
        // topic (book x2 legs + quote).
        client
            .subscribe_book_deltas(SubscribeBookDeltas::new(
                vec!["0xcond-222.PREDICT".parse().unwrap()],
                BookType::L2_MBP,
                None,
                Some(nautilus_model::identifiers::Venue::from("PREDICT")),
                UUID4::new(),
                UnixNanos::default(),
                None,
                false,
                None,
                None,
            ))
            .unwrap();

        // The P0 regression: a fresh client must connect (TaskGroup drain
        // dance) and resolve its configured instruments.
        client.connect().await.expect("fresh client connects");
        assert!(client.is_connected());

        tokio::time::sleep(Duration::from_millis(400)).await;
        let topic = "predictOrderbook/4242";
        // Both legs (book on Yes, quotes on No) share ONE wire subscribe.
        assert_eq!(count(&peers.ws_frames, "subscribe", topic), 1);
        // marketChanged rides along with the orderbook demand.
        assert_eq!(
            count(&peers.ws_frames, "subscribe", "predictMarketChanged/4242"),
            1
        );

        // The pushed book frame publishes the Yes batch, the No projection
        // and both derived quotes.
        let mut saw_yes = false;
        let mut saw_no = false;
        let mut saw_quote = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline && !(saw_yes && saw_no && saw_quote) {
            match events.try_recv() {
                Ok(DataEvent::Data(Data::BookDeltas(batch))) => {
                    match batch.instrument_id.to_string().as_str() {
                        "0xcond-111.PREDICT" => saw_yes = true,
                        "0xcond-222.PREDICT" => saw_no = true,
                        other => panic!("unexpected book instrument {other}"),
                    }
                }
                Ok(DataEvent::Data(Data::Quote(quote))) => {
                    saw_quote = true;
                    assert!(
                        quote.instrument_id.to_string() == "0xcond-222.PREDICT",
                        "only the No leg demanded quotes, got {}",
                        quote.instrument_id
                    );
                    assert_eq!(quote.bid_price.to_string(), "0.508");
                }
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(saw_yes, "Yes batch must publish");
        assert!(saw_no, "No projection batch must publish");
        assert!(saw_quote, "No-leg quote must publish");

        // Release both owners: the wire topic dies exactly once.
        client
            .unsubscribe_book_deltas(&UnsubscribeBookDeltas::new(
                vec!["0xcond-111.PREDICT".parse().unwrap()],
                None,
                Some(nautilus_model::identifiers::Venue::from("PREDICT")),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(count(&peers.ws_frames, "unsubscribe", topic), 0);

        client
            .unsubscribe_book_deltas(&UnsubscribeBookDeltas::new(
                vec!["0xcond-222.PREDICT".parse().unwrap()],
                None,
                Some(nautilus_model::identifiers::Venue::from("PREDICT")),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(count(&peers.ws_frames, "unsubscribe", topic), 0);

        client
            .unsubscribe_quotes(&UnsubscribeQuotes::new(
                vec!["0xcond-222.PREDICT".parse().unwrap()],
                None,
                Some(nautilus_model::identifiers::Venue::from("PREDICT")),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(count(&peers.ws_frames, "unsubscribe", topic), 1);

        client.disconnect().await.unwrap();
        assert!(client.is_disconnected());
    })
    .await
    .expect("test completes within the timeout");
}
