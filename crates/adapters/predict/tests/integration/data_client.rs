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

/// One HTTP peer serving the catalog walk and `/orders/matches`, plus one
/// WS peer.
struct Peers {
    http_addr: std::net::SocketAddr,
    ws_addr: std::net::SocketAddr,
    ws_frames: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    /// `/orders/matches` request counter (poll cadence evidence).
    matches_requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// The body served for each `/orders/matches` request, by request
    /// index (repeats the last entry once exhausted).
    matches_body: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl Peers {
    async fn start() -> Self {
        Self::start_with(None).await
    }

    async fn start_with(extra_frame: Option<&str>) -> Self {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let ws_frames = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let matches_requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let matches_body = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        // HTTP: route by path — catalog walk vs `/orders/matches`.
        {
            let matches_requests = std::sync::Arc::clone(&matches_requests);
            let matches_body = std::sync::Arc::clone(&matches_body);
            tokio::spawn(async move {
                let body = format!("{{\"success\":true,\"data\":[{MARKET_JSON}]}}");
                loop {
                    let Ok((mut stream, _)) = http_listener.accept().await else {
                        break;
                    };
                    let mut buffer = [0u8; 8192];
                    let Ok(read) = stream.read(&mut buffer).await else { break };
                    let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                    let body = if request.starts_with("GET /orders/matches") {
                        let index =
                            matches_requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let bodies = matches_body.lock().unwrap();
                        bodies
                            .get(index)
                            .or(bodies.last())
                            .cloned()
                            .unwrap_or_default()
                    } else {
                        body.clone()
                    };
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
            let extra_frame = extra_frame.map(str::to_string);
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
                                if let Some(extra) = &extra_frame {
                                    ws.send(Message::Text(extra.clone().into()))
                                        .await
                                        .unwrap();
                                }
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
            matches_requests,
            matches_body,
        }
    }

    fn config(&self) -> PredictDataClientConfig {
        Self::config_with_poll(self, 3600)
    }

    fn config_with_poll(&self, trades_poll_interval_secs: u64) -> PredictDataClientConfig {
        PredictDataClientConfig::builder()
            .api_key(SecretString::from("predict-data-key"))
            .trades_poll_interval_secs(trades_poll_interval_secs)
            .base_url_http(format!("http://{}", self.http_addr))
            .base_url_ws(format!("ws://{}", self.ws_addr))
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


fn match_row(settlement: &str, executed_at: &str, amount_wei: &str, price_wei: &str) -> String {
    format!(
        r#"{{"market":{MARKET_JSON},"taker":{{"quoteType":"Ask","amount":"{amount_wei}","price":"{price_wei}","outcome":{{"name":"Yes","indexSet":1,"onChainId":"111"}},"signer":"0xa","hash":"0xo{settlement}","fee":{{"amount":"0","type":"COLLATERAL"}}}},"makers":[],"amountFilled":"{amount_wei}","priceExecuted":"{price_wei}","transactionHash":"0xtx{settlement}","settlementId":"{settlement}","executedAt":"{executed_at}"}}"#
    )
}

#[tokio::test]
async fn trades_poller_dedups_overlaps_and_advances_the_watermark() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let peers = Peers::start().await;
        let (sender, mut events) = tokio::sync::mpsc::unbounded_channel();
        // A fast poll cadence drives multiple ticks within the test; a
        // one-page walk budget forces WalkEnd::PageBudget on every poll.
        let config = {
            let mut config = peers.config_with_poll(1);
            config.trades_poll_max_pages = 1;
            config
        };
        let mut client = PredictDataClient::new(config, sender.into()).unwrap();
        client.connect().await.unwrap();

        client
            .subscribe_trades(SubscribeTrades::new(
                vec!["0xcond-111.PREDICT".parse().unwrap()],
                None,
                Some(nautilus_model::identifiers::Venue::from("PREDICT")),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();

        // Poll 1: two rows and a CONTINUATION CURSOR with a one-page
        // budget — the walk reports PageBudget, yet the watermark must
        // still advance past these rows (newest-first pages).
        *peers.matches_body.lock().unwrap() = vec![format!(
            r#"{{"success":true,"cursor":"MORE","data":[{},{}]}}"#,
            match_row("s1", "2026-11-01T00:00:02Z", "1000000000000000000", "500000000000000000"),
            match_row("s2", "2026-11-01T00:00:01Z", "2000000000000000000", "400000000000000000"),
        )];

        // Poller ticks once: both rows publish, oldest-first.
        let mut ticks = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        while ticks.len() < 2 && tokio::time::Instant::now() < deadline {
            if let Ok(DataEvent::Data(Data::Trade(tick))) = events.try_recv() {
                assert_eq!(tick.instrument_id.to_string(), "0xcond-111.PREDICT");
                ticks.push(tick.ts_event);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(ticks.len(), 2, "both rows publish on the first poll");
        assert!(ticks[0] < ticks[1], "rows publish oldest-first, got {ticks:?}");

        // Poll 2: ONE fresh row only. A wedged watermark (the round-2 bug
        // class) would re-walk from epoch 0 and re-serve the page-1 body —
        // publishing nothing new and failing the assertion below.
        peers.matches_body.lock().unwrap().push(format!(
            r#"{{"success":true,"data":[{}]}}"#,
            match_row("s3", "2026-11-01T00:00:03Z", "3000000000000000000", "300000000000000000"),
        ));
        let mut fresh = 0;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        while fresh < 1 && tokio::time::Instant::now() < deadline {
            if let Ok(DataEvent::Data(Data::Trade(_))) = events.try_recv() {
                fresh += 1;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(fresh, 1, "overlap rows dedup; only fresh rows publish");
        assert!(
            peers.matches_requests.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "the poller must keep polling"
        );

        client.disconnect().await.unwrap();
    })
    .await
    .expect("test completes within the timeout");
}

#[tokio::test]
async fn resolved_market_status_publishes_exact_closes_for_both_legs() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let resolved_frame = r#"{"type":"M","topic":"predictMarketStatus/4242","data":{
            "kind":"marketStatusUpdate","tsMs":1738000000000,"marketId":4242,
            "status":"RESOLVED",
            "marketOutcomes":[
                {"label":"Yes","indexSet":1,"price":"1000000000000000000","onChainId":"111"},
                {"label":"No","indexSet":2,"price":"0","onChainId":"222"}
            ]
        }}"#;
        let peers = Peers::start_with(Some(resolved_frame)).await;
        let (sender, mut events) = tokio::sync::mpsc::unbounded_channel();
        let mut client = PredictDataClient::new(peers.config(), sender.into()).unwrap();
        client.connect().await.unwrap();

        client
            .subscribe_instrument_close(SubscribeInstrumentClose::new(
                vec![
                    "0xcond-111.PREDICT".parse().unwrap(),
                    "0xcond-222.PREDICT".parse().unwrap(),
                ],
                None,
                Some(nautilus_model::identifiers::Venue::from("PREDICT")),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            count(&peers.ws_frames, "subscribe", "predictMarketStatus/4242"),
            1,
            "both legs share one marketStatus wire subscription"
        );

        let mut closes = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while closes.len() < 2 && tokio::time::Instant::now() < deadline {
            if let Ok(DataEvent::Data(Data::InstrumentClose(close))) = events.try_recv() {
                closes.push(close);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(closes.len(), 2, "one close per outcome leg");
        closes.sort_by_key(|close| close.instrument_id.to_string());
        // Asymmetric payouts pin the per-leg mapping: a swapped or
        // complemented derivation would fail these.
        assert_eq!(closes[0].instrument_id.to_string(), "0xcond-111.PREDICT");
        assert_eq!(closes[0].close_price.to_string(), "1.000");
        assert_eq!(closes[1].instrument_id.to_string(), "0xcond-222.PREDICT");
        assert_eq!(closes[1].close_price.to_string(), "0.000");

        client.disconnect().await.unwrap();
    })
    .await
    .expect("test completes within the timeout");
}
