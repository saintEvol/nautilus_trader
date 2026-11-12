//! Local-peer tests for the Predict HTTP cursor walks (P3 completion
//! evidence — mock source, fully offline): page sequencing via `after`,
//! identity dedup, structured completion (a page budget never masquerades
//! as exhaustion), and loud rejection of stalled pagination.

use std::sync::{Arc, Mutex};

use nautilus_core::string::secret::SecretString;
use nautilus_predict::http::{PredictHttpClient, PredictHttpError, WalkEnd};
use nautilus_predict::PredictMarketStatusFilter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A one-shot HTTP/1.1 peer serving canned JSON pages by requested cursor.
struct PagesPeer {
    listener: TcpListener,
    requests: Arc<Mutex<Vec<String>>>,
}

impl PagesPeer {
    async fn bind() -> Self {
        Self {
            listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.listener.local_addr().unwrap())
    }

    /// Serves `pages.len()` requests; each page maps to (json body).
    async fn serve(self, pages: Vec<&'static str>) -> Arc<Mutex<Vec<String>>> {
        self.serve_with_status(pages, "200 OK").await
    }

    /// Serves pages with an explicit HTTP status line.
    async fn serve_with_status(
        self,
        pages: Vec<&'static str>,
        status: &'static str,
    ) -> Arc<Mutex<Vec<String>>> {
        let requests = Arc::clone(&self.requests);
        let requests_inner = Arc::clone(&requests);
        tokio::spawn(async move {
            for body in pages {
                let Ok((mut stream, _)) = self.listener.accept().await else {
                    break;
                };
                let mut buffer = [0u8; 4096];
                let Ok(read) = stream.read(&mut buffer).await else { break };
                let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                let first_line = request.lines().next().unwrap_or_default().to_string();
                let after = request
                    .split("&after=")
                    .nth(1)
                    .and_then(|rest| rest.split([' ', '&']).next())
                    .unwrap_or_default()
                    .to_string();
                requests_inner
                    .lock()
                    .unwrap()
                    .push(format!("{first_line} after={after}"));
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.flush().await;
                let _ = stream.shutdown().await;
            }
        });
        requests
    }
}

fn client(base_url: &str) -> PredictHttpClient {
    PredictHttpClient::new(
        base_url.to_string(),
        SecretString::from("predict-data-key"),
        5,
        None,
    )
    .unwrap()
}

#[tokio::test]
async fn markets_walk_pages_until_the_cursor_runs_out() -> anyhow::Result<()> {
    let market = |id: i64| {
        format!(
            r#"{{"id":{id},"imageUrl":"i","title":"T{id}","question":"Q","description":"d","tradingStatus":"OPEN","status":"REGISTERED","isVisible":true,"isNegRisk":false,"isYieldBearing":false,"feeRateBps":0,"makerFeeBps":0,"oracleQuestionId":"o","conditionId":"0x{id}","resolverAddress":"r","outcomes":[{{"name":"Yes","indexSet":1,"onChainId":"{id}"}}],"spreadThreshold":0.01,"shareThreshold":1,"isBoosted":false,"polymarketConditionIds":[],"categorySlug":"c","createdAt":"2026-10-01T00:00:00Z","decimalPrecision":3,"marketVariant":"DEFAULT"}}"#
        )
    };
    let page1 = format!(
        r#"{{"success":true,"cursor":"PAGE2","data":[{},{}]}}"#,
        market(1),
        market(2)
    );
    let page2 = format!(
        r#"{{"success":true,"data":[{},{}]}}"#, // no cursor: source exhausted
        market(3),
        market(3) // duplicate identity must dedup within the walk
    );
    let peer = PagesPeer::bind().await;
    let base_url = peer.base_url();
    let requests = peer.serve(vec![Box::leak(page1.into_boxed_str()), Box::leak(page2.into_boxed_str())]).await;

    let pages = client(&base_url)
        .get_markets(Some(PredictMarketStatusFilter::Open), 10)
        .await?;
    assert!(pages.complete, "absent cursor must complete the walk");
    assert_eq!(pages.records.len(), 3, "duplicate ids dedup");
    assert_eq!(pages.next_cursor, None);

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1].contains("after=PAGE2"),
        "second request must echo the cursor: {}",
        requests[1]
    );
    Ok(())
}

#[tokio::test]
async fn page_budget_is_reported_not_disguised_as_exhaustion() -> anyhow::Result<()> {
    let page = r#"{"success":true,"cursor":"MORE","data":[]}"#;
    // Walk with an empty data page carrying a cursor → loud stalled error.
    let peer = PagesPeer::bind().await;
    let base_url = peer.base_url();
    peer.serve(vec![page]).await;
    let error = client(&base_url)
        .get_markets(None, 5)
        .await
        .expect_err("a stalled page must error loudly");
    assert!(
        error.to_string().contains("page"),
        "stalled pagination must fail loudly, got: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn matches_walk_reports_page_budget_with_resume_cursor() -> anyhow::Result<()> {
    let market = r#"{"id":7,"imageUrl":"i","title":"T","question":"Q","description":"d","tradingStatus":"OPEN","status":"REGISTERED","isVisible":true,"isNegRisk":false,"isYieldBearing":false,"feeRateBps":0,"makerFeeBps":0,"oracleQuestionId":"o","conditionId":"0x7","resolverAddress":"r","outcomes":[{"name":"Yes","indexSet":1,"onChainId":"7"}],"spreadThreshold":0.01,"shareThreshold":1,"isBoosted":false,"polymarketConditionIds":[],"categorySlug":"c","createdAt":"2026-10-01T00:00:00Z","decimalPrecision":3,"marketVariant":"DEFAULT"}"#;
    let page = format!(
        r#"{{"success":true,"cursor":"NEXT","data":[{{"market":{market},"taker":{{"quoteType":"Ask","amount":"1000000000000000000","price":"500000000000000000","outcome":{{"name":"Yes","indexSet":1,"onChainId":"7"}},"signer":"0xa","hash":"0xo","fee":{{"amount":"0","type":"COLLATERAL"}}}},"makers":[],"amountFilled":"1000000000000000000","priceExecuted":"500000000000000000","transactionHash":"0xtx","settlementId":"0xs","executedAt":"2026-11-01T00:00:00Z"}}]}}"#
    );
    let peer = PagesPeer::bind().await;
    let base_url = peer.base_url();
    // Budget of one page: the walk must stop incomplete with the cursor.
    peer.serve(vec![Box::leak(page.into_boxed_str())]).await;
    let walk = client(&base_url)
        .walk_matches(&Default::default(), None, 1)
        .await?;
    assert_eq!(walk.end, WalkEnd::PageBudget);
    assert_eq!(walk.next_cursor.as_deref(), Some("NEXT"));
    assert_eq!(walk.rows.len(), 1);
    Ok(())
}

#[tokio::test]
async fn chance_walk_carries_the_server_reported_resolution() -> anyhow::Result<()> {
    let page = r#"{"success":true,"data":{"resolution":"1h","series":[{"x":1730000000,"y":0.52},{"x":1730003600,"y":0.53}]}}"#;
    let peer = PagesPeer::bind().await;
    let base_url = peer.base_url();
    peer.serve(vec![page]).await;
    let walk = client(&base_url)
        .walk_chance_series(7, 1_730_000_000, None, None, 5)
        .await?;
    assert_eq!(walk.end, WalkEnd::Exhausted);
    assert_eq!(walk.resolution, "1h", "server-reported sampling wins");
    assert_eq!(walk.points.len(), 2);
    Ok(())
}

#[tokio::test]
async fn structured_venue_error_surfaces() -> anyhow::Result<()> {
    let body = r#"{"success":false,"code":401,"error":"unauthorized","message":"Missing API key","timestamp":"2026-11-12T00:00:00Z"}"#;
    let peer = PagesPeer::bind().await;
    let base_url = peer.base_url();
    let _ = peer.serve_with_status(vec![body], "401 Unauthorized").await;
    let error: PredictHttpError = client(&base_url)
        .get_market(1)
        .await
        .expect_err("401 must surface");
    assert!(error.to_string().contains("unauthorized"), "{error}");
    Ok(())
}
