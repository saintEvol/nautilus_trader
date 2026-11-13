//! Wallet positions reads (`GET /positions/{address}` — public, API key
//! only; POLYMARKET_PREDICT §6.2 positions face).
//!
//! Cursor-paginated (1–100 per page, clamped server-side); each row is a
//! full market+outcome embed with `amount` (share quantity string),
//! `market.decimalPrecision` and `outcome.onChainId` — the token identity.
//! A page budget never masquerades as exhaustion (the walk reports how it
//! ended).

use serde::Deserialize;

use super::client::{PredictHttpClient, WalkEnd};
use super::error::PredictHttpError;
use super::models::PredictMarketDto;

/// One wallet position row (documented PositionData fields consumed by the
/// platform; unknown fields are ignored by serde, the consumed ones carry
/// the exactness the ops portal needs).
#[derive(Clone, Debug, Deserialize)]
pub struct PredictPositionDto {
    pub id: String,
    pub market: PredictMarketDto,
    pub outcome: PredictPositionOutcomeDto,
    /// Share quantity (string; `market.decimalPrecision` decimals).
    pub amount: String,
    #[serde(rename = "valueUsd")]
    pub value_usd: String,
    #[serde(rename = "averageBuyPriceUsd")]
    pub average_buy_price_usd: String,
    #[serde(rename = "pnlUsd")]
    pub pnl_usd: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PredictPositionOutcomeDto {
    pub name: String,
    #[serde(rename = "indexSet")]
    pub index_set: serde_json::Value,
    #[serde(rename = "onChainId")]
    pub on_chain_id: String,
    pub status: String,
}

#[derive(Deserialize)]
struct PositionsPageDto {
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    data: Option<Vec<PredictPositionDto>>,
}

/// Structured outcome of a positions walk.
#[derive(Clone, Debug)]
pub struct PositionsWalk {
    pub rows: Vec<PredictPositionDto>,
    pub end: WalkEnd,
    pub next_cursor: Option<String>,
}

impl PredictHttpClient {
    /// Fetches one page of a wallet's positions (`GET /positions/{address}`).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn get_positions_page(
        &self,
        address: &str,
        first: u32,
        after: Option<&str>,
    ) -> Result<(Vec<PredictPositionDto>, Option<String>), PredictHttpError> {
        #[derive(serde::Serialize)]
        struct Q<'a> {
            first: u32,
            #[serde(skip_serializing_if = "Option::is_none")]
            after: Option<&'a str>,
        }
        let page: PositionsPageDto = self
            .send_get(
                "positions/by-address",
                &format!("/positions/{address}"),
                Some(&Q { first, after }),
            )
            .await?;
        // `data: null` is the documented empty-page shape (matches/trades
        // family) — an explicit empty, never an error.
        Ok((page.data.unwrap_or_default(), page.cursor))
    }

    /// Walks a wallet's positions until the cursor runs out or the page
    /// budget does. Duplicate position ids within a walk are dropped; a
    /// page of exclusively duplicates that still carries a cursor is a
    /// loud pagination error (same rule as the matches walk).
    ///
    /// # Errors
    /// See [`PredictHttpError`].
    pub async fn walk_positions(
        &self,
        address: &str,
        max_pages: u32,
    ) -> Result<PositionsWalk, PredictHttpError> {
        let mut cursor: Option<String> = None;
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut rows = Vec::new();
        for _ in 0..max_pages {
            let (page, next) = self
                .get_positions_page(address, 100, cursor.as_deref())
                .await?;
            let fresh = page.iter().filter(|p| seen.insert(p.id.clone())).count();
            rows.extend(page);
            match next {
                Some(next) if fresh == 0 => {
                    return Err(PredictHttpError::Pagination(
                        "positions/by-address: page contains no new identities".into(),
                    ));
                }
                Some(next) => cursor = Some(next),
                None => {
                    return Ok(PositionsWalk {
                        rows,
                        end: WalkEnd::Exhausted,
                        next_cursor: None,
                    });
                }
            }
        }
        Ok(PositionsWalk {
            rows,
            end: WalkEnd::PageBudget,
            next_cursor: cursor,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 文档形状解码：amount/valueUsd/averageBuyPriceUsd/pnlUsd 字符串 +
    /// market/outcome 嵌入（onChainId = token 身份）；空页 data:null →
    /// 空集不报错。
    #[test]
    fn position_row_decodes_documented_shape() {
        let body = r#"{"success":true,"cursor":null,"data":[{
            "id":"pos-1",
            "market":{
                "id":42,"imageUrl":"https://example/img.png","title":"t",
                "question":"q?","description":"d",
                "tradingStatus":"OPEN","status":"REGISTERED","isVisible":true,
                "isNegRisk":false,"isYieldBearing":false,"feeRateBps":100,
                "makerFeeBps":0,"oracleQuestionId":"0xo","conditionId":"0xc",
                "resolverAddress":"0xr",
                "outcomes":[{"name":"Yes","indexSet":1,"onChainId":"0xtok"}],
                "spreadThreshold":0.01,"shareThreshold":5,"isBoosted":false,
                "polymarketConditionIds":[],"categorySlug":"c",
                "createdAt":"2026-01-01T00:00:00Z","decimalPrecision":2,
                "marketVariant":"DEFAULT","rewards":{"schedule":[]}
            },
            "outcome":{"name":"Yes","indexSet":1,"onChainId":"0xtok","status":"OPEN"},
            "amount":"12.5","valueUsd":"5.1",
            "averageBuyPriceUsd":"0.4","pnlUsd":"0.1"
        }]}"#;
        let page: PositionsPageDto = serde_json::from_str(body).unwrap();
        let rows = page.data.unwrap();
        assert_eq!(rows[0].id, "pos-1");
        assert_eq!(rows[0].amount, "12.5");
        assert_eq!(rows[0].outcome.on_chain_id, "0xtok");
        assert_eq!(rows[0].market.id, 42);
    }

    #[test]
    fn empty_page_is_data_null_not_error() {
        let page: PositionsPageDto =
            serde_json::from_str(r#"{"success":true,"cursor":null,"data":null}"#).unwrap();
        assert!(page.data.unwrap_or_default().is_empty());
        assert!(page.cursor.is_none());
    }
}
