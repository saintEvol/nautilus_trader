use rust_decimal::Decimal;

/// A price-level of a full book frame (price, size) in exact decimals.
pub type BookLevel = (Decimal, Decimal);

/// Projects a Yes-axis full book to its No complement
/// (`POLYMARKET_PREDICT` §5.3): No bid = 1 − Yes ask, No ask = 1 − Yes bid,
/// sizes unchanged. Empty sides stay empty (never a synthetic zero-price
/// quote).
///
/// Exactness rules:
/// - each price must be strictly inside (0, 1) — tick bounds checked by the
///   caller against the market's `decimalPrecision`;
/// - `1 − p` is computed at `precision` decimal places and must be exact —
///   a rounding would misprice the complement, so it errors loudly instead;
/// - sizes are carried verbatim (shares are the same real liquidity).
///
/// # Errors
///
/// Returns an error naming the offending level for: a price outside (0, 1);
/// an inexact complement at the given precision.
pub fn project_no_book(
    yes_bids: &[BookLevel],
    yes_asks: &[BookLevel],
    precision: u32,
) -> Result<(Vec<BookLevel>, Vec<BookLevel>), String> {
    let one = Decimal::ONE;
    let scale = 10u64
        .checked_pow(precision)
        .map(Decimal::from)
        .ok_or_else(|| format!("precision {precision} out of range"))?;

    let complement = |p: &Decimal, side: &'static str| -> Result<Decimal, String> {
        if *p <= Decimal::ZERO || *p >= one {
            return Err(format!("{side} price {p} outside (0,1)"));
        }
        let c = one - *p;
        // Exactness: the complement must carry no more decimals than the
        // market precision (a rounded complement misprices the No view).
        let scaled = c * scale;
        if scaled.fract() != Decimal::ZERO {
            return Err(format!(
                "{side} price {p}: complement {c} is not exact at precision {precision}"
            ));
        }
        Ok(c)
    };

    let mut no_bids = Vec::with_capacity(yes_asks.len());
    for (p, s) in yes_asks {
        no_bids.push((complement(p, "ask")?, *s));
    }
    let mut no_asks = Vec::with_capacity(yes_bids.len());
    for (p, s) in yes_bids {
        no_asks.push((complement(p, "bid")?, *s));
    }
    // Book ordering: bids descend, asks ascend after the flip.
    no_bids.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    no_asks.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    Ok((no_bids, no_asks))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn d(x: i64, scale: u32) -> Decimal {
        Decimal::new(x, scale)
    }

    /// §5.3 anchor: No bid = 1 − Yes ask, No ask = 1 − Yes bid, sizes
    /// unchanged, empty side stays empty.
    #[test]
    fn projects_complement_exactly() {
        let yes_bids = vec![(d(39, 2), d(100, 0)), (d(37, 2), d(50, 0))];
        let yes_asks = vec![(d(41, 2), d(80, 0)), (d(45, 2), d(60, 0))];

        let (no_bids, no_asks) = project_no_book(&yes_bids, &yes_asks, 2).unwrap();

        // No bids from Yes asks: 1−0.41=0.59 (80), 1−0.45=0.55 (60), descending.
        assert_eq!(no_bids, vec![(d(59, 2), d(80, 0)), (d(55, 2), d(60, 0))]);
        // No asks from Yes bids: 1−0.39=0.61 (100), 1−0.37=0.63 (50), ascending.
        assert_eq!(no_asks, vec![(d(61, 2), d(100, 0)), (d(63, 2), d(50, 0))]);
    }

    #[test]
    fn empty_side_stays_empty() {
        let (no_bids, no_asks) = project_no_book(&[(d(40, 2), d(10, 0))], &[], 2).unwrap();
        assert!(no_bids.is_empty(), "Yes empty ask side → No empty bid side");
        assert_eq!(no_asks, vec![(d(60, 2), d(10, 0))]);
    }

    #[test]
    fn out_of_range_price_rejected() {
        let err = project_no_book(&[(Decimal::ONE, d(10, 0))], &[], 2).unwrap_err();
        assert!(err.contains("outside (0,1)"), "{err}");
        let err = project_no_book(&[], &[(Decimal::ZERO, d(10, 0))], 2).unwrap_err();
        assert!(err.contains("outside (0,1)"), "{err}");
    }

    /// Inexact complement at market precision errors loudly (no rounding).
    #[test]
    fn inexact_complement_rejected() {
        // 1 − 0.3333 = 0.6667 at 4dp is exact; at 3dp it would round.
        let ok = project_no_book(&[(d(3333, 4), d(1, 0))], &[], 4).unwrap();
        assert_eq!(ok.1[0].0, d(6667, 4));
        let err = project_no_book(&[(d(33335, 5), d(1, 0))], &[], 3).unwrap_err();
        assert!(err.contains("not exact"), "{err}");
    }
}


/// Cross-page full-frame accumulation (`POLYMARKET_PREDICT` §5.3): a large
/// book may arrive as several pages forming ONE published frame; the frame
/// is complete only when a delta carries `F_LAST`. Incomplete frames are
/// never applied (an engine must not match on half a book).
///
/// Pages keep their arrival order; the first page's Clear anchors the frame
/// and the final page's `F_LAST` closes it. A page without any anchor Clear
/// cannot start a frame (loud error, staging reset).
#[derive(Debug)]
pub struct PredictionFrameStaging {
    instrument_id: nautilus_model::identifiers::InstrumentId,
    deltas: Vec<nautilus_model::data::OrderBookDelta>,
    ts_event: nautilus_core::UnixNanos,
    ts_init: nautilus_core::UnixNanos,
}

/// Result of pushing one page into the staging.
#[derive(Debug)]
pub enum FrameStatus {
    /// More pages are still owed for this frame.
    Staging,
    /// The frame closed on `F_LAST` — complete batch ready to install.
    Complete(nautilus_model::data::OrderBookDeltas),
}

impl PredictionFrameStaging {
    /// Starts a frame from the first page, which must contain a `Clear`
    /// action (a published frame always begins with a cleared book).
    ///
    /// # Errors
    ///
    /// Returns an error when the first page carries no `Clear`.
    pub fn new(first_page: &nautilus_model::data::OrderBookDeltas) -> anyhow::Result<Self> {
        use nautilus_model::enums::BookAction;
        anyhow::ensure!(
            first_page.deltas.iter().any(|d| d.action == BookAction::Clear),
            "frame first page lacks a Clear anchor: {first_page:?}"
        );
        let staging = Self {
            instrument_id: first_page.instrument_id,
            deltas: first_page.deltas.clone(),
            ts_event: first_page.ts_event,
            ts_init: first_page.ts_init,
        };
        Ok(staging)
    }

    /// Pushes the next page. Timestamps must not move backwards within a
    /// frame (pages of one published frame share its publication instant or
    /// advance monotonically).
    ///
    /// # Errors
    ///
    /// Returns an error on backwards timestamps or an instrument mismatch.
    pub fn push_page(
        &mut self,
        page: &nautilus_model::data::OrderBookDeltas,
    ) -> anyhow::Result<FrameStatus> {
        use nautilus_model::enums::RecordFlag;
        anyhow::ensure!(
            page.instrument_id == self.instrument_id,
            "frame page instrument {} != staging {}",
            page.instrument_id,
            self.instrument_id
        );
        anyhow::ensure!(
            page.ts_event >= self.ts_event && page.ts_init >= self.ts_init,
            "frame page timestamps move backwards: page ({}, {}) staging ({}, {})",
            page.ts_event,
            page.ts_init,
            self.ts_event,
            self.ts_init
        );
        self.ts_event = self.ts_event.max(page.ts_event);
        self.ts_init = self.ts_init.max(page.ts_init);

        let closes = page
            .deltas
            .last()
            .is_some_and(|d| RecordFlag::F_LAST.matches(d.flags));
        self.deltas.extend(page.deltas.iter().copied());

        if closes {
            // `OrderBookDeltas::new` derives ts_event/ts_init from the last
            // delta (the F_LAST page) — the frame's publication instant.
            Ok(FrameStatus::Complete(nautilus_model::data::OrderBookDeltas::new(
                self.instrument_id,
                std::mem::take(&mut self.deltas),
            )))
        } else {
            Ok(FrameStatus::Staging)
        }
    }
}


#[cfg(test)]
mod staging_tests {
    use super::*;
    use nautilus_core::UnixNanos;
    use nautilus_model::data::{BookOrder, OrderBookDelta, OrderBookDeltas};
    use nautilus_model::enums::{BookAction, OrderSide, RecordFlag};
    use nautilus_model::identifiers::InstrumentId;
    use nautilus_model::types::{Price, Quantity};
    #[allow(unused_imports)]
    use rust_decimal::Decimal as _DecimalImport;

    fn id() -> InstrumentId {
        InstrumentId::from("0xcond-0xyes.PREDICT")
    }

    fn clear_page(ts: u64) -> OrderBookDeltas {
        let clear = OrderBookDelta::clear(id(), 0, ts.into(), ts.into());
        OrderBookDeltas::new(id(), vec![clear])
    }

    #[allow(clippy::too_many_arguments)]
    fn level_page(ts: u64, price: &str, size: &str, flags: u8) -> OrderBookDeltas {
        let order = BookOrder::new(
            OrderSide::Buy,
            Price::from(price),
            Quantity::from(size),
            0,
        );
        let delta = OrderBookDelta::new(
            id(),
            BookAction::Add,
            order,
            flags,
            0,
            ts.into(),
            ts.into(),
        );
        OrderBookDeltas::new(id(), vec![delta])
    }

    /// 跨页恢复：页累积到 `F_LAST` 才成帧；`F_LAST` 之前的页不产键。
    #[test]
    fn pages_accumulate_until_f_last() {
        let mut staging = PredictionFrameStaging::new(&clear_page(100)).unwrap();
        assert!(matches!(
            staging.push_page(&level_page(101, "0.40", "100", 0)),
            Ok(FrameStatus::Staging)
        ));
        let complete = match staging.push_page(&level_page(
            102,
            "0.39",
            "50",
            RecordFlag::F_LAST as u8,
        )) {
            Ok(FrameStatus::Complete(batch)) => batch,
            other => panic!("expected Complete, got {other:?}"),
        };
        // Clear + two adds, closed by F_LAST on the last delta.
        assert_eq!(complete.deltas.len(), 3);
        assert!(RecordFlag::F_LAST.matches(complete.deltas.last().unwrap().flags));
        assert_eq!(complete.ts_event, UnixNanos::from(102));
    }

    /// 无 Clear 锚的首页 = 非帧（响亮拒）。
    #[test]
    fn first_page_without_clear_rejected() {
        let result = PredictionFrameStaging::new(&level_page(
            100,
            "0.40",
            "100",
            RecordFlag::F_LAST as u8,
        ));
        let err = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("page without Clear must be rejected"),
        };
        assert!(err.contains("Clear anchor"));
    }

    /// 帧内时间戳回退 = 数据破损（响亮拒）。
    #[test]
    fn backwards_timestamps_rejected() {
        let mut staging = PredictionFrameStaging::new(&clear_page(200)).unwrap();
        assert!(staging.push_page(&level_page(150, "0.40", "100", 0)).is_err());
    }
}
