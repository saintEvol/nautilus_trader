//! Engine-level tests for the shared binary-option close settlement
//! (`POLYMARKET_PREDICT` §6.3 A — `settlement::settle_binary_close`, the
//! common path of the backtest `SimulatedExchange` and the sandbox execution
//! client). Offline, fixture-driven: cache + instrument + seeded positions.

use std::cell::RefCell;
use std::rc::Rc;

use nautilus_common::cache::Cache;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::InstrumentClose,
    enums::{InstrumentCloseType, OmsType, OrderSide, OrderType, PositionSide},
    identifiers::{AccountId, ClientOrderId, PositionId, StrategyId, TradeId, TraderId, VenueOrderId},
    instruments::{Instrument, InstrumentAny, stubs::binary_option},
    orders::{Order, OrderAny, builder::OrderTestBuilder},
    position::Position,
    types::{Money, Price, Quantity},
};
use nautilus_execution::engine::settlement::settle_binary_close;

fn account_id() -> AccountId {
    AccountId::from("PREDICT-001")
}

/// A cache holding the binary-option stub and one open BUY position of
/// `quantity` shares opened at `open_px` for `account_id`.
fn cache_with_open_position(quantity: &str, open_px: &str) -> (Rc<RefCell<Cache>>, InstrumentAny) {
    let instrument = InstrumentAny::BinaryOption(binary_option());
    let cache = Rc::new(RefCell::new(Cache::default()));
    cache
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    add_open_position(&cache, &instrument, "1", quantity, open_px);

    (cache, instrument)
}

/// Adds an open BUY position with id-suffix `n` (order/trade/position ids
/// `*-SETTLE-{n}`) so a settlement test can seed several distinct positions.
fn add_open_position(
    cache: &Rc<RefCell<Cache>>,
    instrument: &InstrumentAny,
    n: &str,
    quantity: &str,
    open_px: &str,
) {
    let order = OrderTestBuilder::new(OrderType::Market)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from("S-001"))
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from(format!("O-SETTLE-{n}")))
        .side(OrderSide::Buy)
        .quantity(Quantity::from(quantity))
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();

    let any_order: OrderAny = order.into();
    let fill = nautilus_model::events::OrderFilled::new(
        any_order.trader_id(),
        any_order.strategy_id(),
        any_order.instrument_id(),
        any_order.client_order_id(),
        VenueOrderId::from(format!("V-SETTLE-{n}")),
        account_id(),
        TradeId::from(format!("T-SETTLE-{n}")),
        OrderSide::Buy,
        OrderType::Market,
        Quantity::from(quantity),
        Price::from(open_px),
        instrument.quote_currency(),
        nautilus_model::enums::LiquiditySide::Taker,
        nautilus_core::UUID4::new(),
        UnixNanos::from(100),
        UnixNanos::from(100),
        false,
        Some(PositionId::new(format!("P-SETTLE-{n}"))),
        Some(Money::zero(instrument.quote_currency())),
        None,
    );
    let position = Position::new(instrument, fill);
    cache
        .borrow_mut()
        .add_position(&position, OmsType::Netting)
        .unwrap();
}

fn contract_expired(instrument: &InstrumentAny, price: &str) -> InstrumentClose {
    InstrumentClose::new(
        instrument.id(),
        Price::from(price),
        InstrumentCloseType::ContractExpired,
        UnixNanos::from(300),
        UnixNanos::from(300),
    )
}

/// Proceeds `quantity × payout` per account, `PositionClosed` per position,
/// and the cached position closed at the certified payout — the full §6.3 A
/// settlement shape with no trading artifacts.
#[test]
fn settle_binary_close_computes_proceeds_and_closes_positions() {
    let (cache, instrument) = cache_with_open_position("10.00", "0.400");

    let settlement =
        settle_binary_close(&mut cache.borrow_mut(), contract_expired(&instrument, "1.000"), 300.into())
            .unwrap();

    assert_eq!(
        settlement.proceeds_by_account.get(&account_id()),
        Some(&Money::from("10.00 USDC")),
        "proceeds are quantity × payout in the quote currency"
    );
    let [closed] = settlement.position_events.as_slice() else {
        panic!("expected exactly one PositionClosed, got {:#?}", settlement.position_events)
    };
    assert_eq!(closed.position_id, PositionId::new("P-SETTLE-1"));
    assert_eq!(closed.account_id, account_id());
    assert_eq!(closed.side, PositionSide::Flat);
    assert_eq!(closed.last_qty, Quantity::from("10.00"));
    assert_eq!(closed.last_px, Price::from("1.000"));

    let position = cache
        .borrow()
        .position(&PositionId::new("P-SETTLE-1"))
        .expect("settled position stays cached")
        .clone();
    assert_eq!(position.side, PositionSide::Flat);
    assert_eq!(position.realized_pnl, Some(Money::from("6.00 USDC")));
    assert!(cache.borrow().positions_open(None, None, None, None, None).is_empty());
}

/// A repeated identical close settles nothing further — idempotence without
/// double proceeds.
#[test]
fn settle_binary_close_is_idempotent_for_identical_close() {
    let (cache, instrument) = cache_with_open_position("10.00", "0.400");

    settle_binary_close(&mut cache.borrow_mut(), contract_expired(&instrument, "1.000"), 300.into())
        .unwrap();
    let repeated =
        settle_binary_close(&mut cache.borrow_mut(), contract_expired(&instrument, "1.000"), 400.into())
            .unwrap();

    assert!(
        repeated.proceeds_by_account.is_empty() && repeated.position_events.is_empty(),
        "an identical repeated close must not settle twice"
    );
}

/// A conflicting close price for an already-settled instrument fails loudly.
#[test]
fn settle_binary_close_fails_loudly_on_conflicting_close() {
    let (cache, instrument) = cache_with_open_position("10.00", "0.400");

    settle_binary_close(&mut cache.borrow_mut(), contract_expired(&instrument, "1.000"), 300.into())
        .unwrap();
    let error =
        settle_binary_close(&mut cache.borrow_mut(), contract_expired(&instrument, "0.500"), 400.into())
            .unwrap_err();

    assert!(
        error.to_string().contains("Conflicting instrument close"),
        "expected a loud conflict failure, got {error:#}"
    );
}

/// r9-F4 payout single source: when the cache already holds the authoritative
/// close, a later same-price close object settles remaining positions under
/// the CACHED terms — proceeds and `PositionClosed` events derive from the
/// cached close (A), never from the incoming parameter (B). The echo carries
/// a different `ts_event` so a parameter-sourced settlement record (ts 999)
/// is distinguishable from the authoritative one (ts 300).
#[test]
fn settle_binary_close_proceeds_follow_cached_authoritative_close() {
    let (cache, instrument) = cache_with_open_position("10.00", "0.400");

    // Close A settles the first position and is cached as authoritative.
    settle_binary_close(&mut cache.borrow_mut(), contract_expired(&instrument, "1.000"), 300.into())
        .unwrap();

    // A second position opens on the same instrument after close A cached.
    add_open_position(&cache, &instrument, "2", "10.00", "0.400");

    // Echo close B: same price (no conflict), different object and ts_event.
    let echo = InstrumentClose::new(
        instrument.id(),
        Price::from("1.000"),
        InstrumentCloseType::ContractExpired,
        UnixNanos::from(999),
        UnixNanos::from(999),
    );
    let settlement =
        settle_binary_close(&mut cache.borrow_mut(), echo, 500.into()).unwrap();

    assert_eq!(
        settlement.proceeds_by_account.get(&account_id()),
        Some(&Money::from("10.00 USDC")),
        "proceeds follow the cached authoritative payout"
    );
    let [closed] = settlement.position_events.as_slice() else {
        panic!("expected exactly one PositionClosed, got {:#?}", settlement.position_events)
    };
    assert_eq!(closed.position_id, PositionId::new("P-SETTLE-2"));
    assert_eq!(
        closed.ts_event,
        UnixNanos::from(300),
        "the settlement record carries the authoritative close's ts_event, not the echo's (999)"
    );
}

/// A fractional payout settles at the exact certified price (0.5 pays half
/// the notional — no synthetic fills, no fee model).
#[test]
fn settle_binary_close_supports_fractional_payout() {
    let (cache, instrument) = cache_with_open_position("10.00", "0.400");

    let settlement =
        settle_binary_close(&mut cache.borrow_mut(), contract_expired(&instrument, "0.500"), 300.into())
            .unwrap();

    assert_eq!(
        settlement.proceeds_by_account.get(&account_id()),
        Some(&Money::from("5.00 USDC"))
    );
}

/// A proceeds overflow bails BEFORE any cache side effect: the position stays
/// open, the close is not cached as settled — the caller's retry keeps its
/// full settlement material (no half-settled "position closed but cash not
/// credited" state).
#[test]
fn settle_binary_close_overflow_bails_before_side_effects() {
    // A share count 1.9× the quote-currency Money ceiling: it exceeds the
    // Money range at payout 1.000 yet stays inside the Quantity range —
    // computed from the type bounds so the fixture holds under BOTH
    // precision profiles (this crate tests at standard precision; a
    // high-precision build — e.g. workspace feature unification through
    // nautilus-predict — shifts both ceilings together).
    use nautilus_model::types::{money::MONEY_MAX, quantity::QUANTITY_MAX};
    let overflow_qty_u64 = (MONEY_MAX as u64 * 19) / 10;
    let overflow_qty_f64 = overflow_qty_u64 as f64;
    assert!(
        overflow_qty_f64 > MONEY_MAX && overflow_qty_f64 <= QUANTITY_MAX,
        "fixture must exceed Money max and fit Quantity max in every precision profile"
    );
    let (cache, instrument) = cache_with_open_position(&overflow_qty_u64.to_string(), "0.400");

    let error = settle_binary_close(
        &mut cache.borrow_mut(),
        contract_expired(&instrument, "1.000"),
        300.into(),
    )
    .unwrap_err();

    assert!(
        error.to_string().contains("overflow"),
        "expected an overflow failure, got {error:#}"
    );
    let position = cache
        .borrow()
        .position(&PositionId::new("P-SETTLE-1"))
        .expect("position stays cached")
        .clone();
    assert!(position.is_open(), "the position must not close on bail");
    assert!(
        !cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .is_empty(),
        "no side effect may close the open position"
    );
    assert!(
        cache.borrow().instrument_close(&instrument.id()).is_none(),
        "the overflowing close must not be cached as the settled close"
    );
}
