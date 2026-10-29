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

use super::*;
use nautilus_model::{enums::OptionKind, instruments::CryptoOption};

fn premium_option(inverse: bool) -> InstrumentAny {
    let currency = if inverse {
        Currency::BTC()
    } else {
        Currency::USD()
    };
    InstrumentAny::CryptoOption(
        CryptoOption::builder()
            .instrument_id(InstrumentId::from("BTC-270101-100-C.SIM"))
            .raw_symbol(Symbol::from("BTC-270101-100-C"))
            .underlying(Currency::BTC())
            .quote_currency(Currency::USD())
            .settlement_currency(currency)
            .is_inverse(inverse)
            .option_kind(OptionKind::Call)
            .strike_price(Price::from("100.00"))
            .activation_ns(0.into())
            .expiration_ns(1_798_790_400_000_000_000_u64.into())
            .price_precision(2)
            .size_precision(0)
            .price_increment(Price::from("0.01"))
            .size_increment(Quantity::from(1))
            .multiplier(Quantity::from(1))
            .lot_size(Quantity::from(1))
            .ts_event(0.into())
            .ts_init(0.into())
            .build()
            .unwrap(),
    )
}

fn given_margin_cash(
    portfolio: &mut Portfolio,
    account_id: AccountId,
    cash: Money,
    base_currency: Option<Currency>,
) {
    portfolio.update_account(&AccountState::new(
        account_id,
        AccountType::Margin,
        vec![AccountBalance::new(cash, Money::zero(cash.currency), cash)],
        vec![],
        true,
        uuid4(),
        0.into(),
        0.into(),
        base_currency,
    ));
}

fn price_option(portfolio: &mut Portfolio, instrument: &InstrumentAny, price: Price) {
    let quote = QuoteTick::new(
        instrument.id(),
        price,
        price,
        Quantity::from(100),
        Quantity::from(100),
        0.into(),
        0.into(),
    );
    portfolio.cache().borrow_mut().add_quote(quote).unwrap();
    portfolio.update_quote_tick(&quote);
}

fn assert_premium_equity(
    portfolio: &mut Portfolio,
    instrument: &InstrumentAny,
    account_id: AccountId,
    currency: Currency,
    expected_equity: Decimal,
    expected_unrealized: Decimal,
) {
    let venue = instrument.id().venue;
    let unrealized = portfolio
        .unrealized_pnl_for_account(&instrument.id(), None, Some(&account_id), None)
        .unwrap();
    assert_eq!(
        unrealized,
        Money::from_decimal(expected_unrealized, currency).unwrap()
    );
    let scoped = portfolio.equity(&venue, Some(&account_id));
    let unfiltered = portfolio.equity(&venue, None);
    let snapshot = portfolio.build_snapshot(&account_id).unwrap();
    let expected = Money::from_decimal(expected_equity, currency).unwrap();
    assert_eq!(scoped[&currency], expected);
    assert_eq!(unfiltered[&currency], expected);
    assert_eq!(snapshot.total_equity, vec![expected]);
    assert_eq!(snapshot.unrealized_pnls, vec![unrealized]);
    assert!(!snapshot.is_stale);
    assert!(snapshot.unpriced_instruments.is_empty());
}

#[rstest]
#[case::linear_long(
    false, OrderSide::Buy, dec!(996),
    [dec!(1000), dec!(1002), dec!(1003)], [dec!(0), dec!(2), dec!(1)]
)]
#[case::linear_short(
    false, OrderSide::Sell, dec!(1004),
    [dec!(1000), dec!(998), dec!(997)], [dec!(0), dec!(-2), dec!(-1)]
)]
#[case::inverse_long(
    true, OrderSide::Buy, dec!(999.96),
    [dec!(1000), dec!(1000.02), dec!(1000.03)], [dec!(0), dec!(0.02), dec!(0.01)]
)]
#[case::inverse_short(
    true, OrderSide::Sell, dec!(1000.04),
    [dec!(1000), dec!(999.98), dec!(999.97)], [dec!(0), dec!(-0.02), dec!(-0.01)]
)]
fn test_margin_premium_equity_preserves_cash_and_remaining_option_value(
    mut portfolio: Portfolio,
    #[case] inverse: bool,
    #[case] side: OrderSide,
    #[case] opening_cash: Decimal,
    #[case] equities: [Decimal; 3],
    #[case] unrealized: [Decimal; 3],
) {
    let instrument = premium_option(inverse);
    let currency = instrument.settlement_currency();
    let account_id = AccountId::new("SIM-001");
    let (entry, mark, close) = if inverse {
        (
            Price::from("0.02"),
            Price::from("0.03"),
            Price::from("0.04"),
        )
    } else {
        (
            Price::from("2.00"),
            Price::from("3.00"),
            Price::from("4.00"),
        )
    };
    portfolio
        .cache()
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();

    // The balances are independently given post-fill cash facts, not derived by valuation
    given_margin_cash(
        &mut portfolio,
        account_id,
        Money::from_decimal(opening_cash, currency).unwrap(),
        None,
    );
    let position_id = PositionId::new("P-PREMIUM-EQUITY");
    let opening = make_fill_for_account(
        &instrument,
        account_id,
        side,
        Quantity::from(2),
        entry,
        position_id,
    );
    let mut position = Position::new(&instrument, opening.clone());
    portfolio
        .cache()
        .borrow_mut()
        .add_position(&position, OmsType::Hedging)
        .unwrap();
    price_option(&mut portfolio, &instrument, entry);
    portfolio.update_position(&PositionEvent::PositionOpened(PositionOpened::create(
        &position,
        &opening,
        uuid4(),
        0.into(),
    )));
    assert_premium_equity(
        &mut portfolio,
        &instrument,
        account_id,
        currency,
        equities[0],
        unrealized[0],
    );
    price_option(&mut portfolio, &instrument, mark);
    assert_premium_equity(
        &mut portfolio,
        &instrument,
        account_id,
        currency,
        equities[1],
        unrealized[1],
    );

    // Closing one of two contracts at twice the entry restores cash to exactly 1000
    let closing_side = if side == OrderSide::Buy {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    };
    let closing = make_fill_for_account(
        &instrument,
        account_id,
        closing_side,
        Quantity::from(1),
        close,
        position_id,
    );
    position.apply(&closing);
    assert_eq!(position.quantity, Quantity::from(1));
    portfolio
        .cache()
        .borrow_mut()
        .update_position(&position)
        .unwrap();
    given_margin_cash(
        &mut portfolio,
        account_id,
        Money::from_decimal(dec!(1000), currency).unwrap(),
        None,
    );
    portfolio.update_position(&PositionEvent::PositionChanged(PositionChanged::create(
        &position,
        &closing,
        uuid4(),
        0.into(),
    )));
    price_option(&mut portfolio, &instrument, mark);
    assert_premium_equity(
        &mut portfolio,
        &instrument,
        account_id,
        currency,
        equities[2],
        unrealized[2],
    );
}

#[rstest]
fn test_margin_premium_equity_requires_price_and_converts_actual_mark(
    simple_cache: Cache,
    clock: VirtualClock,
) {
    let config = PortfolioConfig::builder()
        .use_mark_xrates(true)
        .build()
        .unwrap();
    let mut portfolio = Portfolio::new(
        Rc::new(RefCell::new(clock)),
        Rc::new(RefCell::new(simple_cache)),
        Some(config),
    );
    let instrument = premium_option(false);
    portfolio
        .cache()
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    let account_id = AccountId::new("SIM-001");

    // One USD 2 premium paid at USD/EUR 0.9 leaves EUR 998.2 cash
    given_margin_cash(
        &mut portfolio,
        account_id,
        Money::from("998.20 EUR"),
        Some(Currency::EUR()),
    );
    let fill = make_fill_for_account(
        &instrument,
        account_id,
        OrderSide::Buy,
        Quantity::from(1),
        Price::from("2.00"),
        PositionId::new("P-PREMIUM-FX"),
    );
    let position = Position::new(&instrument, fill);
    portfolio
        .cache()
        .borrow_mut()
        .add_position(&position, OmsType::Hedging)
        .unwrap();
    for with_price in [false, true] {
        if with_price {
            price_option(&mut portfolio, &instrument, Price::from("3.00"));
        }
        let incomplete = portfolio.equity(&instrument.id().venue, Some(&account_id));
        assert_eq!(incomplete[&Currency::EUR()], Money::from("998.20 EUR"));
        let snapshot = portfolio.build_snapshot(&account_id).unwrap();
        assert!(snapshot.is_stale);
        assert_eq!(snapshot.unpriced_instruments, vec![instrument.id()]);
    }
    portfolio
        .cache()
        .borrow_mut()
        .set_mark_xrate(Currency::USD(), Currency::EUR(), 0.9);
    price_option(&mut portfolio, &instrument, Price::from("3.00"));

    // Cash 998.2 plus current asset 3*0.9 is 1000.9, difference PnL remains 0.9
    assert_premium_equity(
        &mut portfolio,
        &instrument,
        account_id,
        Currency::EUR(),
        dec!(1000.9),
        dec!(0.9),
    );
}
