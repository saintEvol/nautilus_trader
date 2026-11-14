//! Symbol ↔ instrument identity mapping for Predict.
//!
//! Predict native symbol: `<condition_id>-<outcome_onChainId>` — the on-chain
//! condition id (0x-prefixed hex) joined with the outcome token's decimal
//! on-chain id. The HTTP market id is carried separately in instrument
//! metadata (`market_id`) and can never be recovered from the symbol.

use nautilus_model::identifiers::{InstrumentId, Symbol};

use super::consts::PREDICT_VENUE;

/// Builds the native symbol for one outcome of a market.
#[must_use]
pub fn outcome_symbol(condition_id: &str, on_chain_id: &str) -> String {
    format!("{condition_id}-{on_chain_id}")
}

/// Builds the instrument id for one outcome of a market.
#[must_use]
pub fn outcome_instrument_id(condition_id: &str, on_chain_id: &str) -> InstrumentId {
    InstrumentId::new(
        Symbol::new(outcome_symbol(condition_id, on_chain_id)),
        *PREDICT_VENUE,
    )
}

/// Splits an instrument symbol back into `(condition_id, on_chain_id)`.
///
/// # Errors
///
/// Returns an error when the symbol is not the two-part native form.
pub fn parse_outcome_symbol(symbol: &Symbol) -> Result<(&str, &str), String> {
    let value = symbol.as_str();
    let Some((condition_id, on_chain_id)) = value.rsplit_once('-') else {
        return Err(format!("symbol {value:?} is not <condition_id>-<onChainId>"));
    };
    if condition_id.is_empty() || on_chain_id.is_empty() {
        return Err(format!("symbol {value:?} has an empty component"));
    }
    Ok((condition_id, on_chain_id))
}

/// Splits an instrument id back into `(condition_id, on_chain_id)`.
///
/// # Errors
///
/// Returns an error when the venue is not PREDICT or the symbol is not the
/// native two-part form.
pub fn parse_instrument_id(instrument_id: &InstrumentId) -> Result<(&str, &str), String> {
    if instrument_id.venue != *PREDICT_VENUE {
        return Err(format!(
            "instrument {instrument_id} is not a PREDICT instrument"
        ));
    }
    parse_outcome_symbol(&instrument_id.symbol)
}

/// Converts unix milliseconds to nanoseconds (negative inputs clamp to 0).
#[must_use]
pub(crate) fn unix_ms_to_nanos(ms: i64) -> nautilus_core::UnixNanos {
    nautilus_core::UnixNanos::from(ms.saturating_mul(1_000_000).max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONDITION: &str = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    const ON_CHAIN: &str = "12345";

    #[test]
    fn symbol_round_trips() {
        let id = outcome_instrument_id(CONDITION, ON_CHAIN);
        assert_eq!(id.to_string(), format!("{CONDITION}-{ON_CHAIN}.PREDICT"));
        let (condition, on_chain) = parse_instrument_id(&id).unwrap();
        assert_eq!(condition, CONDITION);
        assert_eq!(on_chain, ON_CHAIN);
    }

    #[test]
    fn non_native_symbol_rejected() {
        let bad = Symbol::new("plain");
        assert!(parse_outcome_symbol(&bad).is_err());
        let empty_part = Symbol::new("0xcond-");
        assert!(parse_outcome_symbol(&empty_part).is_err());
    }

    #[test]
    fn foreign_venue_rejected() {
        let foreign = InstrumentId::from("0xcond-1.BINANCE");
        assert!(parse_instrument_id(&foreign).is_err());
    }
}

