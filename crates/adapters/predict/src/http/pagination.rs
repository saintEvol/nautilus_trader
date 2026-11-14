//! Cursor-pagination result type for Predict list endpoints.
//!
//! Endpoints page via an opaque `cursor` echoed back as `after`. A walk is
//! `complete` only when the server stops returning a cursor; a page budget
//! running out never masquerades as exhaustion.

use super::error::PredictHttpError;

/// Result of a cursor walk over a list endpoint.
#[derive(Clone, Debug)]
pub struct Pages<T> {
    /// Every distinct record collected across the walked pages.
    pub records: Vec<T>,
    /// `true` only when the source reported no further pages.
    pub complete: bool,
    /// Continuation cursor for a later resume when `complete == false`.
    pub next_cursor: Option<String>,
}

/// One step of the cursor protocol: returns an error on a repeated cursor
/// or on an empty page that still carries a continuation cursor (a stalled
/// source), otherwise hands back the cursor for the next page (`None` ends
/// the walk).
pub(super) fn advance(
    endpoint: &'static str,
    previous: &mut Option<String>,
    rows: usize,
    cursor: Option<String>,
) -> Result<Option<String>, PredictHttpError> {
    let next = cursor.filter(|c| !c.is_empty());
    let Some(next) = next else {
        return Ok(None); // Source exhausted.
    };
    if previous.as_deref() == Some(next.as_str()) {
        return Err(PredictHttpError::Pagination(format!(
            "{endpoint}: cursor repeated without progress: {next}"
        )));
    }
    if rows == 0 {
        return Err(PredictHttpError::Pagination(format!(
            "{endpoint}: empty page carries a continuation cursor"
        )));
    }
    *previous = Some(next.clone());
    Ok(Some(next))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_or_empty_cursor_completes() {
        let mut previous = None;
        assert!(advance("/markets", &mut previous, 3, None).unwrap().is_none());
        assert!(
            advance("/markets", &mut previous, 3, Some(String::new()))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn repeated_cursor_is_loud() {
        let mut previous = Some("A".to_string());
        let err = advance("/markets", &mut previous, 2, Some("A".into()))
            .expect_err("repeated cursor must error");
        assert!(err.to_string().contains("repeated"), "{err}");
    }

    #[test]
    fn empty_page_with_cursor_is_stalled() {
        let mut previous = Some("A".to_string());
        let err = advance("/markets", &mut previous, 0, Some("B".into()))
            .expect_err("empty page with cursor must error");
        assert!(err.to_string().contains("empty page"), "{err}");
    }
}
