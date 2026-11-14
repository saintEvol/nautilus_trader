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

use std::{collections::HashSet, convert::Infallible, future::Future};

#[derive(Debug)]
pub(crate) struct FetchOutcome<T> {
    pub(crate) rows: Vec<T>,
    pub(crate) wire: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Completion<S> {
    WireExhausted,
    Stopped(S),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Completed<O, S> {
    pub(crate) output: O,
    pub(crate) completion: Completion<S>,
    /// Cursor that continues the walk from where it stopped (`None` when the
    /// wire was exhausted — no further pages exist). Structured history
    /// outlets expose this for resumable/cancellable archival walks.
    pub(crate) next_cursor: Option<String>,
}

pub(crate) trait PageReducer<T, E> {
    type Output;
    type Stop;

    fn consume(&mut self, rows: Vec<T>) -> Result<Option<Self::Stop>, E>;

    fn finish(self, completion: &Completion<Self::Stop>) -> Result<Self::Output, E>;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Cursor(String);

impl AsRef<str> for Cursor {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug)]
pub(crate) struct CursorProtocol {
    endpoint: &'static str,
    initial: Option<Cursor>,
    dialect: CursorDialect,
    seen: HashSet<Cursor>,
}

#[derive(Debug)]
enum CursorDialect {
    Clob { terminal_cursor: &'static str },
    Gamma,
}

impl CursorProtocol {
    pub(crate) fn clob(
        endpoint: &'static str,
        initial: String,
        terminal_cursor: &'static str,
    ) -> Self {
        let initial = Cursor(initial);
        let seen = HashSet::from([initial.clone()]);
        Self {
            endpoint,
            initial: Some(initial),
            dialect: CursorDialect::Clob { terminal_cursor },
            seen,
        }
    }

    pub(crate) fn gamma(endpoint: &'static str) -> Self {
        Self {
            endpoint,
            initial: None,
            dialect: CursorDialect::Gamma,
            seen: HashSet::new(),
        }
    }

    /// Gamma protocol resuming from a previously reported continuation cursor.
    pub(crate) fn gamma_from(endpoint: &'static str, resume: String) -> Self {
        let resume = Cursor(resume);
        let seen = HashSet::from([resume.clone()]);
        Self {
            endpoint,
            initial: Some(resume),
            dialect: CursorDialect::Gamma,
            seen,
        }
    }

    fn observe(
        &self,
        position: &Option<Cursor>,
        wire: Option<String>,
    ) -> Result<Option<Cursor>, PaginationError> {
        let raw = match (&self.dialect, wire) {
            (CursorDialect::Gamma, None) => return Ok(None),
            (CursorDialect::Clob { .. }, None) => {
                return Err(PaginationError::MissingCursor {
                    endpoint: self.endpoint,
                });
            }
            (_, Some(raw)) => raw,
        };

        match &self.dialect {
            CursorDialect::Clob { terminal_cursor }
                if raw.is_empty() || raw == *terminal_cursor =>
            {
                return Ok(None);
            }
            CursorDialect::Clob { .. } | CursorDialect::Gamma => {}
        }

        let next = Cursor(raw);
        if matches!(&self.dialect, CursorDialect::Clob { .. }) && position.as_ref() == Some(&next) {
            return Err(PaginationError::StalledCursor {
                endpoint: self.endpoint,
                cursor: next.0,
            });
        }

        if self.seen.contains(&next) {
            return Err(PaginationError::RepeatedCursor {
                endpoint: self.endpoint,
                cursor: next.0,
            });
        }

        Ok(Some(next))
    }
}

pub(crate) struct CollectAll<T> {
    rows: Vec<T>,
}

impl<T> CollectAll<T> {
    pub(crate) const fn new() -> Self {
        Self { rows: Vec::new() }
    }
}

impl<T, E> PageReducer<T, E> for CollectAll<T> {
    type Output = Vec<T>;
    type Stop = Infallible;

    fn consume(&mut self, rows: Vec<T>) -> Result<Option<Self::Stop>, E> {
        self.rows.extend(rows);
        Ok(None)
    }

    fn finish(self, _completion: &Completion<Self::Stop>) -> Result<Self::Output, E> {
        Ok(self.rows)
    }
}

pub(crate) struct WindowedCollect<T, S> {
    remaining_skip: usize,
    maximum: Option<usize>,
    rows: Vec<T>,
    stop: S,
}

impl<T, S> WindowedCollect<T, S> {
    pub(crate) const fn new(remaining_skip: usize, maximum: Option<usize>, stop: S) -> Self {
        Self {
            remaining_skip,
            maximum,
            rows: Vec::new(),
            stop,
        }
    }
}

impl<T, E, S> PageReducer<T, E> for WindowedCollect<T, S>
where
    S: Clone,
{
    type Output = Vec<T>;
    type Stop = S;

    fn consume(&mut self, rows: Vec<T>) -> Result<Option<Self::Stop>, E> {
        let skipped = self.remaining_skip.min(rows.len());
        self.remaining_skip -= skipped;
        let retained = rows.into_iter().skip(skipped);

        if let Some(maximum) = self.maximum {
            let remaining = maximum.saturating_sub(self.rows.len());
            self.rows.extend(retained.take(remaining));
            if self.rows.len() >= maximum {
                return Ok(Some(self.stop.clone()));
            }
        } else {
            self.rows.extend(retained);
        }

        Ok(None)
    }

    fn finish(self, _completion: &Completion<Self::Stop>) -> Result<Self::Output, E> {
        Ok(self.rows)
    }
}

pub(crate) struct Paginator<R> {
    endpoint: &'static str,
    protocol: CursorProtocol,
    reducer: R,
    pages: usize,
}

impl<R> Paginator<R> {
    pub(crate) const fn new(endpoint: &'static str, protocol: CursorProtocol, reducer: R) -> Self {
        Self {
            endpoint,
            protocol,
            reducer,
            pages: 0,
        }
    }

    pub(crate) async fn run<T, E, F, Fut, M>(
        mut self,
        mut fetch: F,
        mut map_pagination_error: M,
    ) -> Result<Completed<R::Output, R::Stop>, E>
    where
        R: PageReducer<T, E>,
        F: FnMut(Option<Cursor>) -> Fut,
        Fut: Future<Output = Result<FetchOutcome<T>, E>>,
        M: FnMut(PaginationError) -> E,
    {
        let mut position = self.protocol.initial.clone();
        let (completion, next_cursor) = loop {
            let FetchOutcome { rows, wire } = fetch(position.clone()).await?;

            let page = self.pages.checked_add(1).ok_or_else(|| {
                map_pagination_error(PaginationError::PageCounterOverflow {
                    endpoint: self.endpoint,
                })
            })?;

            let next = self
                .protocol
                .observe(&position, wire)
                .map_err(&mut map_pagination_error)?;

            log::debug!("Fetched {} page {page}: {} rows", self.endpoint, rows.len(),);
            self.pages = page;

            if let Some(stop) = self.reducer.consume(rows)? {
                break (Completion::Stopped(stop), next.map(|c| c.0));
            }

            let Some(next) = next else {
                break (Completion::WireExhausted, None);
            };

            self.protocol.seen.insert(next.clone());
            position = Some(next);
        };

        let output = self.reducer.finish(&completion)?;
        Ok(Completed {
            output,
            completion,
            next_cursor,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serves a scripted page sequence and records the cursor positions the
    /// paginator requests (offline — no HTTP).
    fn fetch_script(
        script: Vec<FetchOutcome<u32>>,
    ) -> (
        impl FnMut(Option<Cursor>) -> std::future::Ready<Result<FetchOutcome<u32>, PaginationError>>,
        std::rc::Rc<std::cell::RefCell<Vec<Option<String>>>>,
    ) {
        let mut remaining = script;
        let positions = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let recorder = positions.clone();
        let fetch = move |position: Option<Cursor>| {
            recorder
                .borrow_mut()
                .push(position.map(|c| c.0));
            std::future::ready(Ok(remaining.remove(0)))
        };
        (fetch, positions)
    }

    /// Exhausted walks carry no continuation cursor; every page was consumed.
    #[tokio::test]
    async fn exhausted_walk_has_no_cursor() {
        let protocol = CursorProtocol::gamma("test");
        let paginator = Paginator::new("test", protocol, CollectAll::new());
        let (fetch, _) = fetch_script(vec![
            FetchOutcome { rows: vec![1], wire: Some("c1".into()) },
            FetchOutcome { rows: vec![2], wire: Some("c2".into()) },
            FetchOutcome { rows: vec![3], wire: None },
        ]);
        let completed = paginator.run(fetch, |e| e).await.unwrap();
        assert_eq!(completed.output, vec![1, 2, 3]);
        assert_eq!(completed.completion, Completion::WireExhausted);
        assert_eq!(completed.next_cursor, None);
    }

    /// A caller-capped stop keeps the continuation cursor for resumption.
    #[tokio::test]
    async fn capped_stop_carries_cursor() {
        let protocol = CursorProtocol::gamma("test");
        let reducer = WindowedCollect::new(0, Some(1), "capped");
        let paginator = Paginator::new("test", protocol, reducer);
        let (fetch, _) = fetch_script(vec![
            FetchOutcome { rows: vec![1, 2], wire: Some("next-page".into()) },
            FetchOutcome { rows: vec![3], wire: None },
        ]);
        let completed = paginator.run(fetch, |e| e).await.unwrap();
        assert_eq!(completed.completion, Completion::Stopped("capped"));
        assert_eq!(
            completed.next_cursor.as_deref(),
            Some("next-page"),
            "resumable walk carries the cursor where it stopped"
        );
    }

    /// gamma_from(resume) seeds the walk at the cursor — no earlier pages are
    /// fetched, and the seed itself is not a repeated-cursor false positive.
    #[tokio::test]
    async fn gamma_from_resumes_at_seed_without_refetch() {
        let protocol = CursorProtocol::gamma_from("test", "seed-cursor".to_string());
        let paginator = Paginator::new("test", protocol, CollectAll::new());
        let (fetch, positions) = fetch_script(vec![
            FetchOutcome { rows: vec![9], wire: Some("after-seed".into()) },
            FetchOutcome { rows: vec![10], wire: None },
        ]);
        let completed = paginator.run(fetch, |e| e).await.unwrap();
        assert_eq!(completed.output, vec![9, 10]);
        assert_eq!(completed.completion, Completion::WireExhausted);
        assert_eq!(completed.next_cursor, None);
        assert_eq!(
            positions.borrow().as_slice(),
            [Some("seed-cursor".to_string()), Some("after-seed".to_string())],
            "first fetch continues from the seed cursor — no refetch of earlier pages"
        );
    }

    /// A repeated cursor is a loud protocol error, never a silent loop.
    #[tokio::test]
    async fn repeated_cursor_errors() {
        let protocol = CursorProtocol::gamma("test");
        let paginator = Paginator::new("test", protocol, CollectAll::<u32>::new());
        let (fetch, _) = fetch_script(vec![
            FetchOutcome { rows: vec![1], wire: Some("loop".into()) },
            FetchOutcome { rows: vec![2], wire: Some("loop".into()) },
        ]);
        let result = paginator.run(fetch, |e| e).await;
        match result {
            Err(PaginationError::RepeatedCursor { cursor, .. }) => assert_eq!(cursor, "loop"),
            other => panic!("expected RepeatedCursor, got {other:?}"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PaginationError {
    #[error("{endpoint} response omitted next_cursor")]
    MissingCursor { endpoint: &'static str },
    #[error("{endpoint} pagination cursor did not advance from {cursor:?}")]
    StalledCursor {
        endpoint: &'static str,
        cursor: String,
    },
    #[error("{endpoint} pagination repeated cursor {cursor:?}")]
    RepeatedCursor {
        endpoint: &'static str,
        cursor: String,
    },
    #[error("{endpoint} pagination page counter overflowed usize")]
    PageCounterOverflow { endpoint: &'static str },
}
