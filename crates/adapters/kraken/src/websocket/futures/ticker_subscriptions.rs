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

use std::{collections::HashMap, time::Duration};

use nautilus_network::{backoff::ExponentialBackoff, websocket::SubscriptionState};
use tokio::time::Instant;
use ustr::Ustr;

use super::{
    messages::{
        KrakenFuturesEvent, KrakenFuturesFeed, KrakenFuturesRequest,
        KrakenFuturesSubscriptionResponse,
    },
    ticker_subscription_batch::TickerSubscriptionBatch,
};

// The bound limits one wire frame, not the desired market universe
const BATCH_SIZE: usize = 50;
const BATCH_WINDOW: Duration = Duration::from_millis(20);
const ACK_TIMEOUT: Duration = Duration::from_secs(5);
const INVALID_PRODUCT_RETRY_DELAY: Duration = Duration::from_secs(30);
const TICKER: &str = "ticker";

/// Schedules ticker requests for the current socket epoch. Nautilus tracks
/// desired/confirmed topics; this tracks only the one in-flight wire batch.
pub(super) struct TickerSubscriptions {
    in_flight: Option<TickerSubscriptionBatch>,
    deadline: Option<Instant>,
    backoff: ExponentialBackoff,
    retry_subscribe_after: HashMap<String, Instant>,
}

impl TickerSubscriptions {
    pub(super) fn new() -> Self {
        Self {
            in_flight: None,
            deadline: None,
            backoff: ExponentialBackoff::new(
                Duration::from_millis(1_200),
                Duration::from_secs(30),
                2.0,
                250,
                false,
            )
            .expect("valid ticker subscription backoff"),
            retry_subscribe_after: HashMap::new(),
        }
    }

    pub(super) const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub(super) fn sync(&mut self, now: Instant) {
        if self.in_flight.is_none() {
            let batch_deadline = now + BATCH_WINDOW;
            self.deadline = Some(
                self.deadline
                    .map_or(batch_deadline, |d| d.min(batch_deadline)),
            );
        }
    }

    pub(super) fn reset(&mut self) {
        self.in_flight = None;
        self.deadline = None;
        self.backoff.reset();
        self.retry_subscribe_after.clear();
    }

    pub(super) fn next_request(
        &mut self,
        subscriptions: &SubscriptionState,
        now: Instant,
    ) -> Option<KrakenFuturesRequest> {
        self.deadline = None;
        if let Some(batch) = self.in_flight.as_mut()
            && !batch.cooling_down
        {
            log::warn!(
                "Kraken Futures ticker acknowledgment timed out: operation={:?}, pending={}",
                batch.event,
                batch.products.len(),
            );
            self.back_off(now);
            return None;
        }
        let retry = self.in_flight.take();
        self.retry_subscribe_after.retain(|_, after| *after > now);

        let ticker = Ustr::from(TICKER);
        // Complete cancellations before adding products on the same connection
        let next = [
            (
                KrakenFuturesEvent::Unsubscribe,
                subscriptions.pending_unsubscribe(),
            ),
            (
                KrakenFuturesEvent::Subscribe,
                subscriptions.pending_subscribe(),
            ),
        ]
        .into_iter()
        .find_map(|(event, pending)| {
            let mut products: Vec<_> = pending
                .get(&ticker)?
                .iter()
                .filter(|id| {
                    event != KrakenFuturesEvent::Subscribe
                        || !self.retry_subscribe_after.contains_key(id.as_str())
                })
                .map(ToString::to_string)
                .collect();
            // Do not let one rejected batch starve untouched products
            products.sort_unstable_by(|a, b| {
                let was_rejected = |id: &String| {
                    retry
                        .as_ref()
                        .is_some_and(|batch| batch.event == event && batch.products.contains(id))
                };
                was_rejected(a).cmp(&was_rejected(b)).then_with(|| a.cmp(b))
            });
            products.truncate(BATCH_SIZE);
            (!products.is_empty()).then_some((event, products))
        });
        let Some((event, products)) = next else {
            self.deadline = self.retry_subscribe_after.values().min().copied();
            return None;
        };

        self.in_flight = Some(TickerSubscriptionBatch {
            event,
            products: products.iter().cloned().collect(),
            cooling_down: false,
        });
        self.deadline = Some(now + ACK_TIMEOUT);
        Some(KrakenFuturesRequest {
            event,
            feed: KrakenFuturesFeed::Ticker,
            product_ids: products,
        })
    }

    pub(super) fn acknowledge(
        &mut self,
        response: &KrakenFuturesSubscriptionResponse,
        subscriptions: &SubscriptionState,
        now: Instant,
    ) {
        if response.feed != KrakenFuturesFeed::Ticker {
            return;
        }
        let Some(batch) = self.in_flight.as_mut() else {
            return;
        };
        let (success, failure) = match batch.event {
            KrakenFuturesEvent::Subscribe => (
                KrakenFuturesEvent::Subscribed,
                KrakenFuturesEvent::SubscribedFailed,
            ),
            KrakenFuturesEvent::Unsubscribe => (
                KrakenFuturesEvent::Unsubscribed,
                KrakenFuturesEvent::UnsubscribedFailed,
            ),
            _ => return,
        };
        if response.event == failure {
            if response
                .product_ids
                .iter()
                .any(|id| batch.products.contains(id))
            {
                log::warn!(
                    "Kraken Futures ticker request rejected: operation={:?}, products={:?}",
                    batch.event,
                    response.product_ids,
                );
                self.back_off(now);
            }
            return;
        }
        if response.event != success {
            return;
        }
        for product in &response.product_ids {
            if !batch.products.remove(product) {
                continue;
            }
            let topic = format!("{TICKER}:{product}");
            match response.event {
                KrakenFuturesEvent::Subscribed => subscriptions.confirm_subscribe(&topic),
                KrakenFuturesEvent::Unsubscribed => {
                    subscriptions.confirm_unsubscribe(&topic);
                    self.retry_subscribe_after.remove(product);
                }
                _ => unreachable!("checked subscription acknowledgment"),
            }
        }
        self.finish_batch(now);
        log::debug!(
            "Kraken Futures ticker acknowledgment: operation={:?}, products={:?}",
            response.event,
            response.product_ids,
        );
    }

    pub(super) fn handle_alert(
        &mut self,
        message: &str,
        subscriptions: &SubscriptionState,
        now: Instant,
    ) {
        // Kraken uses this wording for both subscribe and unsubscribe
        let Some(product) = message
            .strip_prefix("Couldn't subscribe to invalid product `")
            .and_then(|message| message.strip_suffix('`'))
        else {
            return;
        };
        let Some(batch) = self.in_flight.as_mut() else {
            return;
        };
        if !batch.products.remove(product) {
            return;
        }
        match batch.event {
            KrakenFuturesEvent::Unsubscribe => {
                subscriptions.confirm_unsubscribe(&format!("{TICKER}:{product}"));
                self.retry_subscribe_after.remove(product);
            }
            KrakenFuturesEvent::Subscribe => {
                self.retry_subscribe_after
                    .insert(product.to_owned(), now + INVALID_PRODUCT_RETRY_DELAY);
            }
            _ => unreachable!("ticker batch is a subscribe or unsubscribe request"),
        }
        self.finish_batch(now);
    }

    fn finish_batch(&mut self, now: Instant) {
        if self
            .in_flight
            .as_ref()
            .is_some_and(|batch| batch.products.is_empty() && !batch.cooling_down)
        {
            self.in_flight = None;
            self.backoff.reset();
            self.deadline = Some(now + BATCH_WINDOW);
        }
    }

    pub(super) fn back_off(&mut self, now: Instant) {
        // Preserve partial acknowledgments and rebuild the request from the
        // still-pending topics after the cooldown.
        if let Some(batch) = self.in_flight.as_mut()
            && !batch.cooling_down
        {
            batch.cooling_down = true;
            self.deadline = Some(now + self.backoff.next_duration());
        }
    }
}
