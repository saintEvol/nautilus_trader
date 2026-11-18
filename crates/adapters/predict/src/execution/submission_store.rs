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

//! Durable correlation of signed Predict orders before transmission.

use std::fmt::Debug;

use alloy_primitives::B256;
use nautilus_core::UnixNanos;
use nautilus_model::{
    events::OrderInitialized,
    identifiers::{AccountId, ClientOrderId, TraderId},
};
use serde::{Deserialize, Serialize};

/// The exact local order instance associated with a submitted venue hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictSubmission {
    pub account_id: AccountId,
    pub client_order_id: ClientOrderId,
    pub initialized_at: UnixNanos,
    pub order_hash: B256,
    /// Present on submissions written by the native order recovery path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<OrderInitialized>,
}

/// Application-owned durable storage. Successful reservation must be committed
/// before returning. A previously reserved instance must always return false.
#[async_trait::async_trait]
pub trait PredictSubmissionStore: Debug + Send + Sync {
    /// Atomically reserves a signed order instance before it can be transmitted.
    async fn reserve(&self, submission: &PredictSubmission) -> anyhow::Result<bool>;

    /// Loads submissions with native initialization events for this trader and account.
    async fn recoverable(
        &self,
        account: AccountId,
        trader: TraderId,
    ) -> anyhow::Result<Vec<PredictSubmission>>;

    /// Loads the immutable signed hash for the given local order instance.
    async fn load(
        &self,
        account: AccountId,
        client_id: ClientOrderId,
        initialized_at: UnixNanos,
    ) -> anyhow::Result<Option<B256>>;
}
