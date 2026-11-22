// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Optional dispatch-layer observation for live execution reconstruction.
//!
//! The node's dispatch layer is the single place where "the message actually
//! selected for processing" is defined: hooking only strategy callbacks or
//! engine event handlers misses engine-internal ordering (risk decisions, order
//! state machine transitions) that reconstruction must reproduce. An optional
//! [`DispatchObserver`] installed via `LiveNodeBuilder::with_dispatch_observer`
//! receives a lightweight, synchronous notification at every dispatch point:
//! startup pending drains, the steady-state `select!` branches, the shutdown
//! grace window, and the final channel drain.
//!
//! Contract for implementors:
//!
//! - Callbacks fire on the node's owner thread only, in dispatch order.
//! - Implementations must be non-blocking and must never panic: observation is
//!   a side channel; a broken observer must not alter trading behavior. Buffer
//!   or drop observations on overflow instead of waiting.
//! - The borrowed message is only valid for the duration of the call: encode or
//!   copy what is needed before returning. Send only owned, bounded data to any
//!   background worker.
//! - Absence of an observer keeps the original execution behavior unchanged.

use std::any::Any;

/// Lifecycle phase of the node loop at the time of an observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DispatchPhase {
    /// Startup sequencing: connect drains and pre-run pending dispatches.
    Startup,
    /// The steady-state event loop while the node reports `Running`.
    Running,
    /// The shutdown grace window: residual events still dispatch while
    /// `ShuttingDown`.
    ShuttingDown,
    /// The final unconditional channel drain after the loop exits.
    FinalDrain,
}

/// The channel a dispatched message came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DispatchBranch {
    /// Time event channel (timer expirations, scheduled callbacks).
    TimeEvent,
    /// System event channel.
    SystemEvent,
    /// System command channel.
    SystemCommand,
    /// Execution event channel (order/fill/account reports).
    ExecEvent,
    /// Execution command channel (outbound trading commands).
    ExecCommand,
    /// Data event channel (market data).
    DataEvent,
    /// Data command channel (subscribe/unsubscribe and data requests).
    DataCommand,
}

/// A message selected for processing on the node thread.
#[derive(Debug)]
pub struct MessageObservation<'a> {
    /// Node lifecycle phase at dispatch time.
    pub phase: DispatchPhase,
    /// Channel the message was received from.
    pub branch: DispatchBranch,
    /// Whether the message is actually processed (`false` only where a drain
    /// path explicitly discards it — reconstruction must see both facts).
    pub executed: bool,
    /// The message as [`DispatchMessage`](super::DispatchMessage)`<T>` for the
    /// branch's concrete `T`; implementations downcast to read identity fields.
    pub message: &'a dyn Any,
}

/// Loop-level facts that are not a single dispatched message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoopObservation {
    /// The periodic maintenance branch ran: a snapshot of pending queue depths
    /// and whether actor callbacks were still pending. Reconstruction drivers
    /// use this as a quiescence signal between injected inputs.
    Maintenance {
        /// Pending time events.
        time_events: usize,
        /// Pending system events.
        system_events: usize,
        /// Pending system commands.
        system_commands: usize,
        /// Pending execution events.
        exec_events: usize,
        /// Pending execution commands.
        exec_commands: usize,
        /// Pending data events.
        data_events: usize,
        /// Pending data commands.
        data_commands: usize,
        /// Whether actor callbacks were pending at the previous drain.
        callbacks_pending: bool,
    },
    /// The shutdown deadline expired and the loop is about to exit.
    ShutdownDeadlineReached,
    /// The final channel drain completed with the given total message count.
    ChannelsDrained {
        /// Total messages processed by the drain.
        drained: usize,
    },
}

/// Synchronous observer installed at the node's dispatch layer.
pub trait DispatchObserver: std::fmt::Debug + Send + Sync {
    /// Called on the node thread immediately before a received message is
    /// processed. See the module docs for the implementor contract.
    fn observe_message(&self, observation: MessageObservation<'_>);

    /// Called on the node thread for loop-level facts. See [`LoopObservation`].
    fn observe_loop(&self, observation: LoopObservation);
}
