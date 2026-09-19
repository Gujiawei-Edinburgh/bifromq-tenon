/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Per-Channel commands for definition replacement and old Source session finish.
//!
//! This module owns the single pending command slot, the per-replacement decision
//! channel, the coordinator ticket, and the worker-side protocol session. The
//! command slot is one mutex-guarded `Option`, so the pending fact has one
//! representation that both the worker's dispatch and its park recheck read;
//! a channel would need a second flag to become readable without consuming it. Each
//! replacement receives a fresh decision channel, so a late decision can never be
//! observed by a later replacement. The FlowChannel remains responsible only
//! for creating and installing Lua VMs, swapping route bindings, and resolving
//! old pending input. Source session finish uses the same pending slot but only
//! reports once the Source reader has released the Channel's old Completions;
//! it has no replacement decisions or persistent pause state.

use std::error::Error;
use std::fmt;
use std::sync::mpsc::{
    Receiver as MpscReceiver, SyncSender as MpscSender, TryRecvError, sync_channel,
};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::oneshot;

use super::egress::PreparedEgressRoutes;
use super::flow_channel_error::FlowChannelError;
use super::flow_channel_spec::FlowChannelSpec;
use super::flow_control::FlowChannelControl;
use crate::lua::LuaVmErrorKind;
use tenon_ipc::bell::{BellError, BellInterrupter};

/// A route change keeps the existing VM; a definition change prepares a new one.
#[derive(Clone)]
pub(crate) enum ChannelDefinitionChange {
    KeepLua,
    Replace(FlowChannelSpec),
}

// Planned stop must be able to queue Abort even when Cutover won the same race.
const CHANNEL_REPLACEMENT_DECISION_CAPACITY: usize = 2;

/// A failure while publishing a Channel command or a replacement decision.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum FlowChannelCommandControlError {
    /// Another command is pending or a definition replacement is still active.
    CommandAlreadyPending,
    /// The Channel worker no longer owns its command receiver.
    WorkerDisconnected,
    /// The Channel wait could not be interrupted after publishing the request.
    Wake {
        /// Original Bell Region wake failure.
        source: BellError,
    },
}

impl fmt::Display for FlowChannelCommandControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CommandAlreadyPending => "Flow Channel already has a pending command",
            Self::WorkerDisconnected => "Flow Channel command control is disconnected",
            Self::Wake { .. } => "Flow Channel could not be woken for a command",
        })
    }
}

impl Error for FlowChannelCommandControlError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Wake { source } => Some(source),
            Self::CommandAlreadyPending | Self::WorkerDisconnected => None,
        }
    }
}

/// One observable milestone from a coordinated Channel definition replacement.
pub(crate) enum FlowChannelReplacementEvent {
    /// The replacement VM was created without changing the active VM.
    Prepared {
        /// Zero-based Channel index.
        index: u32,
    },
    /// The replacement VM could not be created; the active VM remains unchanged.
    PreparationFailed {
        /// Zero-based Channel index.
        index: u32,
        /// Stable Lua failure category.
        kind: LuaVmErrorKind,
    },
    /// Old pending input was resolved and the replacement VM now owns the paused Channel.
    CutoverComplete {
        /// Zero-based Channel index.
        index: u32,
    },
    /// Preparation was abandoned and the old VM is active again.
    Aborted {
        /// Zero-based Channel index.
        index: u32,
    },
}

enum FlowChannelReplacementDecision {
    Cutover,
    Activate,
    Abort,
}

/// Thread-safe producer for one Channel's single pending command slot.
#[derive(Clone)]
pub(crate) struct FlowChannelCommandControl {
    commands: Weak<Mutex<Option<ChannelCommand>>>,
    active_decision: Arc<Mutex<Option<MpscSender<FlowChannelReplacementDecision>>>>,
    bell: BellInterrupter,
}

impl FlowChannelCommandControl {
    /// Publishes one replacement source and returns its unique decision owner.
    pub(crate) fn begin_replacement(
        &self,
        index: u32,
        definition: ChannelDefinitionChange,
        routes: PreparedEgressRoutes,
        events: MpscSender<FlowChannelReplacementEvent>,
    ) -> Result<FlowChannelReplacementTicket, FlowChannelCommandControlError> {
        let (decision_sender, decisions) = sync_channel(CHANNEL_REPLACEMENT_DECISION_CAPACITY);
        {
            let mut active = lock_active_decision(&self.active_decision);
            if active.is_some() {
                return Err(FlowChannelCommandControlError::CommandAlreadyPending);
            }
            *active = Some(decision_sender.clone());
        }
        let ticket = FlowChannelReplacementTicket {
            decision_sender: Some(decision_sender),
            active_decision: Arc::clone(&self.active_decision),
            bell: self.bell.clone(),
        };
        let request = ChannelReplacementRequest {
            index,
            definition,
            routes,
            decisions,
            events,
        };
        self.send(ChannelCommand::Replace(request))?;
        Ok(ticket)
    }

    /// Requests finite old-session finish after the sole Source writer quiesces.
    /// Dropping the returned observation does not cancel the Channel's work.
    pub(crate) fn finish_source_session(
        &self,
    ) -> Result<oneshot::Receiver<()>, FlowChannelCommandControlError> {
        let (finished, completion) = oneshot::channel();
        self.send(ChannelCommand::FinishSourceSession(finished))?;
        Ok(completion)
    }

    pub(crate) fn abort_active(&self) {
        if let Some(decision) = lock_active_decision(&self.active_decision).as_ref() {
            let _ = decision.try_send(FlowChannelReplacementDecision::Abort);
        }
    }

    fn send(&self, command: ChannelCommand) -> Result<(), FlowChannelCommandControlError> {
        let commands = self
            .commands
            .upgrade()
            .ok_or(FlowChannelCommandControlError::WorkerDisconnected)?;
        let mut slot = lock_commands(&commands);
        if slot.is_some() {
            return Err(FlowChannelCommandControlError::CommandAlreadyPending);
        }
        *slot = Some(command);
        drop(slot);
        self.bell
            .interrupt()
            .map_err(|source| FlowChannelCommandControlError::Wake { source })
    }
}

impl fmt::Debug for FlowChannelCommandControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FlowChannelCommandControl")
            .finish_non_exhaustive()
    }
}

/// The unique decision owner for one accepted Channel definition replacement request.
pub(crate) struct FlowChannelReplacementTicket {
    decision_sender: Option<MpscSender<FlowChannelReplacementDecision>>,
    active_decision: Arc<Mutex<Option<MpscSender<FlowChannelReplacementDecision>>>>,
    bell: BellInterrupter,
}

impl FlowChannelReplacementTicket {
    /// Authorizes the Channel to resolve old pending input and install its prepared VM.
    pub(crate) fn cutover(&self) -> Result<(), FlowChannelCommandControlError> {
        self.send(FlowChannelReplacementDecision::Cutover)
    }

    /// Releases a cut-over Channel only after every sibling has also cut over.
    pub(crate) fn activate(mut self) -> Result<(), FlowChannelCommandControlError> {
        self.send(FlowChannelReplacementDecision::Activate)?;
        *lock_active_decision(&self.active_decision) = None;
        self.decision_sender.take();
        Ok(())
    }

    fn send(
        &self,
        decision: FlowChannelReplacementDecision,
    ) -> Result<(), FlowChannelCommandControlError> {
        self.decision_sender
            .as_ref()
            .ok_or(FlowChannelCommandControlError::WorkerDisconnected)?
            .send(decision)
            .map_err(|_| FlowChannelCommandControlError::WorkerDisconnected)?;
        self.bell
            .interrupt()
            .map_err(|source| FlowChannelCommandControlError::Wake { source })
    }
}

impl Drop for FlowChannelReplacementTicket {
    fn drop(&mut self) {
        let Some(decision_sender) = self.decision_sender.take() else {
            return;
        };
        let _ = decision_sender.try_send(FlowChannelReplacementDecision::Abort);
        *lock_active_decision(&self.active_decision) = None;
        if self.bell.interrupt().is_err() {
            // A failed wake could leave mandatory candidate cleanup blocked forever.
            std::process::abort();
        }
    }
}

fn lock_active_decision(
    active: &Mutex<Option<MpscSender<FlowChannelReplacementDecision>>>,
) -> std::sync::MutexGuard<'_, Option<MpscSender<FlowChannelReplacementDecision>>> {
    active
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) enum ChannelCommand {
    Replace(ChannelReplacementRequest),
    FinishSourceSession(oneshot::Sender<()>),
}

/// Worker-side inbox for the single pending Channel command slot.
pub(super) struct ChannelCommandInbox {
    commands: Arc<Mutex<Option<ChannelCommand>>>,
}

impl ChannelCommandInbox {
    /// Reports whether one command is waiting, without consuming it.
    ///
    /// The Channel's park rechecks exactly the facts its dispatch acts on, so it
    /// must be able to ask this question repeatedly.
    pub(super) fn has_pending(&self) -> bool {
        lock_commands(&self.commands).is_some()
    }

    /// Takes the pending command, if any.
    pub(super) fn try_take(&self) -> Option<ChannelCommand> {
        lock_commands(&self.commands).take()
    }
}

pub(super) fn control_pair(
    bell: BellInterrupter,
) -> (ChannelCommandInbox, FlowChannelCommandControl) {
    let commands = Arc::new(Mutex::new(None));
    let active_decision = Arc::new(Mutex::new(None));
    (
        ChannelCommandInbox {
            commands: Arc::clone(&commands),
        },
        FlowChannelCommandControl {
            commands: Arc::downgrade(&commands),
            active_decision,
            bell,
        },
    )
}

fn lock_commands(
    commands: &Mutex<Option<ChannelCommand>>,
) -> std::sync::MutexGuard<'_, Option<ChannelCommand>> {
    commands
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) struct ChannelReplacementRequest {
    index: u32,
    definition: ChannelDefinitionChange,
    routes: PreparedEgressRoutes,
    decisions: MpscReceiver<FlowChannelReplacementDecision>,
    events: MpscSender<FlowChannelReplacementEvent>,
}

impl ChannelReplacementRequest {
    pub(super) fn into_session(
        self,
        control: Arc<FlowChannelControl>,
    ) -> (
        ChannelDefinitionChange,
        PreparedEgressRoutes,
        ChannelReplacementSession,
    ) {
        let Self {
            index,
            definition,
            routes,
            decisions,
            events,
        } = self;
        (
            definition,
            routes,
            ChannelReplacementSession {
                index,
                decisions,
                events,
                control,
            },
        )
    }
}

/// Outcome after a candidate VM reaches the Prepared protocol boundary.
pub(super) enum CandidatePreparationOutcome {
    /// No decision exists yet; the old VM must keep processing events.
    Pending(ChannelReplacementSession),
    /// The coordinator authorized the destructive data-plane transition.
    Cutover(ChannelReplacementCutoverPermit),
    /// The candidate was abandoned before changing the active VM.
    Aborted(ChannelReplacementSession),
}

/// Worker-side protocol owner before the coordinator authorizes cutover.
pub(super) struct ChannelReplacementSession {
    index: u32,
    decisions: MpscReceiver<FlowChannelReplacementDecision>,
    events: MpscSender<FlowChannelReplacementEvent>,
    control: Arc<FlowChannelControl>,
}

impl ChannelReplacementSession {
    /// Reports candidate failure and its completed cleanup without pausing the old VM.
    pub(super) fn candidate_failed(self, kind: LuaVmErrorKind) -> Result<(), FlowChannelError> {
        self.send_required_event(FlowChannelReplacementEvent::PreparationFailed {
            index: self.index,
            kind,
        })?;
        self.send_aborted_best_effort();
        Ok(())
    }

    /// Reports a ready candidate while leaving the old event loop running.
    pub(super) fn candidate_prepared(&self) -> Result<(), FlowChannelError> {
        self.send_required_event(FlowChannelReplacementEvent::Prepared { index: self.index })
    }

    /// Observes a decision at the next old-VM event boundary without blocking.
    pub(super) fn try_cutover(self) -> Result<CandidatePreparationOutcome, FlowChannelError> {
        if self.is_stopping() {
            return Ok(CandidatePreparationOutcome::Aborted(self));
        }
        let decision = match self.decisions.try_recv() {
            Ok(decision) => decision,
            Err(TryRecvError::Empty) => return Ok(CandidatePreparationOutcome::Pending(self)),
            Err(TryRecvError::Disconnected) if self.is_stopping() => {
                return Ok(CandidatePreparationOutcome::Aborted(self));
            }
            Err(TryRecvError::Disconnected) => {
                return Err(FlowChannelError::InternalInvariantViolation);
            }
        };
        match decision {
            FlowChannelReplacementDecision::Abort => Ok(CandidatePreparationOutcome::Aborted(self)),
            FlowChannelReplacementDecision::Cutover if self.is_stopping() => {
                Ok(CandidatePreparationOutcome::Aborted(self))
            }
            FlowChannelReplacementDecision::Cutover => Ok(CandidatePreparationOutcome::Cutover(
                ChannelReplacementCutoverPermit { session: self },
            )),
            FlowChannelReplacementDecision::Activate => {
                Err(FlowChannelError::InternalInvariantViolation)
            }
        }
    }

    fn send_required_event(
        &self,
        event: FlowChannelReplacementEvent,
    ) -> Result<(), FlowChannelError> {
        match self.events.send(event) {
            Ok(()) => Ok(()),
            Err(_) if self.is_stopping() => Ok(()),
            Err(_) => Err(FlowChannelError::InternalInvariantViolation),
        }
    }

    /// Reports abandonment only after the Channel drops its candidate resources.
    pub(super) fn report_aborted(self) {
        self.send_aborted_best_effort();
    }

    fn send_aborted_best_effort(&self) {
        // The coordinator may deliberately drop a prepared replacement before this event.
        let _ = self
            .events
            .send(FlowChannelReplacementEvent::Aborted { index: self.index });
    }

    fn receive_decision(&self) -> Result<FlowChannelReplacementDecision, FlowChannelError> {
        match self.decisions.recv() {
            Ok(decision) => Ok(decision),
            Err(_) if self.is_stopping() => Ok(FlowChannelReplacementDecision::Abort),
            Err(_) => Err(FlowChannelError::InternalInvariantViolation),
        }
    }

    fn is_stopping(&self) -> bool {
        self.control.is_stopping()
    }
}

/// Proof that the coordinator authorized the Channel's destructive cutover.
pub(super) struct ChannelReplacementCutoverPermit {
    session: ChannelReplacementSession,
}

impl ChannelReplacementCutoverPermit {
    /// Reports the completed data-plane transition and waits for common activation.
    pub(super) fn complete_and_wait_for_activation(self) -> Result<(), FlowChannelError> {
        self.session
            .send_required_event(FlowChannelReplacementEvent::CutoverComplete {
                index: self.session.index,
            })?;
        if self.session.is_stopping() {
            return Ok(());
        }
        match self.session.receive_decision()? {
            FlowChannelReplacementDecision::Activate => Ok(()),
            FlowChannelReplacementDecision::Abort if self.session.is_stopping() => Ok(()),
            FlowChannelReplacementDecision::Abort | FlowChannelReplacementDecision::Cutover => {
                Err(FlowChannelError::InternalInvariantViolation)
            }
        }
    }
}

#[cfg(test)]
mod tests;
