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

//! Single identity and connection-state authority for Runner-launched Pipelines.
//!
//! The process launcher registers an opaque launch before spawning the child.
//! The correctness-control and best-effort diagnostics gRPC adapters then claim
//! that same registration independently. The exact process owner retains the
//! registration until reaping, so neither transport adapter owns or cleans the
//! process.
//!
//! One short mutex critical section is the linearization point for every claim
//! and retirement. No lock is held across asynchronous work. A diagnostics
//! disconnect releases only its stream lease; retiring the process registration
//! ends the exact diagnostic incarnation.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest as _, Sha256};
use tokio::sync::{oneshot, watch};

use super::control::session::RunnerPipelineControlSession;
use crate::identifiers::TenonDocumentId;
use crate::runner::diagnostics::RunnerDiagnostics;

const RUNNER_INSTANCE_ID_LENGTH: usize = 16;
const LAUNCH_SEQUENCE_LENGTH: usize = size_of::<u64>();
const PIPELINE_LAUNCH_ID_LENGTH: usize = RUNNER_INSTANCE_ID_LENGTH + LAUNCH_SEQUENCE_LENGTH;

/// Shared authority for every launch created by one Runner process.
pub(in crate::runner) struct RunnerPipelineLaunchRegistry {
    state: Arc<Mutex<PipelineLaunchRegistryState>>,
}

impl Clone for RunnerPipelineLaunchRegistry {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl RunnerPipelineLaunchRegistry {
    /// Creates an empty process-local launch namespace.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerPipelineLaunchRegistryCreateError`] when the operating
    /// system cannot provide the per-process random identity prefix.
    pub(in crate::runner) fn try_new() -> Result<Self, RunnerPipelineLaunchRegistryCreateError> {
        let mut runner_instance_id = [0; RUNNER_INSTANCE_ID_LENGTH];
        getrandom::fill(&mut runner_instance_id)
            .map_err(RunnerPipelineLaunchRegistryCreateError::RandomnessUnavailable)?;
        Ok(Self {
            state: Arc::new(Mutex::new(PipelineLaunchRegistryState {
                runner_instance_id,
                next_launch_sequence: 0,
                launches: HashMap::new(),
            })),
        })
    }

    /// Registers the Bootstrap that exactly one new child may claim.
    ///
    /// The returned owner must move into the exact child-process owner. Its drop
    /// retires this registration and any diagnostic incarnation attached to it.
    #[must_use = "dropping the returned owner immediately cancels the pending launch"]
    pub(in crate::runner) fn register(
        &self,
        document_id: TenonDocumentId,
        bootstrap: crate::contracts::core::PipelineBootstrap,
    ) -> PendingPipelineLaunch {
        let mut state = self.lock();
        let launch_sequence = state.next_launch_sequence;
        state.next_launch_sequence += 1;
        let launch_id = PipelineLaunchId::new(state.runner_instance_id, launch_sequence);
        let (attachment, attached) = oneshot::channel();
        let (lifetime, _lifetime_receiver) = watch::channel(());
        let replaced = state.launches.insert(
            launch_id.clone(),
            RegisteredPipelineLaunch {
                document_id,
                connection: PipelineLaunchConnectionState::ControlPending(
                    PendingPipelineControlAttachment {
                        bootstrap,
                        attachment,
                    },
                ),
                lifetime,
                active_diagnostics: None,
            },
        );
        assert!(
            replaced.is_none(),
            "A process-local Pipeline launch identity must be unique"
        );
        PendingPipelineLaunch {
            registration: PipelineLaunchRegistration {
                launch_id,
                launches: self.clone(),
            },
            attached,
        }
    }

    /// Atomically claims the pending correctness-control attachment.
    pub(super) fn claim_control(
        &self,
        launch_id: &[u8],
    ) -> Option<PendingPipelineControlAttachment> {
        let mut state = self.lock();
        state
            .launches
            .get_mut(launch_id)?
            .connection
            .claim_control()
    }

    /// Marks a handed-off correctness-control session as fully attached.
    pub(super) fn confirm_control_attachment(&self, launch_id: &[u8]) -> Option<()> {
        let mut state = self.lock();
        let launch = state.launches.get_mut(launch_id)?;
        assert!(
            matches!(
                launch.connection,
                PipelineLaunchConnectionState::ControlAttaching
            ),
            "a handed-off control session must retain its attaching registration"
        );
        launch.connection = PipelineLaunchConnectionState::ControlAttached;
        Some(())
    }

    /// Observes an attached launch without adding a lifecycle or readiness gate.
    pub(in crate::runner) fn metrics_lifetime(
        &self,
        launch_id: &[u8],
    ) -> Option<watch::Receiver<()>> {
        let state = self.lock();
        let launch = state.launches.get(launch_id)?;
        match launch.connection {
            PipelineLaunchConnectionState::ControlAttached
            | PipelineLaunchConnectionState::DiagnosticsAttached => {
                Some(launch.lifetime.subscribe())
            }
            _ => None,
        }
    }

    /// Claims the one current diagnostics stream for an attached Pipeline.
    pub(super) fn claim_diagnostics(
        &self,
        launch_id: &[u8],
        diagnostics: &RunnerDiagnostics,
    ) -> Option<(RunnerPipelineDiagnosticsClaim, watch::Receiver<()>)> {
        let mut state = self.lock();
        let launch_id = state.launches.get_key_value(launch_id)?.0.clone();
        let launch = state.launches.get_mut(&launch_id)?;
        if !matches!(
            launch.connection,
            PipelineLaunchConnectionState::ControlAttached
        ) {
            return None;
        }
        launch.connection = PipelineLaunchConnectionState::DiagnosticsAttached;
        if launch.active_diagnostics.is_none() {
            diagnostics.activate_pipeline_instance(
                &launch.document_id,
                Arc::from(pipeline_instance_id(&launch_id)),
            );
            launch.active_diagnostics = Some(diagnostics.clone());
        }
        let lifetime = launch.lifetime.subscribe();
        let document_id = launch.document_id.clone();
        let claim = RunnerPipelineDiagnosticsClaim {
            launch_id,
            document_id,
            launches: self.clone(),
        };
        Some((claim, lifetime))
    }

    #[allow(
        clippy::expect_used,
        reason = "the short registry critical sections execute no panicking user code"
    )]
    fn lock(&self) -> MutexGuard<'_, PipelineLaunchRegistryState> {
        self.state
            .lock()
            .expect("Pipeline launch registry lock must not be poisoned")
    }

    fn remove(&self, launch_id: &PipelineLaunchId) {
        let retired = {
            let mut state = self.lock();
            state.launches.remove(launch_id)
        };
        let Some(retired) = retired else {
            return;
        };
        if let Some(diagnostics) = retired.active_diagnostics {
            diagnostics.deactivate_pipeline_instance(
                &retired.document_id,
                &pipeline_instance_id(launch_id),
            );
        }
    }
}

impl fmt::Debug for RunnerPipelineLaunchRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunnerPipelineLaunchRegistry")
            .field("launch_count", &self.lock().launches.len())
            .finish()
    }
}

struct PipelineLaunchRegistryState {
    runner_instance_id: [u8; RUNNER_INSTANCE_ID_LENGTH],
    next_launch_sequence: u64,
    launches: HashMap<PipelineLaunchId, RegisteredPipelineLaunch>,
}

struct RegisteredPipelineLaunch {
    document_id: TenonDocumentId,
    connection: PipelineLaunchConnectionState,
    lifetime: watch::Sender<()>,
    // Presence means this exact process incarnation has been activated in the
    // diagnostic fan-out and must be retired when the registration disappears.
    active_diagnostics: Option<RunnerDiagnostics>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct PipelineLaunchId(Arc<[u8]>);

impl PipelineLaunchId {
    fn new(runner_instance_id: [u8; RUNNER_INSTANCE_ID_LENGTH], sequence: u64) -> Self {
        let mut bytes = [0; PIPELINE_LAUNCH_ID_LENGTH];
        bytes[..RUNNER_INSTANCE_ID_LENGTH].copy_from_slice(&runner_instance_id);
        bytes[RUNNER_INSTANCE_ID_LENGTH..].copy_from_slice(&sequence.to_be_bytes());
        Self(Arc::from(bytes))
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Borrow<[u8]> for PipelineLaunchId {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Debug for PipelineLaunchId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PipelineLaunchId")
            .field("length", &self.0.len())
            .finish_non_exhaustive()
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "one legal connection state avoids a second allocation and parallel state fields"
)]
enum PipelineLaunchConnectionState {
    ControlPending(PendingPipelineControlAttachment),
    ControlAttaching,
    ControlAttached,
    DiagnosticsAttached,
}

impl PipelineLaunchConnectionState {
    fn claim_control(&mut self) -> Option<PendingPipelineControlAttachment> {
        let current = std::mem::replace(self, Self::ControlAttaching);
        match current {
            Self::ControlPending(pending) => Some(pending),
            current => {
                *self = current;
                None
            }
        }
    }
}

pub(super) struct PendingPipelineControlAttachment {
    bootstrap: crate::contracts::core::PipelineBootstrap,
    attachment: oneshot::Sender<RunnerPipelineControlSession>,
}

impl PendingPipelineControlAttachment {
    /// Hands the bound session to the exact process owner and returns its
    /// Bootstrap for the response stream.
    pub(super) fn hand_off(
        self,
        session: RunnerPipelineControlSession,
    ) -> Result<crate::contracts::core::PipelineBootstrap, PipelineAttachmentError> {
        self.attachment
            .send(session)
            .map_err(|_| PipelineAttachmentError)?;
        Ok(self.bootstrap)
    }
}

/// Exclusive registration ownership for one child launch.
#[must_use = "dropping this owner cancels an unclaimed Pipeline launch"]
pub(in crate::runner) struct PendingPipelineLaunch {
    registration: PipelineLaunchRegistration,
    attached: oneshot::Receiver<RunnerPipelineControlSession>,
}

impl PendingPipelineLaunch {
    /// Returns the opaque identity passed to the exact child process.
    #[must_use]
    pub(in crate::runner) fn launch_id(&self) -> &[u8] {
        self.registration.launch_id.as_bytes()
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        PipelineLaunchRegistration,
        oneshot::Receiver<RunnerPipelineControlSession>,
    ) {
        (self.registration, self.attached)
    }
}

impl fmt::Debug for PendingPipelineLaunch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingPipelineLaunch")
            .field(
                "launch_id_len",
                &self.registration.launch_id.as_bytes().len(),
            )
            .finish_non_exhaustive()
    }
}

pub(super) struct PipelineLaunchRegistration {
    launch_id: PipelineLaunchId,
    launches: RunnerPipelineLaunchRegistry,
}

impl PipelineLaunchRegistration {
    pub(super) fn remove(&self) {
        self.launches.remove(&self.launch_id);
    }
}

impl Drop for PipelineLaunchRegistration {
    fn drop(&mut self) {
        self.remove();
    }
}

/// Exclusive lease for one live diagnostics stream of an attached Pipeline.
pub(super) struct RunnerPipelineDiagnosticsClaim {
    launch_id: PipelineLaunchId,
    // This freezes the claimed registration's routing identity because that
    // registration may retire while the diagnostics stream is winding down.
    document_id: TenonDocumentId,
    launches: RunnerPipelineLaunchRegistry,
}

impl RunnerPipelineDiagnosticsClaim {
    #[must_use]
    pub(super) const fn document_id(&self) -> &TenonDocumentId {
        &self.document_id
    }

    #[must_use]
    pub(super) fn pipeline_instance_id(&self) -> Box<str> {
        pipeline_instance_id(&self.launch_id)
    }
}

impl Drop for RunnerPipelineDiagnosticsClaim {
    fn drop(&mut self) {
        let mut state = self.launches.lock();
        let Some(launch) = state.launches.get_mut(&self.launch_id) else {
            return;
        };
        assert!(
            matches!(
                launch.connection,
                PipelineLaunchConnectionState::DiagnosticsAttached
            ),
            "a live diagnostics claim must retain its attached registration"
        );
        launch.connection = PipelineLaunchConnectionState::ControlAttached;
    }
}

impl fmt::Debug for RunnerPipelineDiagnosticsClaim {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunnerPipelineDiagnosticsClaim")
            .field("pipeline_instance_id", &self.pipeline_instance_id())
            .finish_non_exhaustive()
    }
}

/// A claimed launch could not hand its bound session to the launch owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runner) struct PipelineAttachmentError;

impl fmt::Display for PipelineAttachmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Pipeline attachment session is unavailable")
    }
}

impl Error for PipelineAttachmentError {}

/// The launch registry cannot establish its process-local identity namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runner) enum RunnerPipelineLaunchRegistryCreateError {
    /// The operating system could not provide random bytes.
    RandomnessUnavailable(getrandom::Error),
}

impl fmt::Display for RunnerPipelineLaunchRegistryCreateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RandomnessUnavailable(_) => {
                formatter.write_str("Pipeline launch identity initialization failed")
            }
        }
    }
}

impl Error for RunnerPipelineLaunchRegistryCreateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RandomnessUnavailable(source) => Some(source),
        }
    }
}

fn pipeline_instance_id(launch_id: &PipelineLaunchId) -> Box<str> {
    URL_SAFE_NO_PAD
        .encode(Sha256::digest(launch_id.as_bytes()))
        .into_boxed_str()
}

#[cfg(test)]
pub(super) mod test_support {
    use super::*;

    pub(in crate::runner) async fn wait_for_attachment(
        pending: &mut PendingPipelineLaunch,
    ) -> Result<RunnerPipelineControlSession, PipelineAttachmentError> {
        (&mut pending.attached)
            .await
            .map_err(|_| PipelineAttachmentError)
    }

    pub(in crate::runner::pipeline) fn contains(
        launches: &RunnerPipelineLaunchRegistry,
        launch_id: &[u8],
    ) -> bool {
        launches.lock().launches.contains_key(launch_id)
    }

    pub(in crate::runner::pipeline) fn is_empty(launches: &RunnerPipelineLaunchRegistry) -> bool {
        launches.lock().launches.is_empty()
    }
}
