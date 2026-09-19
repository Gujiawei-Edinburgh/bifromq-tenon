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

//! Observes current resources without transferring terminal cleanup ownership.
//!
//! An event wait only borrows current. Plugin progress and retry deadlines stay
//! in the existing Instance owners. After selection, retry and status projection
//! follow selection; Source recovery owns asynchronous worker replacement.
//! The caller handles worker exit or shutdown outside its
//! cancelable event selection; a lifecycle error is fatal, never a status update.

use super::super::instance_launch::build_instance_launch;
use crate::contracts::core::PipelineStatusSnapshot;
use crate::pipeline::diagnostics::PipelineDiagnosticsPublisher;
use crate::pipeline::plugin::PluginInstances;
use crate::pipeline::plugin::{PluginControlLauncher, PluginInstanceError, PluginInstanceEvent};
use crate::pipeline::reconfigure::operation::{DataPlaneOperation, select_data_plane_operation};
use crate::pipeline::reconfigure::revision::PipelineRevision;
use crate::pipeline::reconfigure::{
    PipelineReconfigureError, PipelineReconfigurer, ReconfigureShutdown, RuntimeObservation,
};
use std::path::Path;
use tokio::sync::watch;

impl PipelineReconfigurer {
    /// Returns a complete status, observed worker exit, or requested shutdown.
    /// Canceling the wait retains current and all Instance progress. An error
    /// retains current too, but requires fatal Pipeline termination, not retry.
    #[allow(
        clippy::expect_used,
        reason = "the Controller observes runtime events only after successful publication"
    )]
    pub(crate) async fn observe_runtime(&mut self) -> RuntimeObservation {
        let current = self
            .current
            .as_mut()
            .expect("Runtime observation requires current");
        match select_data_plane_operation(
            current.runtime.wait_for_worker_exit(),
            current.plugins.instances.next_event(),
            &mut self.shutdown_receiver,
        )
        .await
        {
            DataPlaneOperation::Completed(event) => event.map(DataPlaneOperation::Completed),
            DataPlaneOperation::WorkerExited => Ok(DataPlaneOperation::WorkerExited),
            DataPlaneOperation::Shutdown(shutdown) => Ok(DataPlaneOperation::Shutdown(shutdown)),
        }
    }

    pub(in crate::pipeline) async fn handle_runtime_observation(
        &mut self,
        observation: RuntimeObservation,
    ) -> Result<PipelineStatusSnapshot, PipelineReconfigureError> {
        match observation {
            Ok(DataPlaneOperation::Completed(event)) => self.handle_runtime_event(event).await,
            Ok(DataPlaneOperation::WorkerExited) => Err(self.finish_worker_failure().await),
            Ok(DataPlaneOperation::Shutdown(_)) => {
                unreachable!("The Controller owns the only shutdown request")
            }
            Err(source) => Err(self.finish_instance_failure(source).await),
        }
    }

    /// Owns selected recovery work; observation never starts this operation.
    #[expect(
        clippy::expect_used,
        reason = "a selected event borrows the published runtime until recovery or terminal cleanup"
    )]
    pub(crate) async fn handle_runtime_event(
        &mut self,
        event: PluginInstanceEvent,
    ) -> Result<PipelineStatusSnapshot, PipelineReconfigureError> {
        if matches!(&event, PluginInstanceEvent::ProcessFailed(_)) {
            self.current
                .as_ref()
                .expect("Process failure requires current")
                .runtime
                .force_wake();
        }
        if let PluginInstanceEvent::ProcessFailed(id) = &event
            && self
                .current
                .as_ref()
                .expect("Event requires current")
                .target
                .document()
                .flows()
                .values()
                .any(|flow| flow.source() == id)
        {
            self.rebuild_source_flows(id.clone()).await?;
        } else {
            let current = self.current.as_mut().expect("Event requires current");
            handle_plugin_event(
                event,
                &mut current.plugins.instances,
                &current.target,
                current.working_directory.path(),
                &self.control,
                &self.diagnostics,
                &self.shutdown_receiver,
                self.environment.available_cpu_count(),
            )
            .map_err(PipelineReconfigureError::PluginInstanceLifecycle)?;
        }
        Ok(self
            .current
            .as_ref()
            .expect("Event handling retains current")
            .status_snapshot())
    }

    /// Handles a retained child using the sole applied revision, without projecting status.
    /// Returns a stop request that prevents retry. Retirement also uses this path
    /// while selected owners are not publishable.
    #[allow(
        clippy::expect_used,
        reason = "Instance events borrow the applied runtime"
    )]
    pub(in crate::pipeline::reconfigure) fn handle_instance_event(
        &mut self,
        event: PluginInstanceEvent,
    ) -> Result<Option<ReconfigureShutdown>, PipelineReconfigureError> {
        if matches!(&event, PluginInstanceEvent::ProcessFailed(_)) {
            self.current
                .as_ref()
                .expect("Process failure requires current")
                .runtime
                .force_wake();
        }
        let current = self
            .current
            .as_mut()
            .expect("Instance events require current");
        handle_reconfigure_plugin_event(
            event,
            &mut current.plugins.instances,
            &current.target,
            current.working_directory.path(),
            &self.control,
            &self.diagnostics,
            &self.shutdown_receiver,
            self.environment.available_cpu_count(),
        )
    }

    /// Completes terminal cleanup after WorkerExited was selected. The caller
    /// must await this operation, independently of further shutdown requests.
    #[allow(
        clippy::expect_used,
        reason = "an observed worker exit retains the published owner until terminal cleanup"
    )]
    pub(crate) async fn finish_worker_failure(&mut self) -> PipelineReconfigureError {
        self.current
            .take()
            .expect("Worker failure cleanup requires the observed current")
            .reject_after_worker_exit()
            .await
    }

    /// Terminates current after an OS/ownership lifecycle failure, retaining its identity.
    #[allow(
        clippy::expect_used,
        reason = "Instance events borrow a published current until terminal cleanup"
    )]
    pub(in crate::pipeline::reconfigure) async fn finish_instance_failure(
        &mut self,
        source: PluginInstanceError,
    ) -> PipelineReconfigureError {
        let current = self
            .current
            .take()
            .expect("Instance failure requires the observed current");
        match current.terminate().await {
            Ok(()) => PipelineReconfigureError::PluginInstanceLifecycle(source),
            Err(error) => PipelineReconfigureError::DataPlaneFailure(Box::new(error)),
        }
    }
}

/// Current and candidate owners use the same retry path with their own revision material.
#[allow(clippy::too_many_arguments)]
pub(super) fn handle_plugin_event(
    event: PluginInstanceEvent,
    instances: &mut PluginInstances,
    target: &PipelineRevision,
    root: &Path,
    control: &PluginControlLauncher,
    diagnostics: &PipelineDiagnosticsPublisher,
    shutdown: &watch::Receiver<Option<ReconfigureShutdown>>,
    available_cpu_count: std::num::NonZeroUsize,
) -> Result<Option<ReconfigureShutdown>, PluginInstanceError> {
    if let PluginInstanceEvent::RestartDue(id) = event {
        // This watch guard linearizes synchronous retry against request().
        // It is never held across an await.
        let shutdown = shutdown.borrow();
        if let Some(shutdown) = *shutdown {
            return Ok(Some(shutdown));
        }
        instances.restart_controlled(
            &id,
            build_instance_launch(target, root, &id, control, available_cpu_count),
            diagnostics.instance_plugin(id.clone()),
        )?;
    }
    Ok(None)
}

/// Source failure during configuration changes invalidates the complete transition.
#[allow(clippy::too_many_arguments)]
pub(super) fn handle_reconfigure_plugin_event(
    event: PluginInstanceEvent,
    instances: &mut PluginInstances,
    target: &PipelineRevision,
    root: &Path,
    control: &PluginControlLauncher,
    diagnostics: &PipelineDiagnosticsPublisher,
    shutdown: &watch::Receiver<Option<ReconfigureShutdown>>,
    available_cpu_count: std::num::NonZeroUsize,
) -> Result<Option<ReconfigureShutdown>, PipelineReconfigureError> {
    if let PluginInstanceEvent::ProcessFailed(id) = &event
        && target
            .document()
            .flows()
            .values()
            .any(|flow| flow.source() == id)
    {
        return Err(PipelineReconfigureError::SourceFailedDuringReconfigure(
            id.clone(),
        ));
    }
    handle_plugin_event(
        event,
        instances,
        target,
        root,
        control,
        diagnostics,
        shutdown,
        available_cpu_count,
    )
    .map_err(PipelineReconfigureError::PluginInstanceLifecycle)
}
