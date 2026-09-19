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

//! Adopts retained resources, activates the new batch, then publishes the full revision.
//!
//! Ready is only a child lifecycle fact. Activation moves the existing owners,
//! releases their common startup decision, and never waits for a child event.
//! Publication checks actual worker exits before the synchronous ownership handoff.
//! A rejected publication returns the entire owner for terminal cleanup.
//! Forced child cleanup borrows that owner; finalization then stops and joins
//! workers, retaining their exact failure before deleting Queue files. Cancellation
//! during finalization uses the runtime's synchronous Drop fallback. Planned
//! shutdown first quiesces Sources and drains actual Queue responsibility.

use super::cutover::{OwnedInstancePlugins, StartedResourceChanges};
use super::{ResourceDirectories, StagedResourceChanges};
use crate::contracts::core::PipelineStatusSnapshot;
use crate::error::ErrorChain;
use crate::identifiers::FlowId;
use crate::pipeline::channel::{ChannelDefinitionChange, PreparedEgressRoutes};
use crate::pipeline::plugin::PluginInstanceError;
use crate::pipeline::reconfigure::PipelineReconfigureError;
use crate::pipeline::reconfigure::PipelineReconfigurer;
use crate::pipeline::reconfigure::operation::{
    BlockingReconfigureJob, spawn_blocking_reconfigure_work,
};
use crate::pipeline::reconfigure::revision::PipelineRevision;
use crate::pipeline::runtime::PreparedFlowDefinitionReplacement;
use crate::pipeline::runtime::{PipelineRuntime, PipelineRuntimeError};
use crate::pipeline::terminate_pipeline;
use crate::time::Deadline;
use std::error::Error;
use std::time::Instant;

mod instance_handoff;
mod runtime_status;
mod shutdown;
mod source_recovery;

impl StartedResourceChanges {
    /// Transfers the prior resource owners unchanged before any candidate is released.
    pub(in crate::pipeline::reconfigure) fn retain_current(
        &mut self,
        current: Option<ActivePipeline>,
    ) {
        if let Some(ActivePipeline {
            mut plugins,
            runtime,
            target: _,
            working_directory,
        }) = current
        {
            self.plugins.instances.append(&mut plugins.instances);
            self.staged.runtime.retain(runtime);
            self.staged.working_directory.retain(working_directory);
        }
    }

    /// Releases the one prepared data plane without polling or replacing any child.
    pub(in crate::pipeline::reconfigure) fn activate(self) -> ActivePipeline {
        let Self { plugins, staged } = self;
        let StagedResourceChanges {
            runtime,
            changes,
            channel_routes,
            working_directory,
        } = staged;
        assert!(
            channel_routes.is_empty(),
            "all in-place routes were handed to their Channels"
        );
        ActivePipeline {
            plugins,
            runtime: runtime.activate(),
            target: changes.target,
            working_directory,
        }
    }
}

/// Complete activated resources, whether awaiting Publish or held as current.
pub(in crate::pipeline::reconfigure) struct ActivePipeline {
    // NOTE: the child guard runs first; runtime Drop then joins workers before
    // the target and final directory owner can be released.
    plugins: OwnedInstancePlugins,
    pub(in crate::pipeline::reconfigure) runtime: PipelineRuntime,
    target: PipelineRevision,
    working_directory: ResourceDirectories,
}

impl ActivePipeline {
    /// Transfers candidate Queues before scheduling the reply-only blocking wait.
    pub(in crate::pipeline::reconfigure) fn begin_definition_replacement(
        &self,
        flow_id: &FlowId,
        definition: ChannelDefinitionChange,
        routes: Vec<PreparedEgressRoutes>,
    ) -> Result<BlockingReconfigureJob<PreparedFlowDefinitionReplacement>, PipelineReconfigureError>
    {
        let replacement = self
            .runtime
            .flow_definition_replacement(flow_id)
            .map_err(|source| PipelineReconfigureError::RuntimeTransition(Box::new(source)))?;
        let pending = replacement.begin(definition, routes);
        Ok(spawn_blocking_reconfigure_work(None, move || {
            pending
                .and_then(|pending| pending.wait())
                .map_err(|source| PipelineReconfigureError::RuntimeTransition(Box::new(source)))
        }))
    }

    /// Borrows the sole applied revision for subsequent resource derivation.
    pub(in crate::pipeline::reconfigure) fn revision(&self) -> &PipelineRevision {
        &self.target
    }

    pub(in crate::pipeline::reconfigure) fn started_at(&self) -> Instant {
        self.runtime.started_at()
    }

    pub(in crate::pipeline::reconfigure) fn has_worker_exited_now(&mut self) -> bool {
        self.runtime.has_worker_exited_now()
    }

    /// Preserves the observed worker failure after completing terminal cleanup.
    pub(in crate::pipeline::reconfigure) async fn reject_after_worker_exit(
        self,
    ) -> PipelineReconfigureError {
        match self.terminate().await {
            Ok(()) => PipelineReconfigureError::DataPlaneStopped,
            Err(source) => PipelineReconfigureError::DataPlaneFailure(Box::new(source)),
        }
    }

    /// Reaps children and joins workers before releasing the final directory owner.
    pub(in crate::pipeline::reconfigure) async fn terminate(
        mut self,
    ) -> Result<(), PipelineRuntimeError> {
        if let Err(error) = self.force_stop().await {
            abort_with_unreaped_plugins(&error);
        }
        self.finish_cleanup().await
    }

    /// Projects one complete status from the original revision and actual child owners.
    fn status_snapshot(&self) -> PipelineStatusSnapshot {
        PipelineStatusSnapshot {
            document_etag: self.target.document_etag().to_owned(),
            plugin_instances: self.plugins.instances.statuses(),
        }
    }

    /// Reaps all children before the caller consumes this owner with `finish_cleanup`.
    /// Once called, only terminal cleanup may continue, including after cancellation.
    async fn force_stop(&mut self) -> Result<(), PluginInstanceError> {
        self.plugins.instances.force_remove_all().await
    }

    /// Stops and joins workers, returning their original failure after resource release.
    /// Requires successful child cleanup; the existing child guard enforces that boundary.
    /// Canceling this final wait still joins workers before releasing the directory.
    async fn finish_cleanup(self) -> Result<(), PipelineRuntimeError> {
        let Self {
            plugins,
            runtime,
            target,
            working_directory,
        } = self;
        drop(plugins);
        let result = runtime.stop_and_join().await;
        drop((target, working_directory));
        result
    }
}

impl PipelineReconfigurer {
    /// Publishes only after Activate; an observed core exit returns all resources.
    /// The caller must terminate the rejected candidate, not retry or publish it.
    pub(in crate::pipeline::reconfigure) fn publish(
        &mut self,
        mut activated: ActivePipeline,
        deadline: Option<Deadline>,
    ) -> Result<PipelineStatusSnapshot, Box<ActivePipeline>> {
        if activated.runtime.has_worker_exited_now() {
            return Err(Box::new(activated));
        }
        if deadline.is_some_and(Deadline::has_elapsed) {
            terminate_pipeline();
        }
        Ok(self.install_current(activated).status_snapshot())
    }
}

/// Leaves live-child resources intact for Runner's terminal process-group cleanup.
fn abort_with_unreaped_plugins(error: &(dyn Error + 'static)) -> ! {
    eprintln!("pipeline.plugin_cleanup_failed: {}", ErrorChain(error));
    std::process::abort()
}

#[cfg(all(test, not(feature = "loom-model")))]
pub(super) mod tests;
