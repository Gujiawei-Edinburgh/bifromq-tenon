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

//! One six-phase application for the complete resource difference.

use crate::time::Deadline;
use std::sync::Arc;

use super::plan::{CompiledResourceChanges, ReconfigurePlan};
use super::resource_stage::ActivePipeline;
use super::resource_stage::StagedResourceChanges;
use crate::pipeline::reconfigure::operation::{
    BlockingReconfigureJob, DataPlaneOperation, ReconfigureProgress, wait_for_shutdown,
};
use crate::pipeline::reconfigure::revision::PipelineRevision;
use crate::pipeline::reconfigure::{
    PipelineApplyOutcome, PipelineReconfigureError, PipelineReconfigurer,
};

impl PipelineReconfigurer {
    /// Applies every resource action. Stop requests must be followed by awaiting
    /// this operation; dropping it cannot transfer child or blocking-task ownership.
    pub(crate) async fn apply(
        &mut self,
        target: PipelineRevision,
        deadline: Option<Deadline>,
    ) -> Result<PipelineApplyOutcome, PipelineReconfigureError> {
        if self
            .current
            .as_mut()
            .is_some_and(ActivePipeline::has_worker_exited_now)
        {
            return Err(self.finish_worker_failure().await);
        }
        if self.shutdown_requested().is_some() {
            return Ok(PipelineApplyOutcome::Stopped);
        }
        let compiled = ReconfigurePlan::derive(
            self.current.as_ref().map(ActivePipeline::revision),
            target,
            self.environment.available_cpu_count(),
        )?
        .compile()?;
        let mut staged = match self.prepare_additions(compiled).await? {
            ReconfigureProgress::Completed(staged) => Some(staged),
            ReconfigureProgress::Stopped => return Ok(PipelineApplyOutcome::Stopped),
        };
        let channels = match self.prepare_lua_changes(&mut staged).await {
            Ok(ReconfigureProgress::Completed(channels)) => channels,
            result => {
                if let Some(staged) = staged.take() {
                    tokio::task::spawn_blocking(move || drop(staged))
                        .await
                        .map_err(PipelineReconfigureError::BlockingTask)?;
                }
                return result.map(|_| PipelineApplyOutcome::Stopped);
            }
        };
        let staged = staged.unwrap_or_else(|| std::process::abort());
        let (mut started, definitions) = match self.handoff_instances(staged, channels).await? {
            ReconfigureProgress::Completed(resources) => resources,
            ReconfigureProgress::Stopped => return Ok(PipelineApplyOutcome::Stopped),
        };

        // Every replaced old child is reaped. Launch through Publish has no Ready wait.
        started.retain_current(self.current.take());
        let activated = started.activate();
        for definition in definitions {
            match definition.activate() {
                Ok(()) => {}
                Err(source) => {
                    return Err(match activated.terminate().await {
                        Ok(()) => PipelineReconfigureError::RuntimeTransition(Box::new(source)),
                        Err(error) => PipelineReconfigureError::DataPlaneFailure(Box::new(error)),
                    });
                }
            }
        }
        match self.publish(activated, deadline) {
            Ok(status) => Ok(PipelineApplyOutcome::Applied(status)),
            Err(rejected) => Err(rejected.reject_after_worker_exit().await),
        }
    }

    /// Discards paused additions, then reaps current children and joins current workers.
    /// Candidate Queue users must be gone before current removes the shared root.
    pub(super) async fn terminate_staged_resources(
        &mut self,
        additions: &mut Option<StagedResourceChanges>,
    ) -> Result<(), PipelineReconfigureError> {
        let staged = match additions.take() {
            Some(mut staged) => Some(
                tokio::task::spawn_blocking(move || {
                    staged.runtime.abort_and_join();
                    staged.channel_routes.clear();
                    staged
                })
                .await
                .map_err(PipelineReconfigureError::BlockingTask)?,
            ),
            None => None,
        };
        let result = match self.current.take() {
            Some(current) => current
                .terminate()
                .await
                .map_err(|source| PipelineReconfigureError::DataPlaneFailure(Box::new(source))),
            None => Ok(()),
        };
        drop(staged);
        result
    }

    /// Waits without owning cleanup; losing selection branches only end borrows.
    pub(super) async fn wait_resource_work<T>(
        &mut self,
        work: &mut BlockingReconfigureJob<T>,
    ) -> ResourceWork<T> {
        loop {
            tokio::select! {
                biased;
                event = async {
                    if self.current.is_some() {
                        self.observe_runtime().await
                    } else {
                        Ok(DataPlaneOperation::Shutdown(wait_for_shutdown(&mut self.shutdown_receiver).await))
                    }
                } => match event {
                    Ok(DataPlaneOperation::Completed(event)) => match self.handle_instance_event(event) {
                        Ok(None) => {},
                        Ok(Some(_)) => return ResourceWork::Shutdown,
                        Err(error) => return ResourceWork::EventFailed(error),
                    }
                    Ok(DataPlaneOperation::WorkerExited) => return ResourceWork::WorkerExited,
                    Ok(DataPlaneOperation::Shutdown(_)) => return ResourceWork::Shutdown,
                    Err(source) => return ResourceWork::EventFailed(PipelineReconfigureError::PluginInstanceLifecycle(source)),
                },
                result = work.wait() => return ResourceWork::Completed(result),
            }
        }
    }
    async fn prepare_additions(
        &mut self,
        compiled: CompiledResourceChanges,
    ) -> Result<ReconfigureProgress<StagedResourceChanges>, PipelineReconfigureError> {
        let retained = self.current.as_ref().map(ActivePipeline::started_at);
        let mut preparation = compiled.begin_stage(
            Arc::clone(&self.environment),
            self.diagnostics.clone(),
            retained,
            self.metrics.clone(),
        );
        match self.wait_resource_work(&mut preparation).await {
            ResourceWork::WorkerExited => {
                // Reclaim candidate workers before final root cleanup.
                let _candidate_result = preparation.cancel_and_discard().await;
                return Err(self.finish_worker_failure().await);
            }
            ResourceWork::Shutdown => {
                let cleanup = preparation.cancel_and_discard().await;
                if self
                    .current
                    .as_mut()
                    .is_some_and(ActivePipeline::has_worker_exited_now)
                {
                    return Err(self.finish_worker_failure().await);
                }
                cleanup?;
                Ok(ReconfigureProgress::Stopped)
            }
            ResourceWork::EventFailed(error) => {
                let _candidate_result = preparation.cancel_and_discard().await;
                if let Some(current) = self.current.take() {
                    current.terminate().await.map_err(|source| {
                        PipelineReconfigureError::DataPlaneFailure(Box::new(source))
                    })?;
                }
                Err(error)
            }
            ResourceWork::Completed(result) => match result {
                Ok(staged) => Ok(ReconfigureProgress::Completed(staged)),
                Err(error) => {
                    // Preparation cleanup can overlap a retained worker's final exit.
                    if self
                        .current
                        .as_mut()
                        .is_some_and(ActivePipeline::has_worker_exited_now)
                    {
                        return Err(self.finish_worker_failure().await);
                    }
                    Err(error)
                }
            },
        }
    }
}

/// The selected external fact; the phase still owns its work and cleanup obligations.
pub(super) enum ResourceWork<T> {
    Completed(Result<T, PipelineReconfigureError>),
    WorkerExited,
    Shutdown,
    EventFailed(PipelineReconfigureError),
}
