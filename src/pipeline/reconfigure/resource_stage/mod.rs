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

//! Prepares each changed resource while retained runtime owners keep running.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use super::plan::CompiledResourceChanges;
use crate::contracts::core::PipelineEnvironment;
use crate::identifiers::FlowId;
use crate::pipeline::channel::PreparedEgressRoutes;
use crate::pipeline::channel::metrics::FlowMetrics;
use crate::pipeline::diagnostics::PipelineDiagnosticsPublisher;
use crate::pipeline::runtime::{PreparedPipelineRuntime, StartupControl};

use super::error::PipelineReconfigureError;
use super::operation::{BlockingReconfigureJob, spawn_blocking_reconfigure_work};

mod activation;
mod cutover;
mod data_plane;
mod directories;
mod instance_launch;
mod queue_layout;

pub(in crate::pipeline::reconfigure) use activation::ActivePipeline;
pub(super) use data_plane::channel_spec;
use directories::ResourceDirectories;

impl CompiledResourceChanges {
    /// Prepares off the control thread, sharing its startup decision with cancellation.
    /// The caller must either wait for the candidate or cancel and await its disposal.
    pub(super) fn begin_stage(
        self,
        environment: Arc<PipelineEnvironment>,
        diagnostics: PipelineDiagnosticsPublisher,
        retained: Option<Instant>,
        metrics: Option<FlowMetrics>,
    ) -> BlockingReconfigureJob<StagedResourceChanges> {
        let startup = Arc::new(StartupControl::new());
        let cancellation = Arc::clone(&startup);
        spawn_blocking_reconfigure_work(Some(Box::new(move || cancellation.abort())), move || {
            self.stage(&environment, diagnostics, startup, retained, metrics)
        })
    }

    /// Prepares this batch behind one startup decision without mutating retained owners.
    fn stage(
        self,
        environment: &PipelineEnvironment,
        diagnostics: PipelineDiagnosticsPublisher,
        startup: Arc<StartupControl>,
        retained: Option<Instant>,
        metrics: Option<FlowMetrics>,
    ) -> Result<StagedResourceChanges, PipelineReconfigureError> {
        let mut working_directory = match &retained {
            Some(_) => ResourceDirectories::retain_root(environment.pipeline_working_directory()),
            None => ResourceDirectories::create_root(environment.pipeline_working_directory())?,
        };
        match data_plane::prepare(
            &self,
            environment,
            diagnostics,
            &mut working_directory,
            startup,
            retained,
            metrics.as_ref(),
        ) {
            Ok((runtime, channel_routes)) => Ok(StagedResourceChanges {
                runtime,
                channel_routes,
                changes: self,
                working_directory,
            }),
            Err(error) => {
                working_directory.remove()?;
                Err(error)
            }
        }
    }
}

/// Only this operation's new workers, launch work and directory cleanup authority.
#[must_use = "staged resources must be advanced or abandoned"]
pub(super) struct StagedResourceChanges {
    // NOTE: field order is a lifecycle contract. Workers are aborted and
    // joined before their Queue files and directories are removed.
    pub(super) runtime: PreparedPipelineRuntime,
    pub(super) changes: CompiledResourceChanges,
    pub(super) channel_routes: BTreeMap<FlowId, Vec<PreparedEgressRoutes>>,
    working_directory: ResourceDirectories,
}

#[cfg(test)]
mod tests;

#[cfg(all(test, not(feature = "loom-model")))]
pub(super) mod test_support {
    pub(in crate::pipeline::reconfigure) use super::activation::tests::with_environment;
    pub(in crate::pipeline) use super::cutover::tests::revision;
}
