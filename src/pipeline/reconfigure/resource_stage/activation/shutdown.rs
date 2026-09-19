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

//! Drains one terminal Pipeline without starting or retrying any Plugin Instance.
//!
//! The current owner stays intact across every wait. Healthy sessions are frozen
//! before quiesce; only those Sources can finish their old Queue responsibility.
//! Sink service remains live until Channels stop and every accepted output is
//! released. Any failure or force request abandons drain for terminal cleanup.

use super::ActivePipeline;
use super::abort_with_unreaped_plugins;
use crate::pipeline::plugin::HandoffReadiness;
use crate::pipeline::reconfigure::{
    PipelineReconfigureError, PipelineReconfigurer, ReconfigureShutdown,
};
use crate::pipeline::runtime::PipelineDrainObservation;

impl PipelineReconfigurer {
    pub(in crate::pipeline) async fn shutdown(
        &mut self,
        shutdown: ReconfigureShutdown,
    ) -> Result<(), PipelineReconfigureError> {
        let Some(mut current) = self.current.take() else {
            return Ok(());
        };
        if shutdown == ReconfigureShutdown::Force {
            return current
                .terminate()
                .await
                .map_err(|source| PipelineReconfigureError::DataPlaneFailure(Box::new(source)));
        }
        let result = tokio::select! {
            biased;
            force = self.shutdown_receiver.wait_for(|request| *request == Some(ReconfigureShutdown::Force)) => {
                if force.is_err() { std::process::abort(); }
                Ok(())
            },
            result = current.planned_shutdown() => result,
        };
        // Also safe after successful reap: there are no remaining owners to stop.
        if let Err(error) = current.force_stop().await {
            abort_with_unreaped_plugins(&error);
        }
        current
            .finish_cleanup()
            .await
            .map_err(|source| PipelineReconfigureError::DataPlaneFailure(Box::new(source)))?;
        result
    }
}

impl ActivePipeline {
    async fn planned_shutdown(&mut self) -> Result<(), PipelineReconfigureError> {
        let selected = self
            .target
            .document()
            .plugin_instances()
            .keys()
            .cloned()
            .collect();
        let healthy = self
            .plugins
            .instances
            .handoff_readiness(&selected)
            .await
            .map_err(PipelineReconfigureError::PluginInstanceLifecycle)?
            .into_iter()
            .filter_map(|(id, readiness)| (readiness == HandoffReadiness::Ready).then_some(id))
            .collect::<std::collections::BTreeSet<_>>();
        tokio::select! {
            biased;
            () = self.runtime.wait_for_worker_exit() => return Err(PipelineReconfigureError::DataPlaneStopped),
            result = self.plugins.instances.quiesce_sources() => result.map_err(PipelineReconfigureError::PluginInstanceLifecycle)?,
        }
        // Publish every finish before awaiting any one Flow; no dead Source is asked
        // to read a Completion, and no acknowledgment or Egress release is fabricated.
        let mut finishes = self
            .target
            .document()
            .flows()
            .iter()
            .filter(|(_, flow)| healthy.contains(flow.source()))
            .map(|(id, _)| self.runtime.begin_source_session_finish(id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| PipelineReconfigureError::RuntimeTransition(Box::new(source)))?;
        tokio::select! {
            biased;
            () = self.runtime.wait_for_worker_exit() => return Err(PipelineReconfigureError::DataPlaneStopped),
            error = self.plugins.instances.wait_for_handoff_failure(&healthy) => return Err(PipelineReconfigureError::PluginInstanceLifecycle(error)),
            result = async {
                for finish in &mut finishes { finish.wait().await?; }
                Ok::<_, crate::pipeline::runtime::PipelineRuntimeError>(())
            } => result.map_err(|source| PipelineReconfigureError::RuntimeTransition(Box::new(source)))?,
        }
        tokio::select! {
            biased;
            error = self.plugins.instances.wait_for_handoff_failure(&healthy) => return Err(PipelineReconfigureError::PluginInstanceLifecycle(error)),
            result = self.runtime.stop_channels_and_drain_egress() => match result.map_err(|source| PipelineReconfigureError::RuntimeTransition(Box::new(source)))? {
                PipelineDrainObservation::Drained => {},
                PipelineDrainObservation::WorkerExited => return Err(PipelineReconfigureError::DataPlaneStopped),
            },
        }
        self.plugins
            .instances
            .shutdown_all()
            .await
            .map_err(PipelineReconfigureError::PluginInstanceLifecycle)
    }
}
