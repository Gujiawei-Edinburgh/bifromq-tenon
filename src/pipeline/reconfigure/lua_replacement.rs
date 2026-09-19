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

//! Integrates existing Channel-local VM replacement into the common apply phases.
//!
//! Prepared commands own no workers or Queue cleanup rights. Stage failure aborts
//! and acknowledges them; after Cutover, any failure terminates the Pipeline.
//! A stopping Pipeline first discards new paused workers, then terminates its
//! live workers to unblock replacement waits, and finally joins the blocking job.

use super::apply::ResourceWork;
use super::plan::FlowChange;
use super::resource_stage::{ActivePipeline, StagedResourceChanges, channel_spec};
use crate::pipeline::channel::ChannelDefinitionChange;
use crate::pipeline::reconfigure::operation::{
    BlockingReconfigureJob, ReconfigureProgress, spawn_blocking_reconfigure_work,
};
use crate::pipeline::reconfigure::{PipelineReconfigureError, PipelineReconfigurer};
use crate::pipeline::runtime::PreparedFlowDefinitionReplacement;

impl PipelineReconfigurer {
    #[allow(
        clippy::expect_used,
        reason = "compiled Replace actions refer to retained resources"
    )]
    pub(super) async fn prepare_lua_changes(
        &mut self,
        additions: &mut Option<StagedResourceChanges>,
    ) -> Result<ReconfigureProgress<Vec<PreparedFlowDefinitionReplacement>>, PipelineReconfigureError>
    {
        let limits = self.environment.lua_limits();
        let mut prepared = Vec::new();
        loop {
            let staged = additions
                .as_mut()
                .expect("Stage retains candidate resources");
            let Some((flow_id, target_routes)) = staged.channel_routes.pop_first() else {
                break;
            };
            let spec =
                match staged.changes.flows[&flow_id] {
                    FlowChange::ReplaceDefinition => ChannelDefinitionChange::Replace(
                        channel_spec(&staged.changes.target, &flow_id, limits),
                    ),
                    FlowChange::UpdateRoutes => ChannelDefinitionChange::KeepLua,
                    _ => unreachable!("new and removed Flows have no in-place commands"),
                };
            let work = self
                .current
                .as_ref()
                .expect("Replace requires current")
                .begin_definition_replacement(&flow_id, spec, target_routes);
            let result = match work {
                Ok(work) => self.wait_definition_work(work, additions).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(ReconfigureProgress::Completed(definition)) => prepared.push(definition),
                result => {
                    self.discard_lua_changes(prepared).await?;
                    return result.map(|_| ReconfigureProgress::Stopped);
                }
            }
        }
        Ok(ReconfigureProgress::Completed(prepared))
    }

    pub(super) async fn discard_lua_changes(
        &mut self,
        definitions: Vec<PreparedFlowDefinitionReplacement>,
    ) -> Result<(), PipelineReconfigureError> {
        if definitions.is_empty() {
            return Ok(());
        }
        if self.current.is_none() {
            // Terminal cleanup already joined every worker, so no handshake remains.
            drop(definitions);
            return Ok(());
        }
        let work = spawn_blocking_reconfigure_work(None, move || {
            let mut outcome = Ok(());
            for definition in definitions {
                let result = definition.abort().map_err(transition_error);
                if outcome.is_ok() {
                    outcome = result;
                }
            }
            outcome
        });
        self.wait_definition_work(work, &mut None).await.map(|_| ())
    }

    async fn wait_definition_work<T: Send + 'static>(
        &mut self,
        mut work: BlockingReconfigureJob<T>,
        additions: &mut Option<StagedResourceChanges>,
    ) -> Result<ReconfigureProgress<T>, PipelineReconfigureError> {
        let event = self.wait_resource_work(&mut work).await;
        if let ResourceWork::Completed(result) = event {
            // Completion consumes the job's wait capability. In particular,
            // a newly observed worker failure must never wait this task twice.
            drop(work);
            if let Err(error) = result {
                // Protocol failure itself proves that Stage cannot restore a
                // usable Channel. Its worker exit report may still be in flight.
                if error.is_definition_control_failure()
                    || self
                        .current
                        .as_mut()
                        .is_some_and(ActivePipeline::has_worker_exited_now)
                {
                    return Err(self.terminate_definition_failure(additions, error).await);
                }
                return Err(error);
            }
            return result.map(ReconfigureProgress::Completed);
        }
        // No candidate thread may retain a Queue when terminal cleanup deletes the root.
        let cleanup = self.terminate_staged_resources(additions).await;
        // Stop has reached the live Channels, including Lua execution and Queue waits.
        // Their protocol closure is expected; actual worker failures come from cleanup.
        let joined = work.cancel_and_discard().await;
        cleanup?;
        match event {
            ResourceWork::Shutdown => {
                if let Err(error) = joined
                    && !error.is_stopped_definition()
                {
                    return Err(error);
                }
                Ok(ReconfigureProgress::Stopped)
            }
            ResourceWork::EventFailed(error) => Err(error),
            ResourceWork::WorkerExited => Err(PipelineReconfigureError::InternalInvariantViolation),
            ResourceWork::Completed(_) => unreachable!("completed work returned before cleanup"),
        }
    }

    async fn terminate_definition_failure(
        &mut self,
        additions: &mut Option<StagedResourceChanges>,
        error: PipelineReconfigureError,
    ) -> PipelineReconfigureError {
        self.terminate_staged_resources(additions)
            .await
            .err()
            .unwrap_or(error)
    }
}

fn transition_error(
    source: crate::pipeline::runtime::PipelineRuntimeError,
) -> PipelineReconfigureError {
    PipelineReconfigureError::RuntimeTransition(Box::new(source))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identifiers::FlowId;
    use crate::lua::LuaVmErrorKind;
    use crate::pipeline::channel::FlowChannelCommandControlError;
    use crate::pipeline::runtime::PipelineRuntimeError;

    #[test]
    fn disconnected_replacement_is_terminal_without_waiting_for_an_exit_report()
    -> Result<(), Box<dyn std::error::Error>> {
        // Classification depends on the protocol result, not on a second,
        // separately scheduled worker-exit notification being available.
        for source in [
            PipelineRuntimeError::InternalEventChannelClosed,
            PipelineRuntimeError::FlowChannelCommandControl {
                flow_id: FlowId::try_from(String::from("telemetry"))?,
                channel_index: 0,
                source: FlowChannelCommandControlError::WorkerDisconnected,
            },
        ] {
            assert!(transition_error(source).is_definition_control_failure());
        }
        Ok(())
    }

    #[test]
    fn acknowledged_candidate_lua_failure_is_not_a_control_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(
            !transition_error(PipelineRuntimeError::FlowChannelReplacementPreparation {
                flow_id: FlowId::try_from(String::from("telemetry"))?,
                channel_index: 0,
                kind: LuaVmErrorKind::TopLevelFailed,
            })
            .is_definition_control_failure()
        );
        Ok(())
    }
}
