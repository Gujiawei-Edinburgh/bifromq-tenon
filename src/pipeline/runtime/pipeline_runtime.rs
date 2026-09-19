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

//! Owns Channel worker lifetimes and finite Queue drain for one Pipeline.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::poll_fn;
use std::task::Poll;
use std::time::Instant;

use super::error::PipelineRuntimeError;
use super::flow_definition_replacement::FlowDefinitionReplacement;
use super::flow_runtime::FlowRuntime;
use super::retirement::PipelineDrainObservation;
use super::source_session_finish::SourceSessionFinish;
use super::worker::{abort_process_if_stop_failed, finish_worker_sets, poll_planned_completion};
use crate::identifiers::{FlowId, PluginInstanceId};

/// The sole owner of every in-process worker in one Pipeline runtime.
pub(crate) struct PipelineRuntime {
    pub(super) flows: BTreeMap<FlowId, FlowRuntime>,
    pub(super) pipeline_started_at: Instant,
}

impl PipelineRuntime {
    /// Waits until one worker exits or the private exit channel breaks.
    ///
    /// This future only waits on a Tokio MPSC receive, so dropping it from a
    /// losing `select!` branch does not consume an exit. The first completed
    /// observation is retained inside the runtime for [`Self::stop_and_join`].
    /// Calling the method again after an observation returns immediately.
    pub(crate) async fn wait_for_worker_exit(&mut self) {
        poll_fn(|context| self.poll_next_worker_exit(context)).await;
    }

    /// Reports whether any worker exit is observable at this instant.
    ///
    /// Reconfiguration calls this immediately before publishing a target
    /// revision. The check retains the observed event so the normal cleanup
    /// path can still report the exact worker failure.
    pub(crate) fn has_worker_exited_now(&mut self) -> bool {
        for flow in self.flows.values_mut() {
            flow.observe_exit_now();
        }
        self.flows.values().any(FlowRuntime::has_observed_event)
    }

    /// Returns a coordinator that stages one Flow's Channel definition in place.
    pub(crate) fn flow_definition_replacement(
        &self,
        flow_id: &FlowId,
    ) -> Result<FlowDefinitionReplacement, PipelineRuntimeError> {
        let flow = flow_entry(&self.flows, flow_id);
        Ok(FlowDefinitionReplacement::new(
            flow_id.clone(),
            flow.command_controls()?,
        ))
    }

    /// Finishes one quiesced healthy Source session on the existing Channels.
    ///
    /// The caller must have observed SourceQuiesced and keep the old Completion
    /// reader alive, without starting a new writer on these Queues. It must
    /// observe Plugin and core-worker failures alongside the returned wait.
    /// A partial send failure or permanent abandonment requires stopping this
    /// runtime; already resolved Source responsibility cannot be rolled back.
    pub(crate) fn begin_source_session_finish(
        &self,
        flow_id: &FlowId,
    ) -> Result<SourceSessionFinish, PipelineRuntimeError> {
        SourceSessionFinish::begin(
            flow_id,
            &flow_entry(&self.flows, flow_id).command_controls()?,
        )
    }

    /// Stops Source/Lua/Completion work, then drains committed Egress only.
    /// Failed Sources need not have a live Completion reader for this shutdown.
    pub(crate) async fn stop_channels_and_drain_egress(
        &mut self,
    ) -> Result<PipelineDrainObservation, PipelineRuntimeError> {
        for flow in self.flows.values() {
            abort_process_if_stop_failed(flow.drain_egress());
        }
        Ok(poll_fn(|context| {
            poll_planned_completion(
                self.flows.values_mut().map(FlowRuntime::worker_set_mut),
                context,
            )
        })
        .await)
    }

    /// Records Sink Instances that left the Pipeline in every Channel that still
    /// waits on them.
    ///
    /// A departed Instance can never release an Egress record a Channel already
    /// committed to it, so this wake is the only event that can end that wait.
    /// A failed wake therefore ends the process, exactly like a failed stop wake.
    pub(crate) fn depart_egress_targets(&self, instances: &BTreeSet<PluginInstanceId>) {
        for flow in self.flows.values() {
            abort_process_if_stop_failed(flow.depart_egress_targets(instances));
        }
    }

    /// Wakes every Channel after a process failure without recording a
    /// permanent departure. A failed Instance may be restarted with the same
    /// identity and still owes releases accepted before it exited.
    pub(crate) fn force_wake(&self) {
        for flow in self.flows.values() {
            abort_process_if_stop_failed(flow.force_wake());
        }
    }

    pub(super) fn poll_next_worker_exit(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> Poll<()> {
        if self.flows.values().any(FlowRuntime::has_observed_event) {
            return Poll::Ready(());
        }
        for flow in self.flows.values_mut() {
            if flow.poll_next_exit(context).is_ready() {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    }

    /// Publishes Stop before dropping route-update guards that wake blocked Channels.
    pub(crate) async fn stop_and_join(mut self) -> Result<(), PipelineRuntimeError> {
        if self.flows.is_empty() {
            return Ok(());
        }
        let mut worker_sets = Vec::new();
        // One completion pass sees every terminal event and can preserve the
        // original failure across the consequence exits caused by shutdown.
        worker_sets
            .try_reserve_exact(self.flows.len())
            .map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
        for flow in self.flows.values_mut() {
            worker_sets.push(flow.begin_final_stop());
        }
        finish_worker_sets(worker_sets).await
    }
}

impl fmt::Debug for PipelineRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PipelineRuntime")
            .field(
                "worker_count",
                &(self
                    .flows
                    .values()
                    .map(FlowRuntime::worker_count)
                    .sum::<usize>()),
            )
            .finish_non_exhaustive()
    }
}

fn flow_entry<'a, T>(flows: &'a BTreeMap<FlowId, T>, flow_id: &FlowId) -> &'a T {
    flows.get(flow_id).unwrap_or_else(|| std::process::abort())
}
