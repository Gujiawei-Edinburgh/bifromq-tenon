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

//! Finite resource retirement with every worker retained in the live runtime.
//!
//! The caller stops the corresponding producers before requesting retirement.
//! Selected clean exits are expected; any other exit still fails the Pipeline.
//! Canceling observation or join only ends a borrow. Terminal cleanup therefore
//! still reaches all workers and wakes shared Egress before joining Channels.

use std::collections::BTreeSet;
use std::task::{Context, Poll};

use super::worker::{
    WorkerCompletionContext, abort_process_if_stop_failed, finish_worker_sets,
    poll_planned_completion,
};
use super::{PipelineDrainObservation, PipelineRuntime, PipelineRuntimeError};
use crate::identifiers::FlowId;

impl PipelineRuntime {
    /// Discards selected Flow execution after its Source process has been reaped.
    /// Existing Egress bytes remain owned by their Queues and Sink readers.
    pub(crate) fn stop_flows(&self, selected: &BTreeSet<FlowId>) {
        for id in selected {
            abort_process_if_stop_failed(self.flows[id].stop());
        }
    }

    /// Signals every selected Flow before the caller begins observing completion.
    pub(crate) fn request_flow_retirement(&self, selected: &BTreeSet<FlowId>) {
        for id in selected {
            abort_process_if_stop_failed(self.flows[id].drain());
        }
    }

    /// Observes expected retirement and all retained worker failures in one pass.
    pub(crate) fn poll_resource_retirement(
        &mut self,
        flows: &BTreeSet<FlowId>,
        context: &mut Context<'_>,
    ) -> Poll<PipelineDrainObservation> {
        for (id, flow) in &mut self.flows {
            if !flows.contains(id)
                && (flow.has_observed_event() || flow.poll_next_exit(context).is_ready())
            {
                return Poll::Ready(PipelineDrainObservation::WorkerExited);
            }
        }
        poll_planned_completion(
            self.flows
                .iter_mut()
                .filter_map(|(id, flow)| flows.contains(id).then(|| flow.worker_set_mut())),
            context,
        )
    }

    /// Joins the drained batch before removing any owner from the live collections.
    pub(crate) async fn finish_resource_retirement(
        &mut self,
        flows: &BTreeSet<FlowId>,
    ) -> Result<(), PipelineRuntimeError> {
        let mut workers = Vec::new();
        workers
            .try_reserve_exact(flows.len())
            .map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
        workers.extend(
            self.flows
                .iter_mut()
                .filter(|(id, _)| flows.contains(*id))
                .map(|(_, flow)| (WorkerCompletionContext::PlannedStop, flow.worker_set_mut())),
        );
        finish_worker_sets(workers).await?;
        self.flows.retain(|id, _| !flows.contains(id));
        Ok(())
    }
}
