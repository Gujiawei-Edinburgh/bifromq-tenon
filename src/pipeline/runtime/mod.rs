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

//! Pipeline-level ownership assembled from Flow-local Channels and shared Egress workers.

mod error;
mod flow_definition_replacement;
mod flow_runtime;
mod pipeline_runtime;
mod prepared_pipeline_runtime;
mod resource_preparation;
mod resource_retirement;
mod retirement;
mod runtime_spec;
mod source_session_finish;
mod startup;
mod worker;

pub(crate) use error::PipelineRuntimeError;
pub(crate) use flow_definition_replacement::{
    PausedFlowDefinitionReplacement, PreparedFlowDefinitionReplacement,
};
pub(crate) use pipeline_runtime::PipelineRuntime;
pub(crate) use prepared_pipeline_runtime::PreparedPipelineRuntime;
pub(crate) use retirement::PipelineDrainObservation;
pub(crate) use runtime_spec::{ChannelRuntimeSpec, FlowRuntimeSpec};
pub(crate) use startup::StartupControl;

#[cfg(test)]
pub(in crate::pipeline) mod test_support {
    pub(in crate::pipeline) use super::prepared_pipeline_runtime::test_support::{
        data_plane_counts, is_pending,
    };
    #[cfg(not(feature = "loom-model"))]
    pub(in crate::pipeline) use super::worker::test_support::{HeldWorkerExit, hold_worker_exit};
}
#[cfg(all(test, feature = "loom-model"))]
mod loom_tests;
#[cfg(all(test, not(feature = "loom-model")))]
mod tests;
