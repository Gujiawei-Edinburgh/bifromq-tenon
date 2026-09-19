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

//! Prepares only new Channel workers; retained Queue writers wait for cutover.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use super::flow_runtime::FlowRuntime;
use super::worker::WorkerTask;
use super::{
    FlowRuntimeSpec, PipelineRuntime, PipelineRuntimeError, PreparedPipelineRuntime, StartupControl,
};
use crate::identifiers::FlowId;
use crate::pipeline::diagnostics::PipelineDiagnosticsPublisher;

impl PipelineRuntime {
    pub(crate) fn started_at(&self) -> Instant {
        self.pipeline_started_at
    }

    pub(crate) fn prepare_resources(
        diagnostics: PipelineDiagnosticsPublisher,
        flows: BTreeMap<FlowId, FlowRuntimeSpec>,
        retained_origin: Option<Instant>,
        startup: Arc<StartupControl>,
    ) -> Result<PreparedPipelineRuntime, PipelineRuntimeError> {
        Self::prepare_resource_workers(
            diagnostics,
            flows,
            retained_origin,
            startup,
            &mut |name: String, task: WorkerTask| {
                thread::Builder::new().name(name).spawn(move || task.run())
            },
        )
    }

    pub(super) fn prepare_resource_workers(
        diagnostics: PipelineDiagnosticsPublisher,
        flow_specs: BTreeMap<FlowId, FlowRuntimeSpec>,
        retained_origin: Option<Instant>,
        startup: Arc<StartupControl>,
        spawner: &mut impl FnMut(String, WorkerTask) -> io::Result<JoinHandle<()>>,
    ) -> Result<PreparedPipelineRuntime, PipelineRuntimeError> {
        let pipeline_started_at = retained_origin.unwrap_or_else(Instant::now);
        let mut flows = BTreeMap::new();
        let mut bindings = Vec::new();
        for (flow_id, spec) in flow_specs {
            let prepared = FlowRuntime::prepare_with_spawner(
                &flow_id,
                spec,
                &diagnostics,
                pipeline_started_at,
                Arc::clone(&startup),
                spawner,
            );
            match prepared {
                Ok((flow, completions)) => {
                    flows.insert(flow_id, flow);
                    bindings.extend(completions);
                }
                Err(error) => {
                    startup.abort();
                    drop(flows);
                    return Err(error);
                }
            }
        }
        Ok(PreparedPipelineRuntime::new(
            Self {
                flows,
                pipeline_started_at,
            },
            startup,
            bindings,
        ))
    }

    pub(super) fn retain(&mut self, mut retained: Self) {
        self.flows.append(&mut retained.flows);
    }
}
