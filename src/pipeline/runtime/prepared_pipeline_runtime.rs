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

//! A complete Pipeline data plane waiting behind its one startup decision.

use std::sync::Arc;

use super::flow_runtime::ChannelBindingCompletion;
use super::{PipelineRuntime, PipelineRuntimeError, StartupControl};

/// Owns every prepared worker until the candidate is activated or abandoned.
#[must_use = "a prepared Pipeline runtime must be activated or explicitly abandoned"]
pub(crate) struct PreparedPipelineRuntime {
    runtime: Option<PipelineRuntime>,
    startup: Arc<StartupControl>,
    bindings: Vec<ChannelBindingCompletion>,
}

impl PreparedPipelineRuntime {
    pub(super) fn new(
        runtime: PipelineRuntime,
        startup: Arc<StartupControl>,
        bindings: Vec<ChannelBindingCompletion>,
    ) -> Self {
        Self {
            runtime: Some(runtime),
            startup,
            bindings,
        }
    }

    /// Called only after old writers have joined and the final Queue files exist.
    pub(crate) fn bind(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), PipelineRuntimeError>> + use<> {
        self.startup.bind();
        let bindings = std::mem::take(&mut self.bindings);
        async move {
            for binding in bindings {
                binding.wait().await?;
            }
            Ok(())
        }
    }

    pub(crate) fn abort_and_join(&mut self) {
        self.startup.abort();
        drop(self.runtime.take());
        self.bindings.clear();
    }

    /// Releases every prepared FlowChannel through the shared startup decision.
    #[must_use]
    pub(crate) fn activate(mut self) -> PipelineRuntime {
        let runtime = self.runtime.take().unwrap_or_else(|| std::process::abort());
        self.startup.activate();
        runtime
    }

    /// Installs rebuilt Flow owners before releasing their common startup decision.
    #[expect(
        clippy::expect_used,
        reason = "activation consumes the sole prepared worker owner exactly once"
    )]
    pub(crate) fn activate_into(mut self, current: &mut PipelineRuntime) {
        let rebuilt = self
            .runtime
            .take()
            .expect("Prepared runtime owns its workers");
        assert!(
            rebuilt
                .flows
                .keys()
                .all(|id| !current.flows.contains_key(id)),
            "Rebuilt Flows must have relinquished their old workers"
        );
        current.retain(rebuilt);
        self.startup.activate();
    }

    /// Adopts all retained workers before the candidate's one activation point.
    pub(crate) fn retain(&mut self, retained: PipelineRuntime) {
        self.runtime
            .as_mut()
            .unwrap_or_else(|| std::process::abort())
            .retain(retained);
    }
}

impl Drop for PreparedPipelineRuntime {
    fn drop(&mut self) {
        // A pending FlowChannel waits only on this decision. Abort must happen
        // before the owned runtime drops and joins its worker threads.
        if self.runtime.is_some() {
            self.startup.abort();
        }
    }
}

#[cfg(test)]
pub(in crate::pipeline) mod test_support {
    use super::super::startup::test_support as startup_test_support;
    use super::PreparedPipelineRuntime;

    pub(in crate::pipeline) fn data_plane_counts(
        prepared: &PreparedPipelineRuntime,
    ) -> (usize, usize) {
        let runtime = prepared
            .runtime
            .as_ref()
            .unwrap_or_else(|| std::process::abort());
        (
            runtime.flows.len(),
            runtime
                .flows
                .values()
                .map(super::super::flow_runtime::FlowRuntime::worker_count)
                .sum(),
        )
    }

    pub(in crate::pipeline) fn is_pending(prepared: &PreparedPipelineRuntime) -> bool {
        startup_test_support::is_pending(&prepared.startup)
    }
}
