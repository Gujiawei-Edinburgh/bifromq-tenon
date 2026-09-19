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

//! Owns this resource batch after Plugin launch and before activation.
//!
//! Each child retains its own startup progress; there is no all-Ready barrier.
//! Event waits and terminal cleanup borrow this persistent owner, so canceling
//! either wait cannot release children, Program material, workers, or Queue files.
//! Terminal cleanup must finish before Drop; OS failures propagate to the caller's
//! fatal Pipeline cleanup path. The Control server remains owned by its caller.

use super::StagedResourceChanges;
use super::instance_launch::build_instance_launch;
use crate::identifiers::PluginInstanceId;
use crate::pipeline::diagnostics::PipelineDiagnosticsPublisher;
use crate::pipeline::plugin::{
    PluginControlLauncher, PluginInstanceError, PluginInstances, PluginRetryBackoff,
};

impl StagedResourceChanges {
    pub(super) fn begin_cutover(self, retry_backoff: PluginRetryBackoff) -> StartedResourceChanges {
        StartedResourceChanges {
            plugins: OwnedInstancePlugins {
                instances: PluginInstances::launch_controlled([], retry_backoff),
            },
            staged: self,
        }
    }
}

/// A launched resource batch whose new data plane is still paused.
#[must_use = "a launched candidate must finish terminal cleanup before being dropped"]
pub(in crate::pipeline::reconfigure) struct StartedResourceChanges {
    pub(super) plugins: OwnedInstancePlugins,
    pub(super) staged: StagedResourceChanges,
}

impl StartedResourceChanges {
    pub(super) fn launch_instances(
        &mut self,
        selected: impl Fn(&PluginInstanceId) -> bool,
        control: &PluginControlLauncher,
        retry_backoff: PluginRetryBackoff,
        diagnostics: &PipelineDiagnosticsPublisher,
    ) {
        let launches = self
            .staged
            .changes
            .launched_instances()
            .filter(|id| selected(id))
            .map(|id| {
                (
                    id.clone(),
                    build_instance_launch(
                        &self.staged.changes.target,
                        self.staged.working_directory.path(),
                        id,
                        control,
                        self.staged.changes.available_cpu_count,
                    ),
                    diagnostics.instance_plugin(id.clone()),
                )
            });
        self.plugins
            .instances
            .append(&mut PluginInstances::launch_controlled(
                launches,
                retry_backoff,
            ));
    }

    /// Reaps the entire child set before relinquishing its ownership.
    /// Once called, only terminal cleanup may continue, including after cancellation or error.
    pub(in crate::pipeline::reconfigure) async fn force_stop(
        &mut self,
    ) -> Result<(), PluginInstanceError> {
        self.plugins.instances.force_remove_all().await
    }
}

/// Carries terminal child ownership unchanged across the activation handoff.
/// No unguarded collection can escape while Queue files depend on its children.
pub(super) struct OwnedInstancePlugins {
    pub(super) instances: PluginInstances,
}

impl Drop for OwnedInstancePlugins {
    fn drop(&mut self) {
        // A nonempty map means terminal ownership has not been handed back,
        // not necessarily that a child is still alive. Aborting on this owner
        // violation prevents Stage Drop from deleting possibly live Queue files.
        if !self.instances.is_empty() {
            std::process::abort();
        }
    }
}

#[cfg(test)]
pub(super) mod tests;
