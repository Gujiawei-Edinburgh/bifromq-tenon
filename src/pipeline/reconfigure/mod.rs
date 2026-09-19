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

//! Applies one complete Pipeline revision.
//!
//! Runner supplies one private working-directory root and one complete
//! Runtime-Integrity-passed revision. This module derives all paths, creates
//! each Queue, starts the in-process runtime and Plugin children, and derives
//! the resulting complete status. [`PipelineReconfigurer`] owns the current
//! runtime internally, and every target enters its single `apply(target)`
//! operation. Every current and target revision is projected into the same
//! fixed resource set. One ordered merge derives Add, Replace, and Remove
//! actions for each stable resource identity. Runtime-equivalent revisions
//! produce no actions, and an absent current revision is simply an empty set.
//! The executor always advances through Derive, Compile, Stage, Cutover,
//! Activate, and Publish; resource-specific work can extend those phases but
//! cannot replace their ordering or failure boundaries.

mod apply;
mod controller;
mod environment;
mod error;
mod lua_replacement;
mod operation;
mod plan;
mod resource_stage;
pub(crate) mod revision;
mod runtime_files;

pub(in crate::pipeline) use controller::start_controller;
pub(crate) use error::PipelineReconfigureError;
pub(crate) use operation::ReconfigureShutdown;

use crate::contracts::core::{PipelineEnvironment, PipelineStatusSnapshot};
use crate::pipeline::channel::metrics::FlowMetrics;
use crate::pipeline::diagnostics::PipelineDiagnosticsPublisher;
use crate::pipeline::plugin::{PluginControlLauncher, PluginInstanceError, PluginInstanceEvent};
use opentelemetry::metrics::Meter;
use resource_stage::ActivePipeline;
use std::sync::Arc;
use tokio::sync::watch;

pub(in crate::pipeline) type RuntimeObservation =
    Result<operation::DataPlaneOperation<PluginInstanceEvent>, PluginInstanceError>;

/// The terminal result of one serialized application.
pub(crate) enum PipelineApplyOutcome {
    Applied(PipelineStatusSnapshot),
    Stopped,
}

pub(crate) struct PipelineReconfigureShutdownHandle {
    sender: watch::Sender<Option<ReconfigureShutdown>>,
}

impl PipelineReconfigureShutdownHandle {
    pub(crate) fn request(&self, requested: ReconfigureShutdown) {
        self.sender.send_if_modified(|current| {
            if current.is_none_or(|current| current < requested) {
                *current = Some(requested);
                true
            } else {
                false
            }
        });
    }
}

/// Owns the immutable launch inputs and the sole current Pipeline runtime.
pub(crate) struct PipelineReconfigurer {
    environment: Arc<PipelineEnvironment>,
    diagnostics: PipelineDiagnosticsPublisher,
    current: Option<ActivePipeline>,
    metrics: Option<FlowMetrics>,
    control: PluginControlLauncher,
    shutdown_sender: watch::Sender<Option<ReconfigureShutdown>>,
    shutdown_receiver: watch::Receiver<Option<ReconfigureShutdown>>,
}

impl PipelineReconfigurer {
    pub(crate) fn new(
        environment: PipelineEnvironment,
        diagnostics: PipelineDiagnosticsPublisher,
        control: PluginControlLauncher,
    ) -> Self {
        let (shutdown_sender, shutdown_receiver) = watch::channel(None);
        Self {
            environment: Arc::new(environment),
            diagnostics,
            current: None,
            metrics: None,
            control,
            shutdown_sender,
            shutdown_receiver,
        }
    }

    pub(super) fn with_metrics(mut self, meter: Option<&Meter>) -> Self {
        self.metrics = meter.map(FlowMetrics::new);
        self
    }

    pub(crate) fn shutdown_handle(&self) -> PipelineReconfigureShutdownHandle {
        PipelineReconfigureShutdownHandle {
            sender: self.shutdown_sender.clone(),
        }
    }

    fn install_current(&mut self, current: ActivePipeline) -> &ActivePipeline {
        assert!(
            self.current.is_none(),
            "Publish must install into an empty current slot"
        );
        self.current.insert(current)
    }

    fn shutdown_requested(&self) -> Option<ReconfigureShutdown> {
        *self.shutdown_receiver.borrow()
    }
}

#[cfg(test)]
pub(super) mod test_support;

#[cfg(any(test, feature = "repository-test-support"))]
pub(crate) mod contract_test_support {
    pub(crate) use super::runtime_files::{
        SINK_DIRECTORY_NAME, SOURCE_DIRECTORY_NAME, egress_queue_path, flow_channel_bell_path,
        loops_bell_path,
    };
}
