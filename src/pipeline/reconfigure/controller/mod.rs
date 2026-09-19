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

//! Connects target revision and shutdown operations to the sole Controller.

use crate::contracts;
use crate::pipeline::controller::PipelineController;
use crate::pipeline::diagnostics::PipelineDiagnosticsPublisher;
use crate::pipeline::plugin::PluginControlLauncher;
use crate::pipeline::reconfigure::PipelineReconfigurer;
use crate::pipeline::reconfigure::revision::PipelineRevision;
use opentelemetry::metrics::Meter;
use std::time;

pub(in crate::pipeline) fn start_controller(
    first_target: PipelineRevision,
    environment: contracts::core::PipelineEnvironment,
    diagnostics: PipelineDiagnosticsPublisher,
    control: PluginControlLauncher,
    meter: Option<&Meter>,
) -> PipelineController {
    let timeout = time::Duration::from_millis(environment.reconfigure_timeout_ms);
    PipelineController::start(
        PipelineReconfigurer::new(environment, diagnostics, control).with_metrics(meter),
        first_target,
        timeout,
    )
}
