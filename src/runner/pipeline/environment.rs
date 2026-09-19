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

//! Projects immutable Runner configuration into one Pipeline launch environment.

use crate::config::RunnerConfig;
use crate::contracts::core::{LuaLimits, PipelineEnvironment, RetryBackoff};
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

#[allow(
    clippy::expect_used,
    reason = "Runner owns validated UTF-8 paths and protocol-sized startup limits"
)]
pub(super) fn build_pipeline_environment(
    config: &RunnerConfig,
    pipeline_working_directory: &Path,
    available_cpu_count: NonZeroUsize,
) -> PipelineEnvironment {
    assert!(
        pipeline_working_directory.is_absolute(),
        "Runner-created Pipeline working paths must remain absolute"
    );
    PipelineEnvironment {
        metrics_node_id: config.metrics().node_id().map(str::to_owned),
        pipeline_working_directory: pipeline_working_directory
            .to_str()
            .expect("Runner-created Pipeline working paths must remain UTF-8")
            .to_owned(),
        lua_limits: Some(LuaLimits {
            cpu_time_limit_ms: duration_millis(config.lua_cpu_time_limit()),
            memory_limit_bytes: u64::try_from(config.lua_memory_limit_bytes().get())
                .expect("supported Runner platforms represent Lua memory limits in u64"),
        }),
        retry_backoff: Some(RetryBackoff {
            initial_delay_ms: duration_millis(config.retry_initial_delay()),
            maximum_delay_ms: duration_millis(config.retry_maximum_delay()),
        }),
        reconfigure_timeout_ms: duration_millis(config.pipeline_reconfigure_timeout()),
        available_cpu_count: available_cpu_count
            .get()
            .try_into()
            .expect("available CPU count fits protocol"),
    }
}

#[allow(
    clippy::expect_used,
    reason = "Runner durations were parsed from protocol-sized u64 milliseconds"
)]
fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis())
        .expect("Runner durations originate from protocol-sized millisecond values")
}
