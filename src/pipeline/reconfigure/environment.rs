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

//! Runtime views of the same-build Runner's frozen Protobuf environment.

use crate::config::ScriptVmLimits;
use crate::contracts::core::{PipelineEnvironment, RetryBackoff};
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

#[allow(
    clippy::expect_used,
    reason = "Runner serializes its complete, already verified environment"
)]
impl PipelineEnvironment {
    pub(crate) fn available_cpu_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.available_cpu_count as usize)
            .expect("Runner preserves its positive available CPU count")
    }
    pub(crate) fn pipeline_working_directory(&self) -> &Path {
        Path::new(&self.pipeline_working_directory)
    }

    pub(crate) fn lua_limits(&self) -> ScriptVmLimits {
        ScriptVmLimits::from_runner(
            self.lua_limits
                .as_ref()
                .expect("Runner supplies the frozen Lua limits"),
        )
    }

    pub(crate) fn retry_backoff(&self) -> &RetryBackoff {
        self.retry_backoff
            .as_ref()
            .expect("Runner supplies the frozen retry backoff")
    }
}

impl RetryBackoff {
    pub(crate) fn initial_delay(&self) -> Duration {
        Duration::from_millis(self.initial_delay_ms)
    }

    pub(crate) fn maximum_delay(&self) -> Duration {
        Duration::from_millis(self.maximum_delay_ms)
    }
}
