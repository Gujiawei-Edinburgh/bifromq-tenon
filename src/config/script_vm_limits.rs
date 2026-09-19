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

//! Defines the shared CPU and memory limits used to compile and execute Lua.

use super::RunnerConfigError;
use serde::Deserialize;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::time::Duration;

/// Shared CPU and memory limits for script compilation and execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScriptVmLimits {
    memory_bytes: NonZeroUsize,
    cpu_time: Duration,
}

impl ScriptVmLimits {
    /// Restores limits from the same-build Runner's frozen configuration.
    #[allow(
        clippy::expect_used,
        reason = "Runner already verified memory capacity for this target platform"
    )]
    pub(crate) fn from_runner(limits: &crate::contracts::core::LuaLimits) -> Self {
        Self {
            memory_bytes: NonZeroUsize::new(
                usize::try_from(limits.memory_limit_bytes)
                    .expect("Runner's Lua memory limit must fit this platform"),
            )
            .expect("Runner's Lua memory limit must be nonzero"),
            cpu_time: Duration::from_millis(limits.cpu_time_limit_ms),
        }
    }

    /// Creates exact non-zero memory and CPU limits.
    ///
    /// # Errors
    ///
    /// Returns [`ScriptVmLimitsError`] when `cpu_time` is zero.
    pub const fn try_new(
        memory_bytes: NonZeroUsize,
        cpu_time: Duration,
    ) -> Result<Self, ScriptVmLimitsError> {
        if cpu_time.is_zero() {
            return Err(ScriptVmLimitsError::CpuTimeZero);
        }
        Ok(Self {
            memory_bytes,
            cpu_time,
        })
    }

    /// Returns the exact Lua VM memory limit in bytes.
    #[must_use]
    pub const fn memory_bytes(self) -> NonZeroUsize {
        self.memory_bytes
    }

    /// Returns the exact per-entry Lua CPU time limit.
    #[must_use]
    pub const fn cpu_time(self) -> Duration {
        self.cpu_time
    }
}

impl TryFrom<RawLuaConfig> for ScriptVmLimits {
    type Error = RunnerConfigError;

    fn try_from(raw: RawLuaConfig) -> Result<Self, Self::Error> {
        let memory = usize::try_from(raw.memory_limit_bytes)
            .ok()
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| RunnerConfigError::value_invalid("/lua/memoryLimitBytes"))?;
        Self::try_new(memory, Duration::from_millis(raw.cpu_time_limit_ms))
            .map_err(|_| RunnerConfigError::value_invalid("/lua/cpuTimeLimitMs"))
    }
}

/// A stable failure to create script VM resource limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScriptVmLimitsError {
    /// A zero CPU limit cannot provide a meaningful Lua compilation budget.
    CpuTimeZero,
}

impl ScriptVmLimitsError {
    /// Returns the stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::CpuTimeZero => "script_vm.cpu_time_zero",
        }
    }
}

impl fmt::Display for ScriptVmLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Script VM CPU time limit must be non-zero")
    }
}

impl Error for ScriptVmLimitsError {}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RawLuaConfig {
    cpu_time_limit_ms: u64,
    memory_limit_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runner_limits_preserve_frozen_configuration() -> Result<(), Box<dyn Error>> {
        let config = crate::RunnerConfig::parse(
            br#"{"stateDirectory":"/var/lib/tenon","http":{"listenAddress":"127.0.0.1:8080"},"pipeline":{"retryBackoff":{"initialDelayMs":100,"maximumDelayMs":30000}},"lua":{"cpuTimeLimitMs":50,"memoryLimitBytes":16777216}}"#,
        )?;
        let limits = config.script_vm_limits();
        let message = crate::contracts::core::LuaLimits {
            cpu_time_limit_ms: u64::try_from(limits.cpu_time().as_millis())?,
            memory_limit_bytes: u64::try_from(limits.memory_bytes().get())?,
        };
        assert_eq!(ScriptVmLimits::from_runner(&message), limits);
        Ok(())
    }
}
