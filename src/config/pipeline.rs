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

//! Validates Pipeline deadlines and retry settings.

use super::RunnerConfigError;
use super::retry_backoff::{RawRetryBackoffConfig, RetryBackoffConfig};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug)]
pub(super) struct PipelineConfig {
    pub(super) startup_timeout: Duration,
    pub(super) shutdown_timeout: Duration,
    pub(super) reconfigure_timeout: Duration,
    pub(super) retry_backoff: RetryBackoffConfig,
}

impl TryFrom<RawPipelineConfig> for PipelineConfig {
    type Error = RunnerConfigError;

    fn try_from(raw: RawPipelineConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            startup_timeout: Duration::from_millis(raw.startup_timeout_ms.unwrap_or(30_000)),
            shutdown_timeout: Duration::from_millis(raw.shutdown_timeout_ms.unwrap_or(30_000)),
            reconfigure_timeout: Duration::from_millis(
                raw.reconfigure_timeout_ms.unwrap_or(30_000),
            ),
            retry_backoff: RetryBackoffConfig::try_from(raw.retry_backoff)?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RawPipelineConfig {
    startup_timeout_ms: Option<u64>,
    shutdown_timeout_ms: Option<u64>,
    reconfigure_timeout_ms: Option<u64>,
    retry_backoff: RawRetryBackoffConfig,
}
