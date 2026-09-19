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

//! Validates the shared Source and process retry range.

use super::RunnerConfigError;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug)]
pub(super) struct RetryBackoffConfig {
    pub(super) initial_delay: Duration,
    pub(super) maximum_delay: Duration,
}

impl TryFrom<RawRetryBackoffConfig> for RetryBackoffConfig {
    type Error = RunnerConfigError;

    fn try_from(raw: RawRetryBackoffConfig) -> Result<Self, Self::Error> {
        if raw.maximum_delay_ms < raw.initial_delay_ms {
            return Err(RunnerConfigError::RetryBackoffInvalid);
        }
        Ok(Self {
            initial_delay: Duration::from_millis(raw.initial_delay_ms),
            maximum_delay: Duration::from_millis(raw.maximum_delay_ms),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RawRetryBackoffConfig {
    initial_delay_ms: u64,
    maximum_delay_ms: u64,
}
