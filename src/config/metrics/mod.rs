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

//! Validates the optional deployment label and HTTP collection wait budget.

use crate::identifiers::validate_local_id;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct MetricsConfig {
    node_id: Option<String>,
    collection_timeout: Duration,
}

impl MetricsConfig {
    pub(crate) fn node_id(&self) -> Option<&str> {
        self.node_id.as_deref()
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.collection_timeout
    }
}

impl TryFrom<RawMetricsConfig> for MetricsConfig {
    type Error = &'static str;

    fn try_from(raw: RawMetricsConfig) -> Result<Self, Self::Error> {
        if let Some(node_id) = &raw.node_id {
            validate_local_id(node_id).map_err(|_| "/metrics/nodeId")?;
        }
        let timeout_ms = raw.collection_timeout_ms.unwrap_or(2000);
        if timeout_ms == 0 {
            return Err("/metrics/collectionTimeoutMs");
        }
        Ok(Self {
            node_id: raw.node_id,
            collection_timeout: Duration::from_millis(timeout_ms),
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RawMetricsConfig {
    node_id: Option<String>,
    collection_timeout_ms: Option<u64>,
}

#[cfg(test)]
mod tests;
