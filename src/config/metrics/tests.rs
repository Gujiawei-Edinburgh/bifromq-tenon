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

use super::*;
use serde_json::json;

#[test]
fn push_settings_and_invalid_pull_values_are_rejected() {
    for value in [
        json!({"endpoint":"http://localhost:4317"}),
        json!({"exportIntervalMs":10000}),
        json!({"exportTimeoutMs":2000}),
        json!({"headers":{}}),
        json!({"tls":{}}),
        json!({"nodeId":""}),
        json!({"nodeId":"invalid\nnode"}),
        json!({"collectionTimeoutMs":0}),
        json!({"collectionTimeoutMs":-1}),
    ] {
        let parsed = serde_json::from_value::<RawMetricsConfig>(value);
        assert!(parsed.is_err() || parsed.is_ok_and(|raw| MetricsConfig::try_from(raw).is_err()));
    }
}

#[test]
fn omission_uses_pull_defaults() -> Result<(), Box<dyn std::error::Error>> {
    let config = MetricsConfig::try_from(serde_json::from_value::<RawMetricsConfig>(json!({}))?)?;
    assert_eq!(config.node_id(), None);
    assert_eq!(config.timeout(), Duration::from_millis(2000));
    let config = MetricsConfig::try_from(serde_json::from_value::<RawMetricsConfig>(
        json!({"nodeId":"gateway-01","collectionTimeoutMs":50}),
    )?)?;
    assert_eq!(config.node_id(), Some("gateway-01"));
    assert_eq!(config.timeout(), Duration::from_millis(50));
    Ok(())
}
