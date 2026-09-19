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

#[test]
fn each_limit_replaces_the_whole_flow_and_exact_shared_processes() -> TestResult {
    for (field, before, after) in [
        ("maxPendingRecords", 1, 3),
        ("maxPendingRecords", 3, 1),
        ("maxRecordBytes", 1024, 4096),
        ("maxRecordBytes", 4096, 1024),
        // Equal aligned capacities still carry different accepted record sizes.
        ("maxRecordBytes", 1025, 1026),
    ] {
        let mut revisions = [base_revision("current")?, base_revision("target")?];
        for (revision, value) in revisions.iter_mut().zip([before, after]) {
            update_document(revision, |document| {
                document["flows"]["telemetry"][field] = json!(value);
                document["flows"]["audit"]["sinks"] = json!(["output-a", "output-b", "archive"]);
            })?;
        }
        let [current, target] = revisions;
        let current = model(current)?;
        let plan = ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?;
        assert_eq!(
            plan.actions.as_ref(),
            [
                ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                    instance_id: instance_id("output-a")?
                }),
                ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                    instance_id: instance_id("source-a")?
                }),
                ResourceAction::Replace(RuntimeResourceIdentity::SourceQueuePair {
                    flow_id: flow_id("telemetry")?,
                    channel_index: 0
                }),
                ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                    flow_id: flow_id("telemetry")?,
                    channel_index: 0
                }),
                ResourceAction::Replace(RuntimeResourceIdentity::EgressQueue {
                    instance_id: instance_id("output-a")?,
                    flow_id: flow_id("telemetry")?,
                    channel_index: 0
                }),
            ],
            "{field}: {before} -> {after}"
        );
        let changes = plan.compile()?;
        assert_eq!(changes.flows.len(), 1);
        assert_eq!(
            changes.flows[&flow_id("telemetry")?],
            super::super::FlowChange::ReplaceQueues
        );
    }
    Ok(())
}

#[test]
fn omitted_and_equivalent_explicit_limits_keep_all_resources() -> TestResult {
    let forms = [None, Some(("100", "262144")), Some(("1e2", "262144.0"))];
    for current_form in forms {
        for target_form in forms {
            let mut revisions = [base_revision("current")?, base_revision("target")?];
            for (revision, form) in revisions.iter_mut().zip([current_form, target_form]) {
                if let Some((pending, bytes)) = form {
                    let pending: Value = serde_json::from_str(pending)?;
                    let bytes: Value = serde_json::from_str(bytes)?;
                    update_document(revision, |document| {
                        document["flows"]["telemetry"]["maxPendingRecords"] = pending;
                        document["flows"]["telemetry"]["maxRecordBytes"] = bytes;
                    })?;
                }
            }
            let [current, target] = revisions;
            assert!(
                ReconfigurePlan::derive(
                    Some(&model(current)?),
                    model(target)?,
                    AVAILABLE_CPU_COUNT
                )?
                .actions
                .is_empty()
            );
        }
    }
    Ok(())
}
