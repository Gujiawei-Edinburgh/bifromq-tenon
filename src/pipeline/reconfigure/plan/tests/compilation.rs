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

//! Every valid resource combination compiles before any resource preparation.

use super::super::{FlowChange, ResourceMutation};
use super::*;
use std::collections::BTreeMap;

#[test]
fn compile_accepts_dual_interface_config_replacement() -> TestResult {
    let current = model(dual_revision("current")?)?;
    let mut target = dual_revision("target")?;
    update_document(&mut target, |document| {
        document["pluginInstances"]["gateway"]["config"] = json!({"endpoint": "changed"});
    })?;
    let compiled =
        ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?.compile()?;
    assert_eq!(
        compiled
            .processes
            .iter()
            .filter(|(_, mutation)| *mutation == ResourceMutation::Replace)
            .count(),
        1
    );
    assert_eq!(compiled.processes.len(), 1);
    assert!(compiled.flows.is_empty());
    assert!(compiled.egress_queues.is_empty());
    Ok(())
}

#[test]
fn compile_accepts_sink_program_version_change_with_identical_payload_material() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    select_program_version(&mut target, "archive", "com.example.archive", "2.0.0", None)?;
    ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?.compile()?;
    Ok(())
}

#[test]
fn compile_accepts_lua_with_changed_delivery() -> TestResult {
    accepts_mixed_change(|document| {
        document["flows"]["telemetry"]["delivery"] = json!("at-most-once")
    })
}

#[test]
fn compile_accepts_lua_with_new_contract_route_on_existing_flow() -> TestResult {
    accepts_mixed_change(|document| {
        document["flows"]["telemetry"]["sinks"] = json!(["output-a", "archive"])
    })
}

#[test]
fn compile_accepts_lua_with_another_same_contract_target() -> TestResult {
    accepts_mixed_change(|document| {
        document["flows"]["telemetry"]["sinks"] = json!(["output-a", "output-b"])
    })
}

#[test]
fn compile_accepts_lua_with_source_rebinding() -> TestResult {
    accepts_mixed_change(|document| {
        document["flows"]["telemetry"]["source"] = json!("source-b");
        document["flows"]["audit"]["source"] = json!("source-a");
    })
}

#[test]
fn compile_accepts_lua_with_queue_layout_change() -> TestResult {
    accepts_mixed_change(|document| document["flows"]["telemetry"]["parallelism"] = json!(0.5))
}

#[test]
fn compile_accepts_lua_with_instance_configuration_change() -> TestResult {
    accepts_mixed_change(|document| {
        document["pluginInstances"]["source-a"]["config"] = json!({"endpoint": "other"})
    })
}

#[test]
fn compile_accepts_lua_with_source_contract_change() -> TestResult {
    accepts_contract_change(0, "SourceRecordPayload")
}

#[test]
fn compile_accepts_lua_with_compatible_program_upgrade() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    select_program_version(&mut target, "source-a", "com.example.source", "2.0.0", None)?;
    update_document(&mut target, |document| {
        document["flows"]["telemetry"]["process"]["script"] =
            json!("function main(event) emit(); emit() end")
    })?;
    ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?.compile()?;
    Ok(())
}

#[test]
fn compile_accepts_lua_with_sink_contract_change() -> TestResult {
    accepts_contract_change(1, "SinkRecordPayload")
}

#[test]
fn compile_accepts_lua_with_equivalent_delivery_and_sink_order() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["flows"]["audit"]["process"]["script"] =
            json!("function main(event) emit(); emit() end");
        document["flows"]["audit"]["delivery"] = json!("at-least-once");
        document["flows"]["audit"]["sinks"] = json!(["archive", "output-b"]);
    })?;
    let compiled =
        ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?.compile()?;
    assert_eq!(
        compiled.flows,
        BTreeMap::from([(flow_id("audit")?, FlowChange::ReplaceDefinition)])
    );
    assert_eq!(
        compiled
            .flows
            .values()
            .filter(|change| **change == FlowChange::Add)
            .count(),
        0
    );
    Ok(())
}

fn accepts_mixed_change(edit: DocumentUpdate) -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["flows"]["telemetry"]["process"]["script"] =
            json!("function main(event) emit(); emit() end");
        document["pluginInstances"]["added-source"] =
            instance("com.example.source", "added-source");
        document["flows"]["added-flow"] = flow("added-source", 1, &["output-a"]);
        edit(document);
    })?;
    ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?.compile()?;
    Ok(())
}

fn accepts_contract_change(program_index: usize, root: &str) -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    target.plugin_programs[program_index].payload_descriptor_set = changed_contract(
        &target.plugin_programs[program_index].payload_descriptor_set,
        root,
    )?;
    update_document(&mut target, |document| {
        document["flows"]["telemetry"]["process"]["script"] =
            json!("function main(event) emit(); emit() end")
    })?;
    ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?.compile()?;
    Ok(())
}

#[test]
fn same_contract_target_change_compiles_to_a_route_update() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["flows"]["telemetry"]["sinks"] = json!(["output-a", "output-b"])
    })?;
    let compiled =
        ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?.compile()?;
    assert_eq!(
        compiled.flows,
        BTreeMap::from([(flow_id("telemetry")?, FlowChange::UpdateRoutes)])
    );
    Ok(())
}
