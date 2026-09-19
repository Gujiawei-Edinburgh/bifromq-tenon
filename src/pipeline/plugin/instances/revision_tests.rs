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

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io;
use std::path::Path;

use rustix::io::Errno;
use rustix::process::{Pid, Signal, WaitOptions, kill_process, waitpid};
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;

use super::PluginInstances;
use crate::contracts::core::{PipelineRevisionPlan, PluginInterface, PluginProgramRuntime};
use crate::identifiers::PluginInstanceId;
use crate::payload_contract::PluginInterface as ContractInterface;
use crate::pipeline::diagnostics::{
    PipelineDiagnosticsPublisher, test_support as diagnostic_test_support,
};
use crate::pipeline::plugin::control::{
    PluginControlLauncher, TestPluginControlServer as PluginControlServer,
};
use crate::pipeline::plugin::lifecycle::{PluginBells, PluginLifecycleError, PluginStatusState};
use crate::pipeline::plugin::test_support::{
    TEST_DEADLINE, controlled_program_command, recorded_pid as controlled_recorded_pid,
    retry_backoff,
};
use crate::pipeline::plugin::{
    ControlledPluginLaunch, PluginInstanceError, PluginInstanceEvent, PluginLaunch,
};
use crate::pipeline::reconfigure::revision::PipelineRevision;
use crate::runner::test_support::valid_program_descriptor;

#[tokio::test(flavor = "current_thread")]
async fn received_instances_share_program_but_not_config_process_or_replacement()
-> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let applied = revision(root.path(), "primary-applied")?;
    let diagnostics = diagnostic_test_support::publisher();
    let mut instances = start_instances(&applied, root.path(), &diagnostics, &launcher)?;
    wait_until_running(&mut instances).await?;

    assert_eq!(applied.programs().len(), 2);
    assert_eq!(instances.owners.len(), 3);
    let mut original_pids = BTreeMap::new();
    for (id, instance) in applied.document().plugin_instances() {
        let launch = launch_material(&applied, root.path(), id)?;
        assert_eq!(
            std::fs::read_to_string(launch.working_directory.join("configs.received"))?,
            format!("{}\n", instance.config())
        );
        assert_eq!(
            std::fs::read_to_string(launch.working_directory.join("program.received"))?.trim(),
            launch
                .program_directory
                .canonicalize()?
                .to_str()
                .ok_or("Program path is not UTF-8")?
        );
        let pid = recorded_pid(root.path(), id)?;
        assert!(!original_pids.values().any(|other| *other == pid));
        assert!(waitpid(Some(pid), WaitOptions::NOHANG)?.is_none());
        original_pids.insert(id.clone(), pid);
    }

    let primary = PluginInstanceId::try_from("primary".to_owned())?;
    let target = revision(root.path(), "primary-target")?;
    let selected = BTreeSet::from([primary.clone()]);
    instances.request_retirement(&selected)?;
    timeout(TEST_DEADLINE, async {
        while !matches!(
            std::future::poll_fn(|context| instances.poll_handoff_event(
                context,
                &BTreeSet::new(),
                &selected
            ))
            .await?,
            super::PluginRetirementEvent::Reaped
        ) {}
        Ok::<_, PluginInstanceError>(())
    })
    .await??;
    let mut replacement = PluginInstances::launch_controlled(
        [(
            primary.clone(),
            controlled_launch_material(&target, root.path(), &primary, &launcher)?,
            diagnostics.instance_plugin(primary.clone()),
        )],
        retry_backoff(),
    );
    instances.append(&mut replacement);
    assert_eq!(
        waitpid(Some(original_pids[&primary]), WaitOptions::NOHANG).err(),
        Some(Errno::CHILD)
    );
    wait_until_running(&mut instances).await?;
    let replacement_pid = recorded_pid(root.path(), &primary)?;
    assert_ne!(replacement_pid, original_pids[&primary]);
    assert_eq!(
        std::fs::read_to_string(root.path().join("instances/primary/configs.received"))?,
        "{\"target\":\"primary-applied\"}\n{\"target\":\"primary-target\"}\n"
    );

    instances.request_retirement(&selected)?;
    timeout(
        TEST_DEADLINE,
        std::future::poll_fn(|context| {
            instances.poll_handoff_event(context, &BTreeSet::new(), &selected)
        }),
    )
    .await??;
    assert!(!instances.owners.contains_key(&primary));
    assert_eq!(
        waitpid(Some(replacement_pid), WaitOptions::NOHANG).err(),
        Some(Errno::CHILD)
    );
    for (id, owner) in instances.owners.iter() {
        assert_eq!(owner.status().0, PluginStatusState::Running);
        assert_eq!(recorded_pid(root.path(), id)?, original_pids[id]);
        assert!(waitpid(Some(original_pids[id]), WaitOptions::NOHANG)?.is_none());
        assert_eq!(
            std::fs::read_to_string(
                root.path()
                    .join("instances")
                    .join(id.as_str())
                    .join("starts.received")
            )?,
            "started\n"
        );
    }
    timeout(TEST_DEADLINE, instances.force_stop_all()).await??;
    timeout(TEST_DEADLINE, instances.force_stop_all()).await??;
    for id in instances.owners.keys() {
        assert_eq!(
            waitpid(Some(original_pids[id]), WaitOptions::NOHANG).err(),
            Some(Errno::CHILD)
        );
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retry_uses_the_selected_revision_and_only_restarts_the_exited_instance()
-> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let applied = revision(root.path(), "primary-applied")?;
    let diagnostics = diagnostic_test_support::publisher();
    let mut instances = start_instances(&applied, root.path(), &diagnostics, &launcher)?;
    wait_until_running(&mut instances).await?;
    let primary = PluginInstanceId::try_from("primary".to_owned())?;
    let original_pid = recorded_pid(root.path(), &primary)?;

    // A pending target must not supply config to a child still owned by applied.
    let target = revision(root.path(), "primary-target")?;
    assert_ne!(applied.document_etag(), target.document_etag());
    kill_process(original_pid, Signal::TERM)?;
    assert_eq!(
        timeout(TEST_DEADLINE, instances.next_event()).await??,
        PluginInstanceEvent::ProcessFailed(primary.clone())
    );
    assert_eq!(
        instances
            .owners
            .get(&primary)
            .ok_or("Primary owner is missing")?
            .status()
            .0,
        PluginStatusState::RestartBackoff
    );
    assert_eq!(
        timeout(TEST_DEADLINE, instances.next_event()).await??,
        PluginInstanceEvent::RestartDue(primary.clone())
    );
    assert_eq!(
        waitpid(Some(original_pid), WaitOptions::NOHANG).err(),
        Some(Errno::CHILD)
    );
    instances.restart_controlled(
        &primary,
        controlled_launch_material(&applied, root.path(), &primary, &launcher)?,
        diagnostics.instance_plugin(primary.clone()),
    )?;
    wait_until_running(&mut instances).await?;
    assert_eq!(instances.owners.len(), 3);
    assert_ne!(recorded_pid(root.path(), &primary)?, original_pid);
    assert_eq!(
        std::fs::read_to_string(root.path().join("instances/primary/configs.received"))?,
        "{\"target\":\"primary-applied\"}\n{\"target\":\"primary-applied\"}\n"
    );
    for id in applied.document().plugin_instances().keys() {
        assert!(waitpid(Some(recorded_pid(root.path(), id)?), WaitOptions::NOHANG)?.is_none());
        assert_eq!(
            std::fs::read_to_string(
                root.path()
                    .join("instances")
                    .join(id.as_str())
                    .join("starts.received")
            )?,
            if id == &primary {
                "started\nstarted\n"
            } else {
                "started\n"
            }
        );
    }
    timeout(TEST_DEADLINE, instances.force_stop_all()).await??;
    server.shutdown().await?;
    Ok(())
}

#[test]
fn lifecycle_errors_report_instance_identity_and_preserve_the_os_cause()
-> Result<(), Box<dyn Error>> {
    let failure = || PluginLifecycleError::Stop(io::Error::other("Signal request failed"));
    let instance =
        PluginInstanceError::new(PluginInstanceId::try_from("primary".to_owned())?, failure());
    assert_eq!(
        instance.to_string(),
        "Plugin Instance lifecycle failed: primary"
    );
    let lifecycle = instance.source().ok_or("Lifecycle error is missing")?;
    assert_eq!(lifecycle.to_string(), "Plugin process could not be stopped");
    assert_eq!(
        lifecycle.source().ok_or("OS error is missing")?.to_string(),
        "Signal request failed"
    );
    Ok(())
}

fn revision(root: &Path, primary_target: &str) -> Result<PipelineRevision, Box<dyn Error>> {
    let mut programs = Vec::new();
    for (name, interface, contract_interface) in [
        (
            "com.example.sensor",
            PluginInterface::Source,
            ContractInterface::Source,
        ),
        (
            "com.example.output",
            PluginInterface::Sink,
            ContractInterface::Sink,
        ),
    ] {
        let directory = root.join(name);
        std::fs::create_dir_all(&directory)?;
        programs.push(PluginProgramRuntime {
            program_name: name.into(),
            exact_version: "1.0.0".into(),
            program_directory: directory
                .to_str()
                .ok_or("Program path is not UTF-8")?
                .into(),
            command: controlled_program_command(contract_interface)?,
            plugin_interface: interface as i32,
            payload_descriptor_set: valid_program_descriptor(contract_interface)?,
        });
    }
    Ok(PipelineRevision::from_runner(PipelineRevisionPlan {
        document_etag: format!("etag-{primary_target}"),
        tenon_document_json: serde_json::to_string(&json!({
            "specVersion": "1", "id": "instance-process-test",
            "pluginInstances": {
                "sensor": {"programName": "com.example.sensor", "exactVersion": "1.0.0", "config": {"target": "sensor"}},
                "primary": {"programName": "com.example.output", "exactVersion": "1.0.0", "config": {"target": primary_target}},
                "backup": {"programName": "com.example.output", "exactVersion": "1.0.0", "config": {"target": "backup"}}
            },
            "flows": {"main": {
                "parallelism": 1, "source": "sensor",
                "process": {"script": "function main(event) emit() end"},
                "sinks": ["primary", "backup"]
            }}
        }))?,
        plugin_programs: programs,
    }))
}

fn launch_material<'a>(
    revision: &'a PipelineRevision,
    root: &Path,
    id: &PluginInstanceId,
) -> Result<PluginLaunch<'a>, Box<dyn Error>> {
    let instance = revision
        .document()
        .plugin_instances()
        .get(id)
        .ok_or("Instance is missing")?;
    let program = revision
        .programs()
        .get(&(
            instance.program_name().clone(),
            instance.exact_version().clone(),
        ))
        .ok_or("Program is missing")?;
    let working_directory = root.join("instances").join(id.as_str());
    std::fs::create_dir_all(&working_directory)?;
    let interface = program.payload_contract().interface();
    let bells = PluginBells {
        source_channel_region: matches!(
            interface,
            ContractInterface::Source | ContractInterface::SourceAndSink
        )
        .then(|| working_directory.join("channels.bells")),
        sink_inputs: matches!(
            interface,
            ContractInterface::Sink | ContractInterface::SourceAndSink
        )
        .then(Vec::new),
    };
    Ok(PluginLaunch {
        program_directory: program.program_directory(),
        command: program.command(),
        working_directory,
        config: instance.config(),
        extra_args: instance.extra_args(),
        env: instance.env(),
        bells,
    })
}

fn controlled_launch_material<'a>(
    revision: &'a PipelineRevision,
    root: &Path,
    id: &PluginInstanceId,
    control: &'a PluginControlLauncher,
) -> Result<ControlledPluginLaunch<'a>, Box<dyn Error>> {
    let launch = launch_material(revision, root, id)?;
    let instance = revision
        .document()
        .plugin_instances()
        .get(id)
        .ok_or("Instance is missing")?;
    let program = revision
        .programs()
        .get(&(
            instance.program_name().clone(),
            instance.exact_version().clone(),
        ))
        .ok_or("Program is missing")?;
    Ok(ControlledPluginLaunch::new(
        launch,
        program.payload_contract().interface(),
        control,
    ))
}

fn start_instances(
    revision: &PipelineRevision,
    root: &Path,
    diagnostics: &PipelineDiagnosticsPublisher,
    control: &PluginControlLauncher,
) -> Result<PluginInstances, Box<dyn Error>> {
    let launches = revision
        .document()
        .plugin_instances()
        .keys()
        .map(|id| {
            let publisher = diagnostics.instance_plugin(id.clone());
            Ok((
                id.clone(),
                controlled_launch_material(revision, root, id, control)?,
                publisher,
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(PluginInstances::launch_controlled(
        launches,
        retry_backoff(),
    ))
}

async fn wait_until_running(instances: &mut PluginInstances) -> Result<(), Box<dyn Error>> {
    timeout(TEST_DEADLINE, async {
        while instances
            .owners
            .iter()
            .any(|(_, owner)| owner.status().0 != PluginStatusState::Running)
        {
            assert_eq!(
                instances.next_event().await?,
                PluginInstanceEvent::StatusChanged
            );
        }
        Ok::<_, Box<dyn Error>>(())
    })
    .await?
}

fn recorded_pid(root: &Path, id: &PluginInstanceId) -> Result<Pid, Box<dyn Error>> {
    controlled_recorded_pid(&root.join("instances").join(id.as_str())).map_err(Into::into)
}
