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

//! Terminal shutdown with child-owned Queues and externally controlled release gates.

use super::configuration_handoff::traffic_record;
use super::*;
use crate::config::ScriptVmLimits;
use crate::tenon_document::UnverifiedTenonDocument;
use crate::tenon_document::verified::TenonDocumentVerifier;
use rustix::process::{Signal, kill_process};

#[tokio::test(flavor = "current_thread")]
async fn shutdown_finishes_all_sources_before_stopping_shared_mutual_and_self_consumers()
-> TestResult {
    for topology in [
        vec![
            ("input-flow", "input", "archive", 2),
            ("other-flow", "other-input", "archive", 1),
        ],
        vec![
            ("a-flow", "dual-a", "dual-b", 1),
            ("b-flow", "dual-b", "dual-a", 1),
        ],
        vec![("self-flow", "dual-b", "dual-b", 1)],
    ] {
        with_environment("normal", &[], async |reconfigurer, _, root, _| {
            let target = shutdown_revision(root, &topology)?;
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let pids = shutdown_pids(root, reconfigurer)?;
            let mut stopping = Box::pin(reconfigurer.shutdown(ReconfigureShutdown::Planned));
            let observing = async {
                for (_, id, _, _) in &topology {
                    wait_for_file(&instance_directory(root, id)?.join("source-quiesced.received")).await?;
                }
                for (flow, _, sink, channels) in &topology {
                    for channel in 0..*channels {
                        let mut queue = sink_reader(root, sink, flow, channel)?;
                        read_record(&mut queue).await?;
                    }
                }
                assert_no_final_shutdown(root, &pids)?;
                for id in pids.keys() {
                    std::fs::write(instance_directory(root, id)?.join("allow-egress"), [])?;
                }
                for (flow, source, _, channels) in &topology {
                    for channel in 0..*channels {
                        timeout(
                            TEST_DEADLINE,
                            wait_for_channel_park(root, source, flow, channel),
                        )
                        .await??;
                    }
                }
                assert_no_final_shutdown(root, &pids)?;
                for id in pids.keys() {
                    std::fs::write(instance_directory(root, id)?.join("allow-completions"), [])?;
                }
                for id in pids.keys() {
                    wait_for_file(&instance_directory(root, id)?.join("shutdown.received")).await?;
                }
                assert!(root.exists(), "Queue files must outlive final child exit");
                for (_, source, _, channels) in &topology {
                    for channel in 0..*channels {
                        let directory = instance_directory(root, source)?;
                        let mut completions = Vec::new();
                        for ordinal in 1..=2 {
                            let completion = IngressCompletion::decode(traffic_record(&directory, "shutdown", &format!("completion-{channel}"), ordinal).await?.as_slice())?;
                            completions.push((completion.record_id, completion.status()));
                        }
                        completions.sort_by_key(|(id, _)| *id);
                        assert_eq!(completions, [(1, IngressCompletionStatus::Ok), (2, IngressCompletionStatus::Retry)]);
                    }
                }
                for sink in topology.iter().map(|(_, _, sink, _)| *sink).collect::<std::collections::BTreeSet<_>>() {
                    let count: usize = topology.iter().filter(|(_, _, target, _)| *target == sink).map(|(_, _, _, channels)| *channels as usize).sum();
                    for ordinal in 1..=count {
                        let record = EgressRecord::decode(traffic_record(&instance_directory(root, sink)?, "shutdown", "egress", ordinal).await?.as_slice())?;
                        assert_eq!(TestPayload::decode(record.payload.as_slice())?.value, "delivered");
                    }
                }
                assert_started_once(root, &pids.keys().map(String::as_str).collect::<Vec<_>>())?;
                for id in pids.keys() {
                    assert!(rustix::process::test_kill_process(pids[id]).is_ok());
                    std::fs::write(instance_directory(root, id)?.join("allow-shutdown"), [])?;
                }
                Ok::<_, Box<dyn Error>>(())
            };
            tokio::select! {
                result = &mut stopping => { result?; return Err("Shutdown completed before the gated responsibilities".into()); },
                result = observing => result?,
            }
            stopping.await?;
            assert!(!root.exists());
            for pid in pids.into_values() { assert_reaped(pid)?; }
            assert!(reconfigurer.current.is_none());
            Ok(())
        }).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_cancels_starting_and_backoff_sources_without_reading_their_completions()
-> TestResult {
    for phase in [
        PluginInstanceState::Starting,
        PluginInstanceState::RestartBackoff,
        PluginInstanceState::StartFailed,
    ] {
        with_environment("normal", &[], async |reconfigurer, _, root, _| {
            let target = next_revision(root, |document| {
                document["pluginInstances"]["input"]["config"]["behavior"] = json!(match phase {
                    PluginInstanceState::Starting => "delay-ready",
                    PluginInstanceState::StartFailed => "exit-before-ready",
                    _ => "normal",
                });
                document["pluginInstances"]["input"]["config"]["traffic"] = json!({
                    "session":"unhealthy", "completionGate":"never-complete",
                    "submissions":[{"channelIndex":0,"recordId":1,"payload":TestPayload{value:"pending".into()}.encode_to_vec()}]
                });
                document["flows"]["input-to-dual"]["process"]["script"] = json!(COMPLETING_LUA);
            })?;
            reconfigurer.apply(target, None).await?;
            let input = instance_directory(root, "input")?;
            match phase {
                PluginInstanceState::RestartBackoff => {
                    wait_for_states(current(reconfigurer)?, &[]).await?;
                    kill_process(recorded_pid(&input)?, Signal::KILL)?;
                    wait_for_states(current(reconfigurer)?, &[("input", phase)]).await?;
                },
                _ => wait_for_states(current(reconfigurer)?, &[("input", phase)]).await?,
            }
            if phase == PluginInstanceState::Starting {
                // Starting precedes child initialization. Keep driving its owner
                // until real pending traffic exists, without releasing Ready.
                let submitted = input.join("traffic-unhealthy-submitted.received");
                loop {
                    tokio::select! {
                        result = current(reconfigurer)?.plugins.instances.next_event() => { result?; }
                        result = wait_for_file(&submitted) => { result?; break; }
                    }
                }
                assert_eq!(states(&current(reconfigurer)?.status_snapshot())["input"], "starting");
            }
            let pids = instance_pids(root)?;
            reconfigurer.shutdown(ReconfigureShutdown::Planned).await?;
            assert!(!root.exists());
            for pid in pids.into_values() { assert_reaped(pid)?; }
            Ok(())
        }).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_failure_or_force_does_not_fabricate_unreleased_egress_success() -> TestResult {
    for interruption in [
        Interruption::PluginFailure,
        Interruption::SourceFailure,
        Interruption::CoreAndPluginFailure,
        Interruption::Force,
    ] {
        with_environment("normal", &[], async |reconfigurer, _, root, _| {
            let target = shutdown_revision(root, &[("source-flow", "input", "archive", 1)])?;
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let pids = shutdown_pids(root, reconfigurer)?;
            let archive = instance_directory(root, "archive")?;
            let input = instance_directory(root, "input")?;
            let mut egress = sink_reader(root, "archive", "source-flow", 0)?;
            let retained_file = std::fs::OpenOptions::new().read(true).write(true).open(egress_queue_path(&archive, "source-flow", 0))?;
            let mut completion = source_completion(root, "input", "source-flow", 0)?;
            let shutdown = reconfigurer.shutdown_handle();
            let mut stopping = Box::pin(reconfigurer.shutdown(ReconfigureShutdown::Planned));
            tokio::select! {
                result = &mut stopping => { result?; return Err("Shutdown completed while Egress was held".into()); },
                result = async {
                    wait_for_file(&input.join("source-quiesced.received")).await?;
                    read_record(&mut egress).await?;
                    assert_no_final_shutdown(root, &pids)?;
                    match interruption {
                        Interruption::PluginFailure => kill_process(pids["archive"], Signal::KILL)?,
                        Interruption::SourceFailure => kill_process(pids["input"], Signal::KILL)?,
                        Interruption::CoreAndPluginFailure => {
                            corrupt_egress_commit(&retained_file)?;
                            egress.release(1)?;
                            kill_process(pids["input"], Signal::KILL)?;
                        },
                        Interruption::Force => shutdown.request(ReconfigureShutdown::Force),
                    }
                    Ok::<_, Box<dyn Error>>(())
                } => result?,
            }
            let result = stopping.await;
            match interruption {
                Interruption::PluginFailure | Interruption::SourceFailure => assert!(matches!(result, Err(PipelineReconfigureError::PluginInstanceLifecycle(_)))),
                Interruption::CoreAndPluginFailure => assert!(matches!(result, Err(PipelineReconfigureError::DataPlaneFailure(source)) if matches!(source.as_ref(), PipelineRuntimeError::FlowChannelFailed { flow_id, .. } if flow_id.as_str() == "source-flow"))),
                Interruption::Force => result?,
            }
            assert!(!root.exists());
            let mut released = [0; 8];
            retained_file.read_exact_at(&mut released, TEST_RELEASE_OFFSET)?;
            if matches!(interruption, Interruption::CoreAndPluginFailure) { assert!(u64::from_le_bytes(released) > 0); } else { assert_eq!(u64::from_le_bytes(released), 0); }
            assert!(matches!(completion.try_read()?, ReadOutcome::Empty));
            for pid in pids.into_values() { assert_reaped(pid)?; }
            Ok(())
        }).await?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Interruption {
    PluginFailure,
    SourceFailure,
    CoreAndPluginFailure,
    Force,
}

fn shutdown_revision(
    root: &Path,
    topology: &[(&str, &str, &str, u32)],
) -> TestResult<PipelineRevision> {
    let target = next_revision(root, |document| {
        document["pluginInstances"]["other-input"] = instance("com.example.input", "other-device");
        let mut instances = serde_json::Map::new();
        document["flows"] = json!({});
        for (id, source, sink, channels) in topology {
            for instance_id in [source, sink] {
                instances.insert(
                    (*instance_id).to_owned(),
                    document["pluginInstances"][instance_id].clone(),
                );
            }
            document["flows"][id] = flow(source, *channels, &[sink]);
            let contract = if *sink == "archive" {
                "com.example.archive"
            } else {
                "com.example.dual"
            };
            document["flows"][id]["process"]["script"] = json!(format!(
                "local b = registry:getBuilder('{contract}@1.0.0'); function main(event) if event.payload.value ~= 'pending' then b:setValue('delivered'); emit(b:build()) end end"
            ));
        }
        for (id, instance) in &mut instances {
            let submissions: Vec<_> = topology.iter().filter(|(_, source, _, _)| *source == id.as_str()).flat_map(|(_, _, _, channels)| {
                (0..*channels).flat_map(|channel| [(1, "delivered"), (2, "pending")].map(|(record, value)| json!({"channelIndex":channel,"recordId":record,"payload":TestPayload{value:value.into()}.encode_to_vec()})))
            }).collect();
            instance["config"]["behavior"] = json!("delay-shutdown");
            instance["config"]["traffic"] = json!({
                "session":"shutdown", "submissions":submissions,
                "completionGate":"allow-completions", "egressGate":"allow-egress"
            });
        }
        document["pluginInstances"] = Value::Object(instances);
    })?;
    let limits = ScriptVmLimits::try_new(
        std::num::NonZeroUsize::new(4 * 1024 * 1024).ok_or("Memory limit must be positive")?,
        std::time::Duration::from_secs(1),
    )?;
    TenonDocumentVerifier::try_new(limits)?
        .verify(UnverifiedTenonDocument::parse(
            target.document().strict_json().as_bytes(),
        )?)
        .map_err(|issues| std::io::Error::other(format!("Invalid shutdown fixture: {issues:?}")))?;
    Ok(target)
}

fn shutdown_pids(
    root: &Path,
    reconfigurer: &mut Reconfigurer,
) -> TestResult<BTreeMap<String, rustix::process::Pid>> {
    current(reconfigurer)?
        .target
        .document()
        .plugin_instances()
        .keys()
        .map(|id| {
            Ok((
                id.as_str().to_owned(),
                recorded_pid(&instance_directory(root, id.as_str())?)?,
            ))
        })
        .collect()
}

fn assert_no_final_shutdown(
    root: &Path,
    pids: &BTreeMap<String, rustix::process::Pid>,
) -> TestResult {
    for id in pids.keys() {
        assert!(
            !instance_directory(root, id)?
                .join("shutdown.received")
                .exists()
        );
    }
    Ok(())
}
