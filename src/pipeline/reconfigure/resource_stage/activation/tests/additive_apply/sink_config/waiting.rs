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

//! Controls real shutdown waits to observe batch ordering and retained work.

use super::super::retained_lifecycle::wait_for_ready_count;
use super::*;
use rustix::process::{Signal, kill_process, test_kill_process};

#[tokio::test(flavor = "current_thread")]
async fn healthy_old_sinks_are_reaped_before_their_replacements_or_additions_start() -> TestResult {
    for first_behavior in ["delay-shutdown", "exit-before-ready"] {
        with_environment("normal", &[], async |reconfigurer, _, root, _| {
            let configure_pair = |document: &mut Value, behavior: &str| {
                document["pluginInstances"]["archive"]["config"]["behavior"] = json!(behavior);
                document["pluginInstances"]["archive-z"] =
                    instance("com.example.archive", "second");
                document["pluginInstances"]["archive-z"]["config"]["behavior"] = json!(behavior);
                document["flows"]["dual-to-archive"]["sinks"] = json!(["archive", "archive-z"]);
            };
            let initial = next_revision(root, |document| {
                document["pluginInstances"]["backup"] = instance("com.example.archive", "retained");
                document["flows"]["input-to-dual"]["sinks"] = json!(["dual-a", "backup"]);
                configure_pair(document, "delay-shutdown");
                document["pluginInstances"]["archive"]["config"]["behavior"] =
                    json!(first_behavior);
            })?;
            reconfigurer.apply(initial, None).await?;
            let exceptions = if first_behavior == "exit-before-ready" {
                vec![("archive", PluginInstanceState::StartFailed)]
            } else {
                Vec::new()
            };
            wait_for_states(current(reconfigurer)?, &exceptions).await?;
            let archive = instance_directory(root, "archive")?;
            let second = instance_directory(root, "archive-z")?;
            let old = recorded_pid(&archive)?;
            let old_second = recorded_pid(&second)?;
            let retained_input = instance_directory(root, "backup")?;
            let input_pid = recorded_pid(&retained_input)?;
            let files = queue_files(root)?;
            let mut output = sink_reader(root, "archive", "dual-to-archive", 0)?;
            let next = next_revision(root, |document| {
                document["pluginInstances"]["backup"] = instance("com.example.archive", "retained");
                document["flows"]["input-to-dual"]["sinks"] = json!(["dual-a", "backup"]);
                configure_pair(document, "normal");
                document["flows"]["dual-to-archive"]["process"]["script"] =
                    json!(format!("setTimeout(0); {ADDED_LUA}"));
                add_source_flow(
                    document,
                    "added-input",
                    "added-flow",
                    "archive",
                    &format!("setTimeout(0); {ADDED_LUA}"),
                );
            })?;
            let applied = during_apply(reconfigurer, next, async {
                // Every changed Queue set waits for installation before a new child starts.
                if first_behavior == "delay-shutdown" {
                    wait_for_file(&archive.join("shutdown.received")).await?;
                    std::fs::write(archive.join("allow-shutdown"), [])?;
                }
                wait_for_file(&second.join("shutdown.received")).await?;
                // Signal 0 observes disappearance without reaping on behalf of the owner.
                while test_kill_process(old) != Err(rustix::io::Errno::SRCH) {
                    tokio::task::yield_now().await;
                }
                // A retained retry must work even with a Stopped owner still in the map.
                kill_process(input_pid, Signal::KILL)?;
                wait_for_ready_count(&retained_input, 2).await?;
                assert_reaped(input_pid)?;
                assert_started_once(root, &["archive-z"])?;
                if first_behavior == "delay-shutdown" {
                    assert_started_once(root, &["archive"])?;
                } else {
                    assert_eq!(recorded_pid(&archive)?, old);
                    assert!(!archive.join("ready.received").exists());
                }
                assert!(
                    !instance_directory(root, "added-input")?
                        .join("starts.received")
                        .exists()
                );
                assert!(matches!(output.try_read()?, ReadOutcome::Empty));
                assert_eq!(queue_files(root)?, files);
                std::fs::write(second.join("allow-shutdown"), [])?;
                Ok(())
            })
            .await?;
            assert!(matches!(applied?, PipelineApplyOutcome::Applied(_)));
            assert_reaped(old)?;
            assert_reaped(old_second)?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            assert_ne!(recorded_pid(&archive)?, old);
            assert_ne!(recorded_pid(&second)?, old_second);
            assert_started_once(root, &["dual-a", "dual-b", "added-input"])?;
            assert_eq!(read_value(&mut output).await?, "added");
            let mut added_output = sink_reader(root, "archive", "added-flow", 0)?;
            assert_eq!(read_value(&mut added_output).await?, "added");
            let mut second_output = sink_reader(root, "archive-z", "dual-to-archive", 0)?;
            assert_eq!(read_value(&mut second_output).await?, "added");
            Ok(())
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retained_startup_retry_and_queue_traffic_continue_during_sink_shutdown() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let initial = next_revision(root, |document| {
            document["pluginInstances"]["backup"] = instance("com.example.archive", "retained");
            document["flows"]["input-to-dual"]["sinks"] = json!(["dual-a", "backup"]);
            document["pluginInstances"]["archive"]["config"]["behavior"] = json!("delay-shutdown");
            document["pluginInstances"]["dual-b"]["config"]["behavior"] = json!("delay-ready");
        })?;
        reconfigurer.apply(initial, None).await?;
        wait_for_states(
            current(reconfigurer)?,
            &[("dual-b", PluginInstanceState::Starting)],
        )
        .await?;
        let archive = instance_directory(root, "archive")?;
        let delayed = instance_directory(root, "dual-b")?;
        wait_for_file(&delayed.join("attach.received")).await?;
        let before = instance_pids(root)?;
        let backup_pid = recorded_pid(&instance_directory(root, "backup")?)?;
        let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
        let mut output = sink_reader(root, "archive", "dual-to-archive", 0)?;
        let next = next_revision(root, |document| {
            document["pluginInstances"]["backup"] = instance("com.example.archive", "retained");
            document["flows"]["input-to-dual"]["sinks"] = json!(["dual-a", "backup"]);
            document["pluginInstances"]["archive"]["config"]["endpoint"] = json!("after");
            document["pluginInstances"]["dual-b"]["config"]["behavior"] = json!("delay-ready");
        })?;
        let applied = during_apply(reconfigurer, next, async {
            wait_for_file(&archive.join("shutdown.received")).await?;
            std::fs::write(delayed.join("allow-ready"), [])?;
            wait_for_ready_count(&delayed, 1).await?;
            kill_process(backup_pid, Signal::KILL)?;
            wait_for_ready_count(&instance_directory(root, "backup")?, 2).await?;
            assert_reaped(backup_pid)?;
            // Even traffic targeting the stopped Sink still enters its retained Queue.
            for id in [1, 2] {
                submit(&mut source, id)?;
                assert_eq!(read_value(&mut output).await?, format!("stable:{id}"));
            }
            assert_started_once(root, &["archive", "dual-a", "dual-b"])?;
            std::fs::write(archive.join("allow-shutdown"), [])?;
            Ok(())
        })
        .await?;
        assert!(matches!(applied?, PipelineApplyOutcome::Applied(_)));
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let after = instance_pids(root)?;
        assert_eq!(after["dual-a"], before["dual-a"]);
        assert_eq!(after["dual-b"], before["dual-b"]);
        assert_eq!(after["input"], before["input"]);
        assert_ne!(
            recorded_pid(&instance_directory(root, "backup")?)?,
            backup_pid
        );
        assert_ne!(after["archive"], before["archive"]);
        submit(&mut source, 3)?;
        assert_eq!(read_value(&mut output).await?, "stable:3");
        Ok(())
    })
    .await
}
