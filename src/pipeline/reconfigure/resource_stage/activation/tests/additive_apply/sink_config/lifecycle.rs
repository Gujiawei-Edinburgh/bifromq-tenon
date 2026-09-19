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

//! Replacement finishes a pending launch, and accepts already failed or backing-off states.

use super::*;
use rustix::io::Errno;
use rustix::process::{Signal, WaitOptions, kill_process, waitpid};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test(flavor = "current_thread")]
async fn starting_sink_finishes_starting_before_it_is_replaced() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let initial = next_revision(root, |document| {
            document["pluginInstances"]["archive"]["config"]["behavior"] = json!("delay-ready");
        })?;
        reconfigurer.apply(initial, None).await?;
        wait_for_states(
            current(reconfigurer)?,
            &[("archive", PluginInstanceState::Starting)],
        )
        .await?;
        let archive = instance_directory(root, "archive")?;
        wait_for_file(&archive.join("attach.received")).await?;
        let old = recorded_pid(&archive)?;
        let next = next_revision(root, |document| {
            document["pluginInstances"]["archive"]["config"]["endpoint"] = json!("after")
        })?;
        // A pending launch may already hold Egress output a Channel committed
        // to it, and only that child can release it. Replacing it therefore
        // waits for the launch to finish instead of force stopping the peer
        // that still owes those releases.
        let outcome = during_apply(reconfigurer, next, async {
            // Give the cutover every chance to act before this check lets the
            // pending launch reach Ready.
            sleep(Duration::from_millis(200)).await;
            assert!(
                waitpid(Some(old), WaitOptions::NOHANG).err() != Some(Errno::CHILD),
                "The pending launch was reaped before it finished starting"
            );
            assert!(!archive.join("allow-ready").exists());
            std::fs::write(archive.join("allow-ready"), [])?;
            Ok(())
        })
        .await?;
        assert!(matches!(outcome, Ok(PipelineApplyOutcome::Applied(_))));
        assert_reaped(old)?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        assert_ne!(recorded_pid(&archive)?, old);
        assert!(archive.join("allow-ready").exists());
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn start_failed_sink_can_be_replaced() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let initial = next_revision(root, |document| {
            document["pluginInstances"]["archive"]["config"]["behavior"] =
                json!("exit-before-ready");
        })?;
        reconfigurer.apply(initial, None).await?;
        wait_for_states(
            current(reconfigurer)?,
            &[("archive", PluginInstanceState::StartFailed)],
        )
        .await?;
        let archive = instance_directory(root, "archive")?;
        let old = recorded_pid(&archive)?;
        assert_reaped(old)?;
        let next = next_revision(root, |document| {
            document["pluginInstances"]["archive"]["config"]["endpoint"] = json!("after")
        })?;
        assert!(matches!(
            reconfigurer.apply(next, None).await?,
            PipelineApplyOutcome::Applied(_)
        ));
        wait_for_states(current(reconfigurer)?, &[]).await?;
        assert_ne!(recorded_pid(&archive)?, old);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn backoff_sink_can_be_replaced_with_target_config() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let archive = instance_directory(root, "archive")?;
            let old = recorded_pid(&archive)?;
            kill_process(old, Signal::KILL)?;
            wait_for_states(
                current(reconfigurer)?,
                &[("archive", PluginInstanceState::RestartBackoff)],
            )
            .await?;
            let next = next_revision(root, |document| {
                document["pluginInstances"]["archive"]["config"]["endpoint"] = json!("after")
            })?;
            assert!(matches!(
                reconfigurer.apply(next, None).await?,
                PipelineApplyOutcome::Applied(_)
            ));
            wait_for_states(current(reconfigurer)?, &[]).await?;
            assert_reaped(old)?;
            let received = std::fs::read_to_string(archive.join("configs.received"))?;
            let configs: Vec<Value> = received
                .lines()
                .map(serde_json::from_str)
                .collect::<Result<_, _>>()?;
            // Stage may legitimately process an already due old retry before Cutover.
            // Once replacement completes, the received material must be the target.
            assert_eq!(
                configs.last().ok_or("Target config is missing")?["endpoint"],
                "after"
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn new_sink_ready_is_not_a_publication_barrier() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let next = next_revision(root, |document| {
                document["pluginInstances"]["archive"]["config"]["behavior"] = json!("delay-ready")
            })?;
            let PipelineApplyOutcome::Applied(status) = reconfigurer.apply(next, None).await?
            else {
                return Err("Replacement unexpectedly stopped".into());
            };
            assert_eq!(status.document_etag, "updated");
            assert_eq!(states(&status)["archive"], "starting");
            wait_for_states(
                current(reconfigurer)?,
                &[("archive", PluginInstanceState::Starting)],
            )
            .await?;
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            assert_eq!(read_value(&mut sink).await?, "stable:1");
            assert_eq!(
                current(reconfigurer)?.status_snapshot().document_etag,
                "updated"
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn new_sink_start_failure_does_not_roll_back_the_published_config() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let next = next_revision(root, |document| {
                document["pluginInstances"]["archive"]["config"]["behavior"] =
                    json!("exit-before-ready")
            })?;
            assert!(matches!(
                reconfigurer.apply(next, None).await?,
                PipelineApplyOutcome::Applied(_)
            ));
            wait_for_states(
                current(reconfigurer)?,
                &[("archive", PluginInstanceState::StartFailed)],
            )
            .await?;
            let status = current(reconfigurer)?.status_snapshot();
            assert_eq!(status.document_etag, "updated");
            assert!(
                status
                    .plugin_instances
                    .iter()
                    .find(|instance| instance.id == "archive")
                    .ok_or("Sink status is missing")?
                    .last_error
                    .is_some()
            );
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            assert_eq!(read_value(&mut sink).await?, "stable:1");
            Ok(())
        },
    )
    .await
}
