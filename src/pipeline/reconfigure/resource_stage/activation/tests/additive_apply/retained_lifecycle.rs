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

//! Candidate preparation must not suspend retained Instance lifecycle work.

use super::failure::extend_initialization_budget;
use super::*;
use crate::pipeline::reconfigure::ReconfigureShutdown;
use std::time::Duration;

#[tokio::test(flavor = "current_thread")]
async fn retained_startup_reaches_ready_while_added_lua_is_still_initializing() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            extend_initialization_budget(reconfigurer, root)?;
            reconfigurer.apply(target, None).await?;
            assert!(
                current(reconfigurer)?
                    .status_snapshot()
                    .plugin_instances
                    .iter()
                    .all(|instance| instance.state() == PluginInstanceState::Starting)
            );
            during_preparation(reconfigurer, root, async {
                for id in ["input", "dual-a", "dual-b", "archive"] {
                    wait_for_ready_count(&instance_directory(root, id)?, 1).await?;
                }
                Ok(())
            })
            .await?;
            assert_eq!(
                current(reconfigurer)?.status_snapshot().document_etag,
                "initial-cutover"
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn retained_sink_failure_is_reaped_and_restarted_during_added_preparation() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            extend_initialization_budget(reconfigurer, root)?;
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let pids = instance_pids(root)?;
            let files = queue_files(root)?;
            during_preparation(reconfigurer, root, async {
                rustix::process::kill_process(pids["archive"], rustix::process::Signal::KILL)?;
                wait_for_ready_count(&instance_directory(root, "archive")?, 2).await?;
                assert_reaped(pids["archive"])?;
                Ok(())
            })
            .await?;
            let after = instance_pids(root)?;
            assert_ne!(after["archive"], pids["archive"]);
            for id in ["input", "dual-a", "dual-b"] {
                assert_eq!(after[id], pids[id]);
            }
            assert_eq!(queue_files(root)?, files);
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn already_failed_worker_is_not_hidden_by_stop_before_reapply() -> TestResult {
    with_environment("normal", &[("dual-to-archive", COUNTING_LUA), ("dual-to-dual", DUAL_EMIT_LUA)], async |reconfigurer, _, root, target| {
        reconfigurer.apply(target, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let pids = instance_pids(root)?;
        let next = next_revision(root, |_| {})?;
        let queue = std::fs::OpenOptions::new().write(true)
            .open(egress_queue_path(&instance_directory(root, "dual-b")?, "dual-to-dual", 0))?;
        let mut held = hold_dual_output(root).await?;
        corrupt_egress_commit(&queue)?;
        held.release(1)?;
        timeout(TEST_DEADLINE, current(reconfigurer)?.runtime.wait_for_worker_exit()).await?;
        reconfigurer.shutdown_handle().request(ReconfigureShutdown::Force);
        assert!(matches!(reconfigurer.apply(next, None).await,
            Err(PipelineReconfigureError::DataPlaneFailure(source))
                if matches!(source.as_ref(), PipelineRuntimeError::FlowChannelFailed { flow_id, .. } if flow_id.as_str() == "dual-to-dual")));
        assert!(reconfigurer.current.is_none());
        assert!(!root.exists());
        for pid in pids.into_values() { assert_reaped(pid)?; }
        Ok(())
    }).await
}

async fn during_preparation(
    reconfigurer: &mut Reconfigurer,
    root: &Path,
    check: impl std::future::Future<Output = TestResult>,
) -> TestResult {
    let next = next_revision(root, |document| {
        add_source_flow(
            document,
            "added-input",
            "added-flow",
            "archive",
            "while true do end",
        )
    })?;
    let shutdown = reconfigurer.shutdown_handle();
    let mut applying = Box::pin(reconfigurer.apply(next, None));
    let checked = tokio::select! {
        result = &mut applying => {
            result?;
            return Err("Apply finished before retained lifecycle work completed".into());
        }
        checked = timeout(TEST_DEADLINE, check) => checked,
    };
    shutdown.request(ReconfigureShutdown::Force);
    let stopped = timeout(TEST_DEADLINE, &mut applying).await;
    if stopped.is_err() {
        applying.as_mut().await?;
    }
    assert!(matches!(stopped??, PipelineApplyOutcome::Stopped));
    drop(applying);
    assert!(!instance_directory(root, "added-input")?.exists());
    checked??;
    Ok(())
}

pub(super) async fn wait_for_ready_count(directory: &Path, count: usize) -> TestResult {
    loop {
        let events = match lifecycle_events(directory) {
            Ok(events) => events,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error.into()),
        };
        if events.lines().filter(|event| *event == "ready").count() == count {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}
