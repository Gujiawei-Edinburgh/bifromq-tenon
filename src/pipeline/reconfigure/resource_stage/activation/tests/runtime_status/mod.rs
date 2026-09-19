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

//! Drives published lifecycle events through the new runtime observer.

use super::*;
use crate::pipeline::reconfigure::operation::DataPlaneOperation;
use crate::pipeline::reconfigure::{PipelineApplyOutcome, ReconfigureShutdown};
use std::fs::File;
use std::io;
use std::time::Duration;

mod source_recovery;
use crate::pipeline::reconfigure::test_support::clock::with_frozen_clock;

#[tokio::test(flavor = "current_thread")]
async fn canceled_ready_observation_retains_progress_and_returns_complete_status() -> TestResult {
    with_environment("delay-ready", &[], async |reconfigurer, _, root, target| {
        apply(reconfigurer, target).await?;
        observed_states(reconfigurer, &[("dual-b", PluginInstanceState::Starting)]).await?;
        let delayed = instance_directory(root, "dual-b")?;
        wait_for_file(&delayed.join("attach.received")).await?;
        let pid = recorded_pid(&delayed)?;
        cancel_pending_observation(reconfigurer).await?;
        assert!(root.exists());
        std::fs::write(delayed.join("allow-ready"), [])?;
        observed_states(reconfigurer, &[]).await?;
        assert_eq!(recorded_pid(&delayed)?, pid);
        assert_eq!(
            std::fs::read_to_string(delayed.join("starts.received"))?,
            "started\n"
        );
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn ready_failure_restarts_only_its_instance_and_retains_unreleased_queue_data() -> TestResult
{
    with_environment(
        "normal",
        &[("dual-to-dual", DUAL_EMIT_LUA)],
        async |reconfigurer, _, root, target| {
            apply(reconfigurer, target).await?;
            observed_states(reconfigurer, &[]).await?;
            let original_pids = instance_pids(root)?;
            let files = queue_files(root)?;
            // Keep the old inodes alive so unlink/recreate cannot reuse their identities.
            let _open_queues = files
                .keys()
                .map(File::open)
                .collect::<io::Result<Vec<_>>>()?;
            let failed = instance_directory(root, "dual-b")?;
            let config = std::fs::read_to_string(failed.join("configs.received"))?;
            let program = std::fs::read_to_string(failed.join("program.received"))?;
            let launch_id = std::fs::read_to_string(failed.join("launch-ids.received"))?;
            let held = hold_dual_output(root).await?;
            drop(held);
            let mut reader = sink_reader(root, "dual-b", "dual-to-dual", 0)?;
            let unreleased = read_record(&mut reader).await?;
            drop(reader);

            with_frozen_clock(async {
                rustix::process::kill_process(
                    original_pids["dual-b"],
                    rustix::process::Signal::KILL,
                )?;
                observed_states(
                    reconfigurer,
                    &[("dual-b", PluginInstanceState::RestartBackoff)],
                )
                .await?;
                assert_reaped(original_pids["dual-b"])?;
                let delay = reconfigurer.environment.retry_backoff().initial_delay();
                let original_deadline = tokio::time::Instant::now() + delay;
                tokio::time::advance(delay / 2).await;
                cancel_pending_observation(reconfigurer).await?;
                tokio::time::advance(delay - delay / 2 + Duration::from_millis(1)).await;
                // A reset at cancellation would not be due at this original deadline.
                observed_states(reconfigurer, &[("dual-b", PluginInstanceState::Starting)]).await?;
                assert_eq!(
                    tokio::time::Instant::now(),
                    original_deadline + Duration::from_millis(1)
                );
                Ok(())
            })
            .await?;
            observed_states(reconfigurer, &[]).await?;

            let restarted_pids = instance_pids(root)?;
            for id in ["input", "dual-a", "archive"] {
                assert_eq!(restarted_pids[id], original_pids[id]);
                assert_eq!(
                    std::fs::read_to_string(instance_directory(root, id)?.join("starts.received"))?,
                    "started\n"
                );
            }
            assert_ne!(restarted_pids["dual-b"], original_pids["dual-b"]);
            assert_eq!(
                std::fs::read_to_string(failed.join("starts.received"))?,
                "started\nstarted\n"
            );
            assert_eq!(
                std::fs::read_to_string(failed.join("configs.received"))?,
                config.repeat(2)
            );
            assert_eq!(
                std::fs::read_to_string(failed.join("program.received"))?,
                program
            );
            let launch_ids = std::fs::read_to_string(failed.join("launch-ids.received"))?;
            let ids: Vec<_> = launch_ids.lines().collect();
            assert_eq!(ids.len(), 2);
            assert_eq!(ids[0], launch_id.trim());
            assert_ne!(ids[0], ids[1]);
            let rebuilt_files = queue_files(root)?;
            assert_eq!(rebuilt_files.len(), files.len());
            for (path, identity) in files {
                if path.starts_with(failed.join("source")) {
                    assert_ne!(rebuilt_files[&path], identity);
                } else {
                    assert_eq!(rebuilt_files[&path], identity);
                }
            }
            // The lifecycle child does not consume Queues. This reader proves that
            // reopening the same Queue retains data until an explicit release.
            let mut reader = sink_reader(root, "dual-b", "dual-to-dual", 0)?;
            assert_eq!(read_record(&mut reader).await?, unreleased);
            reader.release(1)?;
            reconfigurer
                .shutdown_handle()
                .request(ReconfigureShutdown::Force);
            assert!(matches!(
                reconfigurer.observe_runtime().await?,
                DataPlaneOperation::Shutdown(ReconfigureShutdown::Force)
            ));
            assert_eq!(instance_pids(root)?, restarted_pids);
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_ready_failures_double_retry_delay_until_the_configured_cap() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, target| {
        let mut input = crate::pipeline::reconfigure::test_support::bootstrap(root)?;
        let backoff = input
            .environment
            .as_mut()
            .and_then(|environment| environment.retry_backoff.as_mut())
            .ok_or("Fixture retry backoff is missing")?;
        backoff.initial_delay_ms = 10;
        backoff.maximum_delay_ms = 40;
        let environment = input.environment.ok_or("Fixture environment is missing")?;
        reconfigurer.environment = Arc::new(environment);
        apply(reconfigurer, target).await?;
        observed_states(reconfigurer, &[]).await?;
        let failed = instance_directory(root, "dual-b")?;
        for delay_ms in [10, 20, 40, 40] {
            let previous_pid = recorded_pid(&failed)?;
            with_frozen_clock(async {
                rustix::process::kill_process(previous_pid, rustix::process::Signal::KILL)?;
                observed_states(
                    reconfigurer,
                    &[("dual-b", PluginInstanceState::RestartBackoff)],
                )
                .await?;
                assert_reaped(previous_pid)?;
                tokio::time::advance(Duration::from_millis(delay_ms - 1)).await;
                cancel_pending_observation(reconfigurer).await?;
                assert_eq!(recorded_pid(&failed)?, previous_pid);
                // Tokio rounds timer wakeups to the next millisecond.
                tokio::time::advance(Duration::from_millis(2)).await;
                observed_states(reconfigurer, &[("dual-b", PluginInstanceState::Starting)]).await?;
                Ok(())
            })
            .await?;
            observed_states(reconfigurer, &[]).await?;
            assert_ne!(recorded_pid(&failed)?, previous_pid);
        }
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn equivalent_reapply_preserves_the_original_retry_deadline() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, target| {
        let repeated = model(revision(
            root.parent().ok_or("Fixture parent is missing")?,
            "normal",
        )?)?;
        apply(reconfigurer, target).await?;
        observed_states(reconfigurer, &[]).await?;
        let original_pids = instance_pids(root)?;
        with_frozen_clock(async {
            rustix::process::kill_process(original_pids["dual-b"], rustix::process::Signal::KILL)?;
            observed_states(
                reconfigurer,
                &[("dual-b", PluginInstanceState::RestartBackoff)],
            )
            .await?;
            let delay = reconfigurer.environment.retry_backoff().initial_delay();
            let deadline = tokio::time::Instant::now() + delay;
            tokio::time::advance(delay / 2).await;
            apply(reconfigurer, repeated).await?;
            assert_eq!(
                states(&current(reconfigurer)?.status_snapshot())["dual-b"],
                "restart-backoff"
            );
            tokio::time::advance(delay - delay / 2 + Duration::from_millis(1)).await;
            observed_states(reconfigurer, &[("dual-b", PluginInstanceState::Starting)]).await?;
            assert_eq!(
                tokio::time::Instant::now(),
                deadline + Duration::from_millis(1)
            );
            Ok(())
        })
        .await?;
        observed_states(reconfigurer, &[]).await?;
        let current_pids = instance_pids(root)?;
        assert_ne!(current_pids["dual-b"], original_pids["dual-b"]);
        for id in ["input", "dual-a", "archive"] {
            assert_eq!(current_pids[id], original_pids[id]);
        }
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn failure_before_ready_never_acquires_a_retry_deadline() -> TestResult {
    with_environment(
        "exit-before-ready",
        &[],
        async |reconfigurer, _, root, target| {
            apply(reconfigurer, target).await?;
            let files = queue_files(root)?;
            // Keep the old inodes alive so unlink/recreate cannot reuse their identities.
            let _open_queues = files
                .keys()
                .map(File::open)
                .collect::<io::Result<Vec<_>>>()?;
            observed_states(
                reconfigurer,
                &[("dual-b", PluginInstanceState::StartFailed)],
            )
            .await?;
            let failed = instance_directory(root, "dual-b")?;
            let rebuilt = queue_files(root)?;
            for (path, identity) in files {
                if path.starts_with(failed.join("source")) {
                    assert_ne!(rebuilt[&path], identity);
                } else {
                    assert_eq!(rebuilt[&path], identity);
                }
            }
            let pid = recorded_pid(&failed)?;
            assert_reaped(pid)?;
            with_frozen_clock(async {
                let delay = reconfigurer.environment.retry_backoff().initial_delay();
                tokio::time::advance(delay + Duration::from_millis(1)).await;
                cancel_pending_observation(reconfigurer).await?;
                Ok(())
            })
            .await?;
            assert_eq!(recorded_pid(&failed)?, pid);
            assert_eq!(
                std::fs::read_to_string(failed.join("starts.received"))?,
                "started\n"
            );
            assert_eq!(
                states(&current(reconfigurer)?.status_snapshot())["dual-b"],
                "start-failed"
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn completed_stop_request_wins_over_an_already_due_retry() -> TestResult {
    for shutdown in [ReconfigureShutdown::Planned, ReconfigureShutdown::Force] {
        with_environment("normal", &[], async |reconfigurer, _, root, target| {
            apply(reconfigurer, target).await?;
            observed_states(reconfigurer, &[]).await?;
            let failed = instance_directory(root, "dual-b")?;
            let pid = recorded_pid(&failed)?;
            with_frozen_clock(async {
                rustix::process::kill_process(pid, rustix::process::Signal::KILL)?;
                observed_states(
                    reconfigurer,
                    &[("dual-b", PluginInstanceState::RestartBackoff)],
                )
                .await?;
                assert_reaped(pid)?;
                let delay = reconfigurer.environment.retry_backoff().initial_delay();
                tokio::time::advance(delay + Duration::from_millis(1)).await;
                reconfigurer.shutdown_handle().request(shutdown);
                for _ in 0..2 {
                    match reconfigurer.observe_runtime().await? {
                        DataPlaneOperation::Shutdown(observed) => assert_eq!(observed, shutdown),
                        _ => return Err("A due retry escaped a completed stop request".into()),
                    }
                }
                Ok(())
            })
            .await?;
            assert_eq!(
                std::fs::read_to_string(failed.join("starts.received"))?,
                "started\n"
            );
            assert!(reconfigurer.current.is_some());
            assert!(root.exists());
            Ok(())
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn published_worker_failure_is_observed_before_explicit_terminal_cleanup() -> TestResult {
    with_environment("normal", &[("dual-to-dual", DUAL_EMIT_LUA)], async |reconfigurer, _, root, target| {
        apply(reconfigurer, target).await?;
        observed_states(reconfigurer, &[]).await?;
        let pids = instance_pids(root)?;
        let queue = std::fs::OpenOptions::new()
            .write(true)
            .open(egress_queue_path(&instance_directory(root, "dual-b")?, "dual-to-dual", 0))?;
        let mut held = hold_dual_output(root).await?;
        corrupt_egress_commit(&queue)?;
        held.release(1)?;
        timeout(
            TEST_DEADLINE,
            current(reconfigurer)?.runtime.wait_for_worker_exit(),
        )
        .await?;
        // A simultaneous shutdown cannot hide the already observed worker failure.
        reconfigurer
            .shutdown_handle()
            .request(ReconfigureShutdown::Force);
        assert!(matches!(
            reconfigurer.observe_runtime().await?,
            DataPlaneOperation::WorkerExited
        ));
        assert!(reconfigurer.current.is_some());
        assert!(root.exists());
        let error = reconfigurer.finish_worker_failure().await;
        let PipelineReconfigureError::DataPlaneFailure(source) = &error else {
            return Err("Published worker failure lost its original cause".into());
        };
        assert!(matches!(source.as_ref(),
            PipelineRuntimeError::FlowChannelFailed { flow_id, .. } if flow_id.as_str() == "dual-to-dual"
        ));
        assert!(source.source().and_then(Error::source).is_some());
        assert!(reconfigurer.current.is_none());
        assert!(!root.exists());
        for pid in pids.into_values() {
            assert_reaped(pid)?;
        }
        Ok(())
    })
    .await
}

async fn apply(reconfigurer: &mut Reconfigurer, target: PipelineRevision) -> TestResult {
    let PipelineApplyOutcome::Applied(status) = reconfigurer.apply(target, None).await? else {
        return Err("Initial apply unexpectedly stopped".into());
    };
    assert_complete(&status);
    Ok(())
}

async fn observed_states(
    reconfigurer: &mut Reconfigurer,
    exceptions: &[(&str, PluginInstanceState)],
) -> TestResult {
    let expected: BTreeMap<_, _> = ["input", "dual-a", "dual-b", "archive"]
        .into_iter()
        .map(|id| {
            let state = exceptions
                .iter()
                .find(|(exception, _)| *exception == id)
                .map_or(PluginInstanceState::Running, |(_, state)| *state);
            (id.to_owned(), state)
        })
        .collect();
    timeout(TEST_DEADLINE, async {
        loop {
            let snapshot = current(reconfigurer)?.status_snapshot();
            assert_complete(&snapshot);
            let actual: BTreeMap<_, _> = snapshot
                .plugin_instances
                .iter()
                .map(|instance| (instance.id.clone(), instance.state()))
                .collect();
            if actual == expected {
                return Ok(());
            }
            match reconfigurer.observe_runtime().await? {
                DataPlaneOperation::Completed(event) => {
                    let status = reconfigurer.handle_runtime_event(event).await?;
                    assert_complete(&status);
                    assert_eq!(status, current(reconfigurer)?.status_snapshot());
                }
                _ => return Err("Runtime stopped while observing Instance status".into()),
            }
        }
    })
    .await?
}

fn assert_complete(status: &PipelineStatusSnapshot) {
    assert_eq!(status.document_etag, "initial-cutover");
    assert_eq!(
        status
            .plugin_instances
            .iter()
            .map(|instance| instance.id.as_str())
            .collect::<Vec<_>>(),
        ["archive", "dual-a", "dual-b", "input"]
    );
}

async fn cancel_pending_observation(reconfigurer: &mut Reconfigurer) -> TestResult {
    let mut observing = Box::pin(reconfigurer.observe_runtime());
    poll_fn(|context| {
        assert!(observing.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(observing);
    Ok(())
}
