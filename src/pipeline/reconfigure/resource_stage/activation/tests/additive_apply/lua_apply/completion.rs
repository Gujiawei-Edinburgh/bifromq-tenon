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

//! Simulates inherited Completion occupancy after Source restart, not SDK admission.
//! Only the actual Channel writes Completion; the reader holds its shared release.

use super::*;
use crate::pipeline::reconfigure::ReconfigureShutdown;

#[tokio::test(flavor = "current_thread")]
async fn full_completion_holds_all_candidate_timers_until_cutover_finishes() -> TestResult {
    with_environment(
        "normal",
        &[
            ("dual-to-dual", DUAL_EMIT_LUA),
            ("input-to-dual", COMPLETING_LUA),
            ("dual-to-archive", COUNTING_LUA),
        ],
        async |reconfigurer, _, root, target| {
            let captured = Capture::new();
            publish_channel_metrics(reconfigurer, &captured);
            reconfigurer.apply(target, None).await?;
            let (mut source, mut completion) = fill_completion(
                root,
                current(reconfigurer)?.target.document().flows()
                    [&crate::identifiers::FlowId::try_from(String::from("input-to-dual"))?]
                    .max_pending_records()
                    .get(),
            )
            .await?;
            let next = timer_revision(root)?;
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            let applied = during_apply(reconfigurer, next, async {
                wait_for_completion_capacity(&captured, "input-to-dual", 0).await?;
                assert!(
                    matches!(sink.try_read()?, ReadOutcome::Empty),
                    "Prepared and already-switched candidate timers must stay paused"
                );
                completion.release(1)?;
                Ok(())
            })
            .await?;
            assert!(matches!(applied?, PipelineApplyOutcome::Applied(_)));
            let record = IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?;
            assert_eq!(record.record_id, 7);
            assert_eq!(record.status(), IngressCompletionStatus::Retry);
            assert_eq!(read_value(&mut sink).await?, "added");
            let mut added_sink = sink_reader(root, "archive", "added-flow", 0)?;
            assert_eq!(read_value(&mut added_sink).await?, "added");
            completion.release(2)?;
            submit(&mut source, 8)?;
            let record = IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?;
            assert_eq!(record.record_id, 8);
            assert_eq!(record.status(), IngressCompletionStatus::Ok);
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn stop_breaks_full_completion_wait_and_removes_added_and_retained_queues() -> TestResult {
    with_environment(
        "normal",
        &[
            ("dual-to-dual", DUAL_EMIT_LUA),
            ("input-to-dual", COMPLETING_LUA),
            ("dual-to-archive", COUNTING_LUA),
        ],
        async |reconfigurer, _, root, target| {
            let captured = Capture::new();
            publish_channel_metrics(reconfigurer, &captured);
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let pids = instance_pids(root)?;
            let (_, _completion) = fill_completion(
                root,
                current(reconfigurer)?.target.document().flows()
                    [&crate::identifiers::FlowId::try_from(String::from("input-to-dual"))?]
                    .max_pending_records()
                    .get(),
            )
            .await?;
            let next = timer_revision(root)?;
            let shutdown = reconfigurer.shutdown_handle();
            let result = during_apply(reconfigurer, next, async {
                wait_for_completion_capacity(&captured, "input-to-dual", 0).await?;
                shutdown.request(ReconfigureShutdown::Force);
                Ok(())
            })
            .await?;
            assert!(matches!(result?, PipelineApplyOutcome::Stopped));
            // No controller loop owns `current` in this in-process fixture, so the
            // terminal shutdown the controller would run next is the test's own step.
            reconfigurer.shutdown(ReconfigureShutdown::Force).await?;
            assert!(reconfigurer.current.is_none());
            assert!(!root.exists());
            for pid in pids.into_values() {
                assert_reaped(pid)?;
            }
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn core_failure_during_blocked_cutover_keeps_its_cause_and_never_publishes_target()
-> TestResult {
    with_environment("normal", &[("input-to-dual", COMPLETING_LUA), ("dual-to-archive", COUNTING_LUA), ("dual-to-dual", DUAL_EMIT_LUA)], async |reconfigurer, _, root, target| {
        let captured = Capture::new();
        publish_channel_metrics(reconfigurer, &captured);
        reconfigurer.apply(target, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let pids = instance_pids(root)?;
        let (_, _completion) = fill_completion(root, current(reconfigurer)?.target.document().flows()[&crate::identifiers::FlowId::try_from(String::from("input-to-dual"))?].max_pending_records().get()).await?;
        let next = timer_revision(root)?;
        let queue = std::fs::OpenOptions::new().write(true).open(egress_queue_path(&instance_directory(root, "dual-b")?, "dual-to-dual", 0))?;
        let mut held = hold_dual_output(root).await?;
        let result = during_apply(reconfigurer, next, async {
            wait_for_completion_capacity(&captured, "input-to-dual", 0).await?;
            corrupt_egress_commit(&queue)?;
            held.release(1)?;
            Ok(())
        }).await?;
        let Err(PipelineReconfigureError::DataPlaneFailure(source)) = result else {
            return Err("Core failure was replaced by the interrupted replacement handshake".into());
        };
        assert!(matches!(source.as_ref(), PipelineRuntimeError::FlowChannelFailed { flow_id, .. } if flow_id.as_str() == "dual-to-dual"));
        assert!(source.source().and_then(Error::source).is_some());
        assert!(reconfigurer.current.is_none());
        assert!(!root.exists());
        for pid in pids.into_values() { assert_reaped(pid)?; }
        Ok(())
    }).await
}

fn timer_revision(root: &Path) -> TestResult<PipelineRevision> {
    next_revision(root, |document| {
        document["flows"]["dual-to-dual"]["process"]["script"] = json!(DUAL_EMIT_LUA);
        document["flows"]["input-to-dual"]["process"]["script"] =
            json!("function main(event) emit() end");
        document["flows"]["dual-to-archive"]["process"]["script"] =
            json!(format!("setTimeout(0); {ADDED_LUA}"));
        add_source_flow(
            document,
            "added-input",
            "added-flow",
            "archive",
            &format!("setTimeout(0); {ADDED_LUA}"),
        );
    })
}
