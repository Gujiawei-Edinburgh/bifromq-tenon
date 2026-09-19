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

//! Real Queue waits must preserve old responsibility without blocking control work.

use super::super::failure::extend_initialization_budget;
use super::super::retained_lifecycle::wait_for_ready_count;
use super::*;
use crate::pipeline::diagnostics::test_support::interested_flow_channel;
use crate::pipeline::reconfigure::ReconfigureShutdown;

const DUAL_COUNTING_LUA: &str = "local b = registry:getBuilder('com.example.dual@1.0.0'); local count = 0; function main(event) count = count + 1; b:setValue('unaffected:' .. count); emit(b:build()) end";

#[tokio::test(flavor = "current_thread")]
async fn core_failure_during_candidate_initialization_keeps_its_cause() -> TestResult {
    with_environment("normal", &[("dual-to-archive", COUNTING_LUA), ("dual-to-dual", DUAL_EMIT_LUA)], async |reconfigurer, _, root, target| {
        extend_initialization_budget(reconfigurer, root)?;
        let (diagnostics, mut records) = interested_flow_channel(&FlowId::try_from(String::from("dual-to-archive"))?, 0);
        reconfigurer.diagnostics = diagnostics;
        reconfigurer.apply(target, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let pids = instance_pids(root)?;
        let queue = std::fs::OpenOptions::new().write(true).open(egress_queue_path(&instance_directory(root, "dual-b")?, "dual-to-dual", 0))?;
        let mut held = hold_dual_output(root).await?;
        let next = next_revision(root, |document| document["flows"]["dual-to-archive"]["process"]["script"] = json!("print('initializing'); while true do end"))?;
        let result = during_apply(reconfigurer, next, async {
            records.recv().await.ok_or("Initialization diagnostic is missing")?;
            corrupt_egress_commit(&queue)?;
            held.release(1)?;
            Ok(())
        }).await?;
        let Err(PipelineReconfigureError::DataPlaneFailure(source)) = result else {
            return Err("Core failure lost its original cause during Lua preparation".into());
        };
        assert!(matches!(source.as_ref(), PipelineRuntimeError::FlowChannelFailed { flow_id, .. } if flow_id.as_str() == "dual-to-dual"));
        assert!(source.source().and_then(Error::source).is_some());
        assert!(reconfigurer.current.is_none());
        assert!(!root.exists());
        for pid in pids.into_values() { assert_reaped(pid)?; }
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn waiting_for_old_release_keeps_other_flow_and_plugin_retry_live() -> TestResult {
    with_environment(
        "normal",
        &[
            ("dual-to-archive", COUNTING_LUA),
            ("dual-to-dual", DUAL_COUNTING_LUA),
        ],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let pids = instance_pids(root)?;
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            let mut completion = source_completion(root, "dual-b", "dual-to-archive", 0)?;
            let mut other_source = source_writer(root, "dual-a", "dual-to-dual", 0)?;
            let mut other_sink = sink_reader(root, "dual-b", "dual-to-dual", 0)?;
            submit(&mut source, 1)?;
            let old = EgressRecord::decode(read_record(&mut sink).await?.as_slice())?;
            assert_eq!(
                TestPayload::decode(old.payload.as_slice())?.value,
                "stable:1"
            );
            assert!(matches!(completion.try_read()?, ReadOutcome::Empty));
            let next = next_revision(root, |document| {
                document["flows"]["dual-to-archive"]["process"]["script"] = json!(ADDED_LUA);
                document["flows"]["dual-to-dual"]["process"]["script"] = json!(DUAL_COUNTING_LUA);
            })?;
            let applied = during_apply(reconfigurer, next, async {
                for id in [1, 2] {
                    submit(&mut other_source, id)?;
                    assert_eq!(
                        read_value(&mut other_sink).await?,
                        format!("unaffected:{id}")
                    );
                }
                rustix::process::kill_process(pids["archive"], rustix::process::Signal::KILL)?;
                wait_for_ready_count(&instance_directory(root, "archive")?, 2).await?;
                assert_reaped(pids["archive"])?;
                assert!(matches!(completion.try_read()?, ReadOutcome::Empty));
                sink.release(1)?;
                Ok(())
            })
            .await?;
            assert!(matches!(applied?, PipelineApplyOutcome::Applied(_)));
            let done = IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?;
            assert_eq!(done.record_id, 1);
            assert_eq!(done.status(), IngressCompletionStatus::Ok);
            completion.release(1)?;
            submit(&mut source, 2)?;
            assert_eq!(read_value(&mut sink).await?, "added");
            submit(&mut other_source, 3)?;
            assert_eq!(read_value(&mut other_sink).await?, "unaffected:3");
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn stop_interrupts_candidate_lua_and_joins_before_removing_queues() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            extend_initialization_budget(reconfigurer, root)?;
            let (diagnostics, mut records) =
                interested_flow_channel(&FlowId::try_from(String::from("dual-to-archive"))?, 0);
            reconfigurer.diagnostics = diagnostics;
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let pids = instance_pids(root)?;
            let next = next_revision(root, |document| {
                document["flows"]["dual-to-archive"]["process"]["script"] =
                    json!("print('initializing'); while true do end")
            })?;
            let shutdown = reconfigurer.shutdown_handle();
            let stopped = during_apply(reconfigurer, next, async {
                records
                    .recv()
                    .await
                    .ok_or("Candidate initialization diagnostic is missing")?;
                shutdown.request(ReconfigureShutdown::Force);
                Ok(())
            })
            .await?;
            assert!(matches!(stopped?, PipelineApplyOutcome::Stopped));
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
async fn stop_interrupts_replacement_waiting_for_old_sink_release() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            read_record(&mut sink).await?;
            let next = next_revision(root, |document| {
                document["flows"]["dual-to-archive"]["process"]["script"] = json!(ADDED_LUA)
            })?;
            let shutdown = reconfigurer.shutdown_handle();
            // The helper first polls apply, starting work before requesting stop.
            let stopped = during_apply(reconfigurer, next, async {
                shutdown.request(ReconfigureShutdown::Force);
                Ok(())
            })
            .await?;
            assert!(matches!(stopped?, PipelineApplyOutcome::Stopped));
            // The controller follows Stopped with terminal shutdown even when
            // the request arrives before Stage has sent a Channel command.
            reconfigurer.shutdown(ReconfigureShutdown::Force).await?;
            assert!(reconfigurer.current.is_none());
            assert!(!root.exists());
            Ok(())
        },
    )
    .await
}
