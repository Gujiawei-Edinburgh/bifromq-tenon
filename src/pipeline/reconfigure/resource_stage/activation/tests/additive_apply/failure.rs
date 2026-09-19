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

//! Keeps preparation failures and cancellation scoped to the candidate additions.

use super::*;
use crate::contracts::core::pipeline_diagnostic_record;
use crate::lua::LuaVmErrorKind;
use crate::pipeline::channel::FlowChannelError;
use crate::pipeline::diagnostics::test_support::interested_flow_channel;
use crate::pipeline::reconfigure::ReconfigureShutdown;
use crate::pipeline::reconfigure::test_support::bootstrap;

#[tokio::test(flavor = "current_thread")]
async fn later_added_flow_failure_cleans_only_additions_and_keeps_old_lua_running() -> TestResult {
    with_environment("normal", &[("dual-to-archive", COUNTING_LUA)], async |reconfigurer, _, root, target| {
        reconfigurer.apply(target, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let pids = instance_pids(root)?;
        let files = queue_files(root)?;
        let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
        let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
        submit(&mut source, 1)?;
        assert_eq!(read_value(&mut sink).await?, "stable:1");
        let failed = next_revision(root, |document| {
            add_source_flow(document, "added-a", "added-flow-a", "archive", &format!("setTimeout(0); {ADDED_LUA}"));
            add_source_flow(document, "added-z", "added-flow-z", "archive", "error('initialization failed')");
        })?;

        assert!(matches!(reconfigurer.apply(failed, None).await,
            Err(PipelineReconfigureError::RuntimeStart(PipelineRuntimeError::FlowChannelOpen {
                flow_id, channel_index: 0, source: FlowChannelError::LuaVmLoad { kind: LuaVmErrorKind::TopLevelFailed }
            })) if flow_id.as_str() == "added-flow-z"));
        assert_eq!(current(reconfigurer)?.status_snapshot().document_etag, "initial-cutover");
        assert_eq!(instance_pids(root)?, pids);
        assert_eq!(queue_files(root)?, files);
        for id in ["added-a", "added-z"] { assert!(!instance_directory(root, id)?.exists()); }
        assert!(matches!(sink.try_read()?, ReadOutcome::Empty));
        submit(&mut source, 2)?;
        assert_eq!(read_value(&mut sink).await?, "stable:2");
        let repaired = next_revision(root, |document| {
            add_source_flow(document, "added-a", "added-flow-a", "archive", ADDED_LUA);
            add_source_flow(document, "added-z", "added-flow-z", "archive", ADDED_LUA);
        })?;
        assert!(matches!(reconfigurer.apply(repaired, None).await?, PipelineApplyOutcome::Applied(_)));
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let mut added = source_writer(root, "added-z", "added-flow-z", 0)?;
        let mut added_sink = sink_reader(root, "archive", "added-flow-z", 0)?;
        submit(&mut added, 1)?;
        assert_eq!(read_value(&mut added_sink).await?, "added");
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn colliding_instance_directory_is_not_deleted_by_failed_preparation() -> TestResult {
    with_environment("normal", &[("dual-to-archive", COUNTING_LUA)], async |reconfigurer, _, root, target| {
        reconfigurer.apply(target, None).await?;
        let collision = instance_directory(root, "added-input")?;
        std::fs::create_dir(&collision)?;
        std::fs::write(collision.join("sentinel"), b"existing content")?;
        let next = next_revision(root, |document| add_source_flow(document, "added-input", "added-flow", "archive", ADDED_LUA))?;
        assert!(matches!(reconfigurer.apply(next, None).await,
            Err(PipelineReconfigureError::DirectoryCreate { source, .. }) if source.kind() == std::io::ErrorKind::AlreadyExists));
        assert_eq!(std::fs::read(collision.join("sentinel"))?, b"existing content");
        assert_eq!(current(reconfigurer)?.status_snapshot().document_etag, "initial-cutover");
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn stop_during_added_flow_preparation_joins_candidate_without_removing_current() -> TestResult
{
    with_environment("normal", &[("dual-to-archive", COUNTING_LUA)], async |reconfigurer, _, root, target| {
        extend_initialization_budget(reconfigurer, root)?;
        reconfigurer.apply(target, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let files = queue_files(root)?;
        let pids = instance_pids(root)?;
        let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
        let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
        let next = next_revision(root, |document| add_source_flow(document, "added-input", "added-flow", "archive", "print('initializing'); while true do end"))?;
        let (diagnostics, mut records) = interested_flow_channel(&FlowId::try_from(String::from("added-flow"))?, 0);
        reconfigurer.diagnostics = diagnostics;
        let shutdown = reconfigurer.shutdown_handle();
        let mut applying = Box::pin(reconfigurer.apply(next, None));
        let started = tokio::select! {
            record = timeout(TEST_DEADLINE, records.recv()) => record,
            result = &mut applying => { result?; return Err("Apply finished before its candidate was interrupted".into()); }
        };
        // Collect the result first; errors must still request stop and await the same apply.
        let old_traffic: TestResult<String> = async {
            submit(&mut source, 1)?;
            read_value(&mut sink).await
        }.await;
        shutdown.request(ReconfigureShutdown::Force);
        let stopped = timeout(TEST_DEADLINE, &mut applying).await;
        if stopped.is_err() { applying.as_mut().await?; }
        assert!(matches!(stopped??, PipelineApplyOutcome::Stopped));
        drop(applying);
        assert_eq!(old_traffic?, "stable:1");
        assert!(matches!(started?.ok_or("Initialization diagnostic is missing")?.record,
            Some(pipeline_diagnostic_record::Record::Channel(record)) if record.text == "initializing"));
        assert_eq!(current(reconfigurer)?.status_snapshot().document_etag, "initial-cutover");
        assert_eq!(queue_files(root)?, files);
        assert_eq!(instance_pids(root)?, pids);
        assert!(!instance_directory(root, "added-input")?.exists());
        // Shutdown remains requested. The fixture now terminates the retained owners.
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn retained_worker_failure_cancels_added_preparation_then_cleans_the_entire_pipeline()
-> TestResult {
    with_environment("normal", &[("dual-to-archive", COUNTING_LUA), ("dual-to-dual", DUAL_EMIT_LUA)], async |reconfigurer, _, root, target| {
        extend_initialization_budget(reconfigurer, root)?;
        reconfigurer.apply(target, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let pids = instance_pids(root)?;
        let queue = std::fs::OpenOptions::new().write(true).open(egress_queue_path(&instance_directory(root, "dual-b")?, "dual-to-dual", 0))?;
        let mut held = hold_dual_output(root).await?;
        let next = next_revision(root, |document| add_source_flow(document, "added-input", "added-flow", "archive", "print('initializing'); while true do end"))?;
        let (diagnostics, mut records) = interested_flow_channel(&FlowId::try_from(String::from("added-flow"))?, 0);
        reconfigurer.diagnostics = diagnostics;
        let shutdown = reconfigurer.shutdown_handle();
        let mut applying = Box::pin(reconfigurer.apply(next, None));
        let started = tokio::select! {
            record = timeout(TEST_DEADLINE, records.recv()) => record,
            result = &mut applying => { result?; return Err("Apply finished before the retained worker failed".into()); }
        };
        let injected: TestResult = (|| {
            corrupt_egress_commit(&queue)?;
            held.release(1)?;
            Ok(())
        })();
        if injected.is_err() { shutdown.request(ReconfigureShutdown::Force); }
        let result = timeout(TEST_DEADLINE, &mut applying).await;
        if result.is_err() {
            shutdown.request(ReconfigureShutdown::Force);
            let _cleanup_result = applying.as_mut().await;
        }
        let result = result?;
        drop(applying);
        injected?;
        started?.ok_or("Initialization diagnostic is missing")?;
        let Err(PipelineReconfigureError::DataPlaneFailure(source)) = result else {
            return Err("Retained worker failure lost its original cause".into());
        };
        assert!(matches!(source.as_ref(), PipelineRuntimeError::FlowChannelFailed { flow_id, .. } if flow_id.as_str() == "dual-to-dual"));
        assert!(source.source().and_then(Error::source).is_some());
        assert!(reconfigurer.current.is_none());
        assert!(!root.exists());
        for pid in pids.into_values() { assert_reaped(pid)?; }
        Ok(())
    }).await
}

pub(super) fn extend_initialization_budget(
    reconfigurer: &mut Reconfigurer,
    root: &Path,
) -> TestResult {
    let mut bootstrap = bootstrap(root)?;
    bootstrap
        .environment
        .as_mut()
        .and_then(|environment| environment.lua_limits.as_mut())
        .ok_or("Fixture Lua limits are missing")?
        .cpu_time_limit_ms = 60_000;
    let environment = bootstrap
        .environment
        .ok_or("Fixture environment is missing")?;
    reconfigurer.environment = Arc::new(environment);
    Ok(())
}
