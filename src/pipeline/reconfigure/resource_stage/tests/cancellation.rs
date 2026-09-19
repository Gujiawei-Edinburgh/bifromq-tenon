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

use super::*;
use crate::contracts::core::pipeline_diagnostic_record;
use crate::identifiers::FlowId;
use crate::pipeline::diagnostics::test_support::interested_flow_channel;
use crate::pipeline::reconfigure::test_support::bootstrap;
use tokio::time::timeout;

const WAIT_LIMIT: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "current_thread")]
async fn cancelling_running_lua_initialization_cleans_the_entire_candidate() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let mut bootstrap = bootstrap(&root)?;
    bootstrap
        .environment
        .as_mut()
        .and_then(|environment| environment.lua_limits.as_mut())
        .ok_or_else(|| io::Error::other("Fixture Lua limits are missing"))?
        .cpu_time_limit_ms = 60_000;
    let environment = bootstrap
        .environment
        .ok_or("Fixture environment is missing")?;
    let environment = Arc::new(environment);
    let mut revision = candidate_revision("stalled")?;
    set_flow_lua(
        &mut revision,
        "b-to-a",
        "print('initializing'); while true do end",
    )?;
    let flow_id = FlowId::try_from(String::from("b-to-a"))?;
    let (diagnostics, mut records) = interested_flow_channel(&flow_id, 0);
    let mut job =
        ReconfigurePlan::derive(None, model(revision)?, environment.available_cpu_count())?
            .compile()?
            .begin_stage(Arc::clone(&environment), diagnostics, None, None);

    let started = timeout(WAIT_LIMIT, records.recv()).await;
    // Keep cleanup owned even when the diagnostic assertion fails. The Lua
    // CPU budget is longer than both waits, so budget expiry cannot prove cancellation.
    let cancellation = job.cancel_and_discard();
    tokio::pin!(cancellation);
    let cancelled = timeout(WAIT_LIMIT, &mut cancellation).await;
    if cancelled.is_err() {
        cancellation.await?;
    }
    cancelled??;
    let record = started?.ok_or_else(|| io::Error::other("Lua initialization did not report"))?;
    assert!(matches!(record.record,
        Some(pipeline_diagnostic_record::Record::Channel(record))
        if record.flow_id == "b-to-a" && record.text == "initializing"
    ));
    assert!(!root.exists());

    let mut retry =
        initial_plan("repaired")?.begin_stage(environment, test_publisher(), None, None);
    let staged = wait_stage(&mut retry).await?;
    assert_eq!(staged.changes.target.document_etag(), "repaired");
    assert!(prepared_runtime_test_support::is_pending(&staged.runtime));
    tokio::task::spawn_blocking(move || drop(staged)).await?;
    assert!(!root.exists());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn background_stage_returns_original_failure_after_removing_partial_resources() -> TestResult
{
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let environment = Arc::new(environment(&root)?);
    let mut revision = candidate_revision("invalid-lua")?;
    set_flow_lua(&mut revision, "b-to-a", "error('initialization failed')")?;
    let mut job =
        ReconfigurePlan::derive(None, model(revision)?, environment.available_cpu_count())?
            .compile()?
            .begin_stage(environment, test_publisher(), None, None);

    let error = wait_stage(&mut job)
        .await
        .err()
        .ok_or_else(|| io::Error::other("Invalid Lua unexpectedly initialized"))?;
    assert!(matches!(error.downcast_ref::<PipelineReconfigureError>(),
        Some(PipelineReconfigureError::RuntimeStart(PipelineRuntimeError::FlowChannelOpen {
            flow_id, channel_index: 0, source: FlowChannelError::LuaVmLoad { kind: LuaVmErrorKind::TopLevelFailed }
        })) if flow_id.as_str() == "b-to-a"
    ));
    assert!(!root.exists());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn background_stage_keeps_timers_paused_and_can_repeat_the_same_target() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let environment = Arc::new(environment(&root)?);
    for _ in 0..2 {
        let mut revision = candidate_revision("same-target")?;
        set_timer_lua(&mut revision)?;
        let mut job =
            ReconfigurePlan::derive(None, model(revision)?, environment.available_cpu_count())?
                .compile()?
                .begin_stage(Arc::clone(&environment), test_publisher(), None, None);
        let staged = wait_stage(&mut job).await?;
        assert_eq!(
            prepared_runtime_test_support::data_plane_counts(&staged.runtime),
            (2, 3)
        );
        assert!(prepared_runtime_test_support::is_pending(&staged.runtime));
        let archive = root
            .join(INSTANCES_DIRECTORY_NAME)
            .join(ARCHIVE_DIRECTORY_NAME);
        let mut egress = open_reader(
            &egress_queue_path(&archive, "a-to-b", 0),
            &loops_bell_path(&archive.join(SINK_DIRECTORY_NAME)),
            0,
            &flow_channel_bell_path(&root, "a-to-b"),
        )?;
        assert!(matches!(egress.try_read()?, ReadOutcome::Empty));
        drop(egress);
        tokio::task::spawn_blocking(move || drop(staged)).await?;
        assert!(!root.exists());
    }
    Ok(())
}

async fn wait_stage(
    job: &mut BlockingReconfigureJob<StagedResourceChanges>,
) -> Result<StagedResourceChanges, Box<dyn Error>> {
    match timeout(WAIT_LIMIT, job.wait()).await {
        Ok(result) => Ok(result?),
        Err(timeout) => {
            job.cancel_and_discard().await?;
            Err(timeout.into())
        }
    }
}
