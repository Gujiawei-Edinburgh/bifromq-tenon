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

//! Failed candidates never discard the old VM or its pending completion responsibility.

use super::*;
use crate::lua::LuaVmErrorKind;
use crate::pipeline::diagnostics::test_support::interested_flow_channel;

const PENDING_LUA: &str = "local b = registry:getBuilder('com.example.archive@1.0.0'); local count = 0; function main(event) count = count + 1; print('processed'); if count == 3 then b:setValue('old:' .. count); emit(b:build()) end end";

#[test]
fn replacement_transfers_new_queue_before_its_blocking_wait_starts() -> TestResult {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?
        .block_on(with_environment(
            "normal",
            &[("dual-to-archive", COUNTING_LUA)],
            async |reconfigurer, _, root, target| {
                let flow_id = FlowId::try_from(String::from("dual-to-archive"))?;
                let (diagnostics, mut records) = interested_flow_channel(&flow_id, 0);
                reconfigurer.diagnostics = diagnostics;
                reconfigurer.apply(target, None).await?;
                wait_for_states(current(reconfigurer)?, &[]).await?;
                let target = next_revision(root, |document| {
                    document["pluginInstances"]["candidate-archive"] =
                        document["pluginInstances"]["archive"].clone();
                    document["flows"]["dual-to-archive"]["sinks"] =
                        json!(["archive", "candidate-archive"]);
                    document["flows"]["dual-to-archive"]["process"]["script"] =
                        json!("print('candidate prepared'); function main(event) end");
                })?;
                let available_cpu_count = reconfigurer.environment.available_cpu_count();
                let compiled = ReconfigurePlan::derive(
                    Some(current(reconfigurer)?.revision()),
                    target,
                    available_cpu_count,
                )?
                .compile()?;
                let mut staged = compiled
                    .begin_stage(
                        Arc::clone(&reconfigurer.environment),
                        reconfigurer.diagnostics.clone(),
                        Some(current(reconfigurer)?.started_at()),
                        None,
                    )
                    .wait()
                    .await?;
                let routes = staged
                    .channel_routes
                    .remove(&flow_id)
                    .ok_or("candidate routes are missing")?;
                let spec = crate::pipeline::reconfigure::resource_stage::channel_spec(
                    &staged.changes.target,
                    &flow_id,
                    reconfigurer.environment.lua_limits(),
                );
                let (release, released) = std::sync::mpsc::channel::<()>();
                let (entered, entry) = tokio::sync::oneshot::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    let _ = entered.send(());
                    let _ = released.recv();
                });
                entry.await?;
                let work = current(reconfigurer)?.begin_definition_replacement(
                    &flow_id,
                    ChannelDefinitionChange::Replace(spec),
                    routes,
                );
                // The single blocking slot cannot execute the coordinator. A candidate
                // diagnostic proves its command and New writer already reached the worker.
                let prepared = timeout(std::time::Duration::from_secs(1), records.recv()).await;
                drop(release);
                blocker.await?;
                let mut work = work?;
                let definition = work.wait().await?;
                tokio::task::spawn_blocking(move || definition.abort()).await??;
                tokio::task::spawn_blocking(move || drop(staged)).await?;
                let Some(crate::contracts::core::pipeline_diagnostic_record::Record::Channel(
                    record,
                )) = prepared?.and_then(|record| record.record)
                else {
                    return Err(
                        "candidate diagnostic is missing before the background wait starts".into(),
                    );
                };
                assert_eq!(record.text, "candidate prepared");
                assert_eq!(record.flow_id, flow_id.as_str());
                assert_eq!(record.channel_index, 0);
                assert!(root.exists());
                Ok(())
            },
        ))
}

#[tokio::test(flavor = "current_thread")]
async fn successful_cutover_retries_old_pending_in_order_then_processes_new_input() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", PENDING_LUA)],
        async |reconfigurer, _, root, target| {
            let (diagnostics, mut records) =
                interested_flow_channel(&FlowId::try_from(String::from("dual-to-archive"))?, 0);
            reconfigurer.diagnostics = diagnostics;
            reconfigurer.apply(target, None).await?;
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut completion = source_completion(root, "dual-b", "dual-to-archive", 0)?;
            for id in [1, 2] {
                submit(&mut source, id)?;
                timeout(TEST_DEADLINE, records.recv())
                    .await?
                    .ok_or("Old VM did not process the pending input")?;
            }
            let target = next_revision(root, |document| {
                document["flows"]["dual-to-archive"]["process"]["script"] = json!(ADDED_LUA)
            })?;
            reconfigurer.apply(target, None).await?;
            for id in [1, 2] {
                let record =
                    IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?;
                assert_eq!(record.record_id, id);
                assert_eq!(record.status(), IngressCompletionStatus::Retry);
                completion.release(1)?;
            }
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            submit(&mut source, 3)?;
            assert_eq!(read_value(&mut sink).await?, "added");
            let record = IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?;
            assert_eq!(record.record_id, 3);
            assert_eq!(record.status(), IngressCompletionStatus::Ok);
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn later_flow_failure_aborts_prior_candidate_and_preserves_old_pending_state() -> TestResult {
    with_environment("normal", &[("dual-to-archive", PENDING_LUA)], async |reconfigurer, _, root, target| {
        let (diagnostics, mut records) = interested_flow_channel(&FlowId::try_from(String::from("dual-to-archive"))?, 0);
        reconfigurer.diagnostics = diagnostics;
        reconfigurer.apply(target, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let pids = instance_pids(root)?;
        let files = queue_files(root)?;
        let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
        let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
        let mut completion = source_completion(root, "dual-b", "dual-to-archive", 0)?;
        for id in [1, 2] {
            submit(&mut source, id)?;
            timeout(TEST_DEADLINE, records.recv()).await?.ok_or("Old VM did not process the pending input")?;
        }
        assert!(matches!(completion.try_read()?, ReadOutcome::Empty));
        let next = next_revision(root, |document| {
            document["pluginInstances"]["candidate-archive"] = document["pluginInstances"]["archive"].clone();
            document["flows"]["dual-to-archive"]["sinks"] = json!(["archive", "candidate-archive"]);
            document["flows"]["dual-to-archive"]["process"]["script"] = json!(format!("setTimeout(0); {ADDED_LUA}"));
            document["flows"]["input-to-dual"]["process"]["script"] = json!("error('candidate failed')");
        })?;
        assert!(matches!(reconfigurer.apply(next, None).await,
            Err(PipelineReconfigureError::RuntimeTransition(source))
                if matches!(source.as_ref(), PipelineRuntimeError::FlowChannelReplacementPreparation { flow_id, kind: LuaVmErrorKind::TopLevelFailed, .. } if flow_id.as_str() == "input-to-dual")));
        assert_eq!(current(reconfigurer)?.status_snapshot().document_etag, "initial-cutover");
        assert_eq!(instance_pids(root)?, pids);
        assert_eq!(queue_files(root)?, files);
        assert!(!instance_directory(root, "candidate-archive")?.exists());
        assert!(matches!(sink.try_read()?, ReadOutcome::Empty));
        assert!(matches!(completion.try_read()?, ReadOutcome::Empty));
        submit(&mut source, 3)?;
        assert_eq!(read_value(&mut sink).await?, "old:3");
        for id in [1, 2, 3] {
            let record = IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?;
            assert_eq!(record.record_id, id);
            assert_eq!(record.status(), IngressCompletionStatus::Ok);
            completion.release(1)?;
        }
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn added_flow_failure_restores_prepared_old_lua_without_candidate_timer_output() -> TestResult
{
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            assert_eq!(read_value(&mut sink).await?, "stable:1");
            let next = next_revision(root, |document| {
                document["flows"]["dual-to-archive"]["process"]["script"] =
                    json!(format!("setTimeout(0); {ADDED_LUA}"));
                add_source_flow(
                    document,
                    "added-input",
                    "added-flow",
                    "archive",
                    "error('candidate failed')",
                );
            })?;
            assert!(matches!(
                reconfigurer.apply(next, None).await,
                Err(PipelineReconfigureError::RuntimeStart(_))
            ));
            assert!(!instance_directory(root, "added-input")?.exists());
            assert_eq!(
                current(reconfigurer)?.status_snapshot().document_etag,
                "initial-cutover"
            );
            assert!(matches!(sink.try_read()?, ReadOutcome::Empty));
            submit(&mut source, 2)?;
            assert_eq!(read_value(&mut sink).await?, "stable:2");
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn delivery_lua_and_added_flow_changes_publish_together() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let pids = instance_pids(root)?;
            let files = queue_files(root)?;
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            assert_eq!(read_value(&mut sink).await?, "stable:1");
            let next = next_revision(root, |document| {
                document["flows"]["dual-to-archive"]["process"]["script"] = json!(ADDED_LUA);
                document["flows"]["dual-to-archive"]["delivery"] = json!("at-most-once");
                add_source_flow(document, "added-input", "added-flow", "archive", ADDED_LUA);
            })?;
            assert!(matches!(
                reconfigurer.apply(next, None).await?,
                PipelineApplyOutcome::Applied(_)
            ));
            wait_for_states(current(reconfigurer)?, &[]).await?;
            assert!(instance_directory(root, "added-input")?.exists());
            let after = instance_pids(root)?;
            assert_ne!(after["archive"], pids["archive"]);
            assert_reaped(pids["archive"])?;
            for id in ["input", "dual-a", "dual-b"] {
                assert_eq!(after[id], pids[id]);
            }
            assert_eq!(queue_files(root)?, files);
            assert_eq!(
                current(reconfigurer)?.status_snapshot().document_etag,
                "updated"
            );
            submit(&mut source, 2)?;
            assert_eq!(read_value(&mut sink).await?, "added");
            let mut added = source_writer(root, "added-input", "added-flow", 0)?;
            let mut added_sink = sink_reader(root, "archive", "added-flow", 0)?;
            submit(&mut added, 1)?;
            assert_eq!(read_value(&mut added_sink).await?, "added");
            Ok(())
        },
    )
    .await
}
