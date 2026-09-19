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

//! Exercises Lua-only and mixed resource revisions through the same apply entry.

use super::*;

mod completion;
mod failure;
mod waiting;

#[tokio::test(flavor = "current_thread")]
async fn multi_flow_lua_apply_preserves_channel_identity_and_replaces_each_vm() -> TestResult {
    use crate::contracts::core::pipeline_diagnostic_record::Record;
    use crate::pipeline::diagnostics::test_support::interested_flow_channel;
    const OLD: &str = "print('old'); function main(event) emit() end";
    const NEW: &str = "print('new'); local b = registry:getBuilder('com.example.dual@1.0.0'); function main(event) b:setValue('changed'); emit(b:build()) end";
    with_environment(
        "normal",
        &[("input-to-dual", OLD), ("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            let (diagnostics, mut records) =
                interested_flow_channel(&FlowId::try_from(String::from("input-to-dual"))?, 1);
            reconfigurer.diagnostics = diagnostics;
            reconfigurer.apply(target, None).await?;
            let Some(Record::Channel(old)) = timeout(TEST_DEADLINE, records.recv())
                .await?
                .and_then(|record| record.record)
            else {
                return Err("Old Channel diagnostic is missing".into());
            };
            let files = queue_files(root)?;
            let target = next_revision(root, |document| {
                document["flows"]["input-to-dual"]["process"]["script"] = json!(NEW);
                document["flows"]["dual-to-archive"]["process"]["script"] = json!(ADDED_LUA);
            })?;
            reconfigurer.apply(target, None).await?;
            let Some(Record::Channel(new)) = timeout(TEST_DEADLINE, records.recv())
                .await?
                .and_then(|record| record.record)
            else {
                return Err("New VM diagnostic is missing".into());
            };
            assert_eq!(old.text, "old");
            assert_eq!(new.text, "new");
            assert_eq!(new.channel_instance_id, old.channel_instance_id);
            assert_ne!(new.lua_vm_instance_id, old.lua_vm_instance_id);
            assert_eq!(queue_files(root)?, files);
            for index in [0, 1] {
                let mut sink = sink_reader(root, "dual-a", "input-to-dual", index)?;
                let mut source = source_writer(root, "input", "input-to-dual", index)?;
                submit(&mut source, 1)?;
                assert_eq!(read_value(&mut sink).await?, "changed");
            }
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            assert_eq!(read_value(&mut sink).await?, "added");
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn lua_apply_changes_output_without_replacing_processes_or_queues() -> TestResult {
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
                document["flows"]["dual-to-archive"]["delivery"] = json!("at-least-once");
            })?;
            let PipelineApplyOutcome::Applied(status) = reconfigurer.apply(next, None).await?
            else {
                return Err("Lua apply unexpectedly stopped".into());
            };
            assert_eq!(status.document_etag, "updated");
            assert_eq!(instance_pids(root)?, pids);
            assert_eq!(queue_files(root)?, files);
            submit(&mut source, 2)?;
            assert_eq!(read_value(&mut sink).await?, "added");
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn lua_and_added_flow_apply_use_one_target_and_retained_sink() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            let next = next_revision(root, |document| {
                document["flows"]["dual-to-archive"]["process"]["script"] = json!(ADDED_LUA);
                add_source_flow(
                    document,
                    "added-input",
                    "added-flow",
                    "archive",
                    COUNTING_LUA,
                );
            })?;
            assert!(matches!(
                reconfigurer.apply(next, None).await?,
                PipelineApplyOutcome::Applied(_)
            ));
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            assert_eq!(read_value(&mut sink).await?, "added");
            let mut added = source_writer(root, "added-input", "added-flow", 0)?;
            let mut added_sink = sink_reader(root, "archive", "added-flow", 0)?;
            submit(&mut added, 1)?;
            assert_eq!(read_value(&mut added_sink).await?, "stable:1");
            Ok(())
        },
    )
    .await
}
