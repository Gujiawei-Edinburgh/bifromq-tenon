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

//! Limit changes replace exact files and children while preserving adjacent Flow state.

use super::*;
use tenon_ipc::queue::{DataCapacity, HEADER_LEN, Header};

const DUAL_COUNTING: &str = "local b = registry:getBuilder('com.example.dual@1.0.0'); local count = 0; function main(event) count = count + 1; b:setValue('count:' .. count); emit(b:build()) end";

#[tokio::test(flavor = "current_thread")]
async fn lua_emit_uses_each_flow_limit_before_and_after_replacement() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        for bytes in [1024, 4096, 1024] {
            let target = next_revision(root, |document| {
                for (id, contract, limit) in [
                    ("input-to-dual", "com.example.dual@1.0.0", bytes),
                    ("dual-to-archive", "com.example.archive@1.0.0", 4096),
                ] {
                    document["flows"][id]["maxPendingRecords"] = json!(2);
                    document["flows"][id]["maxRecordBytes"] = json!(limit);
                    document["flows"][id]["process"]["script"] = json!(format!(
                        "local b = registry:getBuilder('{contract}'); function main(event) b:setValue(string.rep('x', 1500)); local accepted = pcall(function() emit(b:build()) end); if not accepted then b:setValue('limited'); emit(b:build()) end end"
                    ));
                }
            })?;
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            for (flow, source_id, sink_id, expected) in [
                ("input-to-dual", "input", "dual-a", if bytes == 1024 { String::from("limited") } else { "x".repeat(1500) }),
                ("dual-to-archive", "dual-b", "archive", "x".repeat(1500)),
            ] {
                let mut source = source_writer(root, source_id, flow, 0)?;
                let mut sink = sink_reader(root, sink_id, flow, 0)?;
                submit(&mut source, 1)?;
                assert_eq!(read_value(&mut sink).await?, expected);
                consume_completion(root, source_id, flow, 0, 1).await?;
            }
        }
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn resizing_flow_limits_rebuilds_all_its_queues_and_preserves_shared_flow_state() -> TestResult
{
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let mut retained_source = None;
        let mut retained_sink = None;
        let mut prior_files: Option<std::collections::BTreeMap<PathBuf, (u64, u64)>> = None;
        let mut prior_pids: Option<std::collections::BTreeMap<&'static str, rustix::process::Pid>> =
            None;
        for (round, (pending, bytes)) in [
            (2, 1024),
            (5, 1024),
            (5, 4096),
            (1, 1024),
            (1, 1025),
            (1, 1026),
        ]
        .into_iter()
        .enumerate()
        {
            let target = next_revision(root, |document| {
                let changed = &mut document["flows"]["input-to-dual"];
                changed["maxPendingRecords"] = json!(pending);
                changed["maxRecordBytes"] = json!(bytes);
                changed["process"]["script"] = json!(DUAL_COUNTING);
                // The archive process is also used by an otherwise unchanged Flow.
                changed["sinks"] = json!(["dual-a", "archive"]);
                document["flows"]["dual-to-dual"]["process"]["script"] = json!(DUAL_COUNTING);
            })?;
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let mut files = queue_files(root)?;
            let pids = instance_pids(root)?;
            let input = instance_directory(root, "input")?;
            let dual_a = instance_directory(root, "dual-a")?;
            let archive = instance_directory(root, "archive")?;
            let mut replaced = std::collections::BTreeSet::new();
            for channel in 0..2 {
                for path in [
                    input.join(format!("source/submission-{channel}.queue")),
                    input.join(format!("source/completion-{channel}.queue")),
                    egress_queue_path(&dual_a, "input-to-dual", channel),
                    egress_queue_path(&archive, "input-to-dual", channel),
                ] {
                    let completion = path
                        .file_name()
                        .ok_or("Missing Queue filename")?
                        .to_string_lossy()
                        .starts_with("completion");
                    assert_layout(&path, pending, if completion { 13 } else { bytes })?;
                    let metadata = std::fs::metadata(&path)?;
                    files.insert(path.clone(), (metadata.dev(), metadata.ino()));
                    replaced.insert(path);
                }
            }
            if let Some(before) = &prior_files {
                assert_eq!(
                    files.keys().collect::<Vec<_>>(),
                    before.keys().collect::<Vec<_>>()
                );
                for (path, inode) in &files {
                    if replaced.contains(path) {
                        assert_ne!(*inode, before[path], "{}", path.display());
                    } else {
                        assert_eq!(*inode, before[path], "{}", path.display());
                    }
                }
            }
            if let Some(before) = &prior_pids {
                for id in ["input", "dual-a", "archive"] {
                    assert_reaped(before[id])?;
                    assert_ne!(pids[id], before[id], "{id}");
                }
                assert_eq!(pids["dual-b"], before["dual-b"]);
            }
            for channel in 0..2 {
                let mut source = source_writer(root, "input", "input-to-dual", channel)?;
                let mut sink = sink_reader(root, "dual-a", "input-to-dual", channel)?;
                submit(&mut source, 1)?;
                assert_eq!(read_value(&mut sink).await?, "count:1");
                consume_completion(root, "input", "input-to-dual", channel, 1).await?;
            }
            if round == 0 {
                retained_source = Some(source_writer(root, "dual-a", "dual-to-dual", 0)?);
                retained_sink = Some(sink_reader(root, "dual-b", "dual-to-dual", 0)?);
            }
            let source = retained_source
                .as_mut()
                .ok_or("Missing retained Source writer")?;
            let sink = retained_sink
                .as_mut()
                .ok_or("Missing retained Sink reader")?;
            submit(source, round as u64 + 1)?;
            assert_eq!(read_value(sink).await?, format!("count:{}", round + 1));
            consume_completion(root, "dual-a", "dual-to-dual", 0, round as u64 + 1).await?;
            assert!(!root.join(".candidate").exists());
            prior_files = Some(files);
            prior_pids = Some(pids);
        }
        // Normal Source recovery must retain the currently applied per-Flow limits.
        let input = instance_directory(root, "input")?;
        let pid = recorded_pid(&input)?;
        let egress = egress_queue_path(&instance_directory(root, "dual-a")?, "input-to-dual", 0);
        let retained_egress = std::fs::metadata(&egress)?.ino();
        rustix::process::kill_process(pid, rustix::process::Signal::KILL)?;
        let crate::pipeline::reconfigure::operation::DataPlaneOperation::Completed(event) =
            reconfigurer.observe_runtime().await?
        else {
            return Err("Runtime stopped before Source recovery".into());
        };
        reconfigurer.handle_runtime_event(event).await?;
        assert_reaped(pid)?;
        for channel in 0..2 {
            assert_layout(
                &input.join(format!("source/submission-{channel}.queue")),
                1,
                1026,
            )?;
            assert_layout(
                &input.join(format!("source/completion-{channel}.queue")),
                1,
                13,
            )?;
        }
        assert_eq!(std::fs::metadata(egress)?.ino(), retained_egress);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn failed_limit_replacement_keeps_old_queues_processes_and_lua() -> TestResult {
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
            let failed = next_revision(root, |document| {
                document["flows"]["dual-to-archive"]["maxPendingRecords"] = json!(1);
                document["flows"]["dual-to-archive"]["maxRecordBytes"] = json!(1024);
            })?;
            // An external filesystem collision fails Stage before any old owner is retired.
            // The candidate must neither adopt nor delete an entry it did not create.
            let candidate = root.join(".candidate");
            std::fs::write(&candidate, b"occupied")?;
            assert!(matches!(
                reconfigurer.apply(failed, None).await,
                Err(PipelineReconfigureError::DirectoryCreate { source, .. })
                    if source.kind() == std::io::ErrorKind::AlreadyExists
            ));
            assert_eq!(instance_pids(root)?, pids);
            assert_eq!(queue_files(root)?, files);
            assert_eq!(
                current(reconfigurer)?.status_snapshot().document_etag,
                "initial-cutover"
            );
            assert_eq!(std::fs::read(&candidate)?, b"occupied");
            std::fs::remove_file(candidate)?;
            submit(&mut source, 2)?;
            assert_eq!(read_value(&mut sink).await?, "stable:2");
            Ok(())
        },
    )
    .await
}

fn assert_layout(path: &Path, pending: u64, bytes: u64) -> TestResult {
    use std::io::Read as _;
    let mut file = std::fs::File::open(path)?;
    let capacity = DataCapacity::from_file_len(file.metadata()?.len())?;
    let mut header = [0; HEADER_LEN];
    file.read_exact(&mut header)?;
    let header = Header::decode(capacity, &header)?;
    assert_eq!(header.max_payload_size().get(), bytes);
    assert_eq!(
        capacity.get(),
        (pending + 1) * (8 + bytes).next_multiple_of(8)
    );
    Ok(())
}

async fn consume_completion(
    root: &Path,
    id: &str,
    flow: &str,
    channel: u32,
    record_id: u64,
) -> TestResult {
    let mut completion = source_completion(root, id, flow, channel)?;
    let record = IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?;
    assert_eq!(record.record_id, record_id);
    assert_eq!(record.status(), IngressCompletionStatus::Ok);
    completion.release(1)?;
    Ok(())
}
