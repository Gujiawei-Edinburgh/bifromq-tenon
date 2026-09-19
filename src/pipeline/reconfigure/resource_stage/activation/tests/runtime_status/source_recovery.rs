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
use super::super::additive_apply::read_value;
use super::*;
use std::fs::File;
use std::io;

#[tokio::test(flavor = "current_thread")]
async fn source_restart_preserves_oversubscribed_channels_despite_cpu_quota() -> TestResult {
    let script = "local b = registry:getBuilder('com.example.archive@1.0.0'); local count = 0; function main(event) count = count + 1; b:setValue('count:' .. count); emit(b:build()) end";
    with_environment(
        "normal",
        &[("dual-to-archive", script)],
        async |reconfigurer, _, root, _| {
            let mut target = revision(root.parent().ok_or("Fixture parent is missing")?, "normal")?;
            let mut document: serde_json::Value =
                serde_json::from_str(&target.tenon_document_json)?;
            document["resourceLimits"] = serde_json::json!({"cpu": 1.5});
            document["flows"]["dual-to-archive"]["parallelism"] = serde_json::json!(2);
            document["flows"]["dual-to-archive"]["process"]["script"] = serde_json::json!(script);
            target.tenon_document_json = serde_json::to_string(&document)?;
            apply(reconfigurer, model(target)?).await?;
            observed_states(reconfigurer, &[]).await?;
            let pids = instance_pids(root)?;
            let directory = instance_directory(root, "dual-b")?;
            let submission = directory.join("source/submission-0.queue");
            let output =
                egress_queue_path(&instance_directory(root, "archive")?, "dual-to-archive", 0);
            let output_inode = std::fs::metadata(&output)?.ino();
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut results = source_completion(root, "dual-b", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            assert_eq!(read_value(&mut sink).await?, "count:1");
            read_record(&mut results).await?;
            results.release(1)?;
            drop((source, results));

            with_frozen_clock(async {
                rustix::process::kill_process(pids["dual-b"], rustix::process::Signal::KILL)?;
                observed_states(
                    reconfigurer,
                    &[("dual-b", PluginInstanceState::RestartBackoff)],
                )
                .await?;
                assert_reaped(pids["dual-b"])?;
                tokio::time::advance(
                    reconfigurer.environment.retry_backoff().initial_delay()
                        + Duration::from_millis(1),
                )
                .await;
                observed_states(reconfigurer, &[("dual-b", PluginInstanceState::Starting)]).await?;
                Ok(())
            })
            .await?;
            observed_states(reconfigurer, &[]).await?;
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut commit = [0_u8; 8];
            File::open(&submission)?.read_exact_at(&mut commit, 64)?;
            submit(&mut source, u64::from_le_bytes(commit) + 1)?;
            assert_eq!(
                read_value(&mut sink).await?,
                "count:1",
                "A restarted Source must enter a fresh Flow VM"
            );
            assert_eq!(std::fs::metadata(output)?.ino(), output_inode);
            assert_eq!(reconfigurer.environment.available_cpu_count().get(), 4);
            assert!(!directory.join("source/submission-8.queue").exists());
            for channel in 1..8 {
                let mut source = source_writer(root, "dual-b", "dual-to-archive", channel)?;
                let mut sink = sink_reader(root, "archive", "dual-to-archive", channel)?;
                submit(&mut source, 1)?;
                assert_eq!(read_value(&mut sink).await?, "count:1");
            }
            let after = instance_pids(root)?;
            for id in ["dual-a", "archive", "input"] {
                assert_eq!(after[id], pids[id]);
            }
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn source_failure_discards_unread_input_without_waiting_for_old_egress_release() -> TestResult
{
    let script = "local b = registry:getBuilder('com.example.archive@1.0.0'); local count = 0; function main(event) count = count + 1; b:setValue('count:' .. count); emit(b:build()) end";
    with_environment(
        "normal",
        &[("dual-to-archive", script)],
        async |reconfigurer, _, root, target| {
            apply(reconfigurer, target).await?;
            observed_states(reconfigurer, &[]).await?;
            let pids = instance_pids(root)?;
            let directory = instance_directory(root, "dual-b")?;
            let submission = directory.join("source/submission-0.queue");
            let output =
                egress_queue_path(&instance_directory(root, "archive")?, "dual-to-archive", 0);
            let output_inode = std::fs::metadata(&output)?.ino();
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut results = source_completion(root, "dual-b", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            let old = read_record(&mut sink).await?;
            assert!(matches!(results.try_read()?, ReadOutcome::Empty));
            // Nothing has been released yet, and the old Channel accepted this
            // input anyway and committed its own output.
            submit(&mut source, 2)?;
            read_record(&mut sink).await?;
            assert!(matches!(results.try_read()?, ReadOutcome::Empty));
            drop((source, results));
            rustix::process::kill_process(pids["dual-b"], rustix::process::Signal::KILL)?;
            observed_states(
                reconfigurer,
                &[("dual-b", PluginInstanceState::RestartBackoff)],
            )
            .await?;
            assert_reaped(pids["dual-b"])?;
            assert_eq!(std::fs::metadata(&output)?.ino(), output_inode);
            let mut commit = [0_u8; 8];
            File::open(&submission)?.read_exact_at(&mut commit, 64)?;
            assert_eq!(
                u64::from_le_bytes(commit),
                0,
                "Unread input must not survive recovery"
            );
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut results = source_completion(root, "dual-b", "dual-to-archive", 0)?;
            assert!(matches!(results.try_read()?, ReadOutcome::Empty));
            submit(&mut source, 101)?;
            // Reopening the same Egress proves its committed prefix was preserved.
            drop(sink);
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            assert_eq!(read_record(&mut sink).await?, old);
            sink.release(1)?;
            assert_eq!(read_value(&mut sink).await?, "count:2");
            assert_eq!(read_value(&mut sink).await?, "count:1");
            let done = IngressCompletion::decode(read_record(&mut results).await?.as_slice())?;
            assert_eq!(done.record_id, 101);
            results.release(1)?;
            assert!(matches!(sink.try_read()?, ReadOutcome::Empty));
            assert!(matches!(results.try_read()?, ReadOutcome::Empty));
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn simultaneous_source_failures_rebuild_each_flow_before_either_retry() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, target| {
        apply(reconfigurer, target).await?;
        observed_states(reconfigurer, &[]).await?;
        let pids = instance_pids(root)?;
        let files = queue_files(root)?;
        // Keep the old inodes alive so unlink/recreate cannot reuse their identities.
        let _open_queues = files
            .keys()
            .map(File::open)
            .collect::<io::Result<Vec<_>>>()?;
        with_frozen_clock(async {
            for id in ["dual-a", "dual-b"] {
                rustix::process::kill_process(pids[id], rustix::process::Signal::KILL)?;
            }
            observed_states(
                reconfigurer,
                &[
                    ("dual-a", PluginInstanceState::RestartBackoff),
                    ("dual-b", PluginInstanceState::RestartBackoff),
                ],
            )
            .await?;
            let after = queue_files(root)?;
            let source_directories = [
                instance_directory(root, "dual-a")?.join("source"),
                instance_directory(root, "dual-b")?.join("source"),
            ];
            for (path, identity) in &files {
                if source_directories
                    .iter()
                    .any(|directory| path.starts_with(directory))
                {
                    assert_ne!(after[path], *identity);
                } else {
                    assert_eq!(after[path], *identity);
                }
            }
            for id in ["dual-a", "dual-b"] {
                assert_reaped(pids[id])?;
            }
            Ok(())
        })
        .await?;
        for id in ["input", "archive"] {
            assert_eq!(recorded_pid(&instance_directory(root, id)?)?, pids[id]);
        }
        Ok(())
    })
    .await
}
