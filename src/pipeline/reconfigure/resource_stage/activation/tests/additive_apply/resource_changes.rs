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

//! Mixed resource changes exercise one apply operation and real Queue traffic.

use super::*;
use crate::pipeline::reconfigure::resource_stage::tests::directory_names;
use crate::pipeline::reconfigure::runtime_files::LOOPS_BELL_FILE_NAME;

/// Emits one record per Sink Contract with a Contract-specific value, so each read
/// can tell whose Contract the file it opened actually carries.
const FAN_OUT_LUA: &str = "local archive = registry:getBuilder('com.example.archive@1.0.0'); local dual = registry:getBuilder('com.example.dual@1.0.0'); function main(event) archive:setValue('to-archive'); dual:setValue('to-dual'); emit(archive:build()); emit(dual:build()) end";
const DUAL_VALUE_LUA: &str = "local dual = registry:getBuilder('com.example.dual@1.0.0'); function main(event) dual:setValue('input-to-dual'); emit(dual:build()) end";

#[tokio::test(flavor = "current_thread")]
async fn swapping_two_flow_sources_installs_each_new_queue_set_over_the_old_one() -> TestResult {
    with_environment(
        "normal",
        &[
            ("input-to-dual", DUAL_VALUE_LUA),
            ("dual-to-archive", COUNTING_LUA),
        ],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let input = instance_directory(root, "input")?;
            let dual_a = instance_directory(root, "dual-a")?;
            let dual_b = instance_directory(root, "dual-b")?;
            let archive = instance_directory(root, "archive")?;
            // `input` drives the two-Channel Flow, `dual-b` the single-Channel one.
            assert_eq!(
                directory_names(&input.join("source"))?,
                [
                    String::from("completion-0.queue"),
                    String::from("completion-1.queue"),
                    String::from(LOOPS_BELL_FILE_NAME),
                    String::from("submission-0.queue"),
                    String::from("submission-1.queue"),
                ]
            );
            assert_eq!(
                directory_names(&dual_b.join("source"))?,
                [
                    String::from("completion-0.queue"),
                    String::from(LOOPS_BELL_FILE_NAME),
                    String::from("submission-0.queue"),
                ]
            );
            let old_input_source =
                std::fs::metadata(input.join("source/submission-0.queue"))?.ino();
            let old_dual_b_source =
                std::fs::metadata(dual_b.join("source/submission-0.queue"))?.ino();
            let retained_egress =
                std::fs::metadata(egress_queue_path(&dual_a, "input-to-dual", 0))?.ino();
            let archive_egress =
                std::fs::metadata(egress_queue_path(&archive, "dual-to-archive", 0))?.ino();
            // Both Sinks stay retained across this swap and keep ringing the live
            // Channel mapping, so a Queue-only replacement must reuse the inode.
            let input_region =
                std::fs::metadata(flow_channel_bell_path(root, "input-to-dual"))?.ino();
            let archive_region =
                std::fs::metadata(flow_channel_bell_path(root, "dual-to-archive"))?.ino();

            // Exchange the two Flows' Source instances without touching their Sinks.
            let target = next_revision(root, |document| {
                document["flows"]["input-to-dual"]["source"] = json!("dual-b");
                document["flows"]["input-to-dual"]["process"]["script"] = json!(DUAL_VALUE_LUA);
                document["flows"]["dual-to-archive"]["source"] = json!("input");
            })?;
            let PipelineApplyOutcome::Applied(status) = reconfigurer.apply(target, None).await?
            else {
                return Err("Source swap apply unexpectedly stopped".into());
            };
            assert_eq!(status.document_etag, "updated");
            wait_for_states(current(reconfigurer)?, &[]).await?;

            // Each instance now carries the Channel count of the Flow it took over,
            // in fresh files installed over the removed ones. A crossed pairing would
            // give each instance the other's Channel count.
            assert_eq!(
                directory_names(&input.join("source"))?,
                [
                    String::from("completion-0.queue"),
                    String::from(LOOPS_BELL_FILE_NAME),
                    String::from("submission-0.queue"),
                ]
            );
            assert_eq!(
                directory_names(&dual_b.join("source"))?,
                [
                    String::from("completion-0.queue"),
                    String::from("completion-1.queue"),
                    String::from(LOOPS_BELL_FILE_NAME),
                    String::from("submission-0.queue"),
                    String::from("submission-1.queue"),
                ]
            );
            assert_ne!(
                std::fs::metadata(input.join("source/submission-0.queue"))?.ino(),
                old_input_source
            );
            assert_ne!(
                std::fs::metadata(dual_b.join("source/submission-0.queue"))?.ino(),
                old_dual_b_source
            );
            assert_eq!(
                std::fs::metadata(egress_queue_path(&dual_a, "input-to-dual", 0))?.ino(),
                retained_egress
            );
            assert_eq!(
                std::fs::metadata(egress_queue_path(&archive, "dual-to-archive", 0))?.ino(),
                archive_egress
            );
            // The Sinks kept their mapping, so the Regions they ring must not have moved.
            assert_eq!(
                std::fs::metadata(flow_channel_bell_path(root, "input-to-dual"))?.ino(),
                input_region
            );
            assert_eq!(
                std::fs::metadata(flow_channel_bell_path(root, "dual-to-archive"))?.ino(),
                archive_region
            );
            assert!(!root.join(".candidate").exists());

            // The rebound Source reaches the Sink of the Flow it now drives.
            let mut source = source_writer(root, "input", "dual-to-archive", 0)?;
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            assert_eq!(read_value(&mut sink).await?, "stable:1");
            let mut second_source = source_writer(root, "dual-b", "input-to-dual", 0)?;
            let mut second_sink = sink_reader(root, "dual-a", "input-to-dual", 0)?;
            submit(&mut second_source, 2)?;
            assert_eq!(read_value(&mut second_sink).await?, "input-to-dual");
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn mixed_batch_replaces_layout_and_routes_adds_flow_and_removes_old_owners() -> TestResult {
    with_environment("normal", &[("dual-to-archive", COUNTING_LUA)], async |reconfigurer, _, root, target| {
        reconfigurer.apply(target, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let before = instance_pids(root)?;
        let dual_a = instance_directory(root, "dual-a")?;
        let source_identity = std::fs::metadata(dual_a.join("source/submission-0.queue"))?.ino();
        let dual_b = instance_directory(root, "dual-b")?;
        let sink_identity = std::fs::metadata(egress_queue_path(&dual_b, "dual-to-dual", 0))?.ino();
        let mut retained_source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
        let mut old_sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
        submit(&mut retained_source, 1)?;
        assert_eq!(read_value(&mut old_sink).await?, "stable:1");
        drop(old_sink);
        let mut old_completion = source_completion(root, "dual-b", "dual-to-archive", 0)?;
        assert_eq!(IngressCompletion::decode(read_record(&mut old_completion).await?.as_slice())?.record_id, 1);
        old_completion.release(1)?;

        let target = next_revision(root, |document| {
            document["pluginInstances"].as_object_mut().unwrap_or_else(|| std::process::abort()).remove("input");
            document["pluginInstances"].as_object_mut().unwrap_or_else(|| std::process::abort()).remove("archive");
            document["flows"].as_object_mut().unwrap_or_else(|| std::process::abort()).remove("input-to-dual");
            document["pluginInstances"]["new-archive"] = instance("com.example.archive", "new-disk");
            document["flows"]["dual-to-archive"]["sinks"] = json!(["new-archive"]);
            document["flows"]["dual-to-dual"]["parallelism"] = json!(0.5);
            document["flows"]["dual-to-dual"]["process"]["script"] = json!("local b = registry:getBuilder('com.example.dual@1.0.0'); function main(event) b:setValue('new-layout'); emit(b:build()) end");
            add_source_flow(document, "new-input", "new-flow", "new-archive", ADDED_LUA);
        })?;
        let PipelineApplyOutcome::Applied(status) = reconfigurer.apply(target, None).await? else {
            return Err("Mixed resource apply unexpectedly stopped".into());
        };
        assert_eq!(status.document_etag, "updated");
        wait_for_states(current(reconfigurer)?, &[]).await?;
        for id in ["input", "archive", "dual-a", "dual-b"] { assert_reaped(before[id])?; }
        for id in ["input", "archive"] { assert!(!instance_directory(root, id)?.exists()); }
        assert_ne!(recorded_pid(&dual_b)?, before["dual-b"]);
        assert_ne!(recorded_pid(&dual_a)?, before["dual-a"]);
        assert_ne!(std::fs::metadata(dual_a.join("source/submission-0.queue"))?.ino(), source_identity);
        assert!(dual_a.join("source/submission-1.queue").exists());
        assert!(!dual_a.join("sink").exists());
        assert_eq!(std::fs::metadata(egress_queue_path(&dual_b, "dual-to-dual", 0))?.ino(), sink_identity);
        assert!(egress_queue_path(&dual_b, "dual-to-dual", 1).is_file());
        assert!(!root.join(".candidate").exists());

        let mut new_sink = sink_reader(root, "new-archive", "dual-to-archive", 0)?;
        submit(&mut retained_source, 2)?;
        assert_eq!(read_value(&mut new_sink).await?, "stable:2");
        let mut added_source = source_writer(root, "new-input", "new-flow", 0)?;
        let mut added_sink = sink_reader(root, "new-archive", "new-flow", 0)?;
        submit(&mut added_source, 1)?;
        assert_eq!(read_value(&mut added_sink).await?, "added");
        let mut replaced_source = source_writer(root, "dual-a", "dual-to-dual", 1)?;
        let mut retained_sink = sink_reader(root, "dual-b", "dual-to-dual", 1)?;
        submit(&mut replaced_source, 1)?;
        assert_eq!(read_value(&mut retained_sink).await?, "new-layout");
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn a_second_sink_contract_installs_each_instances_own_egress_files() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let dual_a = instance_directory(root, "dual-a")?;
            let archive = instance_directory(root, "archive")?;
            let retained =
                std::fs::metadata(egress_queue_path(&archive, "dual-to-archive", 0))?.ino();

            // One batch stages egress files for two already-running Sink instances:
            // `archive` gains Channel 1, `dual-a` gains the whole Contract binding.
            let target = next_revision(root, |document| {
                document["flows"]["dual-to-archive"]["sinks"] = json!(["archive", "dual-a"]);
                document["flows"]["dual-to-archive"]["parallelism"] = json!(0.5);
                document["flows"]["dual-to-archive"]["process"]["script"] = json!(FAN_OUT_LUA);
            })?;
            let PipelineApplyOutcome::Applied(status) = reconfigurer.apply(target, None).await?
            else {
                return Err("Second Sink Contract apply unexpectedly stopped".into());
            };
            assert_eq!(status.document_etag, "updated");
            wait_for_states(current(reconfigurer)?, &[]).await?;

            assert_eq!(
                std::fs::metadata(egress_queue_path(&archive, "dual-to-archive", 0))?.ino(),
                retained
            );
            assert!(egress_queue_path(&archive, "dual-to-archive", 1).is_file());
            assert!(egress_queue_path(&dual_a, "dual-to-archive", 0).is_file());
            assert!(egress_queue_path(&dual_a, "dual-to-archive", 1).is_file());
            assert!(!root.join(".candidate").exists());

            // Three files were installed: `archive` Channel 1, and both of `dual-a`.
            // Reading Channel 0 before Channel 1 carries any traffic makes each of the
            // three possible crossings observable: a Channel 0 file swapped with any
            // other leaves its read empty, and swapping the two Channel 1 files shows
            // up as the other Contract's value once Channel 1 does carry a record.
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut archive_sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            let mut dual_sink = sink_reader(root, "dual-a", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            assert_eq!(read_value(&mut archive_sink).await?, "to-archive");
            assert_eq!(read_value(&mut dual_sink).await?, "to-dual");

            let mut second_channel = source_writer(root, "dual-b", "dual-to-archive", 1)?;
            let mut archive_second = sink_reader(root, "archive", "dual-to-archive", 1)?;
            let mut dual_second = sink_reader(root, "dual-a", "dual-to-archive", 1)?;
            submit(&mut second_channel, 1)?;
            assert_eq!(read_value(&mut archive_second).await?, "to-archive");
            assert_eq!(read_value(&mut dual_second).await?, "to-dual");
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn cpu_updates_keep_channels_while_parallelism_resizes_only_affected_flows() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let mut previous_pids: Option<BTreeMap<&str, rustix::process::Pid>> = None;
        let mut previous_queues: Option<BTreeMap<&str, u64>> = None;
        let mut previous_region: Option<u64> = None;
        let mut previous_channels = None;
        assert_eq!(reconfigurer.environment.available_cpu_count().get(), 4);
        for (step, (cpu, parallelism, channels, expected_value)) in [
            (0.5, 1.0, 4, "stable:1"),
            (1.5, 1.0, 4, "stable:2"),
            (1.5, 2.0, 8, "stable:1"),
            (0.5, 2.0, 8, "stable:2"),
            (0.5, 0.25, 1, "stable:1"),
        ]
        .into_iter()
        .enumerate()
        {
            let mut target = revision(root.parent().ok_or("Fixture parent is missing")?, "normal")?;
            target.document_etag = format!("cpu-{step}");
            let mut document: Value = serde_json::from_str(&target.tenon_document_json)?;
            document["resourceLimits"] = json!({"cpu": cpu});
            for flow in document["flows"]
                .as_object_mut()
                .ok_or("Flows are missing")?
                .values_mut()
            {
                flow.as_object_mut()
                    .ok_or("Flow is missing")?
                    .remove("parallelism");
            }
            document["flows"]["dual-to-archive"]["parallelism"] = json!(parallelism);
            document["flows"]["dual-to-archive"]["process"]["script"] = json!(COUNTING_LUA);
            target.tenon_document_json = serde_json::to_string(&document)?;
            reconfigurer.apply(model(target)?, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let pids = instance_pids(root)?;
            let files = ["input", "dual-a", "dual-b"]
                .into_iter()
                .map(|id| {
                    Ok((
                        id,
                        std::fs::metadata(
                            instance_directory(root, id)?.join("source/submission-0.queue"),
                        )?
                        .ino(),
                    ))
                })
                .collect::<TestResult<BTreeMap<_, _>>>()?;
            if let Some(before) = &previous_queues {
                for id in ["input", "dual-a"] {
                    assert_eq!(files[id], before[id]);
                }
            }
            if let Some(before) = &previous_pids {
                for id in ["input", "dual-a"] {
                    assert_eq!(pids[id], before[id]);
                }
                if previous_channels == Some(channels) {
                    assert_eq!(&pids, before);
                    assert_eq!(Some(&files), previous_queues.as_ref());
                } else {
                    for id in ["dual-b", "archive"] {
                        assert_ne!(pids[id], before[id]);
                        assert_reaped(before[id])?;
                    }
                }
            }
            let source_directory = instance_directory(root, "dual-b")?.join("source");
            // One Queue pair per Channel, and the own-loop region that same
            // Channel list numbers the Source's slots in.
            let mut expected = Vec::new();
            for channel in 0..channels {
                expected.push(format!("completion-{channel}.queue"));
                expected.push(format!("submission-{channel}.queue"));
            }
            expected.push(String::from(LOOPS_BELL_FILE_NAME));
            expected.sort_unstable();
            assert_eq!(directory_names(&source_directory)?, expected);
            let archive = instance_directory(root, "archive")?;
            assert!(!egress_queue_path(&archive, "dual-to-archive", channels as u32).exists());
            for channel in 0..channels as u32 {
                let mut source = source_writer(root, "dual-b", "dual-to-archive", channel)?;
                let mut completion = source_completion(root, "dual-b", "dual-to-archive", channel)?;
                let mut sink = sink_reader(root, "archive", "dual-to-archive", channel)?;
                submit(&mut source, step as u64 + 1)?;
                assert_eq!(read_value(&mut sink).await?, expected_value);
                read_record(&mut completion).await?;
                completion.release(1)?;
            }
            // A Flow's Channel Region is numbered by its Channel count, so the file
            // may only survive an apply that keeps that count.
            let region = std::fs::metadata(flow_channel_bell_path(root, "dual-to-archive"))?.ino();
            if let Some(before) = previous_region {
                if previous_channels == Some(channels) {
                    assert_eq!(
                        region, before,
                        "an unchanged Channel count reuses the Region"
                    );
                } else {
                    assert_ne!(
                        region, before,
                        "a changed Channel count replaces the Region"
                    );
                }
            }
            previous_region = Some(region);
            previous_pids = Some(pids);
            previous_queues = Some(files);
            previous_channels = Some(channels);
        }
        Ok(())
    })
    .await
}
