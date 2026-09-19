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

//! Planned worker exits stay owned across cancellation and Egress failure.

use super::flow_reconfiguration::labelled_flow_spec;
use super::*;
use crate::pipeline::runtime::PipelineDrainObservation;
use std::future::{Future as _, poll_fn};
use std::task::Poll;

#[test]
fn expected_retirement_does_not_hide_an_unexpected_sibling_exit_in_either_order() -> io::Result<()>
{
    use super::super::worker::{
        WorkerCompletionContext, WorkerSet, WorkerThread, finish_worker_sets,
    };
    let executor = current_thread_executor()?;
    for reverse in [false, true] {
        let mut sets = Vec::new();
        for name in ["retired", "unexpected"] {
            let flow = flow_id(name)?;
            let worker = PipelineWorker {
                flow_id: flow.clone(),
                channel_index: 0,
            };
            let (exit_sender, exits) = tokio::sync::mpsc::channel(1);
            let task = WorkerTask {
                worker: worker.clone(),
                work: Box::new(move || {
                    Some(WorkerExit {
                        flow_id: flow,
                        channel_index: 0,
                        result: WorkerRunResult::Returned(Ok(())),
                    })
                }),
                exit_sender,
            };
            sets.push(WorkerSet::new(
                exits,
                vec![WorkerThread::new(worker, thread::spawn(move || task.run()))],
            ));
        }
        for set in &mut sets {
            executor.block_on(poll_fn(|context| set.poll_next_exit(context)));
        }
        let mut contexts = vec![
            WorkerCompletionContext::PlannedStop,
            WorkerCompletionContext::FailureAlreadyObserved,
        ];
        if reverse {
            sets.reverse();
            contexts.reverse();
        }
        let inputs = contexts.into_iter().zip(&mut sets).collect::<Vec<_>>();
        assert!(matches!(executor.block_on(finish_worker_sets(inputs)),
            Err(PipelineRuntimeError::WorkerExitedUnexpectedly { worker: PipelineWorker { flow_id, .. } }) if flow_id.as_str() == "unexpected"));
    }
    Ok(())
}

#[test]
fn partial_flow_retirement_cancellation_keeps_expected_exits_and_stops_the_held_output()
-> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let idle = SourceEndpoint::create(directory.path(), 0)?;
    let mut blocked = SourceEndpoint::create(directory.path(), 1)?;
    let contract = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &contract)?;
    let (idle_binding, _idle_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &contract)?;
    let idle_id = flow_id("idle")?;
    let blocked_id = flow_id("blocked")?;
    let mut runtime = start_runtime(
        RuntimeInputs::new(
            test_publisher(),
            BTreeMap::from([
                (
                    idle_id.clone(),
                    labelled_flow_spec("idle", idle.queues(), &idle_binding)?,
                ),
                (
                    blocked_id.clone(),
                    labelled_flow_spec("blocked", blocked.queues(), &binding)?,
                ),
            ]),
        ),
        startup_control(),
    )
    .map_err(io::Error::other)?;
    blocked.submit(1, source_payload("held")?)?;
    sink.wait_record()?;
    let selected = [idle_id.clone(), blocked_id.clone()].into_iter().collect();
    runtime.request_flow_retirement(&selected);
    current_thread_executor()?.block_on(async {
        tokio::time::timeout(
            WAIT_LIMIT,
            poll_fn(|context| {
                assert!(
                    runtime
                        .poll_resource_retirement(&selected, context)
                        .is_pending()
                );
                if runtime
                    .flows
                    .get_mut(&idle_id)
                    .ok_or_else(|| io::Error::other("Idle Flow is missing"))?
                    .worker_set_mut()
                    .observed_exit_count()
                    == 1
                {
                    Poll::Ready(Ok::<_, io::Error>(()))
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .map_err(io::Error::other)??;
        assert_eq!(
            runtime
                .flows
                .get_mut(&blocked_id)
                .ok_or_else(|| io::Error::other("Blocked Flow is missing"))?
                .worker_set_mut()
                .observed_exit_count(),
            0
        );
        runtime.stop_and_join().await.map_err(io::Error::other)
    })
}

#[test]
fn planned_drain_waits_for_actual_egress_release_and_keeps_cleanup_cancellation_safe()
-> io::Result<()> {
    for outcome in [
        DrainOutcome::Released,
        DrainOutcome::Canceled,
        DrainOutcome::CoreFailure,
    ] {
        let directory = tempfile::tempdir()?;
        let mut source = SourceEndpoint::create(directory.path(), 0)?;
        let contract = sink_contract_id("com.example.kafka@1.0.0")?;
        let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &contract)?;
        let spec = runtime_spec(
            r#"local b = registry:getBuilder("com.example.kafka@1.0.0")
            function main(event) b:setLabel("committed"); emit(b:build()) end"#,
            SourceDelivery::AtMostOnce,
            [&contract],
            vec![source.queues()],
            vec![vec![binding]],
        )?;
        let mut runtime = start_runtime(spec, startup_control()).map_err(io::Error::other)?;
        source.submit(1, source_payload("commit")?)?;
        assert_eq!(sink_label(&sink.wait_record()?.payload)?, "committed");
        let executor = current_thread_executor()?;
        executor.block_on(async {
            let mut draining = Box::pin(runtime.stop_channels_and_drain_egress());
            poll_fn(|context| {
                assert!(draining.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            match outcome {
                DrainOutcome::Released => {
                    sink.release(1)?;
                    assert_eq!(
                        tokio::time::timeout(WAIT_LIMIT, &mut draining)
                            .await
                            .map_err(io::Error::other)?
                            .map_err(io::Error::other)?,
                        PipelineDrainObservation::Drained
                    );
                }
                DrainOutcome::Canceled => {}
                DrainOutcome::CoreFailure => {
                    // Corrupt Queue v1 commit before the real reader wakes its writer.
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(&sink.path)?
                        .write_all_at(&1_u64.to_le_bytes(), 64)?;
                    sink.release(1)?;
                    assert_eq!(
                        tokio::time::timeout(WAIT_LIMIT, &mut draining)
                            .await
                            .map_err(io::Error::other)?
                            .map_err(io::Error::other)?,
                        PipelineDrainObservation::WorkerExited
                    );
                }
            }
            drop(draining);
            let result = runtime.stop_and_join().await;
            match outcome {
                DrainOutcome::CoreFailure => assert!(matches!(
                    result,
                    Err(PipelineRuntimeError::FlowChannelFailed { .. })
                )),
                _ => result.map_err(io::Error::other)?,
            }
            Ok::<_, io::Error>(())
        })?;
        if matches!(outcome, DrainOutcome::Canceled) {
            let mut replay = sink.replay_reader()?;
            assert!(matches!(
                replay.try_read().map_err(io::Error::other)?,
                ReadOutcome::Record(_)
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum DrainOutcome {
    Released,
    Canceled,
    CoreFailure,
}
