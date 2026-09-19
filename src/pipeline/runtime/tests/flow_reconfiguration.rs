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

#[test]
fn batch_retirement_waits_for_real_release_and_keeps_unselected_flow_state() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut sources = (0..3)
        .map(|index| SourceEndpoint::create(directory.path(), index))
        .collect::<io::Result<Vec<_>>>()?;
    let contract = sink_contract_id("com.example.kafka@1.0.0")?;
    let mut sinks = Vec::new();
    let ids = [
        flow_id("selected-a")?,
        flow_id("selected-b")?,
        flow_id("kept")?,
    ];
    let mut flows = BTreeMap::new();
    for (index, (id, source)) in ids.iter().zip(&sources).enumerate() {
        let (binding, sink) =
            EgressEndpoint::create_for_channel(directory.path(), index, "kafka", &contract)?;
        sinks.push(sink);
        flows.insert(id.clone(), flow_runtime_spec(
            "local b = registry:getBuilder('com.example.kafka@1.0.0'); local count = 0; function main(event) count = count + 1; b:setLabel(event.payload.deviceId .. ':' .. count); emit(b:build()) end",
            vec![source.queues()], vec![vec![binding.clone()]],
        )?);
    }
    let mut runtime = start_runtime(
        RuntimeInputs::new(test_publisher(), flows),
        startup_control(),
    )
    .map_err(io::Error::other)?;
    for (index, source) in sources.iter_mut().enumerate() {
        source.submit(index as u64, source_payload(ids[index].as_str())?)?;
    }
    for sink in &mut sinks {
        sink.wait_record()?;
    }
    let selected = ids[..2].iter().cloned().collect();
    runtime.request_flow_retirement(&selected);
    assert!(
        runtime
            .poll_resource_retirement(
                &selected,
                &mut std::task::Context::from_waker(std::task::Waker::noop()),
            )
            .is_pending()
    );
    for sink in &mut sinks {
        sink.release(1)?;
    }
    for (index, source) in sources.iter_mut().enumerate() {
        assert_eq!(
            source.wait_completion()?,
            completion(index as u64, IngressCompletionStatus::Ok)
        );
    }
    let executor = current_thread_executor()?;
    assert_eq!(
        executor.block_on(std::future::poll_fn(
            |context| runtime.poll_resource_retirement(&selected, context)
        )),
        super::super::PipelineDrainObservation::Drained
    );
    executor
        .block_on(runtime.finish_resource_retirement(&selected))
        .map_err(io::Error::other)?;
    assert_eq!(runtime.flows.len(), 1);
    sources[2].submit(10, source_payload("kept")?)?;
    assert_eq!(sink_label(&sinks[2].wait_record()?.payload)?, "kept:2");
    sinks[2].release(1)?;
    assert_eq!(
        sources[2].wait_completion()?,
        completion(10, IngressCompletionStatus::Ok)
    );
    executor
        .block_on(runtime.stop_and_join())
        .map_err(io::Error::other)
}

#[test]
fn prepared_definition_keeps_old_data_and_source_finish_running_before_cutover() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let contract = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &contract)?;
    let flow_id = flow_id("changing")?;
    let script = |label: &str| {
        format!(
            "local b = registry:getBuilder('com.example.kafka@1.0.0'); local count = 0; function main(event) count = count + 1; b:setLabel('{label}-' .. count); emit(b:build()) end"
        )
    };
    let flow = flow_runtime_spec(
        &script("old"),
        vec![source.queues()],
        vec![vec![binding.clone()]],
    )?;
    let runtime = start_runtime(
        RuntimeInputs::new(test_publisher(), BTreeMap::from([(flow_id.clone(), flow)])),
        startup_control(),
    )
    .map_err(io::Error::other)?;
    submit_and_expect_label(&mut source, &mut sink, 1, "old-1")?;

    let mut prepared = runtime
        .flow_definition_replacement(&flow_id)
        .map_err(io::Error::other)?
        .begin(
            ChannelDefinitionChange::Replace(channel_spec(
                &script("new"),
                SourceDelivery::AtLeastOnce,
                [&contract],
            )?),
            vec![prepared_routes(vec![binding], Some(&source.channel_region))],
        )
        .and_then(|pending| pending.wait())
        .map_err(io::Error::other)?;
    submit_and_expect_label(&mut source, &mut sink, 2, "old-2")?;
    let mut finished = runtime
        .begin_source_session_finish(&flow_id)
        .map_err(io::Error::other)?;
    let executor = current_thread_executor()?;
    executor
        .block_on(async { tokio::time::timeout(WAIT_LIMIT, finished.wait()).await })
        .map_err(|_| io::Error::other("Prepared Channel blocked old Source session finish"))?
        .map_err(io::Error::other)?;

    prepared.cutover_in_place().map_err(io::Error::other)?;
    prepared
        .into_paused()
        .activate()
        .map_err(io::Error::other)?;
    submit_and_expect_label(&mut source, &mut sink, 3, "new-1")?;
    executor
        .block_on(runtime.stop_and_join())
        .map_err(io::Error::other)
}

#[test]
fn additive_batch_adopts_every_owner_before_one_activation_and_keeps_the_time_origin()
-> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut stable_source = SourceEndpoint::create(directory.path(), 0)?;
    let first_source = SourceEndpoint::create(directory.path(), 1)?;
    let second_source = SourceEndpoint::create(directory.path(), 2)?;
    let contract = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &contract)?;
    let stable = labelled_flow_spec("stable", stable_source.queues(), &binding)?;
    let runtime = start_runtime(
        RuntimeInputs::new(
            test_publisher(),
            BTreeMap::from([(flow_id("stable")?, stable)]),
        ),
        startup_control(),
    )
    .map_err(io::Error::other)?;
    let origin = runtime.pipeline_started_at;
    let (first_binding, mut first_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &contract)?;
    let (second_binding, mut second_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 2, "kafka", &contract)?;
    let timer = |label: &str, queues, binding: EgressBinding| {
        flow_runtime_spec(
            &format!(
                "local b = registry:getBuilder('com.example.kafka@1.0.0'); setTimeout(0); function main(event) b:setLabel('{label}'); emit(b:build()) end"
            ),
            vec![queues],
            vec![vec![binding.clone()]],
        )
    };
    let mut prepared = PipelineRuntime::prepare_resources(
        test_publisher(),
        BTreeMap::from([
            (
                flow_id("added-a")?,
                timer("added-a", first_source.queues(), first_binding)?,
            ),
            (
                flow_id("added-b")?,
                timer("added-b", second_source.queues(), second_binding)?,
            ),
        ]),
        Some(runtime.started_at()),
        startup_control(),
    )
    .map_err(io::Error::other)?;
    prepared.retain(runtime);
    assert_eq!(
        prepared_runtime_test_support::data_plane_counts(&prepared),
        (3, 3)
    );
    assert!(prepared_runtime_test_support::is_pending(&prepared));
    submit_and_expect_label(&mut stable_source, &mut sink, 1, "stable")?;
    assert!(first_sink.try_record()?.is_none());
    assert!(second_sink.try_record()?.is_none());
    current_thread_executor()?
        .block_on(prepared.bind())
        .map_err(io::Error::other)?;
    let runtime = prepared.activate();
    assert_eq!(runtime.pipeline_started_at, origin);
    let mut labels = HashSet::new();
    for sink in [&mut first_sink, &mut second_sink] {
        labels.insert(sink_label(&sink.wait_record()?.payload)?);
        sink.release(1)?;
    }
    assert_eq!(
        labels,
        HashSet::from([String::from("added-a"), String::from("added-b")])
    );
    current_thread_executor()?
        .block_on(runtime.stop_and_join())
        .map_err(io::Error::other)
}

#[test]
fn an_empty_resource_batch_has_nothing_to_observe_or_join() -> io::Result<()> {
    let mut prepared = PipelineRuntime::prepare_resources(
        test_publisher(),
        BTreeMap::new(),
        None,
        startup_control(),
    )
    .map_err(io::Error::other)?;
    current_thread_executor()?
        .block_on(prepared.bind())
        .map_err(io::Error::other)?;
    let mut runtime = prepared.activate();
    assert!(!runtime.has_worker_exited_now());
    current_thread_executor()?
        .block_on(runtime.stop_and_join())
        .map_err(io::Error::other)
}

#[test]
fn add_replace_and_delete_one_flow_preserve_the_unrelated_flow_and_egress() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut stable_source = SourceEndpoint::create(directory.path(), 0)?;
    let mut first_candidate_source = SourceEndpoint::create(directory.path(), 1)?;
    let mut replacement_source = SourceEndpoint::create(directory.path(), 2)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (changed_binding, mut changed_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let stable_flow_id = flow_id("stable")?;
    let changed_flow_id = flow_id("changed")?;
    let stable_flow = flow_runtime_spec(
        r#"
        local sink = registry:getBuilder("com.example.kafka@1.0.0")
        local count = 0

        function main(event)
            count = count + 1
            sink:setLabel("stable:" .. count)
            emit(sink:build())
        end
        "#,
        vec![stable_source.queues()],
        vec![vec![binding.clone()]],
    )?;
    let initial_changed_flow =
        labelled_flow_spec("initial", first_candidate_source.queues(), &changed_binding)?;
    let mut runtime = start_runtime(
        RuntimeInputs::new(
            test_publisher(),
            BTreeMap::from([
                (stable_flow_id.clone(), stable_flow),
                (changed_flow_id.clone(), initial_changed_flow),
            ]),
        ),
        startup_control(),
    )
    .map_err(io::Error::other)?;
    let executor = current_thread_executor()?;

    submit_and_expect_label(&mut stable_source, &mut sink, 301, "stable:1")?;
    submit_and_expect_label(
        &mut first_candidate_source,
        &mut changed_sink,
        302,
        "initial",
    )?;
    retire_flow(&executor, &mut runtime, &changed_flow_id)?;
    submit_and_expect_label(&mut stable_source, &mut sink, 303, "stable:2")?;

    let mut candidate = PipelineRuntime::prepare_resources(
        test_publisher(),
        BTreeMap::from([(
            changed_flow_id.clone(),
            labelled_flow_spec(
                "candidate",
                first_candidate_source.queues(),
                &changed_binding,
            )?,
        )]),
        Some(runtime.started_at()),
        startup_control(),
    )
    .map_err(io::Error::other)?;
    first_candidate_source.submit(304, source_payload("before-activation")?)?;
    assert!(sink.try_record()?.is_none());
    assert!(first_candidate_source.try_completion()?.is_none());

    candidate.retain(runtime);
    executor
        .block_on(candidate.bind())
        .map_err(io::Error::other)?;
    runtime = candidate.activate();
    assert_eq!(
        sink_label(&changed_sink.wait_record()?.payload)?,
        "candidate"
    );
    changed_sink.release(1)?;
    assert_eq!(
        first_candidate_source.wait_completion()?,
        completion(304, IngressCompletionStatus::Ok)
    );
    stable_source.submit(305, source_payload("stable")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "stable:3");
    assert!(stable_source.try_completion()?.is_none());

    let mut replacement = PipelineRuntime::prepare_resources(
        test_publisher(),
        BTreeMap::from([(
            changed_flow_id.clone(),
            labelled_flow_spec("replacement", replacement_source.queues(), &changed_binding)?,
        )]),
        Some(runtime.started_at()),
        startup_control(),
    )
    .map_err(io::Error::other)?;
    retire_flow(&executor, &mut runtime, &changed_flow_id)?;
    first_candidate_source.submit(306, source_payload("retired")?)?;
    assert!(first_candidate_source.try_completion()?.is_none());
    assert!(sink.try_record()?.is_none());

    replacement.retain(runtime);
    executor
        .block_on(replacement.bind())
        .map_err(io::Error::other)?;
    runtime = replacement.activate();
    sink.release(1)?;
    assert_eq!(
        stable_source.wait_completion()?,
        completion(305, IngressCompletionStatus::Ok)
    );
    submit_and_expect_label(
        &mut replacement_source,
        &mut changed_sink,
        307,
        "replacement",
    )?;
    submit_and_expect_label(&mut stable_source, &mut sink, 308, "stable:4")?;

    retire_flow(&executor, &mut runtime, &changed_flow_id)?;
    replacement_source.submit(309, source_payload("deleted")?)?;
    assert!(replacement_source.try_completion()?.is_none());
    assert!(sink.try_record()?.is_none());
    submit_and_expect_label(&mut stable_source, &mut sink, 310, "stable:5")?;
    assert_eq!(runtime.flows.len(), 1);
    assert!(runtime.flows.contains_key(&stable_flow_id));

    executor
        .block_on(runtime.stop_and_join())
        .map_err(io::Error::other)
}

#[test]
fn failed_candidate_preparation_leaves_both_live_flows_untouched() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut stable_source = SourceEndpoint::create(directory.path(), 0)?;
    let mut target_source = SourceEndpoint::create(directory.path(), 1)?;
    let ready_candidate_source = SourceEndpoint::create(directory.path(), 2)?;
    let missing_candidate = TestChannelQueues::at(directory.path(), 3)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (target_binding, mut target_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let (extra_binding, mut extra_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 2, "kafka", &sink_contract_id)?;
    let stable_flow_id = flow_id("stable")?;
    let target_flow_id = flow_id("target")?;
    let flows = BTreeMap::from([
        (
            stable_flow_id,
            labelled_flow_spec("stable", stable_source.queues(), &binding)?,
        ),
        (
            target_flow_id.clone(),
            labelled_flow_spec("original", target_source.queues(), &target_binding)?,
        ),
    ]);
    let runtime = start_runtime(
        RuntimeInputs::new(test_publisher(), flows),
        startup_control(),
    )
    .map_err(io::Error::other)?;
    let candidate = flow_runtime_spec(
        r#"
        local sink = registry:getBuilder("com.example.kafka@1.0.0")
        setTimeout(0)

        function main(event)
            if event.type == "timer" then
                sink:setLabel("candidate-leak")
                emit(sink:build())
            end
        end
        "#,
        vec![ready_candidate_source.queues(), missing_candidate],
        vec![vec![target_binding], vec![extra_binding]],
    )?;

    let error = match PipelineRuntime::prepare_resources(
        test_publisher(),
        BTreeMap::from([(target_flow_id.clone(), candidate)]),
        Some(runtime.started_at()),
        startup_control(),
    ) {
        Ok(_) => return Err(io::Error::other("Broken candidate unexpectedly prepared")),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelOpen {
            flow_id,
            channel_index: 1,
            ..
        } if flow_id == target_flow_id
    ));
    assert!(sink.try_record()?.is_none());
    assert!(target_sink.try_record()?.is_none());
    assert!(extra_sink.try_record()?.is_none());
    assert_eq!(runtime.flows.len(), 2);

    submit_and_expect_label(&mut target_source, &mut target_sink, 311, "original")?;
    submit_and_expect_label(&mut stable_source, &mut sink, 312, "stable")?;
    current_thread_executor()?
        .block_on(runtime.stop_and_join())
        .map_err(io::Error::other)
}

#[test]
fn cancelled_retirement_wait_resumes_after_the_shared_release() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let flow_id = flow_id("retiring")?;
    let flow = labelled_flow_spec("blocked", source.queues(), &binding)?;
    let mut runtime = start_runtime(
        RuntimeInputs::new(test_publisher(), BTreeMap::from([(flow_id.clone(), flow)])),
        startup_control(),
    )
    .map_err(io::Error::other)?;
    let executor = current_thread_executor()?;

    source.submit(321, source_payload("blocked")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "blocked");
    let flows = BTreeSet::from([flow_id]);
    runtime.request_flow_retirement(&flows);
    {
        let mut wait = std::pin::pin!(std::future::poll_fn(
            |context| runtime.poll_resource_retirement(&flows, context)
        ));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(wait.as_mut(), &mut context).is_pending());
    }

    sink.release(1)?;
    let observation = executor
        .block_on(async {
            tokio::time::timeout(
                WAIT_LIMIT,
                std::future::poll_fn(|context| runtime.poll_resource_retirement(&flows, context)),
            )
            .await
        })
        .map_err(|_| io::Error::other("Retiring Flow did not resume after Egress release"))?;
    assert_eq!(observation, PipelineDrainObservation::Drained);
    executor
        .block_on(runtime.finish_resource_retirement(&flows))
        .map_err(io::Error::other)?;
    assert_eq!(
        source.wait_completion()?,
        completion(321, IngressCompletionStatus::Ok)
    );
    assert!(runtime.flows.is_empty());

    executor
        .block_on(runtime.stop_and_join())
        .map_err(io::Error::other)
}

#[test]
fn worker_failure_during_retirement_stops_the_complete_pipeline() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut broken_source = SourceEndpoint::create(directory.path(), 0)?;
    let mut retiring_source = SourceEndpoint::create(directory.path(), 1)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (broken_binding, _broken_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let broken_flow_id = flow_id("broken")?;
    let retiring_flow_id = flow_id("retiring")?;
    let flows = BTreeMap::from([
        (
            broken_flow_id.clone(),
            flow_runtime_spec(
                "function main(event) end",
                vec![broken_source.queues()],
                vec![vec![broken_binding]],
            )?,
        ),
        (
            retiring_flow_id.clone(),
            labelled_flow_spec("blocked", retiring_source.queues(), &binding)?,
        ),
    ]);
    let mut runtime = start_runtime(
        RuntimeInputs::new(test_publisher(), flows),
        startup_control(),
    )
    .map_err(io::Error::other)?;
    let executor = current_thread_executor()?;

    retiring_source.submit(331, source_payload("blocked")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "blocked");
    let flows = BTreeSet::from([retiring_flow_id]);
    runtime.request_flow_retirement(&flows);
    broken_source.submit_raw(&[0x0a, 0x02, b'x'])?;
    let observation = executor
        .block_on(async {
            tokio::time::timeout(
                WAIT_LIMIT,
                std::future::poll_fn(|context| runtime.poll_resource_retirement(&flows, context)),
            )
            .await
        })
        .map_err(|_| io::Error::other("Pipeline failure did not interrupt Flow retirement"))?;
    assert_eq!(observation, PipelineDrainObservation::WorkerExited);
    let error = match executor.block_on(runtime.stop_and_join()) {
        Ok(()) => {
            return Err(io::Error::other(
                "Observed worker failure unexpectedly stopped cleanly",
            ));
        }
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelFailed {
            flow_id,
            channel_index: 0,
            ..
        } if flow_id == broken_flow_id
    ));
    Ok(())
}

pub(super) fn labelled_flow_spec(
    label: &str,
    queues: TestChannelQueues,
    binding: &EgressBinding,
) -> io::Result<FlowRuntimeSpec> {
    flow_runtime_spec(
        &format!(
            r#"
            local sink = registry:getBuilder("com.example.kafka@1.0.0")

            function main(event)
                sink:setLabel("{label}")
                emit(sink:build())
            end
            "#
        ),
        vec![queues],
        vec![vec![binding.clone()]],
    )
}

fn submit_and_expect_label(
    source: &mut SourceEndpoint,
    sink: &mut EgressEndpoint,
    record_id: u64,
    label: &str,
) -> io::Result<()> {
    source.submit(record_id, source_payload(label)?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, label);
    sink.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(record_id, IngressCompletionStatus::Ok)
    );
    Ok(())
}

fn retire_flow(
    executor: &Runtime,
    runtime: &mut PipelineRuntime,
    flow_id: &FlowId,
) -> io::Result<()> {
    let flows = BTreeSet::from([flow_id.clone()]);
    runtime.request_flow_retirement(&flows);
    let observation = executor
        .block_on(async {
            tokio::time::timeout(
                WAIT_LIMIT,
                std::future::poll_fn(|context| runtime.poll_resource_retirement(&flows, context)),
            )
            .await
        })
        .map_err(|_| io::Error::other("Flow did not drain"))?;
    assert_eq!(observation, PipelineDrainObservation::Drained);
    executor
        .block_on(runtime.finish_resource_retirement(&flows))
        .map_err(io::Error::other)
}
