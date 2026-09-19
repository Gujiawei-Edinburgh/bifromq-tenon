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
fn multiple_flows_keep_routes_local_with_independent_queues_to_shared_instances() -> io::Result<()>
{
    let directory = tempfile::tempdir()?;
    let gateway_a_directory = directory.path().join("gateway-a");
    let gateway_b_directory = directory.path().join("gateway-b");
    let audit_directory = directory.path().join("audit");
    std::fs::create_dir_all(&gateway_a_directory)?;
    std::fs::create_dir_all(&gateway_b_directory)?;
    std::fs::create_dir_all(&audit_directory)?;

    // Source and Egress Queues under `gateway-a` model one dual-interface
    // instance. The self route must still cross the ordinary Queue boundary.
    let mut a_to_b_source = SourceEndpoint::create(&gateway_a_directory, 0)?;
    let mut a_self_source = SourceEndpoint::create(&gateway_a_directory, 1)?;
    let mut b_to_a_source = SourceEndpoint::create(&gateway_b_directory, 0)?;
    let gateway = sink_contract_id("com.example.gateway@1.0.0")?;
    let (gateway_a_binding, mut gateway_a_sink) =
        EgressEndpoint::create(&gateway_a_directory, "gateway-a", &gateway)?;
    let (gateway_b_binding, mut gateway_b_sink) =
        EgressEndpoint::create(&gateway_b_directory, "gateway-b", &gateway)?;
    let (audit_binding, mut audit_sink) =
        EgressEndpoint::create(&audit_directory, "audit", &gateway)?;

    let (incoming_binding, mut incoming_sink) =
        EgressEndpoint::create_for_channel(&gateway_a_directory, 0, "gateway-a", &gateway)?;
    let mut self_binding = gateway_a_binding;
    self_binding.1.extend(audit_binding.1);
    let flows = BTreeMap::from([
        (
            flow_id("a-self")?,
            flow_runtime_spec(
                r#"
                local gateway = registry:getBuilder("com.example.gateway@1.0.0")

                function main(event)
                    gateway:setLabel("a-self")
                    emit(gateway:build())
                end
                "#,
                vec![a_self_source.queues()],
                vec![vec![self_binding]],
            )?,
        ),
        (
            flow_id("a-to-b")?,
            flow_runtime_spec(
                r#"
                local gateway = registry:getBuilder("com.example.gateway@1.0.0")

                function main(event)
                    gateway:setLabel("a-to-b")
                    emit(gateway:build())
                end
                "#,
                vec![a_to_b_source.queues()],
                vec![vec![gateway_b_binding]],
            )?,
        ),
        (
            flow_id("b-to-a")?,
            flow_runtime_spec(
                r#"
                local gateway = registry:getBuilder("com.example.gateway@1.0.0")

                function main(event)
                    gateway:setLabel("b-to-a")
                    emit(gateway:build())
                end
                "#,
                vec![b_to_a_source.queues()],
                vec![vec![incoming_binding]],
            )?,
        ),
    ]);
    let runtime = RunningRuntime::start(RuntimeInputs::new(test_publisher(), flows))?;
    let pipeline = runtime
        .runtime
        .as_ref()
        .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
    assert_eq!(pipeline.flows.len(), 3);
    assert!(pipeline.flows.values().all(|flow| flow.worker_count() == 1));

    a_to_b_source.submit(201, source_payload("outbound")?)?;
    assert_eq!(
        sink_label(
            &gateway_b_sink
                .wait_record()
                .map_err(|error| io::Error::other(format!("a-to-b output: {error}")))?
                .payload,
        )?,
        "a-to-b"
    );
    gateway_b_sink.release(1)?;
    assert_eq!(
        a_to_b_source.wait_completion()?,
        completion(201, IngressCompletionStatus::Ok)
    );
    assert!(gateway_a_sink.try_record()?.is_none());
    assert!(audit_sink.try_record()?.is_none());

    b_to_a_source.submit(202, source_payload("inbound")?)?;
    assert_eq!(
        sink_label(
            &incoming_sink
                .wait_record()
                .map_err(|error| io::Error::other(format!("b-to-a output: {error}")))?
                .payload,
        )?,
        "b-to-a"
    );
    incoming_sink.release(1)?;
    assert_eq!(
        b_to_a_source.wait_completion()?,
        completion(202, IngressCompletionStatus::Ok)
    );
    assert!(gateway_b_sink.try_record()?.is_none());
    assert!(audit_sink.try_record()?.is_none());

    a_self_source.submit(203, source_payload("self")?)?;
    assert_eq!(
        sink_label(
            &gateway_a_sink
                .wait_record()
                .map_err(|error| io::Error::other(format!("a-self output: {error}")))?
                .payload,
        )?,
        "a-self"
    );
    assert_eq!(
        sink_label(
            &audit_sink
                .wait_record()
                .map_err(|error| io::Error::other(format!("a-self audit output: {error}")))?
                .payload,
        )?,
        "a-self"
    );
    gateway_a_sink.release(1)?;
    assert!(a_self_source.try_completion()?.is_none());
    audit_sink.release(1)?;
    assert_eq!(
        a_self_source.wait_completion()?,
        completion(203, IngressCompletionStatus::Ok)
    );

    let mut runtime = runtime;
    let mut active = runtime
        .runtime
        .take()
        .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
    runtime.executor.block_on(async {
        let flows = active.flows.keys().cloned().collect();
        active.request_flow_retirement(&flows);
        let observation = tokio::time::timeout(
            WAIT_LIMIT,
            std::future::poll_fn(|context| active.poll_resource_retirement(&flows, context)),
        )
        .await
        .map_err(|_| io::Error::other("Flow channels did not drain"))?;
        assert_eq!(observation, PipelineDrainObservation::Drained);
        let repeated = tokio::time::timeout(
            Duration::from_millis(50),
            std::future::poll_fn(|context| active.poll_resource_retirement(&flows, context)),
        )
        .await
        .map_err(|_| io::Error::other("Drained Flow channels were reported as pending"))?;
        assert_eq!(repeated, PipelineDrainObservation::Drained);
        active
            .finish_resource_retirement(&flows)
            .await
            .map_err(io::Error::other)?;
        active.stop_and_join().await.map_err(io::Error::other)
    })
}

#[test]
fn slow_sink_blocks_only_the_flow_that_targets_it() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut slow_source = SourceEndpoint::create(directory.path(), 0)?;
    let mut fast_source = SourceEndpoint::create(directory.path(), 1)?;
    let slow = sink_contract_id("com.example.slow@1.0.0")?;
    let fast = sink_contract_id("com.example.fast@1.0.0")?;
    let (slow_binding, mut slow_sink) = EgressEndpoint::create_with_capacity(
        directory.path(),
        "slow",
        &slow,
        egress_capacity_for("slow-output")?,
    )?;
    let (fast_binding, mut fast_sink) = EgressEndpoint::create(directory.path(), "fast", &fast)?;
    let flows = BTreeMap::from([
        (
            flow_id("slow-flow")?,
            flow_runtime_spec(
                r#"
                local sink = registry:getBuilder("com.example.slow@1.0.0")
                function main(event)
                    sink:setLabel("slow-output")
                    emit(sink:build())
                end
                "#,
                vec![slow_source.queues()],
                vec![vec![slow_binding]],
            )?,
        ),
        (
            flow_id("fast-flow")?,
            flow_runtime_spec(
                r#"
                local sink = registry:getBuilder("com.example.fast@1.0.0")
                function main(event)
                    sink:setLabel("fast-output")
                    emit(sink:build())
                end
                "#,
                vec![fast_source.queues()],
                vec![vec![fast_binding]],
            )?,
        ),
    ]);
    let runtime = RunningRuntime::start(RuntimeInputs::new(test_publisher(), flows))?;

    slow_source.submit(211, source_payload("slow")?)?;
    assert_eq!(
        sink_label(&slow_sink.wait_record()?.payload)?,
        "slow-output"
    );
    assert!(slow_source.try_completion()?.is_none());

    fast_source.submit(212, source_payload("fast")?)?;
    assert_eq!(
        sink_label(&fast_sink.wait_record()?.payload)?,
        "fast-output"
    );
    fast_sink.release(1)?;
    assert_eq!(
        fast_source.wait_completion()?,
        completion(212, IngressCompletionStatus::Ok)
    );
    assert!(slow_source.try_completion()?.is_none());

    slow_sink.release(1)?;
    assert_eq!(
        slow_source.wait_completion()?,
        completion(211, IngressCompletionStatus::Ok)
    );
    runtime.stop()
}

#[test]
fn one_flow_failure_stops_and_joins_every_flow() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let healthy_source = SourceEndpoint::create(directory.path(), 0)?;
    let mut broken_source = SourceEndpoint::create(directory.path(), 1)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, _sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (broken_binding, _broken_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let flows = BTreeMap::from([
        (
            flow_id("a-healthy")?,
            flow_runtime_spec(
                "function main(event) end",
                vec![healthy_source.queues()],
                vec![vec![binding]],
            )?,
        ),
        (
            flow_id("z-broken")?,
            flow_runtime_spec(
                "function main(event) end",
                vec![broken_source.queues()],
                vec![vec![broken_binding]],
            )?,
        ),
    ]);
    let runtime = RunningRuntime::start(RuntimeInputs::new(test_publisher(), flows))?;

    broken_source.submit_raw(&[0x0a, 0x02, b'x'])?;
    let error = runtime.wait_for_failure()?;
    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelFailed {
            flow_id,
            channel_index: 0,
            ..
        } if flow_id.as_str() == "z-broken"
    ));
    Ok(())
}

#[test]
fn one_flow_startup_failure_aborts_every_flow_before_activation() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let ready_source = SourceEndpoint::create(directory.path(), 0)?;
    let missing_source = TestChannelQueues::at(directory.path(), 1)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (broken_binding, mut broken_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let flows = BTreeMap::from([
        (
            flow_id("a-ready")?,
            flow_runtime_spec(
                r#"
                local sink = registry:getBuilder("com.example.kafka@1.0.0")
                setTimeout(0)
                function main(event)
                    if event.type == "timer" then
                        sink:setLabel("startup-leak")
                        emit(sink:build())
                    end
                end
                "#,
                vec![ready_source.queues()],
                vec![vec![binding]],
            )?,
        ),
        (
            flow_id("b-broken")?,
            flow_runtime_spec(
                "function main(event) end",
                vec![missing_source],
                vec![vec![broken_binding]],
            )?,
        ),
    ]);

    let error = start_error(RuntimeInputs::new(test_publisher(), flows))?;
    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelOpen {
            flow_id,
            channel_index: 0,
            ..
        } if flow_id.as_str() == "b-broken"
    ));
    assert!(sink.try_record()?.is_none());
    assert!(broken_sink.try_record()?.is_none());
    Ok(())
}
