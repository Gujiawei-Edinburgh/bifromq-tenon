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
use crate::runner::pipeline::test_support::wait_for_attachment;

#[tokio::test(flavor = "current_thread")]
async fn independent_diagnostics_stream_receives_interest_and_fans_out_records() -> io::Result<()> {
    let endpoints = endpoints()?;
    let mut pending = register(&endpoints, PipelineBootstrap::default())?;
    let launch_id = pending.launch_id().to_vec();
    let server = TestServer::start(endpoints).await?;
    let (pipeline, control_response) = server
        .open(launch_id.clone())
        .await
        .map_err(io::Error::other)?;
    let session = wait_for_attachment(&mut pending)
        .await
        .map_err(io::Error::other)?;
    let pipeline_id = document_id()?;
    let flow_id =
        crate::identifiers::FlowId::try_from(String::from("main")).map_err(io::Error::other)?;
    let source_id = PluginInstanceId::try_from("source").map_err(io::Error::other)?;
    let mut subscription = server
        .diagnostics
        .subscribe(
            pipeline_id,
            RunnerDiagnosticTarget::FlowChannel {
                flow_id: flow_id.clone(),
                channel_index: 2,
            },
        )
        .map_err(io::Error::other)?;
    let _attached = subscription.next().await;
    let pipeline_id = document_id()?;
    let mut source_subscription = server
        .diagnostics
        .subscribe(
            pipeline_id,
            RunnerDiagnosticTarget::Plugin(source_id.clone()),
        )
        .map_err(io::Error::other)?;
    let _source_attached = source_subscription.next().await;

    let channel = server.connect().await.map_err(io::Error::other)?;
    let diagnostics = PipelineDiagnostics::start(channel, launch_id);
    let publisher = diagnostics.publisher().flow_channel(&flow_id, 2).lua_vm();
    let source_publisher = diagnostics.publisher().instance_plugin(source_id);
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !publisher.is_enabled() || !source_publisher.is_enabled() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| io::Error::other("Diagnostics interest did not reach Pipeline"))?;
    publisher.publish_print(String::from("runtime value").into_boxed_str(), false, false);
    let event = tokio::time::timeout(TEST_TIMEOUT, subscription.next())
        .await
        .map_err(|_| io::Error::other("Diagnostic fan-out timed out"))?
        .ok_or_else(|| io::Error::other("Diagnostic subscription ended"))?;
    assert!(matches!(
        event,
        RunnerDiagnosticFrame::Diagnostic(record)
            if matches!(
                &record.source,
                RunnerDiagnosticSource::FlowChannel {
                    channel_index: 2,
                    lua_vm_instance_id: 1,
                    ..
                }
            )
                && record.text.as_ref() == "runtime value"
    ));
    source_publisher.publish(
        PluginDiagnosticStream::Stderr,
        String::from("source failed").into_boxed_str(),
        false,
        false,
    );
    let source_event = tokio::time::timeout(TEST_TIMEOUT, source_subscription.next())
        .await
        .map_err(|_| io::Error::other("Source diagnostic fan-out timed out"))?
        .ok_or_else(|| io::Error::other("Source diagnostic subscription ended"))?;
    assert!(matches!(
        source_event,
        RunnerDiagnosticFrame::Diagnostic(record)
            if matches!(
                &record.source,
                RunnerDiagnosticSource::Plugin {
                    plugin_process_instance_id: 1,
                    stream: RunnerPluginDiagnosticStream::Stderr,
                    ..
                }
            ) && record.text.as_ref() == "source failed"
    ));

    diagnostics.shutdown().await;
    drop(session);
    drop(pipeline);
    drop(control_response);
    drop(pending);
    server.finish().await
}
#[tokio::test(flavor = "current_thread")]
async fn diagnostics_claim_is_bound_to_one_attached_launch_and_can_reconnect() -> io::Result<()> {
    let endpoints = endpoints()?;
    let diagnostics = endpoints.diagnostics.clone();
    let pending = register(&endpoints, PipelineBootstrap::default())?;
    let launch_id = pending.launch_id().to_vec();
    assert!(
        endpoints
            .launches
            .claim_diagnostics(&launch_id, &diagnostics)
            .is_none()
    );

    let control_claim = endpoints
        .launches
        .claim_control(&launch_id)
        .ok_or_else(|| io::Error::other("Control launch could not be claimed"))?;
    assert!(
        endpoints
            .launches
            .confirm_control_attachment(&launch_id)
            .is_some()
    );
    let (first, mut lifetime) = endpoints
        .launches
        .claim_diagnostics(&launch_id, &diagnostics)
        .ok_or_else(|| io::Error::other("Attached diagnostics launch could not be claimed"))?;
    assert_eq!(first.document_id().as_str(), "pipeline-a");
    assert!(
        endpoints
            .launches
            .claim_diagnostics(&launch_id, &diagnostics)
            .is_none()
    );
    let instance_id = first.pipeline_instance_id();
    let mut interest = diagnostics.interest(first.document_id());

    drop(first);
    let (second, _second_lifetime) = endpoints
        .launches
        .claim_diagnostics(&launch_id, &diagnostics)
        .ok_or_else(|| io::Error::other("Diagnostics reconnect was rejected"))?;
    assert_eq!(second.document_id().as_str(), "pipeline-a");
    assert_eq!(second.pipeline_instance_id(), instance_id);
    drop(second);
    drop(control_claim);
    drop(pending);
    assert!(lifetime.changed().await.is_err());
    assert!(interest.changed().await.is_err());
    assert!(
        endpoints
            .launches
            .claim_diagnostics(&launch_id, &diagnostics)
            .is_none()
    );
    Ok(())
}
