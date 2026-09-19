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
use crate::identifiers::{FlowId, PluginInstanceId};
use crate::runner::diagnostics::{RunnerDiagnostic, RunnerDiagnosticSubscription};
use crate::runner::pipeline::test_support::wait_for_attachment;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tokio_stream::Stream as _;

#[tokio::test(flavor = "current_thread")]
async fn instance_subscriptions_keep_processes_and_stdout_stderr_sequences_distinct()
-> io::Result<()> {
    with_diagnostics(async |runner, pipeline, pipeline_id| {
        let id_a = PluginInstanceId::try_from(String::from("device/a")).map_err(io::Error::other)?;
        let id_b = PluginInstanceId::try_from(String::from("device/b")).map_err(io::Error::other)?;
        let mut a = runner.subscribe(pipeline_id.clone(), RunnerDiagnosticTarget::Plugin(id_a.clone())).map_err(io::Error::other)?;
        let mut b = runner.subscribe(pipeline_id.clone(), RunnerDiagnosticTarget::Plugin(id_b.clone())).map_err(io::Error::other)?;
        assert!(matches!(a.next().await, Some(RunnerDiagnosticFrame::Attached)));
        assert!(matches!(b.next().await, Some(RunnerDiagnosticFrame::Attached)));
        let publisher = pipeline.publisher();
        let plugin_a = publisher.instance_plugin(id_a.clone());
        let plugin_b = publisher.instance_plugin(id_b.clone());
        while !plugin_a.is_enabled() || !plugin_b.is_enabled() { tokio::task::yield_now().await; }

        plugin_a.publish(PluginDiagnosticStream::Stdout, "a-out".into(), false, false);
        plugin_b.publish(PluginDiagnosticStream::Stderr, "b-error".into(), false, false);
        plugin_a.publish(PluginDiagnosticStream::Stderr, "a-error".into(), false, false);
        let a_out = next_record(&mut a).await?;
        let a_error = next_record(&mut a).await?;
        let b_error = next_record(&mut b).await?;
        assert_eq!((a_out.text.as_ref(), a_error.text.as_ref(), b_error.text.as_ref()), ("a-out", "a-error", "b-error"));
        assert_eq!((a_out.sequence, a_error.sequence, b_error.sequence), (0, 1, 0));
        for (record, expected_id, expected_incarnation, expected_stream) in [
            (&a_out, &id_a, 1, RunnerPluginDiagnosticStream::Stdout),
            (&a_error, &id_a, 1, RunnerPluginDiagnosticStream::Stderr),
            (&b_error, &id_b, 2, RunnerPluginDiagnosticStream::Stderr),
        ] {
            assert!(matches!(&record.source, RunnerDiagnosticSource::Plugin { plugin_instance_id, plugin_process_instance_id, stream } if plugin_instance_id == expected_id && *plugin_process_instance_id == expected_incarnation && *stream == expected_stream));
        }
        assert_pending(&mut a).await?;
        assert_pending(&mut b).await?;

        let replacement = publisher.instance_plugin(id_a);
        replacement.publish(PluginDiagnosticStream::Stdout, "replacement".into(), false, false);
        let record = next_record(&mut a).await?;
        assert_eq!(record.sequence, 0);
        assert!(matches!(record.source, RunnerDiagnosticSource::Plugin { plugin_process_instance_id: 3, .. }));

        drop(b);
        while plugin_b.is_enabled() { tokio::task::yield_now().await; }
        assert!(plugin_a.is_enabled());
        plugin_a.publish(PluginDiagnosticStream::Stdout, "a-remains".into(), false, false);
        assert_eq!(next_record(&mut a).await?.text.as_ref(), "a-remains");
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn equal_channel_indices_in_different_flows_are_selected_independently() -> io::Result<()> {
    with_diagnostics(async |runner, pipeline, pipeline_id| {
        let flow_a = FlowId::try_from(String::from("telemetry/a")).map_err(io::Error::other)?;
        let flow_b = FlowId::try_from(String::from("commands/b")).map_err(io::Error::other)?;
        let mut a = runner.subscribe(pipeline_id.clone(), RunnerDiagnosticTarget::FlowChannel { flow_id: flow_a.clone(), channel_index: 0 }).map_err(io::Error::other)?;
        let mut b = runner.subscribe(pipeline_id.clone(), RunnerDiagnosticTarget::FlowChannel { flow_id: flow_b.clone(), channel_index: 0 }).map_err(io::Error::other)?;
        let _ = a.next().await;
        let _ = b.next().await;
        let publisher = pipeline.publisher();
        let channel_a = publisher.flow_channel(&flow_a, 0);
        let vm_a = channel_a.lua_vm();
        let vm_b = publisher.flow_channel(&flow_b, 0).lua_vm();
        let unselected = publisher.flow_channel(&flow_a, 1).lua_vm();
        while !vm_a.is_enabled() || !vm_b.is_enabled() { tokio::task::yield_now().await; }
        assert!(!unselected.is_enabled());

        vm_a.publish_print("telemetry".into(), false, false);
        vm_b.publish_print("command".into(), false, false);
        let first_a = next_record(&mut a).await?;
        let first_b = next_record(&mut b).await?;
        assert_eq!((first_a.text.as_ref(), first_b.text.as_ref()), ("telemetry", "command"));
        assert!(matches!(&first_a.source, RunnerDiagnosticSource::FlowChannel { flow_id, channel_index: 0, channel_instance_id: 1, lua_vm_instance_id: 1, .. } if flow_id == &flow_a));
        assert!(matches!(&first_b.source, RunnerDiagnosticSource::FlowChannel { flow_id, channel_index: 0, channel_instance_id: 2, lua_vm_instance_id: 1, .. } if flow_id == &flow_b));
        assert_pending(&mut a).await?;
        assert_pending(&mut b).await?;

        let replacement_vm = channel_a.lua_vm();
        replacement_vm.publish_print("new-vm".into(), false, false);
        let replaced = next_record(&mut a).await?;
        assert_eq!(replaced.sequence, 1);
        assert!(matches!(replaced.source, RunnerDiagnosticSource::FlowChannel { channel_instance_id: 1, lua_vm_instance_id: 2, .. }));
        drop(a);
        while vm_a.is_enabled() { tokio::task::yield_now().await; }
        assert!(vm_b.is_enabled());
        vm_b.publish_print("still-selected".into(), false, false);
        assert_eq!(next_record(&mut b).await?.text.as_ref(), "still-selected");
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn a_slow_instance_subscriber_does_not_block_another_subscriber() -> io::Result<()> {
    with_diagnostics(async |runner, pipeline, pipeline_id| {
        let id = PluginInstanceId::try_from(String::from("device")).map_err(io::Error::other)?;
        let target = RunnerDiagnosticTarget::Plugin(id.clone());
        let mut slow = runner
            .subscribe(pipeline_id.clone(), target.clone())
            .map_err(io::Error::other)?;
        let mut fast = runner
            .subscribe(pipeline_id.clone(), target)
            .map_err(io::Error::other)?;
        let _ = slow.next().await;
        let _ = fast.next().await;
        let plugin = pipeline.publisher().instance_plugin(id);
        while !plugin.is_enabled() {
            tokio::task::yield_now().await;
        }
        // Awaiting the fast subscriber proves each record reached Runner; the
        // slow subscriber alone accumulates its bounded backlog.
        for sequence in 0..300 {
            plugin.publish(
                PluginDiagnosticStream::Stdout,
                "record".into(),
                false,
                false,
            );
            assert_eq!(next_record(&mut fast).await?.sequence, sequence);
        }
        for expected in 0..256 {
            assert_eq!(next_record(&mut slow).await?.sequence, expected);
        }
        assert_pending(&mut slow).await?;
        plugin.publish(
            PluginDiagnosticStream::Stdout,
            "after-gap".into(),
            false,
            false,
        );
        assert_eq!(next_record(&mut fast).await?.sequence, 300);
        assert_eq!(next_record(&mut slow).await?.sequence, 300);
        Ok(())
    })
    .await
}

async fn with_diagnostics(
    case: impl AsyncFnOnce(&RunnerDiagnostics, &PipelineDiagnostics, &TenonDocumentId) -> io::Result<()>,
) -> io::Result<()> {
    let endpoints = endpoints()?;
    let mut pending = register(&endpoints, PipelineBootstrap::default())?;
    let launch_id = pending.launch_id().to_vec();
    let server = TestServer::start(endpoints).await?;
    let (control, response) = server
        .open(launch_id.clone())
        .await
        .map_err(io::Error::other)?;
    let session = wait_for_attachment(&mut pending)
        .await
        .map_err(io::Error::other)?;
    let diagnostics =
        PipelineDiagnostics::start(server.connect().await.map_err(io::Error::other)?, launch_id);
    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        case(&server.diagnostics, &diagnostics, &document_id()?),
    )
    .await
    .map_err(|_| io::Error::other("Instance/Flow diagnostics scenario timed out"))
    .and_then(|result| result);
    diagnostics.shutdown().await;
    drop(session);
    drop(control);
    drop(response);
    drop(pending);
    server.finish().await?;
    result
}

#[tokio::test(flavor = "current_thread")]
async fn a_broken_uds_link_reconnects_with_the_latest_instance_selection() -> io::Result<()> {
    let endpoints = endpoints()?;
    let launches = endpoints.launches.clone();
    let mut pending = register(&endpoints, PipelineBootstrap::default())?;
    let launch_id = pending.launch_id().to_vec();
    let server = TestServer::start(endpoints).await?;
    let (control, response) = server
        .open(launch_id.clone())
        .await
        .map_err(io::Error::other)?;
    let session = wait_for_attachment(&mut pending)
        .await
        .map_err(io::Error::other)?;
    let link = DiagnosticLink::start(Path::new(server.socket_path.as_ref()))?;
    let channel = Endpoint::from_shared(format!("unix://{}", link.socket_path.display()))
        .map_err(io::Error::other)?
        .connect()
        .await
        .map_err(io::Error::other)?;
    let pipeline = PipelineDiagnostics::start(channel, launch_id.clone());
    let result = tokio::time::timeout(TEST_TIMEOUT, async {
        let pipeline_id = document_id()?;
        let a = PluginInstanceId::try_from(String::from("before")).map_err(io::Error::other)?;
        let b = PluginInstanceId::try_from(String::from("after")).map_err(io::Error::other)?;
        let mut old_subscription = server
            .diagnostics
            .subscribe(
                pipeline_id.clone(),
                RunnerDiagnosticTarget::Plugin(a.clone()),
            )
            .map_err(io::Error::other)?;
        let _ = old_subscription.next().await;
        let publisher = pipeline.publisher();
        let old_plugin = publisher.instance_plugin(a);
        let new_plugin = publisher.instance_plugin(b.clone());
        while !old_plugin.is_enabled() {
            tokio::task::yield_now().await;
        }
        old_plugin.publish(
            PluginDiagnosticStream::Stdout,
            "before-break".into(),
            false,
            false,
        );
        assert_eq!(
            next_record(&mut old_subscription).await?.text.as_ref(),
            "before-break"
        );

        let resume = link.break_connection().await?;
        while old_plugin.is_enabled() {
            tokio::task::yield_now().await;
        }
        drop(old_subscription);
        let mut subscription = server
            .diagnostics
            .subscribe(pipeline_id, RunnerDiagnosticTarget::Plugin(b))
            .map_err(io::Error::other)?;
        let _ = subscription.next().await;
        resume
            .send(())
            .map_err(|_| io::Error::other("Diagnostic link ended before reconnect"))?;
        while !new_plugin.is_enabled() {
            tokio::task::yield_now().await;
        }
        assert!(!old_plugin.is_enabled());
        new_plugin.publish(
            PluginDiagnosticStream::Stderr,
            "after-reconnect".into(),
            false,
            false,
        );
        let record = next_record(&mut subscription).await?;
        assert_eq!(record.text.as_ref(), "after-reconnect");
        assert!(matches!(
            record.source,
            RunnerDiagnosticSource::Plugin {
                plugin_process_instance_id: 2,
                stream: RunnerPluginDiagnosticStream::Stderr,
                ..
            }
        ));
        Ok::<(), io::Error>(())
    })
    .await
    .map_err(|_| io::Error::other("Diagnostic reconnect scenario timed out"))
    .and_then(|result| result);
    pipeline.shutdown().await;
    // Once shutdown completes, the same live control launch can be claimed
    // again: no receive task or diagnostic lease may retain it indefinitely.
    let released = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if let Some(claim) = launches.claim_diagnostics(&launch_id, &server.diagnostics) {
                drop(claim);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| io::Error::other("Diagnostic lease survived shutdown"));
    link.finish().await?;
    drop(session);
    drop(control);
    drop(response);
    drop(pending);
    server.finish().await?;
    result.and(released)
}

/// A real byte-forwarding UDS link with a controlled external disconnect window.
struct DiagnosticLink {
    _directory: TempDir,
    socket_path: PathBuf,
    disconnect: mpsc::Sender<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
    task: JoinHandle<io::Result<()>>,
}

impl DiagnosticLink {
    fn start(backend: &Path) -> io::Result<Self> {
        let directory = tempfile::tempdir_in("/tmp")?;
        let socket_path = directory.path().join("link.sock");
        let listener = UnixListener::bind(&socket_path)?;
        let backend = backend.to_owned();
        let (disconnect, mut requests) = mpsc::channel::<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>(1);
        let task = tokio::spawn(async move {
            loop {
                let (mut client, _) = listener.accept().await?;
                let mut upstream = tokio::net::UnixStream::connect(&backend).await?;
                tokio::select! {
                    result = tokio::io::copy_bidirectional(&mut client, &mut upstream) => {
                        match result {
                            Ok(_) => {}
                            // A peer can close while its final HTTP/2 bytes are
                            // being forwarded, both on shutdown and forced loss.
                            Err(error) if matches!(error.kind(), io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset) => {}
                            Err(error) => return Err(error),
                        }
                    }
                    request = requests.recv() => {
                        let Some((disconnected, resume)) = request else { return Ok(()); };
                        drop(client);
                        drop(upstream);
                        let _ = disconnected.send(());
                        let _ = resume.await;
                    }
                }
            }
        });
        Ok(Self {
            _directory: directory,
            socket_path,
            disconnect,
            task,
        })
    }

    async fn break_connection(&self) -> io::Result<tokio::sync::oneshot::Sender<()>> {
        let (disconnected, observed) = tokio::sync::oneshot::channel();
        let (resume, wait) = tokio::sync::oneshot::channel();
        self.disconnect
            .send((disconnected, wait))
            .await
            .map_err(io::Error::other)?;
        observed.await.map_err(io::Error::other)?;
        Ok(resume)
    }

    async fn finish(mut self) -> io::Result<()> {
        self.task.abort();
        match (&mut self.task).await {
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(io::Error::other(error)),
            Ok(result) => result,
        }
    }
}

impl Drop for DiagnosticLink {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn next_record(
    subscription: &mut RunnerDiagnosticSubscription,
) -> io::Result<Arc<RunnerDiagnostic>> {
    match subscription.next().await {
        Some(RunnerDiagnosticFrame::Diagnostic(record)) => Ok(record),
        Some(RunnerDiagnosticFrame::Attached | RunnerDiagnosticFrame::Closed) | None => {
            Err(io::Error::other("Expected a diagnostic record"))
        }
    }
}

async fn assert_pending(subscription: &mut RunnerDiagnosticSubscription) -> io::Result<()> {
    poll_fn(|context| {
        Poll::Ready(match Pin::new(&mut *subscription).poll_next(context) {
            Poll::Pending => Ok(()),
            Poll::Ready(_) => Err(io::Error::other(
                "A subscription received an unexpected record",
            )),
        })
    })
    .await
}
