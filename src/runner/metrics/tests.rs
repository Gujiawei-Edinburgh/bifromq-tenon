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
use crate::contracts::core;
use crate::identifiers::TenonDocumentId;
use crate::metrics::CoreProcess;
use crate::runner::pipeline::test_support::PendingPipelineLaunch;
use crate::runner::pipeline::{RunnerPipelineControl, RunnerPipelineLaunchRegistry};
use opentelemetry::KeyValue;
use prost::Message as _;
use std::future::{Future as _, poll_fn};
use std::io;
use std::task::Poll;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::transport::{Channel, Endpoint, Server};

struct Fixture {
    metrics: RunnerMetrics,
    launches: RunnerPipelineLaunchRegistry,
    channel: Channel,
    _server: JoinSet<Result<(), tonic::transport::Error>>,
    _directory: tempfile::TempDir,
}

impl Fixture {
    async fn start() -> io::Result<Self> {
        let directory = tempfile::tempdir_in("/tmp")?;
        let path = directory.path().join("metrics.sock");
        let listener = UnixListener::bind(&path)?;
        let metrics = RunnerMetrics::new(
            crate::metrics::test_support::runner()?,
            Duration::from_secs(60),
        );
        let launches = RunnerPipelineLaunchRegistry::try_new().map_err(io::Error::other)?;
        let control = RunnerPipelineControl::new(launches.clone()).into_service();
        let service = metrics.clone().into_service(launches.clone());
        let mut server = JoinSet::new();
        server.spawn(
            Server::builder()
                .add_service(control)
                .add_service(service)
                .serve_with_incoming(UnixListenerStream::new(listener)),
        );
        let channel = Endpoint::from_shared(format!("unix://{}", path.display()))
            .map_err(io::Error::other)?
            .connect()
            .await
            .map_err(io::Error::other)?;
        Ok(Self {
            metrics,
            launches,
            channel,
            _server: server,
            _directory: directory,
        })
    }

    async fn launch(&self, id: &str) -> io::Result<Launch> {
        let pending = self.launches.register(
            TenonDocumentId::try_from(id).map_err(io::Error::other)?,
            core::PipelineBootstrap::default(),
        );
        let (outgoing, messages) = mpsc::channel(1);
        outgoing
            .send(core::PipelineToRunner {
                message: Some(core::pipeline_to_runner::Message::Attach(
                    core::PipelineAttach {
                        launch_id: pending.launch_id().to_vec(),
                    },
                )),
            })
            .await
            .map_err(io::Error::other)?;
        let mut client =
            core::pipeline_control_client::PipelineControlClient::new(self.channel.clone());
        let mut incoming = client
            .run(ReceiverStream::new(messages))
            .await
            .map_err(io::Error::other)?
            .into_inner();
        assert!(
            incoming
                .message()
                .await
                .map_err(io::Error::other)?
                .is_some()
        );
        Ok(Launch {
            pending,
            _outgoing: outgoing,
            _incoming: incoming,
        })
    }

    async fn connect(&self, launch_id: &[u8]) -> Result<Peer, tonic::Status> {
        let (snapshots, outgoing) = mpsc::channel(1);
        snapshots
            .send(core::PipelineMetricsToRunner {
                message: Some(core::pipeline_metrics_to_runner::Message::Attach(
                    core::PipelineMetricsAttach {
                        launch_id: launch_id.to_vec(),
                    },
                )),
            })
            .await
            .map_err(|_| tonic::Status::cancelled("Test stream closed"))?;
        let mut client =
            core::pipeline_metrics_client::PipelineMetricsClient::new(self.channel.clone())
                .max_encoding_message_size(usize::MAX)
                .max_decoding_message_size(usize::MAX);
        let requests = client
            .stream(ReceiverStream::new(outgoing))
            .await?
            .into_inner();
        Ok(Peer {
            requests,
            snapshots,
        })
    }
}

struct Launch {
    pending: PendingPipelineLaunch,
    _outgoing: mpsc::Sender<core::PipelineToRunner>,
    _incoming: tonic::Streaming<core::RunnerToPipeline>,
}

struct Peer {
    requests: tonic::Streaming<core::RunnerToPipelineMetrics>,
    snapshots: mpsc::Sender<core::PipelineMetricsToRunner>,
}

impl Peer {
    async fn reply(&self, snapshot: MetricsData) -> io::Result<()> {
        self.snapshots
            .send(core::PipelineMetricsToRunner {
                message: Some(core::pipeline_metrics_to_runner::Message::Snapshot(
                    core::PipelineMetricsSnapshot {
                        metrics: snapshot.encode_to_vec(),
                    },
                )),
            })
            .await
            .map_err(io::Error::other)
    }
}

fn data(points: usize) -> io::Result<MetricsData> {
    let runtime = MetricsRuntime::start(
        None,
        CoreProcess::Pipeline {
            document_id: "test",
            launch_id: b"test",
        },
    )
    .map_err(io::Error::other)?;
    let counter = runtime
        .meter()
        .u64_counter("tenon.flow.input.records")
        .with_unit("{record}")
        .build();
    for index in 0..points {
        counter.add(
            1,
            &[
                KeyValue::new("tenon.flow.id", format!("{}-{index}", "f".repeat(110))),
                KeyValue::new("tenon.channel.index", 0_i64),
            ],
        );
    }
    let snapshot = runtime.collect(&["tenon.flow.input.records".to_owned()]);
    runtime.shutdown();
    Ok(snapshot)
}

fn selected() -> Vec<String> {
    vec!["tenon.flow.input.records".to_owned()]
}

#[tokio::test(flavor = "current_thread")]
async fn busy_stream_is_skipped_and_successful_collection_reuses_it() -> io::Result<()> {
    let fixture = Fixture::start().await?;
    let launch = fixture.launch("pipeline").await?;
    let mut peer = fixture
        .connect(launch.pending.launch_id())
        .await
        .map_err(io::Error::other)?;
    let include = selected();
    let mut first = Box::pin(fixture.metrics.collect(&include));
    tokio::select! {
        result = &mut first => return Err(io::Error::other(format!("Collection finished before a reply: {result:?}"))),
        request = peer.requests.message() => assert_eq!(request.map_err(io::Error::other)?.map(|request| request.include), Some(include.clone())),
    }
    assert!(
        fixture
            .metrics
            .collect(&include)
            .await
            .resource_metrics
            .is_empty()
    );
    peer.reply(data(1)?).await?;
    assert_eq!(first.await.resource_metrics.len(), 1);
    let next = fixture.metrics.collect(&include);
    let reply = async {
        assert!(
            peer.requests
                .message()
                .await
                .map_err(io::Error::other)?
                .is_some()
        );
        peer.reply(data(2)?).await
    };
    let (snapshot, result) = tokio::join!(next, reply);
    result?;
    assert_eq!(snapshot.resource_metrics.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn healthy_large_snapshot_is_consumed_while_another_source_waits() -> io::Result<()> {
    let fixture = Fixture::start().await?;
    let slow = fixture.launch("slow").await?;
    let healthy = fixture.launch("healthy").await?;
    let mut slow_peer = fixture
        .connect(slow.pending.launch_id())
        .await
        .map_err(io::Error::other)?;
    let mut healthy_peer = fixture
        .connect(healthy.pending.launch_id())
        .await
        .map_err(io::Error::other)?;
    let include = selected();
    let mut collection = Box::pin(fixture.metrics.collect(&include));
    let requests = async {
        let (slow, healthy) = tokio::join!(
            slow_peer.requests.message(),
            healthy_peer.requests.message()
        );
        assert!(slow.map_err(io::Error::other)?.is_some());
        assert!(healthy.map_err(io::Error::other)?.is_some());
        Ok::<_, io::Error>(())
    };
    tokio::select! {
        _ = &mut collection => return Err(io::Error::other("Collection returned before responses")),
        result = requests => result?,
    }
    let snapshot = data(2000)?;
    assert!(snapshot.encoded_len() > 256 * 1024);
    healthy_peer.reply(snapshot).await?;
    // Prove the healthy response is read and the session returned before allowing the slow deadline to expire.
    tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| {
            assert!(collection.as_mut().poll(cx).is_pending());
            let ready = fixture
                .metrics
                .connections()
                .get(healthy.pending.launch_id())
                .and_then(Weak::upgrade)
                .is_some_and(|connection| connection.lock().is_ok_and(|slot| slot.is_some()));
            if ready {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .map_err(io::Error::other)?;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    let snapshot = collection.await;
    assert_eq!(snapshot.resource_metrics.len(), 1);
    assert_eq!(
        snapshot.resource_metrics[0].scope_metrics[0].metrics[0].name,
        include[0]
    );
    assert!(
        slow_peer
            .requests
            .message()
            .await
            .map_err(io::Error::other)?
            .is_none()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_closes_only_pending_sessions_and_allows_reconnection() -> io::Result<()> {
    let fixture = Fixture::start().await?;
    let launch = fixture.launch("pipeline").await?;
    let mut peer = fixture
        .connect(launch.pending.launch_id())
        .await
        .map_err(io::Error::other)?;
    let include = selected();
    let mut collection = Box::pin(fixture.metrics.collect(&include));
    tokio::select! {
        _ = &mut collection => return Err(io::Error::other("Collection returned before cancellation")),
        request = peer.requests.message() => assert!(request.map_err(io::Error::other)?.is_some()),
    }
    drop(collection);
    assert!(
        peer.requests
            .message()
            .await
            .map_err(io::Error::other)?
            .is_none()
    );
    // An old reply is unable to become a new stream's response, whether its local sender has noticed closure yet or not.
    let _closed = peer.reply(data(1)?).await;
    drop(peer);
    let mut peer = fixture
        .connect(launch.pending.launch_id())
        .await
        .map_err(io::Error::other)?;
    let reply = async {
        assert!(
            peer.requests
                .message()
                .await
                .map_err(io::Error::other)?
                .is_some()
        );
        peer.reply(data(2)?).await
    };
    let (snapshot, result) = tokio::join!(fixture.metrics.collect(&include), reply);
    result?;
    let Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum)) =
        &snapshot.resource_metrics[0].scope_metrics[0].metrics[0].data
    else {
        return Err(io::Error::other("Missing counter"));
    };
    assert_eq!(sum.data_points.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retired_launch_closes_idle_and_pending_streams_and_cannot_reattach() -> io::Result<()> {
    for pending in [false, true] {
        let fixture = Fixture::start().await?;
        let launch = fixture.launch("pipeline").await?;
        let launch_id = launch.pending.launch_id().to_vec();
        let mut peer = fixture
            .connect(&launch_id)
            .await
            .map_err(io::Error::other)?;
        let include = selected();
        let mut collection = Box::pin(fixture.metrics.collect(&include));
        if pending {
            tokio::select! {
                _ = &mut collection => return Err(io::Error::other("Collection returned before retirement")),
                request = peer.requests.message() => assert!(request.map_err(io::Error::other)?.is_some()),
            }
        }
        drop(launch);
        assert!(
            peer.requests
                .message()
                .await
                .map_err(io::Error::other)?
                .is_none()
        );
        let snapshot = tokio::time::timeout(Duration::from_secs(5), collection)
            .await
            .map_err(io::Error::other)?;
        assert!(snapshot.resource_metrics.is_empty());
        assert!(
            matches!(fixture.connect(&launch_id).await, Err(status) if status.code() == tonic::Code::FailedPrecondition)
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_pending_and_duplicate_launches_are_not_accepted() -> io::Result<()> {
    let fixture = Fixture::start().await?;
    assert!(
        matches!(fixture.connect(b"unknown").await, Err(status) if status.code() == tonic::Code::FailedPrecondition)
    );
    let pending = fixture.launches.register(
        TenonDocumentId::try_from("pending").map_err(io::Error::other)?,
        core::PipelineBootstrap::default(),
    );
    assert!(
        matches!(fixture.connect(pending.launch_id()).await, Err(status) if status.code() == tonic::Code::FailedPrecondition)
    );
    let launch = fixture.launch("pipeline").await?;
    let _peer = fixture
        .connect(launch.pending.launch_id())
        .await
        .map_err(io::Error::other)?;
    assert!(
        matches!(fixture.connect(launch.pending.launch_id()).await, Err(status) if status.code() == tonic::Code::AlreadyExists)
    );
    Ok(())
}
