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

use super::super::published_revisions::PublishedPipelineRevisions;
use super::*;
use crate::runner::pipeline::test_support::wait_for_attachment;

#[tokio::test(flavor = "current_thread")]
async fn attached_session_exchanges_status_and_latest_revision_on_the_same_stream() -> io::Result<()>
{
    let endpoints = endpoints()?;
    let bootstrap = PipelineBootstrap::default();
    let mut published = PublishedPipelineRevisions::new(LaunchFixture::new()?.target);
    let mut pending = register(&endpoints, bootstrap.clone())?;
    let launch_id = pending.launch_id().to_vec();
    let status = PipelineStatusSnapshot {
        document_etag: status_etag(),
        plugin_instances: vec![
            PluginInstanceStatus {
                id: String::from("source"),
                state: PluginInstanceState::Running as i32,
                last_error: None,
            },
            PluginInstanceStatus {
                id: String::from("primary"),
                state: PluginInstanceState::Starting as i32,
                last_error: None,
            },
        ],
    };
    let server = TestServer::start(endpoints).await?;
    let (pipeline, mut response) = server.open(launch_id).await.map_err(io::Error::other)?;
    let mut session = wait_for_attachment(&mut pending)
        .await
        .map_err(io::Error::other)?;

    let first = response
        .message()
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Attached stream did not receive Bootstrap"))?;
    assert_eq!(
        first,
        RunnerToPipeline {
            message: Some(runner_to_pipeline::Message::Bootstrap(bootstrap)),
        }
    );
    pipeline
        .send(PipelineToRunner {
            message: Some(pipeline_to_runner::Message::StatusSnapshot(status)),
        })
        .await
        .map_err(io::Error::other)?;
    let status = session
        .receive_pipeline_message()
        .await
        .map_err(io::Error::other)?;
    let received_status = published.observe(status);
    assert_eq!(
        received_status.document_etag().strong_value(),
        status_etag()
    );
    assert!(
        received_status
            .snapshot()
            .plugin_instances
            .iter()
            .any(|instance| instance.id == "source"
                && instance.state == PluginInstanceState::Running as i32)
    );

    session
        .publish_revision(PipelineRevisionPlan {
            document_etag: String::from("document-etag-stale"),
            ..PipelineRevisionPlan::default()
        })
        .map_err(io::Error::other)?;
    let revision = PipelineRevisionPlan {
        document_etag: String::from("document-etag-b"),
        ..PipelineRevisionPlan::default()
    };
    session
        .publish_revision(revision.clone())
        .map_err(io::Error::other)?;
    let second = response
        .message()
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Attached stream did not receive Revision Plan"))?;
    assert_eq!(
        second,
        RunnerToPipeline {
            message: Some(runner_to_pipeline::Message::RevisionPlan(revision)),
        }
    );

    drop(session);
    let end = tokio::time::timeout(TEST_TIMEOUT, response.message())
        .await
        .map_err(|_| io::Error::other("Runner session close did not end the response stream"))?
        .map_err(io::Error::other)?;
    assert!(end.is_none());
    drop(pipeline);
    server.finish().await
}
#[tokio::test(flavor = "current_thread")]
async fn private_control_service_accepts_a_valid_large_topology_status() -> io::Result<()> {
    const TONIC_DEFAULT_DECODING_LIMIT_BYTES: usize = 4 * 1024 * 1024;
    const INSTANCE_COUNT: usize = 35_000;

    let endpoints = endpoints()?;
    let mut pending = register(&endpoints, PipelineBootstrap::default())?;
    let launch_id = pending.launch_id().to_vec();
    let server = TestServer::start(endpoints).await?;
    let (pipeline, _response) = server.open(launch_id).await.map_err(io::Error::other)?;
    let mut session = wait_for_attachment(&mut pending)
        .await
        .map_err(io::Error::other)?;
    let status = PipelineToRunner {
        message: Some(pipeline_to_runner::Message::StatusSnapshot(
            PipelineStatusSnapshot {
                document_etag: "a".repeat(64),
                plugin_instances: (0..INSTANCE_COUNT)
                    .map(|index| PluginInstanceStatus {
                        id: format!("{index:05}-{}", "x".repeat(115)),
                        state: PluginInstanceState::Running as i32,
                        last_error: None,
                    })
                    .collect(),
            },
        )),
    };
    assert!(status.encoded_len() > TONIC_DEFAULT_DECODING_LIMIT_BYTES);

    pipeline.send(status).await.map_err(io::Error::other)?;
    let received = tokio::time::timeout(TEST_TIMEOUT, session.receive_pipeline_message())
        .await
        .map_err(|_| io::Error::other("Large Pipeline status receive timed out"))?
        .map_err(io::Error::other)?;
    let Some(pipeline_to_runner::Message::StatusSnapshot(received)) = received.message else {
        return Err(io::Error::other("Large Pipeline status was not received"));
    };
    let expected_last_sink_id = format!("{:05}-{}", INSTANCE_COUNT - 1, "x".repeat(115));
    assert_eq!(received.plugin_instances.len(), INSTANCE_COUNT);
    assert_eq!(
        received
            .plugin_instances
            .last()
            .map(|sink| sink.id.as_str()),
        Some(expected_last_sink_id.as_str())
    );

    drop(session);
    drop(pipeline);
    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn attached_session_reports_pipeline_request_stream_disconnect() -> io::Result<()> {
    let endpoints = endpoints()?;
    let mut pending = register(&endpoints, PipelineBootstrap::default())?;
    let launch_id = pending.launch_id().to_vec();
    let server = TestServer::start(endpoints).await?;
    let (pipeline, mut response) = server.open(launch_id).await.map_err(io::Error::other)?;
    let mut session = wait_for_attachment(&mut pending)
        .await
        .map_err(io::Error::other)?;
    let bootstrap = response
        .message()
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Attached stream did not receive Bootstrap"))?;
    assert!(matches!(
        bootstrap.message,
        Some(runner_to_pipeline::Message::Bootstrap(_))
    ));

    drop(pipeline);
    let result = tokio::time::timeout(TEST_TIMEOUT, session.receive_pipeline_message())
        .await
        .map_err(|_| io::Error::other("Pipeline request stream did not disconnect"))?;
    let Err(error) = result else {
        return Err(io::Error::other(
            "Closed Pipeline request stream was accepted",
        ));
    };
    assert!(matches!(
        error,
        RunnerPipelineControlSessionError::Disconnected
    ));

    drop(session);
    drop(response);
    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_launch_owner_after_attach_closes_the_bound_session() -> io::Result<()> {
    let endpoints = endpoints()?;
    let pending = register(&endpoints, PipelineBootstrap::default())?;
    let launch_id = pending.launch_id().to_vec();
    let server = TestServer::start(endpoints).await?;
    let (pipeline, mut response) = server.open(launch_id).await.map_err(io::Error::other)?;

    drop(pending);
    let bootstrap = response
        .message()
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Attached stream did not receive Bootstrap"))?;
    assert!(matches!(
        bootstrap.message,
        Some(runner_to_pipeline::Message::Bootstrap(_))
    ));
    let end = tokio::time::timeout(TEST_TIMEOUT, response.message())
        .await
        .map_err(|_| io::Error::other("Cancelled launch owner left its session open"))?
        .map_err(io::Error::other)?;
    assert!(end.is_none());

    drop(pipeline);
    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_status_receive_keeps_the_next_message_available() -> io::Result<()> {
    let endpoints = endpoints()?;
    let mut pending = register(&endpoints, PipelineBootstrap::default())?;
    let launch_id = pending.launch_id().to_vec();
    let mut published = PublishedPipelineRevisions::new(LaunchFixture::new()?.target);
    let server = TestServer::start(endpoints).await?;
    let (pipeline, mut response) = server.open(launch_id).await.map_err(io::Error::other)?;
    let mut session = wait_for_attachment(&mut pending)
        .await
        .map_err(io::Error::other)?;
    let bootstrap = response
        .message()
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Attached stream did not receive Bootstrap"))?;
    assert!(matches!(
        bootstrap.message,
        Some(runner_to_pipeline::Message::Bootstrap(_))
    ));

    let mut receive = Box::pin(session.receive_pipeline_message());
    tokio::select! {
        result = &mut receive => {
            return Err(io::Error::other(format!(
                "Empty status receive completed unexpectedly: {result:?}"
            )));
        }
        () = tokio::task::yield_now() => {}
    }
    drop(receive);

    pipeline
        .send(PipelineToRunner {
            message: Some(pipeline_to_runner::Message::StatusSnapshot(
                PipelineStatusSnapshot {
                    document_etag: status_etag(),
                    plugin_instances: vec![
                        PluginInstanceStatus {
                            id: String::from("source"),
                            state: PluginInstanceState::Running as i32,
                            last_error: None,
                        },
                        PluginInstanceStatus {
                            id: String::from("primary"),
                            state: PluginInstanceState::Running as i32,
                            last_error: None,
                        },
                    ],
                },
            )),
        })
        .await
        .map_err(io::Error::other)?;
    let status = session
        .receive_pipeline_message()
        .await
        .map_err(io::Error::other)?;
    let status = published.observe(status);
    assert_eq!(status.document_etag().strong_value(), status_etag());

    drop(session);
    drop(pipeline);
    drop(response);
    server.finish().await
}
