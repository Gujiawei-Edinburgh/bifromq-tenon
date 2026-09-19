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
async fn two_streams_can_claim_one_pending_launch_only_once() -> io::Result<()> {
    let endpoints = endpoints()?;
    let bootstrap = PipelineBootstrap::default();
    let pending = register(&endpoints, bootstrap.clone())?;
    let launch_id = pending.launch_id().to_vec();
    let server = TestServer::start(endpoints).await?;

    let (first, second) = tokio::join!(server.attach(launch_id.clone()), server.attach(launch_id),);
    let (accepted, rejected) = match (first, second) {
        (Ok(accepted), Err(rejected)) | (Err(rejected), Ok(accepted)) => (accepted, rejected),
        _ => {
            return Err(io::Error::other(
                "Exactly one competing Attach must be accepted",
            ));
        }
    };
    assert_eq!(rejected.code(), Code::FailedPrecondition);
    let response = tokio::time::timeout(TEST_TIMEOUT, accepted.into_inner().message())
        .await
        .map_err(|_| io::Error::other("Test Bootstrap receive timed out"))?
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Accepted stream did not receive Bootstrap"))?;
    assert_eq!(
        response,
        RunnerToPipeline {
            message: Some(runner_to_pipeline::Message::Bootstrap(bootstrap)),
        }
    );

    drop(pending);
    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_launch_ownership_removes_an_unclaimed_registration() -> io::Result<()> {
    let endpoints = endpoints()?;
    let pending = register(&endpoints, PipelineBootstrap::default())?;
    let launch_id = pending.launch_id().to_vec();
    drop(pending);
    let server = TestServer::start(endpoints).await?;

    let Err(rejected) = server.attach(launch_id).await else {
        return Err(io::Error::other("Cancelled launch was accepted"));
    };
    assert_eq!(rejected.code(), Code::FailedPrecondition);

    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_attachment_wait_keeps_the_launch_owned_until_explicit_drop() -> io::Result<()> {
    let endpoints = endpoints()?;
    let mut pending = register(&endpoints, PipelineBootstrap::default())?;
    let launch_id = pending.launch_id().to_vec();
    let mut attachment = Box::pin(wait_for_attachment(&mut pending));

    tokio::select! {
        result = &mut attachment => {
            return Err(io::Error::other(format!(
                "Unclaimed attachment wait completed unexpectedly: {result:?}"
            )));
        }
        () = tokio::task::yield_now() => {}
    }
    drop(attachment);
    assert!(
        launch_registry_test_support::contains(&endpoints.launches, &launch_id),
        "Cancelling the wait must not abandon the launch owner"
    );
    drop(pending);
    let server = TestServer::start(endpoints).await?;

    let Err(rejected) = server.attach(launch_id).await else {
        return Err(io::Error::other(
            "Explicitly dropped launch remained registered",
        ));
    };
    assert_eq!(rejected.code(), Code::FailedPrecondition);

    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_attach_does_not_consume_the_pending_launch() -> io::Result<()> {
    let endpoints = endpoints()?;
    let pending = register(&endpoints, PipelineBootstrap::default())?;
    let launch_id = pending.launch_id().to_vec();
    let server = TestServer::start(endpoints).await?;

    let mut unknown = launch_id.clone();
    unknown[0] ^= 0xff;
    let Err(rejected) = server.attach(unknown).await else {
        return Err(io::Error::other("Unknown launch identity was accepted"));
    };
    assert_eq!(rejected.code(), Code::FailedPrecondition);
    let accepted = server.attach(launch_id).await;
    assert!(accepted.is_ok());
    drop(accepted);

    drop(pending);
    server.finish().await
}
#[test]
fn registrations_receive_distinct_launch_identities() -> io::Result<()> {
    let endpoints = endpoints()?;
    let first = register(&endpoints, PipelineBootstrap::default())?;
    let second = register(&endpoints, PipelineBootstrap::default())?;

    assert_ne!(first.launch_id(), second.launch_id());
    Ok(())
}

#[test]
fn old_cleanup_ownership_cannot_remove_a_later_launch() -> io::Result<()> {
    let endpoints = endpoints()?;
    let first = register(&endpoints, PipelineBootstrap::default())?;
    let first_claim = endpoints.launches.claim_control(first.launch_id());
    assert!(first_claim.is_some());
    drop(first_claim);

    let second = register(&endpoints, PipelineBootstrap::default())?;
    assert_ne!(first.launch_id(), second.launch_id());

    drop(first);

    assert!(
        endpoints
            .launches
            .claim_control(second.launch_id())
            .is_some()
    );
    Ok(())
}
