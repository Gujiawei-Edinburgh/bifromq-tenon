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

//! Runner adapter for the correctness-critical Pipeline control gRPC stream.
//!
//! This adapter receives the first Attach, asks the shared launch registry to
//! claim the exact pending child, and hands the bound control session back to
//! that child's owner. It does not create processes, own launch identity, or
//! participate in best-effort diagnostics.
//!
//! The response stream sends the unique Bootstrap first and then the latest
//! complete revision. Both directions stay on one correctness-critical stream;
//! no forwarding task or duplicate revision queue exists.

use std::fmt;

use tokio::sync::watch;
use tokio_stream::adapters::{Chain, FilterMap};
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Once, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use super::super::launch_registry::RunnerPipelineLaunchRegistry;
use crate::contracts::core;

use super::session::RunnerPipelineControlSession;

/// Correctness-control transport adapter for one Runner launch registry.
pub(in crate::runner) struct RunnerPipelineControl {
    launches: RunnerPipelineLaunchRegistry,
}

impl RunnerPipelineControl {
    #[must_use]
    pub(in crate::runner) fn new(launches: RunnerPipelineLaunchRegistry) -> Self {
        Self { launches }
    }

    /// Exposes the private correctness-control service.
    pub(in crate::runner) fn into_service(
        self,
    ) -> core::pipeline_control_server::PipelineControlServer<Self> {
        core::pipeline_control_server::PipelineControlServer::new(self)
            .max_decoding_message_size(usize::MAX)
    }

    async fn open(
        &self,
        mut inbound: Streaming<core::PipelineToRunner>,
    ) -> Result<Response<RunnerToPipelineStream>, Status> {
        let envelope = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("Pipeline Attach is missing"))?;
        let Some(core::pipeline_to_runner::Message::Attach(attach)) = envelope.message else {
            unreachable!("Pipeline begins its control stream with Attach");
        };
        let pending = self
            .launches
            .claim_control(&attach.launch_id)
            .ok_or_else(|| Status::failed_precondition("Pipeline launch is not pending"))?;
        let (revisions, revision_stream) = watch::channel(None);
        let session = RunnerPipelineControlSession::new(inbound, revisions);
        let bootstrap = pending
            .hand_off(session)
            .map_err(|_| Status::cancelled("Pipeline launch owner is no longer waiting"))?;
        self.launches
            .confirm_control_attachment(&attach.launch_id)
            .ok_or_else(|| Status::cancelled("Pipeline launch owner ended during attachment"))?;
        let bootstrap = core::RunnerToPipeline {
            message: Some(core::runner_to_pipeline::Message::Bootstrap(bootstrap)),
        };
        let stream =
            tokio_stream::once(Ok(bootstrap)).chain(WatchStream::new(revision_stream).filter_map(
                revision_envelope
                    as fn(
                        Option<core::PipelineRevisionPlan>,
                    ) -> Option<Result<core::RunnerToPipeline, Status>>,
            ));
        Ok(Response::new(stream))
    }
}

impl fmt::Debug for RunnerPipelineControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunnerPipelineControl")
            .field("launches", &self.launches)
            .finish()
    }
}

impl Clone for RunnerPipelineControl {
    fn clone(&self) -> Self {
        Self {
            launches: self.launches.clone(),
        }
    }
}

#[tonic::async_trait]
impl core::pipeline_control_server::PipelineControl for RunnerPipelineControl {
    type RunStream = RunnerToPipelineStream;

    async fn run(
        &self,
        request: Request<Streaming<core::PipelineToRunner>>,
    ) -> Result<Response<Self::RunStream>, Status> {
        self.open(request.into_inner()).await
    }
}

type RunnerToPipelineStream = Chain<
    Once<Result<core::RunnerToPipeline, Status>>,
    FilterMap<
        WatchStream<Option<core::PipelineRevisionPlan>>,
        fn(Option<core::PipelineRevisionPlan>) -> Option<Result<core::RunnerToPipeline, Status>>,
    >,
>;

fn revision_envelope(
    revision: Option<core::PipelineRevisionPlan>,
) -> Option<Result<core::RunnerToPipeline, Status>> {
    revision.map(|revision| {
        Ok(core::RunnerToPipeline {
            message: Some(core::runner_to_pipeline::Message::RevisionPlan(revision)),
        })
    })
}
