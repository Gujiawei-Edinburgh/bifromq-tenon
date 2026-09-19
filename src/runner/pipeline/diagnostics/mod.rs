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

//! Runner endpoint for the independent best-effort Pipeline diagnostics stream.

mod inbound;
mod stream;
mod wire;

use super::launch_registry::RunnerPipelineLaunchRegistry;
use crate::contracts::core;
use crate::runner::diagnostics::RunnerDiagnostics;
use std::sync::Arc;
use stream::RunnerPipelineDiagnosticsStream;
use tonic::{Request, Response, Status, Streaming};

/// Best-effort diagnostics transport adapter for one shared launch registry.
pub(in crate::runner) struct RunnerPipelineDiagnostics {
    launches: RunnerPipelineLaunchRegistry,
    diagnostics: RunnerDiagnostics,
}

impl RunnerPipelineDiagnostics {
    #[must_use]
    pub(in crate::runner) fn new(
        launches: RunnerPipelineLaunchRegistry,
        diagnostics: RunnerDiagnostics,
    ) -> Self {
        Self {
            launches,
            diagnostics,
        }
    }

    pub(in crate::runner) fn into_service(
        self,
    ) -> core::pipeline_diagnostics_server::PipelineDiagnosticsServer<Self> {
        core::pipeline_diagnostics_server::PipelineDiagnosticsServer::new(self)
    }

    async fn open(
        &self,
        mut inbound: Streaming<core::PipelineDiagnosticsToRunner>,
    ) -> Result<RunnerPipelineDiagnosticsStream, Status> {
        let launch_id = inbound::launch_id(&mut inbound).await?;
        let (claim, lifetime) = self
            .launches
            .claim_diagnostics(&launch_id, &self.diagnostics)
            .ok_or_else(|| {
                Status::failed_precondition(
                    "Pipeline diagnostics launch is unavailable or already attached",
                )
            })?;
        let document_id = claim.document_id().clone();
        let pipeline_instance_id = Arc::<str>::from(claim.pipeline_instance_id());
        let interest = self.diagnostics.interest(&document_id);
        let task = tokio::spawn(inbound::receive_batches(
            inbound,
            document_id,
            pipeline_instance_id,
            self.diagnostics.clone(),
        ));
        Ok(RunnerPipelineDiagnosticsStream::new(
            claim, lifetime, interest, task,
        ))
    }
}

impl Clone for RunnerPipelineDiagnostics {
    fn clone(&self) -> Self {
        Self {
            launches: self.launches.clone(),
            diagnostics: self.diagnostics.clone(),
        }
    }
}

#[tonic::async_trait]
impl core::pipeline_diagnostics_server::PipelineDiagnostics for RunnerPipelineDiagnostics {
    type StreamStream = RunnerPipelineDiagnosticsStream;

    async fn stream(
        &self,
        request: Request<Streaming<core::PipelineDiagnosticsToRunner>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        self.open(request.into_inner()).await.map(Response::new)
    }
}
