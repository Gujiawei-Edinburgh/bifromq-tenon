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

//! One attached Runner-to-Pipeline correctness-control session.
//!
//! This module owns only the bound gRPC request stream and the latest complete
//! revision slot. Launch identity, child-process ownership, and diagnostics are
//! deliberately outside this module.

use std::error::Error;
use std::fmt;

use tokio::sync::watch;
use tonic::Streaming;

/// One Runner-owned control session after a Pipeline has attached.
pub(in crate::runner) struct RunnerPipelineControlSession {
    inbound: Streaming<crate::contracts::core::PipelineToRunner>,
    revisions: watch::Sender<Option<crate::contracts::core::PipelineRevisionPlan>>,
}

impl RunnerPipelineControlSession {
    #[must_use]
    pub(in crate::runner::pipeline) fn new(
        inbound: Streaming<crate::contracts::core::PipelineToRunner>,
        revisions: watch::Sender<Option<crate::contracts::core::PipelineRevisionPlan>>,
    ) -> Self {
        Self { inbound, revisions }
    }

    /// Publishes the latest complete Runtime-Integrity-passed revision to the
    /// attached Pipeline.
    ///
    /// Complete revisions are current state rather than an event log. If the
    /// transport has not consumed an older pending revision, this value replaces
    /// it without growing an in-memory queue.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerPipelineControlSessionError::Disconnected`] after the response
    /// stream has closed.
    pub(in crate::runner::pipeline) fn publish_revision(
        &self,
        revision: crate::contracts::core::PipelineRevisionPlan,
    ) -> Result<(), RunnerPipelineControlSessionError> {
        self.revisions
            .send(Some(revision))
            .map_err(|_| RunnerPipelineControlSessionError::Disconnected)
    }

    /// Receives the next Pipeline-to-Runner envelope from the bound stream.
    ///
    /// The process owner associates the status with its retained revision
    /// before exposing it. This transport layer does not select revisions
    /// because old and new document states can cross during an update.
    ///
    /// # Cancel safety
    ///
    /// Cancelling this wait does not discard the next envelope. The bound
    /// stream remains inside this session and can be polled again.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerPipelineControlSessionError`] when the stream fails or closes.
    pub(in crate::runner) async fn receive_pipeline_message(
        &mut self,
    ) -> Result<crate::contracts::core::PipelineToRunner, RunnerPipelineControlSessionError> {
        self.inbound
            .message()
            .await
            .map_err(RunnerPipelineControlSessionError::ControlStream)?
            .ok_or(RunnerPipelineControlSessionError::Disconnected)
    }
}

impl fmt::Debug for RunnerPipelineControlSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunnerPipelineControlSession")
            .finish_non_exhaustive()
    }
}

/// A bound Runner-Pipeline control session cannot continue.
#[derive(Debug)]
pub(in crate::runner) enum RunnerPipelineControlSessionError {
    /// The gRPC transport rejected an inbound control message.
    ControlStream(tonic::Status),
    /// The attached Pipeline or the local response stream has closed.
    Disconnected,
}

impl fmt::Display for RunnerPipelineControlSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ControlStream(_) => formatter.write_str("Pipeline control stream failed"),
            Self::Disconnected => formatter.write_str("Pipeline control stream disconnected"),
        }
    }
}

impl Error for RunnerPipelineControlSessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ControlStream(source) => Some(source),
            Self::Disconnected => None,
        }
    }
}
