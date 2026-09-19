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

//! Outbound diagnostics interest stream and inbound receive-task owner.

use std::future::Future as _;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tokio_stream::wrappers::WatchStream;
use tonic::Status;

use crate::runner::diagnostics::RunnerDiagnosticTarget;

use super::super::launch_registry::RunnerPipelineDiagnosticsClaim;
use crate::contracts::core::RunnerToPipelineDiagnostics;

/// Owns one diagnostics stream lease and its inbound receive task.
pub(in crate::runner) struct RunnerPipelineDiagnosticsStream {
    claim: RunnerPipelineDiagnosticsClaim,
    lifetime: WatchStream<()>,
    interest: WatchStream<Arc<[RunnerDiagnosticTarget]>>,
    task: Option<JoinHandle<Status>>,
}

impl RunnerPipelineDiagnosticsStream {
    /// Binds the exact launch claim, lifetime, latest interest, and receive task.
    pub(super) fn new(
        claim: RunnerPipelineDiagnosticsClaim,
        lifetime: tokio::sync::watch::Receiver<()>,
        interest: tokio::sync::watch::Receiver<Arc<[RunnerDiagnosticTarget]>>,
        task: JoinHandle<Status>,
    ) -> Self {
        Self {
            claim,
            lifetime: WatchStream::from_changes(lifetime),
            interest: WatchStream::new(interest),
            task: Some(task),
        }
    }
}

impl Stream for RunnerPipelineDiagnosticsStream {
    type Item = Result<RunnerToPipelineDiagnostics, Status>;

    #[allow(
        clippy::expect_used,
        reason = "the active-stream branch proves ownership of the receive task"
    )]
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(task) = self.task.as_mut() else {
            return Poll::Ready(None);
        };

        match Pin::new(task).poll(context) {
            Poll::Ready(Ok(status)) => {
                self.task.take();
                return Poll::Ready(Some(Err(status)));
            }
            Poll::Ready(Err(error)) => {
                self.task.take();
                return Poll::Ready(Some(Err(Status::internal(format!(
                    "Pipeline diagnostics task failed: {error}"
                )))));
            }
            Poll::Pending => {}
        }

        loop {
            match Pin::new(&mut self.lifetime).poll_next(context) {
                Poll::Ready(Some(())) => {}
                Poll::Ready(None) => {
                    self.task
                        .take()
                        .expect("an active diagnostics stream must own its receive task")
                        .abort();
                    return Poll::Ready(None);
                }
                Poll::Pending => break,
            }
        }

        Pin::new(&mut self.interest)
            .poll_next(context)
            .map(|interest| interest.map(|targets| Ok(super::wire::interest(&targets))))
    }
}

impl Drop for RunnerPipelineDiagnosticsStream {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl std::fmt::Debug for RunnerPipelineDiagnosticsStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunnerPipelineDiagnosticsStream")
            .field("claim", &self.claim)
            .field(
                "task_finished",
                &self.task.as_ref().is_none_or(JoinHandle::is_finished),
            )
            .finish_non_exhaustive()
    }
}
