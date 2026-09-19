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

//! The single retention owner for revisions published to one Pipeline attempt.
//!
//! The process owner supplies complete immutable targets and inbound statuses.
//! This owner records a target only after transport acceptance, then releases
//! older references after the Pipeline reports a retained revision. It owns
//! no process, Store, latest-target slot, or second applied state. The process
//! owner retains this collection until its cleanup has completed.

use super::RunnerPipelineControlSessionError;
use super::target::{PipelineLifecycleTarget, PipelineRunningState};
use crate::contracts::core::{PipelineToRunner, pipeline_to_runner};
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::process_resources;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

/// Ordered complete targets that a live Pipeline may still use or report.
pub(super) struct PublishedPipelineRevisions {
    entries: VecDeque<Arc<PipelineLifecycleTarget>>,
}

impl PublishedPipelineRevisions {
    pub(super) fn new(bootstrap: Arc<PipelineLifecycleTarget>) -> Self {
        Self {
            entries: VecDeque::from([bootstrap]),
        }
    }

    /// Associates a received status with the earliest retained revision and releases its predecessors.
    #[allow(
        clippy::expect_used,
        reason = "the same-build Pipeline reports only Runner-issued revisions on its bound stream"
    )]
    pub(super) fn observe(&mut self, envelope: PipelineToRunner) -> PipelineRunningState {
        let Some(pipeline_to_runner::Message::StatusSnapshot(status)) = envelope.message else {
            unreachable!("An attached Pipeline sends only status snapshots");
        };
        let etag = TenonDocumentEtag::from_strong_value(&status.document_etag)
            .expect("Pipeline status must preserve its Runner-issued ETag");
        // Select the first retained occurrence: an older status can cross a
        // newer outbound revert, so a later occurrence could release a Program
        // that the Pipeline is still using.
        let index = self
            .entries
            .iter()
            .position(|entry| entry.document_etag() == etag)
            .expect("Pipeline status must refer to a retained published revision");
        let status = self.entries[index].running_state(status);
        self.entries.drain(..index);
        status
    }

    /// Skips the already published target and retains each successfully sent owner.
    /// A failed send leaves the collection unchanged. The upstream lifecycle
    /// retains its latest target and decides whether to retry or end the attempt.
    pub(super) fn publish(
        &mut self,
        target: Arc<PipelineLifecycleTarget>,
        publish: impl FnOnce(&PipelineLifecycleTarget) -> Result<(), RunnerPipelineControlSessionError>,
    ) -> Result<(), RunnerPipelineControlSessionError> {
        if self.latest_is(&target) {
            return Ok(());
        }
        publish(&target)?;
        self.entries.push_back(target);
        Ok(())
    }

    #[allow(
        clippy::expect_used,
        reason = "bootstrap is retained until a successor is retained"
    )]
    pub(super) fn requires_resource_replacement(&self, target: &PipelineLifecycleTarget) -> bool {
        let current = self
            .entries
            .back()
            .expect("a live process always retains a revision");
        process_resources::requires_replacement(current.document(), target.document())
    }

    #[allow(
        clippy::expect_used,
        reason = "bootstrap is retained until a successor is retained"
    )]
    pub(super) fn latest_etag(&self) -> TenonDocumentEtag {
        self.entries
            .back()
            .expect("a live process always retains a revision")
            .document_etag()
    }

    fn latest_is(&self, target: &PipelineLifecycleTarget) -> bool {
        self.entries
            .back()
            .is_some_and(|entry| entry.document_etag() == target.document_etag())
    }
}

impl fmt::Debug for PublishedPipelineRevisions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublishedPipelineRevisions")
            .field(
                "document_etags",
                &self
                    .entries
                    .iter()
                    .map(|entry| entry.document_etag())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests;
