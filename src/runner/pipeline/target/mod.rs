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

//! Complete revision targets and applied views for the retained Program chain.
//!
//! A target owns the resolver's exact Document and Program references plus the
//! independently authored source ETag. Wire messages are projected on demand.
//! Applied views retain only the Document and one complete status snapshot;
//! executable Program ownership stays with the process owner's target chain.
//! No Store access, process creation, or management state lives here.

use super::environment::build_pipeline_environment;
use super::runtime_resolution::ResolvedPipelinePlan;
use crate::config::RunnerConfig;
use crate::contracts::core::{
    PipelineBootstrap, PipelineRevisionPlan as ProtocolRevisionPlan, PipelineStatusSnapshot,
};
use crate::identifiers::TenonDocumentId;
use crate::runner::document_store::TenonDocumentEtag;
use crate::tenon_document::verified::VerifiedTenonDocument;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug)]
pub(in crate::runner) struct PipelineLifecycleTarget {
    plan: ResolvedPipelinePlan,
    // The original JSONC byte identity cannot be recovered from the semantic Document.
    document_etag: TenonDocumentEtag,
}

impl PipelineLifecycleTarget {
    pub(in crate::runner) const fn new(
        plan: ResolvedPipelinePlan,
        document_etag: TenonDocumentEtag,
    ) -> Self {
        Self {
            plan,
            document_etag,
        }
    }

    pub(super) fn document_id(&self) -> &TenonDocumentId {
        self.plan.document().id()
    }

    pub(super) fn document(&self) -> &VerifiedTenonDocument {
        self.plan.document()
    }

    pub(in crate::runner) fn revision(&self) -> ProtocolRevisionPlan {
        ProtocolRevisionPlan {
            document_etag: self.document_etag.strong_value(),
            tenon_document_json: self.plan.document().strict_json(),
            plugin_programs: self.plan.programs().runtimes(),
        }
    }

    pub(in crate::runner) fn bootstrap(
        &self,
        config: &RunnerConfig,
        working_directory: &Path,
    ) -> PipelineBootstrap {
        PipelineBootstrap {
            revision_plan: Some(self.revision()),
            environment: Some(build_pipeline_environment(
                config,
                working_directory,
                self.plan.available_cpu_count(),
            )),
        }
    }

    pub(in crate::runner) fn document_etag(&self) -> TenonDocumentEtag {
        self.document_etag
    }

    pub(super) fn running_state(&self, snapshot: PipelineStatusSnapshot) -> PipelineRunningState {
        PipelineRunningState {
            document: Arc::clone(self.plan.document()),
            snapshot,
        }
    }
}

#[derive(Debug)]
pub(in crate::runner) struct PipelineRunningState {
    document: Arc<VerifiedTenonDocument>,
    // A received applied snapshot remains distinct from subsequent target updates.
    snapshot: PipelineStatusSnapshot,
}

impl PipelineRunningState {
    #[allow(
        clippy::expect_used,
        reason = "the snapshot is accepted only for a retained Runner-issued ETag"
    )]
    pub(in crate::runner) fn document_etag(&self) -> TenonDocumentEtag {
        TenonDocumentEtag::from_strong_value(&self.snapshot.document_etag)
            .expect("applied snapshots retain the Runner-issued ETag")
    }

    pub(in crate::runner) fn document(&self) -> &VerifiedTenonDocument {
        &self.document
    }

    pub(in crate::runner) const fn snapshot(&self) -> &PipelineStatusSnapshot {
        &self.snapshot
    }
}

#[cfg(test)]
mod tests;
