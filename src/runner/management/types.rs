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

//! Adapter-independent requests, results, and read models.
//!
//! These types express Runner business meaning only. HTTP headers, quoted
//! strings, status codes, and JSON layout are converted in the adapter. This
//! module owns no state, scheduling, persistence, or shutdown behavior.

use crate::contracts::core::PluginInstanceState;
use crate::identifiers::{ExactVersion, PluginInstanceId, ProgramName, TenonDocumentId};
use crate::payload_contract::PluginInterface;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::extensions::ExecutionDenied;
use crate::runner::pipeline::RuntimeResolutionIssue;
use crate::runner::plugin::platform::Platform;
use crate::runner::process_resources;
use crate::tenon_document::{TenonDocumentSyntaxError, TenonDocumentVerificationError};
use process_resources::ResourceLimitsState;

/// The Runner owner has entered shutdown and cannot answer the operation.
#[derive(Debug)]
pub(crate) struct RunnerUnavailable;

pub(crate) enum DocumentValidationFailure {
    Syntax(TenonDocumentSyntaxError),
    Verification(TenonDocumentVerificationError),
}

impl From<TenonDocumentSyntaxError> for DocumentValidationFailure {
    fn from(value: TenonDocumentSyntaxError) -> Self {
        Self::Syntax(value)
    }
}

impl From<TenonDocumentVerificationError> for DocumentValidationFailure {
    fn from(value: TenonDocumentVerificationError) -> Self {
        Self::Verification(value)
    }
}

pub(crate) enum PutDocumentFailure {
    ExecutionDenied(ExecutionDenied),
    Validation(DocumentValidationFailure),
    IdMismatch,
    Write(DocumentWriteFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PutDocumentPrecondition {
    Create,
    Replace(TenonDocumentEtag),
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeleteDocumentPrecondition {
    Match(TenonDocumentEtag),
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PutDocumentOutcome {
    Created,
    Replaced,
    Unchanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DocumentWriteFailure {
    PreconditionRequired,
    PreconditionFailed,
    Store,
}

pub(crate) struct DocumentSummary {
    pub(crate) id: TenonDocumentId,
    pub(crate) etag: TenonDocumentEtag,
}

pub(crate) struct DocumentContent {
    pub(crate) source: Box<[u8]>,
    pub(crate) etag: TenonDocumentEtag,
}

pub(crate) struct PipelineStatus {
    pub(crate) id: TenonDocumentId,
    pub(crate) document_etag: TenonDocumentEtag,
    pub(crate) convergence: PipelineConvergence,
}

pub(crate) enum PipelineConvergence {
    Unready { applied: Option<TenonDocumentEtag> },
    Starting,
    Updating { applied: TenonDocumentEtag },
    Running { applied: TenonDocumentEtag },
    RestartBackoff,
}

impl PipelineConvergence {
    pub(crate) fn applied_document_etag(&self) -> Option<&TenonDocumentEtag> {
        match self {
            Self::Unready { applied } => applied.as_ref(),
            Self::Updating { applied } | Self::Running { applied } => Some(applied),
            Self::Starting | Self::RestartBackoff => None,
        }
    }
}

// These values capture the same management query; later changes cannot mix
// current diagnostics with a different applied revision.
pub(crate) struct PipelineDetails {
    pub(crate) status: PipelineStatus,
    pub(crate) runtime_issues: Box<[RuntimeResolutionIssue]>,
    pub(crate) plugin_instances: Box<[PluginInstanceView]>,
    pub(crate) resource_limits: Option<ResourceLimitsState>,
    pub(crate) last_error: Option<PipelineAttemptError>,
}

/// Historical failure of an attempted revision, retained only during backoff.
#[derive(Clone)]
pub(crate) struct PipelineAttemptError {
    pub(crate) document_etag: Option<TenonDocumentEtag>,
    pub(crate) code: &'static str,
    pub(crate) message: &'static str,
}

pub(crate) struct PluginInstanceView {
    pub(crate) id: PluginInstanceId,
    pub(crate) program_name: ProgramName,
    pub(crate) exact_version: ExactVersion,
    pub(crate) state: PluginInstanceState,
    pub(crate) last_error: Option<ProcessErrorView>,
}

pub(crate) struct ProcessErrorView {
    pub(crate) code: Box<str>,
    pub(crate) message: Box<str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PluginResourceKind {
    Entry,
    ConfigSchema,
    PayloadContract,
}

/// Selects Programs that implement one requested interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PluginProgramInterfaceFilter {
    /// Includes Source-only and SourceAndSink Programs.
    Source,
    /// Includes Sink-only and SourceAndSink Programs.
    Sink,
}

impl PluginProgramInterfaceFilter {
    /// Reports whether the Program implements the selected interface.
    #[must_use]
    pub(crate) const fn matches(self, interface: PluginInterface) -> bool {
        match (self, interface) {
            (Self::Source, PluginInterface::Source | PluginInterface::SourceAndSink)
            | (Self::Sink, PluginInterface::Sink | PluginInterface::SourceAndSink) => true,
            (Self::Source, PluginInterface::Sink) | (Self::Sink, PluginInterface::Source) => false,
        }
    }
}

/// The same minimal Program summary used by list and exact lookup.
pub(crate) struct PluginProgramView {
    /// The validated reverse-domain Program name.
    pub(crate) program_name: ProgramName,
    /// The validated immutable package version.
    pub(crate) exact_version: ExactVersion,
    /// The package author's validated display name.
    pub(crate) display_name: String,
    /// The package author's validated plain-text description.
    pub(crate) description: String,
    /// The interfaces implemented by this Program.
    pub(crate) interface: PluginInterface,
    /// The package author's immutable supported target declaration.
    pub(crate) platforms: Box<[Platform]>,
}

/// An available Program summary or its original validated contract bytes.
pub(crate) enum PluginProgramResource {
    /// The minimal Program summary.
    Entry(PluginProgramView),
    /// The original JSON Schema bytes.
    ConfigSchema(Box<[u8]>),
    /// The original FileDescriptorSet bytes.
    PayloadContract(Box<[u8]>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PluginOperationFailure {
    PlatformMismatch { platforms: Box<[Platform]> },
    TooLarge,
    Conflict { code: &'static str },
    Invalid { code: &'static str },
}

pub(crate) enum PluginDeleteFailure {
    InUse {
        referenced_by: Box<[TenonDocumentId]>,
    },
}
