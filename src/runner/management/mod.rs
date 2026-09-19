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

//! Stable management interface shared by Runner management adapters.
//!
//! HTTP is one adapter over this interface; a future TUI or another local
//! adapter can use the same typed operations without knowing the Runner main
//! loop or duplicating management behavior. One supervisor owns bounded reads,
//! parallel side-effect-free preparation, serial durable mutations, current
//! Program Store state, and Pipeline convergence. Adapters only parse their
//! transport and project these owned results.

mod jobs;
mod metrics;
mod plugin_upload;
mod state;
mod supervisor;
mod types;

pub(crate) use plugin_upload::RunnerPluginUpload;
pub(crate) use state::PipelineDirective;
pub(crate) use supervisor::{
    RunnerManagementEvent, RunnerManagementSupervisor, RunnerManagementSupervisorError,
};
pub(crate) use types::*;

use crate::identifiers::{ExactVersion, ProgramName, TenonDocumentId};
pub(in crate::runner) use crate::runner::pipeline::PipelineRunningState;
use crate::runner::plugin::store::PluginProgramInstallResult;
use tokio::sync::{mpsc, oneshot};

type ManagementReply<T> = oneshot::Sender<Result<T, RunnerUnavailable>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum PipelinePublication {
    Open,
    Closed,
}

/// One bounded read of the current management facts.
enum RunnerManagementQuery {
    ListDocuments(ManagementReply<Box<[DocumentSummary]>>),
    GetDocument(TenonDocumentId, ManagementReply<Option<DocumentContent>>),
    ListPipelines(ManagementReply<Box<[PipelineStatus]>>),
    GetPipeline(TenonDocumentId, ManagementReply<Option<PipelineDetails>>),
    ListPrograms {
        interface: Option<PluginProgramInterfaceFilter>,
        response: ManagementReply<Box<[PluginProgramView]>>,
    },
    GetProgram {
        program_name: ProgramName,
        exact_version: ExactVersion,
        resource: PluginResourceKind,
        response: ManagementReply<Option<PluginProgramResource>>,
    },
}

impl RunnerManagementQuery {
    fn reject_shutting_down(self) {
        match self {
            Self::ListDocuments(response) => reject(response),
            Self::GetDocument(_, response) => reject(response),
            Self::ListPipelines(response) => reject(response),
            Self::GetPipeline(_, response) => reject(response),
            Self::ListPrograms { response, .. } => reject(response),
            Self::GetProgram { response, .. } => reject(response),
        }
    }
}

/// Read-only analysis that does not change durable or in-memory Runner facts.
/// Validation work that can run concurrently before a document reaches the
/// serial durable commit boundary.
enum RunnerManagementDocumentPreparation {
    PutDocument {
        id: TenonDocumentId,
        precondition: PutDocumentPrecondition,
        source: Box<[u8]>,
        response: ManagementReply<Result<PutDocumentOutcome, PutDocumentFailure>>,
    },
}

impl RunnerManagementDocumentPreparation {
    fn reject_shutting_down(self) {
        match self {
            Self::PutDocument { response, .. } => reject(response),
        }
    }
}

/// One durable mutation in the Runner's serial commit order.
enum RunnerManagementMutation {
    DeleteDocument {
        id: TenonDocumentId,
        precondition: DeleteDocumentPrecondition,
        response: ManagementReply<Result<(), DocumentWriteFailure>>,
    },
    InstallProgram {
        package: plugin_upload::PluginUploadReader,
        response: ManagementReply<Result<PluginProgramInstallResult, PluginOperationFailure>>,
    },
    DeleteProgram {
        program_name: ProgramName,
        exact_version: ExactVersion,
        response: ManagementReply<Result<(), PluginDeleteFailure>>,
    },
}

impl RunnerManagementMutation {
    fn reject_shutting_down(self) {
        match self {
            Self::DeleteDocument { response, .. } => reject(response),
            Self::InstallProgram { response, .. } => reject(response),
            Self::DeleteProgram { response, .. } => reject(response),
        }
    }
}

fn reject<T>(response: ManagementReply<T>) {
    let _ = response.send(Err(RunnerUnavailable));
}

/// Cloneable typed interface used by every Runner management adapter.
#[derive(Clone)]
pub(crate) struct RunnerManagementClient {
    queries: mpsc::Sender<RunnerManagementQuery>,
    document_preparations: mpsc::Sender<RunnerManagementDocumentPreparation>,
    mutations: mpsc::Sender<RunnerManagementMutation>,
}

impl RunnerManagementClient {
    pub(crate) async fn list_documents(&self) -> Result<Box<[DocumentSummary]>, RunnerUnavailable> {
        Self::request(&self.queries, RunnerManagementQuery::ListDocuments).await
    }

    pub(crate) async fn document(
        &self,
        id: TenonDocumentId,
    ) -> Result<Option<DocumentContent>, RunnerUnavailable> {
        Self::request(&self.queries, |response| {
            RunnerManagementQuery::GetDocument(id, response)
        })
        .await
    }

    pub(crate) async fn put_document(
        &self,
        id: TenonDocumentId,
        precondition: PutDocumentPrecondition,
        source: Box<[u8]>,
    ) -> Result<Result<PutDocumentOutcome, PutDocumentFailure>, RunnerUnavailable> {
        Self::request(&self.document_preparations, |response| {
            RunnerManagementDocumentPreparation::PutDocument {
                id,
                precondition,
                source,
                response,
            }
        })
        .await
    }

    pub(crate) async fn delete_document(
        &self,
        id: TenonDocumentId,
        precondition: DeleteDocumentPrecondition,
    ) -> Result<Result<(), DocumentWriteFailure>, RunnerUnavailable> {
        Self::request(&self.mutations, |response| {
            RunnerManagementMutation::DeleteDocument {
                id,
                precondition,
                response,
            }
        })
        .await
    }

    pub(crate) async fn list_pipelines(&self) -> Result<Box<[PipelineStatus]>, RunnerUnavailable> {
        Self::request(&self.queries, RunnerManagementQuery::ListPipelines).await
    }

    pub(crate) async fn pipeline(
        &self,
        id: TenonDocumentId,
    ) -> Result<Option<PipelineDetails>, RunnerUnavailable> {
        Self::request(&self.queries, |response| {
            RunnerManagementQuery::GetPipeline(id, response)
        })
        .await
    }

    /// Starts a bounded upload into the uniquely owned Program Store.
    ///
    /// # Errors
    ///
    /// Returns `RunnerUnavailable` when management is shutting down.
    pub(crate) async fn begin_program_install(
        &self,
    ) -> Result<RunnerPluginUpload, RunnerUnavailable> {
        let (upload, package, response) = RunnerPluginUpload::channel();
        self.mutations
            .send(RunnerManagementMutation::InstallProgram { package, response })
            .await
            .map_err(|_| RunnerUnavailable)?;
        Ok(upload)
    }

    /// Lists validated Programs from the single recovered Store.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerUnavailable`] once the management owner stops serving.
    pub(crate) async fn list_programs(
        &self,
        interface: Option<PluginProgramInterfaceFilter>,
    ) -> Result<Box<[PluginProgramView]>, RunnerUnavailable> {
        Self::request(&self.queries, |response| {
            RunnerManagementQuery::ListPrograms {
                interface,
                response,
            }
        })
        .await
    }

    /// Reads one validated Program summary or original contract material.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerUnavailable`] once the management owner stops serving.
    pub(crate) async fn program(
        &self,
        program_name: ProgramName,
        exact_version: ExactVersion,
        resource: PluginResourceKind,
    ) -> Result<Option<PluginProgramResource>, RunnerUnavailable> {
        Self::request(&self.queries, |response| {
            RunnerManagementQuery::GetProgram {
                program_name,
                exact_version,
                resource,
                response,
            }
        })
        .await
    }

    /// Durably removes an exact Program after all reference gates pass.
    ///
    /// # Errors
    ///
    /// Returns an in-use conflict while a Document or live owner references
    /// the Program. Store persistence failures terminate the Runner and close
    /// this interface instead of becoming a recoverable operation failure.
    pub(crate) async fn delete_program(
        &self,
        program_name: ProgramName,
        exact_version: ExactVersion,
    ) -> Result<Result<(), PluginDeleteFailure>, RunnerUnavailable> {
        Self::request(&self.mutations, |response| {
            RunnerManagementMutation::DeleteProgram {
                program_name,
                exact_version,
                response,
            }
        })
        .await
    }

    async fn request<T, C>(
        sender: &mpsc::Sender<C>,
        command: impl FnOnce(ManagementReply<T>) -> C,
    ) -> Result<T, RunnerUnavailable> {
        let (response, result) = oneshot::channel();
        sender
            .send(command(response))
            .await
            .map_err(|_| RunnerUnavailable)?;
        result.await.map_err(|_| RunnerUnavailable)?
    }
}

/// A complete lifecycle fact published by one Pipeline task.
pub(crate) struct PipelineStateUpdate {
    pub(crate) document_id: TenonDocumentId,
    pub(crate) state: PipelineLifecycleState,
}

#[derive(Clone, Copy)]
pub(crate) enum PipelineLifecycleRole {
    Current,
    Retiring,
}

pub(crate) enum PipelineLifecycleState {
    Starting,
    Running(PipelineRunningState),
    RestartBackoff(PipelineAttemptError),
}

#[cfg(test)]
pub(crate) mod test_support;
