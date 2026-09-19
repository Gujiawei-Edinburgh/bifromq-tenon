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

//! Failures and restart diagnostics for one Pipeline lifecycle owner.

use crate::error::ErrorChain;
use crate::identifiers::TenonDocumentId;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::management::PipelineAttemptError;
use crate::runner::pipeline::{
    PipelineDirectoryCleanupError, RunnerPipelineLaunchError, RunnerPipelineProcessError,
    RunnerPipelineStartupError, RunnerPipelineStartupFailure,
};
use crate::runner::process_tree::PipelineShutdownError;
use std::error::Error;
use std::fmt;
use std::io;
use std::process::ExitStatus;

#[derive(Debug)]
pub(in crate::runner) enum PipelineRestartCause {
    Launch(RunnerPipelineLaunchError),
    Startup(RunnerPipelineStartupError),
    Exited(ExitStatus),
    Runtime(RunnerPipelineProcessError),
}

impl PipelineRestartCause {
    pub(super) fn log_creation_failure(&self, document_id: &TenonDocumentId) {
        if matches!(
            self,
            Self::Launch(
                RunnerPipelineLaunchError::ResourceLimits(_)
                    | RunnerPipelineLaunchError::SpawnFailed(_)
            )
        ) {
            eprintln!(
                "{}: Pipeline {}: {}",
                self.code(),
                document_id.as_str(),
                ErrorChain(self)
            );
        }
    }

    pub(super) fn view(&self, document_etag: TenonDocumentEtag) -> PipelineAttemptError {
        let message = match self {
            Self::Launch(RunnerPipelineLaunchError::ResourceLimits(_)) => {
                "Pipeline resource limits could not be applied; check the Runner deployment and requested limits"
            }
            Self::Launch(_) | Self::Startup(_) => "Pipeline startup failed",
            Self::Exited(_) => "Pipeline process exited unexpectedly",
            Self::Runtime(_) => "Pipeline control failed",
        };
        PipelineAttemptError {
            document_etag: match self {
                Self::Launch(_) | Self::Startup(_) => Some(document_etag),
                // Published pending targets do not identify which active apply failed.
                Self::Exited(_) | Self::Runtime(_) => None,
            },
            code: self.code(),
            message,
        }
    }

    const fn code(&self) -> &'static str {
        match self {
            Self::Launch(RunnerPipelineLaunchError::ResourceLimits(_)) => {
                "resource_limits_apply_failed"
            }
            Self::Launch(RunnerPipelineLaunchError::SpawnFailed(_)) => {
                "runner.pipeline_spawn_failed"
            }
            Self::Launch(RunnerPipelineLaunchError::AttachmentTimedOut)
            | Self::Startup(RunnerPipelineStartupError::Failed(
                RunnerPipelineStartupFailure::TimedOut,
            )) => "runner.pipeline_startup_timed_out",
            Self::Launch(_) | Self::Startup(_) => "runner.pipeline_startup_failed",
            Self::Exited(_) => "runner.pipeline_exited",
            Self::Runtime(_) => "runner.pipeline_control_failed",
        }
    }
}

impl fmt::Display for PipelineRestartCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Launch(_) => formatter.write_str("Pipeline restart is required after launch"),
            Self::Startup(_) => formatter.write_str("Pipeline restart is required after startup"),
            Self::Exited(status) => write!(formatter, "Pipeline process exited with {status}"),
            Self::Runtime(_) => formatter.write_str("Pipeline control failed"),
        }
    }
}

impl Error for PipelineRestartCause {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Launch(source) => Some(source),
            Self::Startup(source) => Some(source),
            Self::Runtime(source) => Some(source),
            Self::Exited(_) => None,
        }
    }
}

/// A fatal failure while Runner owns one exact Pipeline lifecycle.
#[derive(Debug)]
pub(in crate::runner) enum RunnerPipelineLifecycleError {
    ResourceCleanupAfterFailure {
        primary: Box<Self>,
        cleanup: io::Error,
    },
    /// Synchronous launch failed before it returned a process owner.
    Launch(RunnerPipelineLaunchError),
    LaunchCleanupRetried {
        primary: RunnerPipelineLaunchError,
        retry: io::Result<()>,
    },
    StartupCleanupRetried {
        primary: RunnerPipelineStartupError,
        retry: io::Result<()>,
    },
    Shutdown(PipelineShutdownError),
    ShutdownCleanupRetried {
        primary: PipelineShutdownError,
        retry: io::Result<()>,
    },
    AttemptCleanupAfterFailure {
        primary: PipelineRestartCause,
        cleanup: ProcessCleanupFailure,
    },
    DirectoryCleanup(PipelineDirectoryCleanupError),
    DirectoryCleanupAfterShutdownTimeout(PipelineDirectoryCleanupError),
    CleanupAfterRestart {
        primary: Box<PipelineRestartCause>,
        cleanup: PipelineDirectoryCleanupError,
    },
    StateReceiverClosed,
    StateReceiverCleanupAfterFailure(ProcessCleanupFailure),
}

impl RunnerPipelineLifecycleError {
    pub(in crate::runner::pipeline_supervisor) const fn includes_shutdown_timeout(&self) -> bool {
        match self {
            Self::ResourceCleanupAfterFailure { primary, .. } => {
                primary.includes_shutdown_timeout()
            }
            Self::Shutdown(error) | Self::ShutdownCleanupRetried { primary: error, .. } => {
                error.deadline_elapsed()
            }
            Self::DirectoryCleanupAfterShutdownTimeout(_) => true,
            Self::Launch(_)
            | Self::LaunchCleanupRetried { .. }
            | Self::StartupCleanupRetried { .. }
            | Self::AttemptCleanupAfterFailure { .. }
            | Self::DirectoryCleanup(_)
            | Self::CleanupAfterRestart { .. }
            | Self::StateReceiverClosed
            | Self::StateReceiverCleanupAfterFailure(_) => false,
        }
    }

    pub(in crate::runner::pipeline_supervisor) const fn process_owner_recovered(&self) -> bool {
        match self {
            Self::ResourceCleanupAfterFailure { .. } => false,
            // A spawn failure can also carry a directory cleanup error, without
            // any child to reap. Only adoption failure leaves an unowned process.
            Self::Launch(error) => !matches!(
                error,
                RunnerPipelineLaunchError::ProcessTreeOwnershipFailed(_)
            ),
            Self::LaunchCleanupRetried { retry, .. }
            | Self::StartupCleanupRetried { retry, .. }
            | Self::ShutdownCleanupRetried { retry, .. } => retry.is_ok(),
            Self::AttemptCleanupAfterFailure { cleanup, .. }
            | Self::StateReceiverCleanupAfterFailure(cleanup) => cleanup.retry.is_ok(),
            Self::Shutdown(error) => !error.requires_cleanup_retry(),
            Self::DirectoryCleanup(cleanup)
            | Self::DirectoryCleanupAfterShutdownTimeout(cleanup)
            | Self::CleanupAfterRestart { cleanup, .. } => {
                !matches!(cleanup, PipelineDirectoryCleanupError::ResourceGroup(_))
            }
            Self::StateReceiverClosed => true,
        }
    }
}

impl fmt::Display for RunnerPipelineLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceCleanupAfterFailure { primary, cleanup } => write!(
                formatter,
                "{}; resource group cleanup also failed: {}",
                ErrorChain(primary.as_ref()),
                ErrorChain(cleanup)
            ),
            Self::Launch(_) | Self::LaunchCleanupRetried { retry: Ok(()), .. } => {
                formatter.write_str("Pipeline lifecycle could not continue after launch")
            }
            Self::LaunchCleanupRetried {
                primary,
                retry: Err(cleanup),
            } => write!(
                formatter,
                "Pipeline process owner remained unrecovered after launch: {}; process cleanup retry also failed: {}",
                ErrorChain(primary),
                ErrorChain(cleanup)
            ),
            Self::StartupCleanupRetried { retry: Ok(()), .. } => {
                formatter.write_str("Pipeline lifecycle could not continue after startup")
            }
            Self::StartupCleanupRetried {
                primary,
                retry: Err(cleanup),
            } => write!(
                formatter,
                "Pipeline process owner remained unrecovered after startup: {}; process cleanup retry also failed: {}",
                ErrorChain(primary),
                ErrorChain(cleanup)
            ),
            Self::Shutdown(_) | Self::ShutdownCleanupRetried { retry: Ok(()), .. } => {
                formatter.write_str("Pipeline lifecycle could not complete planned shutdown")
            }
            Self::ShutdownCleanupRetried {
                primary,
                retry: Err(cleanup),
            } => write!(
                formatter,
                "Pipeline process owner remained unrecovered after planned shutdown: {}; process cleanup retry also failed: {}",
                ErrorChain(primary),
                ErrorChain(cleanup)
            ),
            Self::AttemptCleanupAfterFailure { primary, cleanup } => match &cleanup.retry {
                Ok(()) => write!(
                    formatter,
                    "{}: {}; Pipeline process cleanup also failed: {}",
                    primary.code(),
                    ErrorChain(primary),
                    ErrorChain(&cleanup.initial)
                ),
                Err(retry) => write!(
                    formatter,
                    "{}: {}; Pipeline process cleanup failed: {}; cleanup retry also failed: {}",
                    primary.code(),
                    ErrorChain(primary),
                    ErrorChain(&cleanup.initial),
                    ErrorChain(retry)
                ),
            },
            Self::DirectoryCleanup(_) => {
                formatter.write_str("Pipeline lifecycle could not remove its working directory")
            }
            Self::DirectoryCleanupAfterShutdownTimeout(error) => write!(
                formatter,
                "Pipeline shutdown exceeded its deadline; {}",
                ErrorChain(error)
            ),
            Self::CleanupAfterRestart { primary, cleanup } => write!(
                formatter,
                "{}: {}; {}",
                primary.code(),
                ErrorChain(primary.as_ref()),
                ErrorChain(cleanup)
            ),
            Self::StateReceiverClosed => {
                formatter.write_str("Runner Pipeline state receiver closed")
            }
            Self::StateReceiverCleanupAfterFailure(cleanup) => match &cleanup.retry {
                Ok(()) => write!(
                    formatter,
                    "Runner Pipeline state receiver closed; process cleanup initially failed: {}",
                    ErrorChain(&cleanup.initial)
                ),
                Err(retry) => write!(
                    formatter,
                    "Runner Pipeline state receiver closed; process cleanup failed: {}; cleanup retry also failed: {}",
                    ErrorChain(&cleanup.initial),
                    ErrorChain(retry)
                ),
            },
        }
    }
}

impl Error for RunnerPipelineLifecycleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ResourceCleanupAfterFailure { .. } => None,
            Self::Launch(source)
            | Self::LaunchCleanupRetried {
                primary: source,
                retry: Ok(()),
            } => Some(source),
            Self::StartupCleanupRetried {
                primary: source,
                retry: Ok(()),
            } => Some(source),
            Self::Shutdown(source)
            | Self::ShutdownCleanupRetried {
                primary: source,
                retry: Ok(()),
            } => Some(source),
            Self::DirectoryCleanup(source) => Some(source),
            Self::StateReceiverCleanupAfterFailure(ProcessCleanupFailure {
                initial,
                retry: Ok(()),
            }) => Some(initial),
            Self::LaunchCleanupRetried { retry: Err(_), .. }
            | Self::StartupCleanupRetried { retry: Err(_), .. }
            | Self::ShutdownCleanupRetried { retry: Err(_), .. }
            | Self::AttemptCleanupAfterFailure { .. }
            | Self::DirectoryCleanupAfterShutdownTimeout(_)
            | Self::CleanupAfterRestart { .. }
            | Self::StateReceiverClosed
            | Self::StateReceiverCleanupAfterFailure(ProcessCleanupFailure {
                retry: Err(_), ..
            }) => None,
        }
    }
}

/// The initial cleanup failure and the later retry's independent result.
#[derive(Debug)]
pub(in crate::runner) struct ProcessCleanupFailure {
    pub(super) initial: io::Error,
    pub(super) retry: io::Result<()>,
}

#[cfg(test)]
mod tests;
