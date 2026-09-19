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

struct Expected {
    recovered: bool,
    timed_out: bool,
    diagnostic: &'static str,
}

fn assert_failure(error: RunnerPipelineLifecycleError, expected: Expected) {
    assert_eq!(error.process_owner_recovered(), expected.recovered);
    assert_eq!(error.includes_shutdown_timeout(), expected.timed_out);
    assert_eq!(ErrorChain(&error).to_string(), expected.diagnostic);
}

fn launch_cleanup_failure() -> RunnerPipelineLaunchError {
    RunnerPipelineLaunchError::CleanupFailed {
        failure: Box::new(RunnerPipelineLaunchError::AttachmentTimedOut),
        cleanup: io::Error::other("initial cleanup"),
    }
}

fn startup_cleanup_failure() -> RunnerPipelineStartupError {
    RunnerPipelineStartupError::CleanupFailed {
        failure: RunnerPipelineStartupFailure::TimedOut,
        cleanup: io::Error::other("initial cleanup"),
    }
}

fn shutdown_cleanup_failure() -> PipelineShutdownError {
    PipelineShutdownError::BeforeDeadline {
        operation: "termination request failed",
        primary: io::Error::other("termination"),
        cleanup: Some(io::Error::other("initial cleanup")),
    }
}

fn restart_cause() -> PipelineRestartCause {
    PipelineRestartCause::Runtime(RunnerPipelineProcessError::ProcessTreeWait(
        io::Error::other("runtime wait"),
    ))
}

#[test]
fn launch_directory_failure_does_not_claim_an_unreaped_process() {
    assert_failure(
        RunnerPipelineLifecycleError::Launch(RunnerPipelineLaunchError::CleanupFailed {
            failure: Box::new(RunnerPipelineLaunchError::SpawnFailed(io::Error::other(
                "spawn",
            ))),
            cleanup: io::Error::other("directory cleanup"),
        }),
        Expected {
            recovered: true,
            timed_out: false,
            diagnostic: "Pipeline lifecycle could not continue after launch: Pipeline launch failed: Pipeline process could not be started: spawn; Pipeline process cleanup also failed: directory cleanup",
        },
    );
}

#[test]
fn launch_ownership_failure_remains_unrecovered() {
    assert_failure(
        RunnerPipelineLifecycleError::Launch(
            RunnerPipelineLaunchError::ProcessTreeOwnershipFailed(io::Error::other("ownership")),
        ),
        Expected {
            recovered: false,
            timed_out: false,
            diagnostic: "Pipeline lifecycle could not continue after launch: Pipeline process group ownership could not be established: ownership",
        },
    );
}

#[test]
fn launch_cleanup_retry_preserves_both_results() {
    assert_failure(
        RunnerPipelineLifecycleError::LaunchCleanupRetried {
            primary: launch_cleanup_failure(),
            retry: Ok(()),
        },
        Expected {
            recovered: true,
            timed_out: false,
            diagnostic: "Pipeline lifecycle could not continue after launch: Pipeline launch failed: Pipeline process attachment timed out; Pipeline process cleanup also failed: initial cleanup",
        },
    );
    assert_failure(
        RunnerPipelineLifecycleError::LaunchCleanupRetried {
            primary: launch_cleanup_failure(),
            retry: Err(io::Error::other("retry cleanup")),
        },
        Expected {
            recovered: false,
            timed_out: false,
            diagnostic: "Pipeline process owner remained unrecovered after launch: Pipeline launch failed: Pipeline process attachment timed out; Pipeline process cleanup also failed: initial cleanup; process cleanup retry also failed: retry cleanup",
        },
    );
}

#[test]
fn startup_cleanup_retry_preserves_both_results() {
    assert_failure(
        RunnerPipelineLifecycleError::StartupCleanupRetried {
            primary: startup_cleanup_failure(),
            retry: Ok(()),
        },
        Expected {
            recovered: true,
            timed_out: false,
            diagnostic: "Pipeline lifecycle could not continue after startup: Pipeline startup failed: Pipeline process startup timed out; Pipeline process cleanup also failed: initial cleanup",
        },
    );
    assert_failure(
        RunnerPipelineLifecycleError::StartupCleanupRetried {
            primary: startup_cleanup_failure(),
            retry: Err(io::Error::other("retry cleanup")),
        },
        Expected {
            recovered: false,
            timed_out: false,
            diagnostic: "Pipeline process owner remained unrecovered after startup: Pipeline startup failed: Pipeline process startup timed out; Pipeline process cleanup also failed: initial cleanup; process cleanup retry also failed: retry cleanup",
        },
    );
}

#[test]
fn shutdown_failure_after_successful_initial_cleanup_remains_recovered() {
    assert_failure(
        RunnerPipelineLifecycleError::Shutdown(PipelineShutdownError::BeforeDeadline {
            operation: "termination request failed",
            primary: io::Error::other("termination"),
            cleanup: None,
        }),
        Expected {
            recovered: true,
            timed_out: false,
            diagnostic: "Pipeline lifecycle could not complete planned shutdown: termination request failed: termination",
        },
    );
}

#[test]
fn shutdown_cleanup_retry_preserves_both_results() {
    assert_failure(
        RunnerPipelineLifecycleError::ShutdownCleanupRetried {
            primary: shutdown_cleanup_failure(),
            retry: Ok(()),
        },
        Expected {
            recovered: true,
            timed_out: false,
            diagnostic: "Pipeline lifecycle could not complete planned shutdown: termination request failed: termination; process-group cleanup also failed: initial cleanup",
        },
    );
    assert_failure(
        RunnerPipelineLifecycleError::ShutdownCleanupRetried {
            primary: shutdown_cleanup_failure(),
            retry: Err(io::Error::other("retry cleanup")),
        },
        Expected {
            recovered: false,
            timed_out: false,
            diagnostic: "Pipeline process owner remained unrecovered after planned shutdown: termination request failed: termination; process-group cleanup also failed: initial cleanup; process cleanup retry also failed: retry cleanup",
        },
    );
    assert_failure(
        RunnerPipelineLifecycleError::ShutdownCleanupRetried {
            primary: PipelineShutdownError::CleanupAfterDeadline(io::Error::other(
                "initial cleanup",
            )),
            retry: Err(io::Error::other("retry cleanup")),
        },
        Expected {
            recovered: false,
            timed_out: true,
            diagnostic: "Pipeline process owner remained unrecovered after planned shutdown: Pipeline shutdown deadline elapsed and process-group cleanup failed: initial cleanup; process cleanup retry also failed: retry cleanup",
        },
    );
}

#[test]
fn attempt_cleanup_retry_keeps_the_original_restart_reason() {
    assert_failure(
        RunnerPipelineLifecycleError::AttemptCleanupAfterFailure {
            primary: restart_cause(),
            cleanup: ProcessCleanupFailure {
                initial: io::Error::other("initial cleanup"),
                retry: Ok(()),
            },
        },
        Expected {
            recovered: true,
            timed_out: false,
            diagnostic: "runner.pipeline_control_failed: Pipeline control failed: Pipeline process tree wait failed: runtime wait; Pipeline process cleanup also failed: initial cleanup",
        },
    );
    assert_failure(
        RunnerPipelineLifecycleError::AttemptCleanupAfterFailure {
            primary: restart_cause(),
            cleanup: ProcessCleanupFailure {
                initial: io::Error::other("initial cleanup"),
                retry: Err(io::Error::other("retry cleanup")),
            },
        },
        Expected {
            recovered: false,
            timed_out: false,
            diagnostic: "runner.pipeline_control_failed: Pipeline control failed: Pipeline process tree wait failed: runtime wait; Pipeline process cleanup failed: initial cleanup; cleanup retry also failed: retry cleanup",
        },
    );
}

#[test]
fn state_receiver_failure_preserves_cleanup_and_diagnostic_results() {
    assert_failure(
        RunnerPipelineLifecycleError::StateReceiverClosed,
        Expected {
            recovered: true,
            timed_out: false,
            diagnostic: "Runner Pipeline state receiver closed",
        },
    );
    assert_failure(
        RunnerPipelineLifecycleError::StateReceiverCleanupAfterFailure(ProcessCleanupFailure {
            initial: io::Error::other("initial cleanup"),
            retry: Ok(()),
        }),
        Expected {
            recovered: true,
            timed_out: false,
            diagnostic: "Runner Pipeline state receiver closed; process cleanup initially failed: initial cleanup: initial cleanup",
        },
    );
    assert_failure(
        RunnerPipelineLifecycleError::StateReceiverCleanupAfterFailure(ProcessCleanupFailure {
            initial: io::Error::other("initial cleanup"),
            retry: Err(io::Error::other("retry cleanup")),
        }),
        Expected {
            recovered: false,
            timed_out: false,
            diagnostic: "Runner Pipeline state receiver closed; process cleanup failed: initial cleanup; cleanup retry also failed: retry cleanup",
        },
    );
}

#[test]
fn kernel_group_cleanup_failure_preserves_runtime_files_after_leader_reaping() {
    // An OS error at this ownership boundary must prevent the caller from
    // deleting memory-mapped files, even when direct children were reaped.
    let cleanup =
        || PipelineDirectoryCleanupError::ResourceGroup(io::Error::from_raw_os_error(libc::EIO));
    for error in [
        RunnerPipelineLifecycleError::DirectoryCleanup(cleanup()),
        RunnerPipelineLifecycleError::DirectoryCleanupAfterShutdownTimeout(cleanup()),
        RunnerPipelineLifecycleError::CleanupAfterRestart {
            primary: Box::new(restart_cause()),
            cleanup: cleanup(),
        },
        RunnerPipelineLifecycleError::ResourceCleanupAfterFailure {
            primary: Box::new(RunnerPipelineLifecycleError::StateReceiverClosed),
            cleanup: io::Error::from_raw_os_error(libc::EIO),
        },
    ] {
        assert!(!error.process_owner_recovered());
    }
}
