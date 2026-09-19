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

use std::fmt;
use std::future::Future as _;
use std::pin::Pin;
use std::process::ExitStatus;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::time::Sleep;

use super::super::control::PluginControlSessionError;
use super::super::lifecycle::{PluginLifecycleError, PluginProcess};
use crate::contracts::core::ControlError;

pub(super) enum ControlledRuntimeFailure {
    ProcessExited(ExitStatus),
    Control(PluginControlSessionError),
}

impl ControlledRuntimeFailure {
    fn control_error(&self) -> ControlError {
        match self {
            Self::ProcessExited(_) => ControlError {
                code: "plugin.exited_after_ready".into(),
                message: "Plugin process exited after Ready and is waiting to restart".into(),
            },
            Self::Control(_) => ControlError {
                code: "plugin.control_stream_failed".into(),
                message: "Plugin control stream failed after Ready and is waiting to restart"
                    .into(),
            },
        }
    }
}

impl fmt::Debug for ControlledRuntimeFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProcessExited(status) => formatter
                .debug_tuple("ProcessExited")
                .field(status)
                .finish(),
            Self::Control(source) => formatter.debug_tuple("Control").field(source).finish(),
        }
    }
}

#[derive(Debug)]
pub(super) struct ControlledRuntimeFailureCleanup {
    process: PluginProcess,
    failure: Option<ControlledRuntimeFailure>,
    stop_error: Option<std::io::Error>,
}

impl ControlledRuntimeFailureCleanup {
    pub(super) fn new(mut process: PluginProcess, failure: ControlledRuntimeFailure) -> Self {
        let stop_error = process.request_force_stop().err();
        Self {
            process,
            failure: Some(failure),
            stop_error,
        }
    }

    pub(super) fn poll(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<ControlledRuntimeFailure, PluginLifecycleError>> {
        if let Some(source) = self.stop_error.take() {
            return Poll::Ready(Err(PluginLifecycleError::Stop(source)));
        }
        match self.process.poll_exit(context) {
            Poll::Ready(Ok(_)) => Poll::Ready(
                self.failure
                    .take()
                    .ok_or(PluginLifecycleError::InvalidTransition),
            ),
            Poll::Ready(Err(source)) => Poll::Ready(Err(PluginLifecycleError::Status(source))),
            Poll::Pending => Poll::Pending,
        }
    }

    pub(super) fn request_force_stop(&mut self) -> Result<(), PluginLifecycleError> {
        self.process
            .request_force_stop()
            .map_err(PluginLifecycleError::Stop)
    }

    pub(super) async fn reap_after_stop(&mut self) -> Result<(), PluginLifecycleError> {
        self.process
            .reap_after_stop()
            .await
            .map_err(PluginLifecycleError::Stop)
    }
}

pub(super) struct ControlledRestartBackoff {
    failure: ControlledRuntimeFailure,
    delay: Duration,
    sleep: Pin<Box<Sleep>>,
}

impl ControlledRestartBackoff {
    pub(super) fn new(failure: ControlledRuntimeFailure, delay: Duration) -> Self {
        Self {
            failure,
            delay,
            sleep: Box::pin(tokio::time::sleep(delay)),
        }
    }

    pub(super) fn poll_due(&mut self, context: &mut Context<'_>) -> bool {
        self.sleep.as_mut().poll(context).is_ready()
    }

    pub(super) fn control_error(&self) -> ControlError {
        self.failure.control_error()
    }
}

impl fmt::Debug for ControlledRestartBackoff {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlledRestartBackoff")
            .field("failure", &self.failure)
            .field("delay", &self.delay)
            .finish_non_exhaustive()
    }
}
