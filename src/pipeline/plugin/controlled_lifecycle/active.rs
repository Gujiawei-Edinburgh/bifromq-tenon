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
use std::future::Future;
use std::pin::Pin;
use std::process::ExitStatus;
use std::task::{Context, Poll};

use super::super::control::{
    PluginControlSessionError, QuiescedPluginControl, ReadyPluginControl, ShutdownPluginControl,
};
use super::super::lifecycle::{PluginLifecycleError, PluginProcess};
use super::retry::ControlledRuntimeFailure;

type SourceQuiesceFuture =
    Pin<Box<dyn Future<Output = Result<QuiescedPluginControl, PluginControlSessionError>> + Send>>;

pub(super) struct ReadyControlledPlugin {
    pub(super) process: PluginProcess,
    pub(super) control: ReadyPluginControl,
}

impl ReadyControlledPlugin {
    pub(super) fn poll(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<ControlledRuntimeFailure, PluginLifecycleError>> {
        poll_active_failure(&mut self.process, context, |context| {
            match &mut self.control {
                ReadyPluginControl::SourceCapable(control) => control.poll_failure(context),
                ReadyPluginControl::SinkOnly(control) => control.poll_failure(context),
            }
        })
    }

    pub(super) fn begin_source_quiesce(
        self: Box<Self>,
    ) -> Result<ReadySourceQuiesce, (Box<PluginProcess>, PluginControlSessionError)> {
        let Self { process, control } = *self;
        match control {
            ReadyPluginControl::SourceCapable(control) => match control.begin_source_quiesce() {
                Ok(control) => Ok(ReadySourceQuiesce::Quiescing(QuiescingControlledPlugin {
                    process: Some(process),
                    control: Box::pin(control.wait_for_source_quiesced()),
                })),
                Err(source) => Err((Box::new(process), source)),
            },
            ReadyPluginControl::SinkOnly(control) => {
                Ok(ReadySourceQuiesce::SinkOnly(Box::new(Self {
                    process,
                    control: ReadyPluginControl::SinkOnly(control),
                })))
            }
        }
    }

    pub(super) fn shutdown_sink(
        self: Box<Self>,
    ) -> Result<ControlledPluginShutdown, ReadyShutdownFailure> {
        let Self { process, control } = *self;
        match control {
            ReadyPluginControl::SourceCapable(control) => {
                Err(ReadyShutdownFailure::SourceCapable(Box::new(Self {
                    process,
                    control: ReadyPluginControl::SourceCapable(control),
                })))
            }
            ReadyPluginControl::SinkOnly(control) => match control.shutdown() {
                Ok(control) => Ok(ControlledPluginShutdown::new(process, control)),
                Err(source) => Err(ReadyShutdownFailure::Control {
                    process: Box::new(process),
                    source,
                }),
            },
        }
    }

    pub(super) fn is_sink_only(&self) -> bool {
        matches!(self.control, ReadyPluginControl::SinkOnly(_))
    }

    pub(super) fn request_force_stop(&mut self) -> Result<(), PluginLifecycleError> {
        self.process
            .request_force_stop()
            .map_err(PluginLifecycleError::Stop)
    }

    #[cfg(feature = "repository-test-support")]
    pub(super) fn close_lifetime_channel(&mut self) -> Result<(), PluginLifecycleError> {
        self.process.close_lifetime_channel();
        Ok(())
    }

    pub(super) async fn reap_after_stop(&mut self) -> Result<(), PluginLifecycleError> {
        self.process
            .reap_after_stop()
            .await
            .map_err(PluginLifecycleError::Stop)
    }
}

impl fmt::Debug for ReadyControlledPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadyControlledPlugin")
            .field("process_id", &self.process.process_id())
            .field("control", &self.control)
            .finish()
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "this one-shot transfer moves directly into the inline lifecycle state; boxing would allocate only to unbox"
)]
pub(super) enum ReadySourceQuiesce {
    SinkOnly(Box<ReadyControlledPlugin>),
    Quiescing(QuiescingControlledPlugin),
}

pub(super) enum ReadyShutdownFailure {
    SourceCapable(Box<ReadyControlledPlugin>),
    Control {
        process: Box<PluginProcess>,
        source: PluginControlSessionError,
    },
}

pub(super) struct QuiescingControlledPlugin {
    process: Option<PluginProcess>,
    control: SourceQuiesceFuture,
}

impl QuiescingControlledPlugin {
    pub(super) fn poll(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<QuiescedControlledPlugin, QuiesceFailure>> {
        let process = self
            .process
            .as_mut()
            .ok_or(QuiesceFailure::InvalidTransition)?;
        match process.poll_exit(context) {
            Poll::Ready(Ok(status)) => {
                return Poll::Ready(Err(QuiesceFailure::ProcessExited(status)));
            }
            Poll::Ready(Err(source)) => return Poll::Ready(Err(QuiesceFailure::Status(source))),
            Poll::Pending => {}
        }
        match self.control.as_mut().poll(context) {
            Poll::Ready(Ok(control)) => {
                let process = self
                    .process
                    .take()
                    .ok_or(QuiesceFailure::InvalidTransition)?;
                Poll::Ready(Ok(QuiescedControlledPlugin { process, control }))
            }
            Poll::Ready(Err(source)) => {
                let process = self
                    .process
                    .take()
                    .ok_or(QuiesceFailure::InvalidTransition)?;
                Poll::Ready(Err(QuiesceFailure::Control { process, source }))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    pub(super) fn request_force_stop(&mut self) -> Result<(), PluginLifecycleError> {
        self.process
            .as_mut()
            .map_or(Ok(()), PluginProcess::request_force_stop)
            .map_err(PluginLifecycleError::Stop)
    }

    pub(super) async fn reap_after_stop(&mut self) -> Result<(), PluginLifecycleError> {
        match self.process.as_mut() {
            Some(process) => process
                .reap_after_stop()
                .await
                .map_err(PluginLifecycleError::Stop),
            None => Ok(()),
        }
    }
}

impl fmt::Debug for QuiescingControlledPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuiescingControlledPlugin")
            .field(
                "process_id",
                &self.process.as_ref().and_then(PluginProcess::process_id),
            )
            .finish_non_exhaustive()
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "the failed process moves directly into inline ForceStopping; this transient result owns no queue or collection"
)]
pub(super) enum QuiesceFailure {
    ProcessExited(ExitStatus),
    Control {
        process: PluginProcess,
        source: PluginControlSessionError,
    },
    Status(std::io::Error),
    InvalidTransition,
}

pub(super) struct QuiescedControlledPlugin {
    process: PluginProcess,
    control: QuiescedPluginControl,
}

impl QuiescedControlledPlugin {
    pub(super) fn poll(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<ControlledRuntimeFailure, PluginLifecycleError>> {
        poll_active_failure(&mut self.process, context, |context| {
            self.control.poll_failure(context)
        })
    }

    pub(super) fn shutdown(
        self,
    ) -> Result<ControlledPluginShutdown, (Box<PluginProcess>, PluginControlSessionError)> {
        let Self { process, control } = self;
        match control.shutdown() {
            Ok(control) => Ok(ControlledPluginShutdown::new(process, control)),
            Err(source) => Err((Box::new(process), source)),
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

impl fmt::Debug for QuiescedControlledPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuiescedControlledPlugin")
            .field("process_id", &self.process.process_id())
            .finish_non_exhaustive()
    }
}

pub(super) struct ControlledPluginShutdown {
    process: PluginProcess,
    control: Option<ShutdownPluginControl>,
    control_failure: Option<PluginControlSessionError>,
}

impl ControlledPluginShutdown {
    fn new(process: PluginProcess, control: ShutdownPluginControl) -> Self {
        Self {
            process,
            control: Some(control),
            control_failure: None,
        }
    }

    pub(super) fn request_force_stop(&mut self) -> Result<(), PluginLifecycleError> {
        self.process
            .request_force_stop()
            .map_err(PluginLifecycleError::Stop)
    }

    pub(super) async fn finish(&mut self) -> Result<(), PluginLifecycleError> {
        loop {
            let Some(control) = self.control.as_mut() else {
                self.process
                    .wait_for_exit()
                    .await
                    .map_err(PluginLifecycleError::Stop)?;
                return match self.control_failure.take() {
                    Some(source) => Err(PluginLifecycleError::Control(source)),
                    None => Ok(()),
                };
            };
            if self.process.has_exited() {
                let result = control.wait_for_stream_end().await;
                self.control = None;
                if let Err(source @ PluginControlSessionError::UnexpectedMessage { .. }) = result {
                    self.control_failure = Some(source);
                }
                continue;
            }
            tokio::select! {
                result = self.process.wait_for_exit() => {
                    result.map_err(PluginLifecycleError::Stop)?;
                }
                result = control.wait_for_stream_end() => {
                    self.control = None;
                    if let Err(source @ PluginControlSessionError::UnexpectedMessage { .. }) = result {
                        self.process
                            .request_force_stop()
                            .map_err(PluginLifecycleError::Stop)?;
                        self.control_failure = Some(source);
                    }
                }
            }
        }
    }
}

impl fmt::Debug for ControlledPluginShutdown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlledPluginShutdown")
            .field("process_id", &self.process.process_id())
            .field("stream_open", &self.control.is_some())
            .field("control_failure", &self.control_failure)
            .finish()
    }
}

fn poll_active_failure(
    process: &mut PluginProcess,
    context: &mut Context<'_>,
    poll_control: impl FnOnce(&mut Context<'_>) -> Poll<PluginControlSessionError>,
) -> Poll<Result<ControlledRuntimeFailure, PluginLifecycleError>> {
    match process.poll_exit(context) {
        Poll::Ready(Ok(status)) => {
            return Poll::Ready(Ok(ControlledRuntimeFailure::ProcessExited(status)));
        }
        Poll::Ready(Err(source)) => {
            return Poll::Ready(Err(PluginLifecycleError::Status(source)));
        }
        Poll::Pending => {}
    }
    poll_control(context).map(|source| Ok(ControlledRuntimeFailure::Control(source)))
}
