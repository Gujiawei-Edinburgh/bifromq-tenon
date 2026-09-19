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

//! Phase-specific ownership of one attached Plugin lifecycle stream.

use std::error::Error;
use std::fmt;
use std::future::poll_fn;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::Stream as _;
use tonic::Streaming;

use crate::contracts::plugin::{
    PipelineToPlugin, PluginToPipeline, QuiesceSource, Shutdown, pipeline_to_plugin,
    plugin_to_pipeline,
};
use crate::payload_contract::PluginInterface;

pub(super) struct PluginControlTransport {
    pub(super) inbound: Streaming<PluginToPipeline>,
    pub(super) outbound: UnboundedSender<Result<PipelineToPlugin, tonic::Status>>,
}

pub(super) struct AttachedPluginControlTransport {
    pub(super) interface: PluginInterface,
    pub(super) inbound: Streaming<PluginToPipeline>,
    pub(super) outbound: UnboundedSender<Result<PipelineToPlugin, tonic::Status>>,
}

/// One attached stream that has not yet reported local interface readiness.
pub(in crate::pipeline::plugin) struct AttachedPluginControl {
    connection: PluginControlConnection,
    interface: PluginInterface,
}

impl AttachedPluginControl {
    pub(super) fn new(transport: AttachedPluginControlTransport) -> Self {
        Self {
            connection: PluginControlConnection {
                inbound: transport.inbound,
                outbound: transport.outbound,
            },
            interface: transport.interface,
        }
    }

    /// Requires Ready as the first message after Attach.
    pub(in crate::pipeline::plugin) async fn wait_for_ready(
        mut self,
    ) -> Result<ReadyPluginControl, PluginControlSessionError> {
        let message = self.connection.receive().await?;
        if !matches!(message, plugin_to_pipeline::Message::Ready(_)) {
            return Err(PluginControlSessionError::UnexpectedMessage {
                expected: ExpectedPluginMessage::Ready,
            });
        }
        Ok(match self.interface {
            PluginInterface::Source | PluginInterface::SourceAndSink => {
                ReadyPluginControl::SourceCapable(SourceCapablePluginControl {
                    connection: self.connection,
                })
            }
            PluginInterface::Sink => ReadyPluginControl::SinkOnly(SinkOnlyPluginControl {
                connection: self.connection,
            }),
        })
    }
}

impl fmt::Debug for AttachedPluginControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttachedPluginControl")
            .field("interface", &self.interface)
            .finish_non_exhaustive()
    }
}

/// The only legal control shape after one Plugin reports Ready.
#[derive(Debug)]
pub(in crate::pipeline::plugin) enum ReadyPluginControl {
    SourceCapable(SourceCapablePluginControl),
    SinkOnly(SinkOnlyPluginControl),
}

/// A ready Source-capable Instance that has not begun quiescing.
#[derive(Debug)]
pub(in crate::pipeline::plugin) struct SourceCapablePluginControl {
    connection: PluginControlConnection,
}

impl SourceCapablePluginControl {
    /// Sends the sole QuiesceSource command and advances the local protocol phase.
    pub(in crate::pipeline::plugin) fn begin_source_quiesce(
        self,
    ) -> Result<QuiescingPluginControl, PluginControlSessionError> {
        self.connection
            .send(pipeline_to_plugin::Message::QuiesceSource(QuiesceSource {}))?;
        Ok(QuiescingPluginControl {
            connection: self.connection,
        })
    }

    /// Polls for the only possible post-Ready outcome without losing progress on cancellation.
    pub(in crate::pipeline::plugin) fn poll_failure(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<PluginControlSessionError> {
        self.connection.poll_failure(context)
    }
}

/// A ready Sink-only Instance that may proceed directly to final shutdown.
#[derive(Debug)]
pub(in crate::pipeline::plugin) struct SinkOnlyPluginControl {
    connection: PluginControlConnection,
}

impl SinkOnlyPluginControl {
    /// Sends the sole final Shutdown command.
    pub(in crate::pipeline::plugin) fn shutdown(
        self,
    ) -> Result<ShutdownPluginControl, PluginControlSessionError> {
        shutdown(self.connection)
    }

    /// Polls for the only possible post-Ready outcome without losing progress on cancellation.
    pub(in crate::pipeline::plugin) fn poll_failure(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<PluginControlSessionError> {
        self.connection.poll_failure(context)
    }
}

/// A Source-capable Instance waiting for its SourceQuiesced boundary.
#[derive(Debug)]
pub(in crate::pipeline::plugin) struct QuiescingPluginControl {
    connection: PluginControlConnection,
}

impl QuiescingPluginControl {
    /// Requires SourceQuiesced as the next Plugin message.
    pub(in crate::pipeline::plugin) async fn wait_for_source_quiesced(
        mut self,
    ) -> Result<QuiescedPluginControl, PluginControlSessionError> {
        let message = self.connection.receive().await?;
        if !matches!(message, plugin_to_pipeline::Message::SourceQuiesced(_)) {
            return Err(PluginControlSessionError::UnexpectedMessage {
                expected: ExpectedPluginMessage::SourceQuiesced,
            });
        }
        Ok(QuiescedPluginControl {
            connection: self.connection,
        })
    }
}

/// A Source-capable Instance that can no longer create new Submission records.
#[derive(Debug)]
pub(in crate::pipeline::plugin) struct QuiescedPluginControl {
    connection: PluginControlConnection,
}

impl QuiescedPluginControl {
    /// Sends the sole final Shutdown command.
    pub(in crate::pipeline::plugin) fn shutdown(
        self,
    ) -> Result<ShutdownPluginControl, PluginControlSessionError> {
        shutdown(self.connection)
    }

    pub(in crate::pipeline::plugin) fn poll_failure(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<PluginControlSessionError> {
        self.connection.poll_failure(context)
    }
}

/// A stream whose final Shutdown command has been published.
#[derive(Debug)]
pub(in crate::pipeline::plugin) struct ShutdownPluginControl {
    connection: PluginControlConnection,
}

impl ShutdownPluginControl {
    /// Accepts only an ordinary client-stream end after final Shutdown.
    pub(in crate::pipeline::plugin) async fn wait_for_stream_end(
        &mut self,
    ) -> Result<(), PluginControlSessionError> {
        poll_fn(|context| self.poll_stream_end(context)).await
    }

    /// Polls the final stream boundary without losing progress when the owner wait is canceled.
    fn poll_stream_end(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), PluginControlSessionError>> {
        match Pin::new(&mut self.connection.inbound).poll_next(context) {
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Ready(Some(Ok(_))) => {
                Poll::Ready(Err(PluginControlSessionError::UnexpectedMessage {
                    expected: ExpectedPluginMessage::StreamEnd,
                }))
            }
            Poll::Ready(Some(Err(source))) => Poll::Ready(Err(
                PluginControlSessionError::ControlStream(Box::new(source)),
            )),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn shutdown(
    connection: PluginControlConnection,
) -> Result<ShutdownPluginControl, PluginControlSessionError> {
    connection.send(pipeline_to_plugin::Message::Shutdown(Shutdown {}))?;
    Ok(ShutdownPluginControl { connection })
}

struct PluginControlConnection {
    inbound: Streaming<PluginToPipeline>,
    // The phase-specific API emits at most QuiesceSource and Shutdown, so this
    // unbounded transport adapter cannot grow with workload or peer behavior.
    outbound: UnboundedSender<Result<PipelineToPlugin, tonic::Status>>,
}

impl PluginControlConnection {
    async fn receive(&mut self) -> Result<plugin_to_pipeline::Message, PluginControlSessionError> {
        let envelope = self
            .inbound
            .message()
            .await
            .map_err(|source| PluginControlSessionError::ControlStream(Box::new(source)))?
            .ok_or(PluginControlSessionError::Disconnected)?;
        envelope
            .message
            .ok_or(PluginControlSessionError::MessageMissing)
    }

    fn send(&self, message: pipeline_to_plugin::Message) -> Result<(), PluginControlSessionError> {
        self.outbound
            .send(Ok(PipelineToPlugin {
                message: Some(message),
            }))
            .map_err(|_| PluginControlSessionError::ResponseStreamDisconnected)
    }

    fn poll_failure(&mut self, context: &mut Context<'_>) -> Poll<PluginControlSessionError> {
        match Pin::new(&mut self.inbound).poll_next(context) {
            Poll::Ready(Some(Ok(_))) => Poll::Ready(PluginControlSessionError::UnexpectedMessage {
                expected: ExpectedPluginMessage::NoMessage,
            }),
            Poll::Ready(Some(Err(source))) => {
                Poll::Ready(PluginControlSessionError::ControlStream(Box::new(source)))
            }
            Poll::Ready(None) => Poll::Ready(PluginControlSessionError::Disconnected),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl fmt::Debug for PluginControlConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginControlConnection")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline::plugin) enum ExpectedPluginMessage {
    Ready,
    SourceQuiesced,
    NoMessage,
    StreamEnd,
}

impl fmt::Display for ExpectedPluginMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready => formatter.write_str("Ready"),
            Self::SourceQuiesced => formatter.write_str("SourceQuiesced"),
            Self::NoMessage => formatter.write_str("no Plugin message"),
            Self::StreamEnd => formatter.write_str("stream end"),
        }
    }
}

/// One attached lifecycle stream can no longer follow its legal phase order.
#[derive(Debug)]
pub(in crate::pipeline::plugin) enum PluginControlSessionError {
    ControlStream(Box<tonic::Status>),
    Disconnected,
    MessageMissing,
    UnexpectedMessage { expected: ExpectedPluginMessage },
    ResponseStreamDisconnected,
}

impl fmt::Display for PluginControlSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ControlStream(_) => formatter.write_str("Plugin control stream failed"),
            Self::Disconnected => formatter.write_str("Plugin control stream disconnected"),
            Self::MessageMissing => formatter.write_str("Plugin control message is missing"),
            Self::UnexpectedMessage { expected } => {
                write!(formatter, "Plugin control stream expected {expected}")
            }
            Self::ResponseStreamDisconnected => {
                formatter.write_str("Plugin control response stream disconnected")
            }
        }
    }
}

impl Error for PluginControlSessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ControlStream(source) => Some(source.as_ref()),
            Self::Disconnected
            | Self::MessageMissing
            | Self::UnexpectedMessage { .. }
            | Self::ResponseStreamDisconnected => None,
        }
    }
}
