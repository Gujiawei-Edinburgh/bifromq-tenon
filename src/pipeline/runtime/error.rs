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

//! Runtime error taxonomy shared by Channel, Egress, and worker ownership.
//!
//! [`PipelineRuntimeError`] is the stable failure returned to the Pipeline
//! Controller. [`PipelineRuntimeShutdownError`] is deliberately private to the
//! ownership layer: a wake failure means a Drop path cannot prove that joining
//! will terminate, so the owner aborts the Pipeline process instead of exposing
//! a second recoverable shutdown protocol.

use std::error::Error;
use std::fmt;
use std::io;

use crate::identifiers::FlowId;
use crate::lua::LuaVmErrorKind;
use crate::pipeline::channel::{
    ChannelWakeError, FlowChannelCommandControlError, FlowChannelError,
};

/// One long-lived worker owned by a running Pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PipelineWorker {
    /// Flow that owns this worker.
    pub(crate) flow_id: FlowId,
    /// Zero-based index within that Flow.
    pub(crate) channel_index: u32,
}

impl fmt::Display for PipelineWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Flow '{}' channel {}",
            self.flow_id.as_str(),
            self.channel_index
        )
    }
}

/// A stable failure while assembling or running one Pipeline runtime.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum PipelineRuntimeError {
    /// One FlowChannel could not open its Queue pair or exact Lua VM.
    FlowChannelOpen {
        /// Flow that owns the FlowChannel.
        flow_id: FlowId,
        /// Zero-based index within that Flow.
        channel_index: u32,
        /// Original channel construction failure.
        source: FlowChannelError,
    },
    /// One FlowChannel panicked while opening its Queue pair or Lua VM.
    FlowChannelOpenPanicked {
        /// Flow that owns the FlowChannel.
        flow_id: FlowId,
        /// Zero-based index within that Flow.
        channel_index: u32,
    },
    /// One FlowChannel could not accept a command or a replacement decision.
    FlowChannelCommandControl {
        /// Flow that owns the FlowChannel.
        flow_id: FlowId,
        /// Zero-based index within that Flow.
        channel_index: u32,
        /// Original control failure.
        source: FlowChannelCommandControlError,
    },
    /// One target FlowChannel definition could not create its Lua VM.
    FlowChannelReplacementPreparation {
        /// Flow that owns the FlowChannel.
        flow_id: FlowId,
        /// Zero-based index within that Flow.
        channel_index: u32,
        /// Stable Lua failure category.
        kind: LuaVmErrorKind,
    },
    /// The operating system could not create one required worker thread.
    WorkerSpawn {
        /// Worker whose thread could not be created.
        worker: PipelineWorker,
        /// Original operating-system failure.
        source: io::Error,
    },
    /// One running FlowChannel terminated with a real runtime failure.
    FlowChannelFailed {
        /// Flow that owns the FlowChannel.
        flow_id: FlowId,
        /// Zero-based index within that Flow.
        channel_index: u32,
        /// Original channel failure.
        source: FlowChannelError,
    },
    /// One worker panicked after successful startup.
    WorkerPanicked {
        /// Worker whose invariant failed.
        worker: PipelineWorker,
    },
    /// One worker returned normally before planned Pipeline shutdown.
    WorkerExitedUnexpectedly {
        /// Worker that returned without a stop request.
        worker: PipelineWorker,
    },
    /// A private startup or worker-completion channel closed too early.
    InternalEventChannelClosed,
    /// Runtime bookkeeping could not reserve its fixed worker set.
    ResourceLimitExceeded,
}

impl fmt::Display for PipelineRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FlowChannelOpen {
                flow_id,
                channel_index,
                ..
            } => {
                write!(
                    formatter,
                    "Flow '{}' channel {channel_index} could not be opened",
                    flow_id.as_str()
                )
            }
            Self::FlowChannelOpenPanicked {
                flow_id,
                channel_index,
            } => {
                write!(
                    formatter,
                    "Flow '{}' channel {channel_index} panicked during startup",
                    flow_id.as_str()
                )
            }
            Self::FlowChannelCommandControl {
                flow_id,
                channel_index,
                ..
            } => {
                write!(
                    formatter,
                    "Flow '{}' channel {channel_index} command control failed",
                    flow_id.as_str()
                )
            }
            Self::FlowChannelReplacementPreparation {
                flow_id,
                channel_index,
                kind,
            } => {
                write!(
                    formatter,
                    "Flow '{}' channel {channel_index} target Lua VM failed: {kind:?}",
                    flow_id.as_str()
                )
            }
            Self::WorkerSpawn { worker, .. } => {
                write!(formatter, "{worker} thread could not be created")
            }
            Self::FlowChannelFailed {
                flow_id,
                channel_index,
                ..
            } => {
                write!(
                    formatter,
                    "Flow '{}' channel {channel_index} failed",
                    flow_id.as_str()
                )
            }
            Self::WorkerPanicked { worker } => write!(formatter, "{worker} panicked"),
            Self::WorkerExitedUnexpectedly { worker } => {
                write!(formatter, "{worker} exited before Pipeline shutdown")
            }
            Self::InternalEventChannelClosed => {
                formatter.write_str("Pipeline runtime event channel closed unexpectedly")
            }
            Self::ResourceLimitExceeded => {
                formatter.write_str("Pipeline runtime resource limit was exceeded")
            }
        }
    }
}

impl Error for PipelineRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::FlowChannelOpen { source, .. } | Self::FlowChannelFailed { source, .. } => {
                Some(source)
            }
            Self::FlowChannelCommandControl { source, .. } => Some(source),
            Self::WorkerSpawn { source, .. } => Some(source),
            Self::FlowChannelOpenPanicked { .. }
            | Self::FlowChannelReplacementPreparation { .. }
            | Self::WorkerPanicked { .. }
            | Self::WorkerExitedUnexpectedly { .. }
            | Self::InternalEventChannelClosed
            | Self::ResourceLimitExceeded => None,
        }
    }
}

/// A failure while waking every worker for planned Pipeline shutdown.
#[derive(Debug)]
#[non_exhaustive]
pub(super) struct PipelineRuntimeShutdownError {
    pub(super) flow_id: FlowId,
    pub(super) channel_index: u32,
    pub(super) source: ChannelWakeError,
}

impl fmt::Display for PipelineRuntimeShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Flow '{}' channel {} could not be stopped",
            self.flow_id.as_str(),
            self.channel_index
        )
    }
}

impl Error for PipelineRuntimeShutdownError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
