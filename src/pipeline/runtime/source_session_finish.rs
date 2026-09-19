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

//! Finite old-Source completion observation across one Flow's existing Channels.
//!
//! Workers and Queue owners stay in the live PipelineRuntime. This operation
//! owns only reply receivers; cancelling a borrowed wait keeps all progress.
//! Dropping the operation does not cancel Channel work. The caller must then
//! stop the runtime and join its workers, retaining any real worker failure
//! ahead of the reply-channel closure that failure may have caused.

use tokio::sync::oneshot;

use super::error::PipelineRuntimeError;
use crate::identifiers::FlowId;
use crate::pipeline::channel::FlowChannelCommandControl;

/// One outstanding old-session finish with resumable completion observation.
pub(crate) struct SourceSessionFinish {
    completions: Vec<oneshot::Receiver<()>>,
}

impl SourceSessionFinish {
    #[expect(
        clippy::expect_used,
        reason = "Live Channel indices were bounded to u32 during Flow construction"
    )]
    pub(super) fn begin(
        flow_id: &FlowId,
        channels: &[FlowChannelCommandControl],
    ) -> Result<Self, PipelineRuntimeError> {
        let mut completions = Vec::new();
        completions
            .try_reserve_exact(channels.len())
            .map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
        for (index, channel) in channels.iter().enumerate() {
            let channel_index =
                u32::try_from(index).expect("Channel count was bounded when the Flow was created");
            completions.push(channel.finish_source_session().map_err(|source| {
                PipelineRuntimeError::FlowChannelCommandControl {
                    flow_id: flow_id.clone(),
                    channel_index,
                    source,
                }
            })?);
        }
        Ok(Self { completions })
    }

    /// Waits until all old Completions have crossed their Source consume boundary.
    /// No await separates receipt consumption from removing it, so cancellation
    /// of this borrowed future cannot lose progress or resend a Channel command.
    ///
    /// # Errors
    ///
    /// A closed reply means finish was interrupted or failed, not that it
    /// succeeded. The caller must stop and join the runtime to retain the real
    /// worker cause; it must also observe worker and Plugin exits while waiting.
    pub(crate) async fn wait(&mut self) -> Result<(), PipelineRuntimeError> {
        while let Some(completion) = self.completions.last_mut() {
            completion
                .await
                .map_err(|_| PipelineRuntimeError::InternalEventChannelClosed)?;
            self.completions.pop();
        }
        Ok(())
    }
}
