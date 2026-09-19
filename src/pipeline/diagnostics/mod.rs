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

//! Best-effort diagnostics lifecycle owned by one Pipeline process.
//!
//! Workers share producers, not transport state. The sole task batches records,
//! reconnects independently of correctness control, and is stopped by this owner.

mod publication;
mod publisher;
mod transport;
mod wire;

pub(crate) use publisher::{
    ChannelDiagnosticPublisher, LuaDiagnosticPublisher, PipelineDiagnosticsPublisher,
    PluginDiagnosticPublisher,
};

use publisher::PipelineDiagnosticsShared;
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use transport::run_transport;
use wire::CurrentDiagnosticInterest;

const EVENT_QUEUE_CAPACITY: usize = 1_024;

/// Lifecycle owner for the independent Pipeline diagnostics task.
pub(crate) struct PipelineDiagnostics {
    publisher: PipelineDiagnosticsPublisher,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl PipelineDiagnostics {
    /// Starts a reconnecting best-effort stream over the existing UDS channel.
    #[must_use]
    pub(crate) fn start(channel: Channel, launch_id: Vec<u8>) -> Self {
        let (events, receiver) = mpsc::channel(EVENT_QUEUE_CAPACITY);
        let shared = Arc::new(PipelineDiagnosticsShared {
            interest: RwLock::new(CurrentDiagnosticInterest::default()),
            events,
            next_channel_instance_id: AtomicU64::new(1),
            next_plugin_process_instance_id: AtomicU64::new(1),
        });
        let publisher = PipelineDiagnosticsPublisher {
            shared: Arc::clone(&shared),
        };
        let (shutdown, shutdown_requested) = oneshot::channel();
        let task = tokio::spawn(run_transport(
            channel,
            launch_id,
            shared,
            receiver,
            shutdown_requested,
        ));
        Self {
            publisher,
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    #[must_use]
    pub(crate) fn publisher(&self) -> PipelineDiagnosticsPublisher {
        self.publisher.clone()
    }

    /// Stops and joins the diagnostics task without waiting for remote drain.
    #[allow(
        clippy::expect_used,
        reason = "the owned diagnostics task contains no detached panic boundary"
    )]
    pub(crate) async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.await
                .expect("Pipeline diagnostics transport task must not panic");
        }
    }
}

impl Drop for PipelineDiagnostics {
    fn drop(&mut self) {
        self.shutdown.take();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl fmt::Debug for PipelineDiagnostics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PipelineDiagnostics")
            .field("publisher", &self.publisher)
            .field(
                "task_finished",
                &self.task.as_ref().is_none_or(JoinHandle::is_finished),
            )
            .finish_non_exhaustive()
    }
}

fn unix_millis() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs() * 1_000 + u64::from(duration.subsec_millis()))
}

#[cfg(feature = "repository-test-support")]
pub(in crate::pipeline) mod repository_test_support {
    use super::*;
    use crate::identifiers::PluginInstanceId;

    /// Creates one interested Plugin producer for the repository test facade.
    #[must_use]
    pub(in crate::pipeline) fn capture_plugin(
        id: PluginInstanceId,
    ) -> (
        PluginDiagnosticPublisher,
        tokio::sync::mpsc::Receiver<crate::contracts::core::PipelineDiagnosticRecord>,
    ) {
        let (events, receiver) = tokio::sync::mpsc::channel(64);
        let publisher = PipelineDiagnosticsPublisher {
            shared: Arc::new(PipelineDiagnosticsShared {
                interest: RwLock::new(CurrentDiagnosticInterest {
                    plugin_instances: std::collections::HashSet::from([id.as_str().to_owned()]),
                    ..CurrentDiagnosticInterest::default()
                }),
                events,
                next_channel_instance_id: AtomicU64::new(1),
                next_plugin_process_instance_id: AtomicU64::new(1),
            }),
        };
        (publisher.instance_plugin(id), receiver)
    }
}

#[cfg(test)]
pub(in crate::pipeline) mod test_support;

#[cfg(test)]
mod tests;
