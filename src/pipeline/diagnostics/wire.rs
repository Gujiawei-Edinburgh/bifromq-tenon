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

//! The sole diagnostic connection and bounded batch collection.

use crate::contracts::core;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tokio_stream::{StreamExt as _, wrappers::ReceiverStream};
use tonic::{Status, Streaming, transport::Channel};

const OUTBOUND_QUEUE_CAPACITY: usize = 8;
const MAX_BATCH_RECORDS: usize = 64;

/// One latest selection received from Runner, never a second subscription owner.
#[derive(Debug, Default)]
pub(super) struct CurrentDiagnosticInterest {
    pub(super) flow_channels: HashMap<String, HashSet<u32>>,
    pub(super) plugin_instances: HashSet<String>,
}

pub(super) struct DiagnosticConnection {
    outbound: mpsc::Sender<core::PipelineDiagnosticsToRunner>,
    inbound: Streaming<core::RunnerToPipelineDiagnostics>,
}

impl DiagnosticConnection {
    pub(super) async fn connect(channel: Channel, launch_id: Vec<u8>) -> Result<Self, Status> {
        let (outbound, receiver) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
        let attach = core::PipelineDiagnosticsToRunner {
            message: Some(core::pipeline_diagnostics_to_runner::Message::Attach(
                core::PipelineDiagnosticsAttach { launch_id },
            )),
        };
        let response = core::pipeline_diagnostics_client::PipelineDiagnosticsClient::new(channel)
            .stream(tokio_stream::once(attach).chain(ReceiverStream::new(receiver)))
            .await?;
        Ok(Self {
            outbound,
            inbound: response.into_inner(),
        })
    }

    pub(super) async fn interest(&mut self) -> Result<Option<CurrentDiagnosticInterest>, Status> {
        Ok(self
            .inbound
            .message()
            .await?
            .and_then(|message| message.interest)
            .map(|interest| {
                let mut flow_channels: HashMap<String, HashSet<u32>> = HashMap::new();
                for channel in interest.channels {
                    flow_channels
                        .entry(channel.flow_id)
                        .or_default()
                        .insert(channel.channel_index);
                }
                CurrentDiagnosticInterest {
                    flow_channels,
                    plugin_instances: interest.plugin_instance_ids.into_iter().collect(),
                }
            }))
    }

    pub(super) async fn send(
        &self,
        records: Vec<core::PipelineDiagnosticRecord>,
    ) -> Result<(), ()> {
        self.outbound
            .send(core::PipelineDiagnosticsToRunner {
                message: Some(core::pipeline_diagnostics_to_runner::Message::Batch(
                    core::PipelineDiagnosticBatch { records },
                )),
            })
            .await
            .map_err(|_| ())
    }
}

#[allow(
    clippy::expect_used,
    reason = "the transport retains the shared event sender until shutdown"
)]
pub(super) async fn collect_batch(
    events: &mut mpsc::Receiver<core::PipelineDiagnosticRecord>,
) -> Vec<core::PipelineDiagnosticRecord> {
    let record = events
        .recv()
        .await
        .expect("diagnostic sender lives as long as its transport");
    let mut records = Vec::with_capacity(MAX_BATCH_RECORDS);
    records.push(record);
    while records.len() < MAX_BATCH_RECORDS {
        let Ok(record) = events.try_recv() else {
            break;
        };
        records.push(record);
    }
    records
}
