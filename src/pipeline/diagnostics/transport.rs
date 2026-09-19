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

//! The single batching, reconnect, cancellation, and shutdown loop for Pipeline diagnostics.

use crate::contracts::core::PipelineDiagnosticRecord;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;

use super::publisher::PipelineDiagnosticsShared;
use super::wire::{DiagnosticConnection, collect_batch};

const RECONNECT_DELAY: Duration = Duration::from_millis(250);

pub(super) async fn run_transport(
    channel: Channel,
    launch_id: Vec<u8>,
    shared: Arc<PipelineDiagnosticsShared>,
    mut events: mpsc::Receiver<PipelineDiagnosticRecord>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        match run_session(
            channel.clone(),
            launch_id.clone(),
            &shared,
            &mut events,
            &mut shutdown,
        )
        .await
        {
            DiagnosticsSessionOutcome::Shutdown => return,
            DiagnosticsSessionOutcome::Disconnected => {
                shared.clear_interest();
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return,
                    () = tokio::time::sleep(RECONNECT_DELAY) => {}
                }
            }
        }
    }
}

async fn run_session(
    channel: Channel,
    launch_id: Vec<u8>,
    shared: &PipelineDiagnosticsShared,
    events: &mut mpsc::Receiver<PipelineDiagnosticRecord>,
    shutdown: &mut oneshot::Receiver<()>,
) -> DiagnosticsSessionOutcome {
    let connection = tokio::select! {
        biased;
        _ = &mut *shutdown => return DiagnosticsSessionOutcome::Shutdown,
        connection = DiagnosticConnection::connect(channel, launch_id) => connection,
    };
    let Ok(mut connection) = connection else {
        return DiagnosticsSessionOutcome::Disconnected;
    };
    loop {
        tokio::select! {
            biased;
            _ = &mut *shutdown => return DiagnosticsSessionOutcome::Shutdown,
            interest = connection.interest() => {
                let Ok(Some(interest)) = interest else { return DiagnosticsSessionOutcome::Disconnected; };
                shared.replace_interest(interest);
            }
            batch = collect_batch(events) => {
                // Collection has no suspension after its first recv, so cancellation
                // cannot consume a partial batch before another branch wins.
                let sent = tokio::select! {
                    biased;
                    _ = &mut *shutdown => return DiagnosticsSessionOutcome::Shutdown,
                    sent = connection.send(batch) => sent,
                };
                if sent.is_err() { return DiagnosticsSessionOutcome::Disconnected; }
            }
        }
    }
}

enum DiagnosticsSessionOutcome {
    Shutdown,
    Disconnected,
}
