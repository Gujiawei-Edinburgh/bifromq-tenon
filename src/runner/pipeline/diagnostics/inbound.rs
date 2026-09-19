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

//! Attachment and batch-receive lifecycle for the private diagnostics stream.

use crate::contracts::core;
use crate::identifiers::TenonDocumentId;
use crate::runner::diagnostics::RunnerDiagnostics;
use std::sync::Arc;
use tonic::{Status, Streaming};

pub(super) async fn launch_id(
    inbound: &mut Streaming<core::PipelineDiagnosticsToRunner>,
) -> Result<Vec<u8>, Status> {
    match inbound
        .message()
        .await?
        .ok_or_else(|| Status::invalid_argument("Pipeline diagnostics Attach is missing"))?
        .message
    {
        Some(core::pipeline_diagnostics_to_runner::Message::Attach(attach)) => Ok(attach.launch_id),
        Some(core::pipeline_diagnostics_to_runner::Message::Batch(_)) | None => Err(
            Status::invalid_argument("Pipeline diagnostics Attach must be the first message"),
        ),
    }
}

pub(super) async fn receive_batches(
    mut inbound: Streaming<core::PipelineDiagnosticsToRunner>,
    document_id: TenonDocumentId,
    pipeline_instance_id: Arc<str>,
    diagnostics: RunnerDiagnostics,
) -> Status {
    loop {
        let envelope = match inbound.message().await {
            Ok(Some(envelope)) => envelope,
            Ok(None) => return Status::cancelled("Pipeline diagnostics stream disconnected"),
            Err(status) => return status,
        };
        let batch = match envelope.message {
            Some(core::pipeline_diagnostics_to_runner::Message::Batch(batch)) => batch,
            Some(core::pipeline_diagnostics_to_runner::Message::Attach(_)) | None => {
                return Status::invalid_argument(
                    "Pipeline diagnostics stream contains an unexpected message",
                );
            }
        };
        for record in batch.records {
            match super::wire::normalize_record(record, Arc::clone(&pipeline_instance_id)) {
                Ok(record) => diagnostics.publish(&document_id, record),
                Err(status) => return status,
            }
        }
    }
}
