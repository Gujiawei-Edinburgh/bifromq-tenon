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

//! Pipeline diagnostics wire conversion into the Runner subscriber model.

use crate::contracts::core::{
    ChannelDiagnosticKind, FlowChannelDiagnosticSelector, PipelineDiagnosticRecord,
    PipelineDiagnosticsInterest, PluginDiagnosticStream, RunnerToPipelineDiagnostics,
    pipeline_diagnostic_record,
};
use crate::identifiers::{FlowId, PluginInstanceId};
use crate::runner::diagnostics::{
    RunnerChannelDiagnosticKind, RunnerDiagnostic, RunnerDiagnosticSource, RunnerDiagnosticTarget,
    RunnerPluginDiagnosticStream,
};
use std::sync::Arc;
use tonic::Status;

pub(super) fn interest(targets: &[RunnerDiagnosticTarget]) -> RunnerToPipelineDiagnostics {
    let mut channels = Vec::new();
    let mut plugin_instance_ids = Vec::new();
    for target in targets {
        match target {
            RunnerDiagnosticTarget::FlowChannel {
                flow_id,
                channel_index,
            } => channels.push(FlowChannelDiagnosticSelector {
                flow_id: flow_id.as_str().to_owned(),
                channel_index: *channel_index,
            }),
            RunnerDiagnosticTarget::Plugin(id) => plugin_instance_ids.push(id.as_str().to_owned()),
        }
    }
    RunnerToPipelineDiagnostics {
        interest: Some(PipelineDiagnosticsInterest {
            channels,
            plugin_instance_ids,
        }),
    }
}

#[allow(
    clippy::expect_used,
    reason = "the same-build Pipeline publisher only serializes validated identifiers and enums"
)]
pub(super) fn normalize_record(
    record: PipelineDiagnosticRecord,
    pipeline_instance_id: Arc<str>,
) -> Result<RunnerDiagnostic, Status> {
    match record.record {
        Some(pipeline_diagnostic_record::Record::Channel(record)) => Ok(RunnerDiagnostic {
            pipeline_instance_id,
            source: RunnerDiagnosticSource::FlowChannel {
                flow_id: FlowId::try_from(record.flow_id)
                    .expect("the Pipeline publisher serializes a validated FlowId"),
                channel_index: record.channel_index,
                channel_instance_id: record.channel_instance_id,
                lua_vm_instance_id: record.lua_vm_instance_id,
                kind: match ChannelDiagnosticKind::try_from(record.kind)
                    .expect("the Pipeline publisher serializes a ChannelDiagnosticKind variant")
                {
                    ChannelDiagnosticKind::Print => RunnerChannelDiagnosticKind::Print,
                    ChannelDiagnosticKind::Error => RunnerChannelDiagnosticKind::Error,
                },
                phase: optional_error_field(record.phase),
                code: optional_error_field(record.code),
            },
            observed_at_unix_millis: record.observed_at_unix_millis,
            text: record.text.into_boxed_str(),
            truncated: record.truncated,
            invalid_utf8: record.invalid_utf8,
            sequence: record.sequence,
        }),
        Some(pipeline_diagnostic_record::Record::Plugin(record)) => {
            let stream = match PluginDiagnosticStream::try_from(record.stream)
                .expect("the Pipeline publisher serializes a PluginDiagnosticStream variant")
            {
                PluginDiagnosticStream::Stdout => RunnerPluginDiagnosticStream::Stdout,
                PluginDiagnosticStream::Stderr => RunnerPluginDiagnosticStream::Stderr,
            };
            Ok(RunnerDiagnostic {
                pipeline_instance_id,
                source: RunnerDiagnosticSource::Plugin {
                    plugin_instance_id: PluginInstanceId::try_from(record.plugin_instance_id)
                        .expect("the Pipeline publisher serializes a validated PluginInstanceId"),
                    plugin_process_instance_id: record.plugin_process_instance_id,
                    stream,
                },
                observed_at_unix_millis: record.observed_at_unix_millis,
                text: record.text.into_boxed_str(),
                truncated: record.truncated,
                invalid_utf8: record.invalid_utf8,
                sequence: record.sequence,
            })
        }
        None => Err(Status::invalid_argument(
            "Pipeline diagnostic record is missing",
        )),
    }
}

/// The publisher sends an empty string for a field that does not apply.
fn optional_error_field(value: String) -> Option<Box<str>> {
    (!value.is_empty()).then(|| value.into_boxed_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_record_is_a_protocol_error() {
        let result = normalize_record(PipelineDiagnosticRecord::default(), Arc::from("pipeline"));
        assert!(matches!(result, Err(status) if status.code() == tonic::Code::InvalidArgument));
    }
}
