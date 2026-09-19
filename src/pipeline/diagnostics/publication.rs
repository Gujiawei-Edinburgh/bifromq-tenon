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

//! Converts a rendered line directly into the bounded diagnostic event queue.

use super::publisher::ChannelDiagnosticTarget;
use crate::contracts::core;
use crate::identifiers::PluginInstanceId;
use tokio::sync::mpsc;

/// One rendered line; ownership moves directly into the wire record.
pub(super) struct DiagnosticLine {
    pub(super) observed_at_unix_millis: u64,
    pub(super) text: Box<str>,
    pub(super) truncated: bool,
    pub(super) invalid_utf8: bool,
    pub(super) sequence: u64,
}

/// One rendered Channel line with its already-decided kind and stable identity.
pub(super) struct ChannelDiagnosticLine {
    pub(super) kind: core::ChannelDiagnosticKind,
    pub(super) phase: &'static str,
    pub(super) code: &'static str,
    pub(super) line: DiagnosticLine,
}

pub(super) fn channel(
    events: &mpsc::Sender<core::PipelineDiagnosticRecord>,
    target: &ChannelDiagnosticTarget,
    channel_instance_id: u64,
    lua_vm_instance_id: u64,
    channel_line: ChannelDiagnosticLine,
) {
    let ChannelDiagnosticLine {
        kind,
        phase,
        code,
        line,
    } = channel_line;
    let _ = events.try_send(core::PipelineDiagnosticRecord {
        record: Some(core::pipeline_diagnostic_record::Record::Channel(
            core::ChannelDiagnosticRecord {
                flow_id: target.flow_id.as_str().to_owned(),
                channel_index: target.channel_index,
                channel_instance_id,
                lua_vm_instance_id,
                observed_at_unix_millis: line.observed_at_unix_millis,
                text: line.text.into(),
                truncated: line.truncated,
                invalid_utf8: line.invalid_utf8,
                sequence: line.sequence,
                kind: kind as i32,
                phase: phase.to_owned(),
                code: code.to_owned(),
            },
        )),
    });
}

pub(super) fn plugin(
    events: &mpsc::Sender<core::PipelineDiagnosticRecord>,
    id: &PluginInstanceId,
    plugin_process_instance_id: u64,
    stream: core::PluginDiagnosticStream,
    line: DiagnosticLine,
) {
    let _ = events.try_send(core::PipelineDiagnosticRecord {
        record: Some(core::pipeline_diagnostic_record::Record::Plugin(
            core::PluginDiagnosticRecord {
                plugin_instance_id: id.as_str().to_owned(),
                plugin_process_instance_id,
                stream: stream as i32,
                observed_at_unix_millis: line.observed_at_unix_millis,
                text: line.text.into(),
                truncated: line.truncated,
                invalid_utf8: line.invalid_utf8,
                sequence: line.sequence,
            },
        )),
    });
}
