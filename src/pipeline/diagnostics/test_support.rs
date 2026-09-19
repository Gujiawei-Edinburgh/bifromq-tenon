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

use super::publisher::{
    ChannelDiagnosticPublisher, PipelineDiagnosticsPublisher, PipelineDiagnosticsShared,
    PluginDiagnosticPublisher,
};
use super::wire::CurrentDiagnosticInterest;
use crate::identifiers::{FlowId, PluginInstanceId};
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};

pub(in crate::pipeline) fn interested_instance(
    id: PluginInstanceId,
) -> (
    PluginDiagnosticPublisher,
    tokio::sync::mpsc::Receiver<crate::contracts::core::PipelineDiagnosticRecord>,
) {
    let (publisher, receiver) = interested_instances([id.clone()], 1);
    (publisher.instance_plugin(id), receiver)
}

pub(in crate::pipeline) fn interested_instances(
    ids: impl IntoIterator<Item = PluginInstanceId>,
    capacity: usize,
) -> (
    PipelineDiagnosticsPublisher,
    tokio::sync::mpsc::Receiver<crate::contracts::core::PipelineDiagnosticRecord>,
) {
    instance_flow_publisher(
        CurrentDiagnosticInterest {
            plugin_instances: ids.into_iter().map(|id| id.as_str().to_owned()).collect(),
            ..CurrentDiagnosticInterest::default()
        },
        capacity,
    )
}

pub(in crate::pipeline) fn publisher() -> PipelineDiagnosticsPublisher {
    let (events, _receiver) = tokio::sync::mpsc::channel(1);
    PipelineDiagnosticsPublisher {
        shared: Arc::new(PipelineDiagnosticsShared {
            interest: RwLock::new(CurrentDiagnosticInterest::default()),
            events,
            next_channel_instance_id: AtomicU64::new(1),
            next_plugin_process_instance_id: AtomicU64::new(1),
        }),
    }
}

#[cfg(not(feature = "loom-model"))]
pub(in crate::pipeline) fn interested_flow_channel(
    flow_id: &FlowId,
    channel_index: u32,
) -> (
    PipelineDiagnosticsPublisher,
    tokio::sync::mpsc::Receiver<crate::contracts::core::PipelineDiagnosticRecord>,
) {
    instance_flow_publisher(
        CurrentDiagnosticInterest {
            flow_channels: std::collections::HashMap::from([(
                flow_id.as_str().to_owned(),
                HashSet::from([channel_index]),
            )]),
            ..CurrentDiagnosticInterest::default()
        },
        16,
    )
}

pub(in crate::pipeline) fn channel_publisher(channel_index: u32) -> ChannelDiagnosticPublisher {
    publisher().flow_channel(&flow_id(), channel_index)
}

pub(in crate::pipeline) fn interested_channel(
    channel_index: u32,
) -> (
    ChannelDiagnosticPublisher,
    tokio::sync::mpsc::Receiver<crate::contracts::core::PipelineDiagnosticRecord>,
) {
    let (publisher, receiver) = instance_flow_publisher(
        CurrentDiagnosticInterest {
            flow_channels: std::collections::HashMap::from([(
                String::from("main"),
                HashSet::from([channel_index]),
            )]),
            ..CurrentDiagnosticInterest::default()
        },
        16,
    );
    (publisher.flow_channel(&flow_id(), channel_index), receiver)
}

pub(in crate::pipeline) fn uninterested_channel(
    channel_index: u32,
) -> (
    ChannelDiagnosticPublisher,
    tokio::sync::mpsc::Receiver<crate::contracts::core::PipelineDiagnosticRecord>,
) {
    let (publisher, receiver) = instance_flow_publisher(CurrentDiagnosticInterest::default(), 16);
    (publisher.flow_channel(&flow_id(), channel_index), receiver)
}

pub(in crate::pipeline) fn interested_source() -> (
    PluginDiagnosticPublisher,
    tokio::sync::mpsc::Receiver<crate::contracts::core::PipelineDiagnosticRecord>,
) {
    source(true)
}

pub(in crate::pipeline) fn uninterested_source() -> (
    PluginDiagnosticPublisher,
    tokio::sync::mpsc::Receiver<crate::contracts::core::PipelineDiagnosticRecord>,
) {
    source(false)
}

#[allow(clippy::expect_used, reason = "the fixed fixture Flow id is valid")]
pub(super) fn flow_id() -> FlowId {
    FlowId::try_from(String::from("main")).expect("the fixture Flow id is valid")
}

#[allow(
    clippy::expect_used,
    reason = "the fixed fixture Plugin Instance id is valid"
)]
pub(in crate::pipeline) fn plugin_id() -> PluginInstanceId {
    PluginInstanceId::try_from(String::from("source"))
        .expect("the fixture Plugin Instance id is valid")
}

fn source(
    interested: bool,
) -> (
    PluginDiagnosticPublisher,
    tokio::sync::mpsc::Receiver<crate::contracts::core::PipelineDiagnosticRecord>,
) {
    let (events, receiver) = tokio::sync::mpsc::channel(16);
    let publisher = PipelineDiagnosticsPublisher {
        shared: Arc::new(PipelineDiagnosticsShared {
            interest: RwLock::new(CurrentDiagnosticInterest {
                plugin_instances: if interested {
                    HashSet::from([String::from("source")])
                } else {
                    HashSet::new()
                },
                ..CurrentDiagnosticInterest::default()
            }),
            events,
            next_channel_instance_id: AtomicU64::new(1),
            next_plugin_process_instance_id: AtomicU64::new(1),
        }),
    };
    (publisher.instance_plugin(plugin_id()), receiver)
}

fn instance_flow_publisher(
    interest: CurrentDiagnosticInterest,
    capacity: usize,
) -> (
    PipelineDiagnosticsPublisher,
    tokio::sync::mpsc::Receiver<crate::contracts::core::PipelineDiagnosticRecord>,
) {
    let (events, receiver) = tokio::sync::mpsc::channel(capacity);
    let publisher = PipelineDiagnosticsPublisher {
        shared: Arc::new(PipelineDiagnosticsShared {
            interest: RwLock::new(interest),
            events,
            next_channel_instance_id: AtomicU64::new(1),
            next_plugin_process_instance_id: AtomicU64::new(1),
        }),
    };
    (publisher, receiver)
}
