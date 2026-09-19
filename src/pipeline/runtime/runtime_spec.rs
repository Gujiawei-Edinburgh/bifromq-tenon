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

//! Complete immutable inputs and exclusive Queue material for each Flow Channel.

use crate::pipeline::channel::metrics::FlowObservation;
use std::sync::Arc;

use crate::pipeline::channel::{
    FlowChannelBells, FlowChannelQueuePaths, FlowChannelSpec, PreparedEgressRoutes,
};

#[derive(Debug)]
pub(crate) struct ChannelRuntimeSpec {
    pub(super) queues: FlowChannelQueuePaths,
    pub(super) bells: FlowChannelBells,
    pub(super) routes: PreparedEgressRoutes,
}

impl ChannelRuntimeSpec {
    pub(crate) fn new(
        queues: FlowChannelQueuePaths,
        bells: FlowChannelBells,
        routes: PreparedEgressRoutes,
    ) -> Self {
        Self {
            queues,
            bells,
            routes,
        }
    }
}

#[derive(Debug)]
pub(crate) struct FlowRuntimeSpec {
    pub(super) metrics: Option<Arc<FlowObservation>>,
    pub(super) channel_spec: FlowChannelSpec,
    pub(super) channels: Box<[ChannelRuntimeSpec]>,
}

impl FlowRuntimeSpec {
    pub(crate) fn new(
        channel_spec: FlowChannelSpec,
        channels: Vec<ChannelRuntimeSpec>,
        metrics: Option<Arc<FlowObservation>>,
    ) -> Self {
        Self {
            metrics,
            channel_spec,
            channels: channels.into_boxed_slice(),
        }
    }
}
