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

//! One Flow Channel and its narrowly scoped control handles.

mod egress;
mod flow_channel;
mod flow_channel_command;
mod flow_channel_error;
mod flow_channel_spec;
mod flow_control;
pub(crate) mod metrics;
mod queue_paths;
mod wake;

pub(crate) use egress::{PreparedEgressQueue, PreparedEgressRoutes};
pub(crate) use flow_channel::FlowChannel;
pub(crate) use flow_channel_command::{
    ChannelDefinitionChange, FlowChannelCommandControl, FlowChannelCommandControlError,
    FlowChannelReplacementEvent, FlowChannelReplacementTicket,
};
pub(crate) use flow_channel_error::FlowChannelError;
pub(crate) use flow_channel_spec::FlowChannelSpec;
pub(crate) use flow_control::FlowChannelControl;
pub(crate) use queue_paths::{FlowChannelBells, FlowChannelQueuePaths};
pub(crate) use wake::{ChannelWake, ChannelWakeError};
