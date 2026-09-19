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

//! Tonic adapter for attaching one Plugin stream to one registered launch.

use std::fmt;

use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::launch_registry::{PLUGIN_LAUNCH_ID_LENGTH, PluginControlLaunchRegistry};
use super::session::PluginControlTransport;
use crate::contracts::plugin::plugin_lifecycle_server::{PluginLifecycle, PluginLifecycleServer};
use crate::contracts::plugin::{PipelineToPlugin, PluginToPipeline, plugin_to_pipeline};

#[derive(Clone)]
pub(super) struct PluginControlAdapter {
    launches: PluginControlLaunchRegistry,
}

impl PluginControlAdapter {
    pub(super) fn new(launches: PluginControlLaunchRegistry) -> Self {
        Self { launches }
    }

    pub(super) fn into_service(self) -> PluginLifecycleServer<Self> {
        PluginLifecycleServer::new(self)
    }
}

impl fmt::Debug for PluginControlAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginControlAdapter")
            .field("launches", &self.launches)
            .finish()
    }
}

#[tonic::async_trait]
impl PluginLifecycle for PluginControlAdapter {
    type RunStream = UnboundedReceiverStream<Result<PipelineToPlugin, Status>>;

    async fn run(
        &self,
        request: Request<Streaming<PluginToPipeline>>,
    ) -> Result<Response<Self::RunStream>, Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("Plugin Attach is missing"))?;
        let launch_id = parse_launch_id(first)?;
        let pending = self
            .launches
            .claim(&launch_id)
            .ok_or_else(|| Status::failed_precondition("Plugin launch is not pending"))?;
        let (outbound, responses) = mpsc::unbounded_channel();
        pending
            .hand_off(PluginControlTransport { inbound, outbound })
            .map_err(|_| Status::cancelled("Plugin launch owner is no longer waiting"))?;
        Ok(Response::new(UnboundedReceiverStream::new(responses)))
    }
}

fn parse_launch_id(envelope: PluginToPipeline) -> Result<[u8; PLUGIN_LAUNCH_ID_LENGTH], Status> {
    let attach = match envelope.message {
        Some(plugin_to_pipeline::Message::Attach(attach)) => attach,
        Some(_) => return Err(Status::invalid_argument("Plugin Attach must be first")),
        None => return Err(Status::invalid_argument("Plugin Attach envelope is empty")),
    };
    attach
        .launch_id
        .try_into()
        .map_err(|_| Status::invalid_argument("Plugin launch id must contain exactly 16 bytes"))
}
