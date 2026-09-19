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

//! Pipeline-private Plugin lifecycle transport.
//!
//! One [`PluginControlServer`] owns the UDS listener and the launch registry for
//! a Pipeline process. Registering a launch returns the only owner allowed to
//! receive that launch's attached stream. The transport validates message order
//! and exposes phase-specific session types; Plugin child ownership and retry
//! policy remain outside this module.

mod adapter;
mod launch_registry;
mod server;
mod session;

pub(super) use launch_registry::{PLUGIN_LAUNCH_ID_LENGTH, PluginControlAttachmentError};
pub(in crate::pipeline) use server::{
    PluginControlLauncher, PluginControlServer, PluginControlServerError,
};
pub(super) use session::{
    AttachedPluginControl, PluginControlSessionError, QuiescedPluginControl, ReadyPluginControl,
    ShutdownPluginControl,
};

#[cfg(any(test, feature = "repository-test-support"))]
mod test_server;
#[cfg(any(test, feature = "repository-test-support"))]
pub(in crate::pipeline) use test_server::TestPluginControlServer;

#[cfg(test)]
mod tests;
