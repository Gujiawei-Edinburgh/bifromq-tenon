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

//! Owns the temporary directory normally supplied by a real Runner launch.

use std::io;
use std::os::unix::fs::PermissionsExt as _;

use super::{PluginControlLauncher, PluginControlServer};

pub(in crate::pipeline) struct TestPluginControlServer {
    // Abort the service before the test directory is dropped on early return.
    server: PluginControlServer,
    directory: tempfile::TempDir,
}

impl TestPluginControlServer {
    pub(in crate::pipeline) fn start() -> io::Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("tenon-plugin-test-")
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir_in("/tmp")?;
        let server =
            PluginControlServer::start(directory.path().join(crate::PLUGIN_CONTROL_SOCKET_NAME))
                .map_err(io::Error::other)?;
        Ok(Self { server, directory })
    }

    pub(in crate::pipeline) fn launcher(&self) -> PluginControlLauncher {
        self.server.launcher()
    }

    pub(in crate::pipeline) async fn shutdown(self) -> io::Result<()> {
        self.server.shutdown().await.map_err(io::Error::other)?;
        self.directory.close()
    }
}
